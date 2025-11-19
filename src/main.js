import archiver from "archiver";
import { Writable } from "stream";
import { Client, Storage, Databases, Query, Permission, Role } from "node-appwrite";

export default async function prepareDownload(context) {
  context.log("🔹 Starting download preparation…");

  // --------------------------------------------
  // Parse Request Body
  // --------------------------------------------
  let payload = {};
  try {
    payload = JSON.parse(context.req.bodyRaw || "{}");
  } catch (e) {
    return context.res.json({ statusCode: 400, error: "Invalid JSON" });
  }

  const { eventId } = payload;
  if (!eventId) {
    return context.res.json({ statusCode: 400, error: "Missing eventId" });
  }

  // --------------------------------------------
  // Init Appwrite
  // --------------------------------------------
  const client = new Client()
    .setEndpoint(process.env.APPWRITE_ENDPOINT)
    .setProject(process.env.APPWRITE_PROJECT_ID)
    .setKey(process.env.APPWRITE_API_KEY);

  const storage = new Storage(client);
  const databases = new Databases(client);

  const DB = process.env.APPWRITE_DATABASE_ID;
  const PHOTO_COLLECTION = process.env.APPWRITE_PHOTO_COLLECTION_ID;
  const EVENT_COLLECTION = process.env.APPWRITE_EVENT_COLLECTION_ID;
  const DL_COLLECTION = process.env.APPWRITE_DOWNLOAD_COLLECTION_ID;
  const PHOTO_BUCKET = process.env.APPWRITE_BUCKET_ID;
  const DOWNLOAD_BUCKET = process.env.APPWRITE_DOWNLOAD_BUCKET_ID;

  const currentUserId = context.req.headers["x-appwrite-user-id"];

  if (!currentUserId) {
    return context.res.json({ statusCode: 401, error: "Unauthorized - User not logged in" });
  }

  try {
    // --------------------------------------------
    // Check event ownership
    // --------------------------------------------
    const eventDoc = await databases.getDocument(DB, EVENT_COLLECTION, eventId);

    if (String(eventDoc.user_id) !== String(currentUserId)) {
      return context.res.json({
        statusCode: 403,
        error: "Forbidden — you do not own this event.",
      });
    }

    // --------------------------------------------
    // Fetch all photos from DB (Optimized)
    // --------------------------------------------
    context.log(`📸 Fetching photos for event: ${eventId}`);
    const allPhotos = [];
    let offset = 0;
    const BATCH_SIZE = 100;

    while (true) {
      const res = await databases.listDocuments(DB, PHOTO_COLLECTION, [
        Query.equal("event_id", eventId),
        Query.limit(BATCH_SIZE),
        Query.offset(offset),
        Query.select(["$id", "file_id", "file_size", "file_type", "file_name"]),
      ]);

      if (!res.documents.length) break;

      allPhotos.push(...res.documents);
      offset += res.documents.length;

      // Safety check to prevent infinite loops
      if (offset > 10000) {
        context.log("⚠️ Photo limit reached (10,000)");
        break;
      }
    }

    if (allPhotos.length === 0) {
      return context.res.json({ statusCode: 404, error: "No photos found for this event" });
    }

    context.log(`✅ Found ${allPhotos.length} photos`);

    // --------------------------------------------
    // ZIP chunking config
    // --------------------------------------------
    const MAX_MB = 2000; // 2GB limit with buffer
    const MIN_FILES_PER_ZIP = 1; // Minimum files before creating new chunk
    let currentZipSizeMB = 0;
    let currentZipFileCount = 0;
    let zipIndex = 1;
    let totalProcessed = 0;

    let archive = null;
    let zipChunks = null;
    let zipStream = null;

    const createdDownloads = [];
    const failedPhotos = [];

    // --------------------------------------------
    // Helper: Start new ZIP
    // --------------------------------------------
    const startZip = () => {
      context.log(`📦 Starting ZIP part ${zipIndex}...`);
      archive = archiver("zip", { 
        zlib: { level: 0 }, // No compression for speed
        statConcurrency: 1
      });
      
      zipChunks = [];
      zipStream = new Writable({
        write(chunk, enc, cb) {
          zipChunks.push(chunk);
          cb();
        },
      });

      archive.pipe(zipStream);

      // Error handling for archiver
      archive.on("error", (err) => {
        context.error(`❌ Archive error: ${err.message}`);
        throw err;
      });

      archive.on("warning", (err) => {
        if (err.code === "ENOENT") {
          context.log(`⚠️ Archive warning: ${err.message}`);
        } else {
          throw err;
        }
      });

      currentZipSizeMB = 0;
      currentZipFileCount = 0;
    };

    // --------------------------------------------
    // Helper: Finalize & upload ZIP
    // --------------------------------------------
    const finalizeAndUploadZip = async () => {
      context.log(`🔒 Finalizing ZIP part ${zipIndex} (${currentZipFileCount} files, ${currentZipSizeMB.toFixed(2)}MB)`);
      
      await archive.finalize();
      await new Promise((resolve) => zipStream.on("finish", resolve));

      const zipBuffer = Buffer.concat(zipChunks);
      const zipSizeMB = (zipBuffer.length / 1024 / 1024).toFixed(2);
      
      const eventName = (eventDoc.event_name || "photos")
        .replace(/[^a-zA-Z0-9_-]/g, "_")
        .substring(0, 50);
      
      const zipFilename = zipIndex > 1 
        ? `${eventName}_part${zipIndex}.zip` 
        : `${eventName}.zip`;

      context.log(`⬆️ Uploading ${zipFilename} (${zipSizeMB}MB)...`);

      // Upload file
      const fileId = `dl_${eventId}_${Date.now()}_${zipIndex}`;
      const fileObj = {
        name: zipFilename,
        type: "application/zip",
        size: zipBuffer.length,
        arrayBuffer: async () =>
          zipBuffer.buffer.slice(
            zipBuffer.byteOffset,
            zipBuffer.byteOffset + zipBuffer.byteLength
          ),
        slice: (start, end) =>
          new Blob([zipBuffer.slice(start, end)], { type: "application/zip" }),
      };

      const uploaded = await storage.createFile(
        DOWNLOAD_BUCKET,
        fileId,
        fileObj,
        [
          Permission.read(Role.user(currentUserId)),
          Permission.update(Role.user(currentUserId)),
          Permission.delete(Role.user(currentUserId)),
        ]
      );

      context.log(`✅ File uploaded: ${uploaded.$id}`);

      // Create download DB record with metadata
      const dlDoc = await databases.createDocument(
        DB, 
        DL_COLLECTION, 
        "unique()", 
        {
          user_id: currentUserId,
          event_id: eventId,
          file_id: uploaded.$id,
          file_name: zipFilename,
          size_mb: parseFloat(zipSizeMB),
          photo_count: currentZipFileCount,
          chunk_index: zipIndex,
          status: "ready",
          created_at: new Date().toISOString(),
        },
        [
          Permission.read(Role.user(currentUserId)),
          Permission.update(Role.user(currentUserId)),
          Permission.delete(Role.user(currentUserId)),
        ]
      );

      context.log(`✅ Database record created: ${dlDoc.$id}`);
      createdDownloads.push({
        id: dlDoc.$id,
        fileId: uploaded.$id,
        filename: zipFilename,
        sizeMB: parseFloat(zipSizeMB),
        photoCount: currentZipFileCount,
      });

      zipIndex++;
    };

    // --------------------------------------------
    // Begin first ZIP
    // --------------------------------------------
    startZip();

    // --------------------------------------------
    // Process photos one by one
    // --------------------------------------------
    context.log(`🔄 Processing ${allPhotos.length} photos...`);
    
    for (let i = 0; i < allPhotos.length; i++) {
      const photo = allPhotos[i];
      totalProcessed++;

      try {
        const fileId = photo.file_id;
        const sizeMB = parseFloat(photo.file_size || 0);

        // Log progress every 50 photos
        if (totalProcessed % 50 === 0) {
          context.log(`📊 Progress: ${totalProcessed}/${allPhotos.length} photos processed`);
        }

        // If adding this file exceeds limit → close current ZIP & start new
        if (currentZipSizeMB + sizeMB > MAX_MB && currentZipFileCount >= MIN_FILES_PER_ZIP) {
          await finalizeAndUploadZip();
          startZip();
        }

        // Download file from storage
        const data = await storage.getFileDownload(PHOTO_BUCKET, fileId);
        const buffer = Buffer.from(data);

        // Determine file extension
        const ext = photo.file_type?.replace("image/", "") || 
                   photo.file_name?.split(".").pop() || 
                   "jpg";
        
        const filename = photo.file_name || `photo_${photo.$id}.${ext}`;

        // Add to archive
        archive.append(buffer, {
          name: filename,
          date: new Date(photo.$createdAt || Date.now()),
        });

        currentZipSizeMB += sizeMB;
        currentZipFileCount++;

      } catch (err) {
        context.error(`❌ Failed to process photo ${photo.$id}: ${err.message}`);
        failedPhotos.push({
          id: photo.$id,
          error: err.message,
        });
        // Continue processing remaining photos
      }
    }

    // Finalize last ZIP if it has files
    if (currentZipFileCount > 0) {
      await finalizeAndUploadZip();
    }

    // --------------------------------------------
    // Final Response
    // --------------------------------------------
    const responseData = {
      statusCode: 200,
      message: "Download prepared successfully",
      summary: {
        totalPhotos: allPhotos.length,
        processedPhotos: totalProcessed - failedPhotos.length,
        failedPhotos: failedPhotos.length,
        totalChunks: createdDownloads.length,
        totalSizeMB: createdDownloads.reduce((sum, dl) => sum + dl.sizeMB, 0).toFixed(2),
      },
      downloads: createdDownloads,
    };

    if (failedPhotos.length > 0) {
      responseData.failures = failedPhotos;
      responseData.message += ` (${failedPhotos.length} photos failed)`;
    }

    context.log(`🎉 Download preparation complete!`);
    return context.res.json(responseData);

  } catch (err) {
    context.error(`❌ Critical error: ${err.message}`);
    context.error(err.stack);
    
    return context.res.json({ 
      statusCode: 500, 
      error: "Internal server error",
      message: err.message,
      details: process.env.NODE_ENV === "development" ? err.stack : undefined,
    });
  }
}