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

  // Logged-in user
  const currentUserId = context.req.headers["x-appwrite-user-id"];

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
    // Fetch all photos from DB
    // --------------------------------------------
    const allPhotos = [];
    let offset = 0;

    while (true) {
      const res = await databases.listDocuments(DB, PHOTO_COLLECTION, [
        Query.equal("event_id", eventId),
        Query.limit(100),
        Query.offset(offset),
      ]);

      if (!res.documents.length) break;

      allPhotos.push(...res.documents);
      offset += res.documents.length;
    }

    if (allPhotos.length === 0) {
      return context.res.json({ statusCode: 404, error: "No photos found" });
    }

    // --------------------------------------------
    // ZIP chunking config
    // --------------------------------------------
    const MAX_MB = 2048; // 2GB limit
    let currentZipSizeMB = 0;
    let currentZipFileCount = 0;
    let zipIndex = 1;

    let archive = null;
    let zipChunks = null;
    let zipStream = null;

    const createdDownloads = [];

    // --------------------------------------------
    // Helper: Start new ZIP
    // --------------------------------------------
    const startZip = () => {
      archive = archiver("zip", { zlib: { level: 0 } });
      zipChunks = [];
      zipStream = new Writable({
        write(chunk, enc, cb) {
          zipChunks.push(chunk);
          cb();
        },
      });
      archive.pipe(zipStream);
      currentZipSizeMB = 0;
      currentZipFileCount = 0;
    };

    // --------------------------------------------
    // Helper: Finalize & upload ZIP
    // --------------------------------------------
    const finalizeAndUploadZip = async () => {
      await archive.finalize();
      await new Promise((resolve) => zipStream.on("finish", resolve));

      const zipBuffer = Buffer.concat(zipChunks);
      const zipFilename =
        (eventDoc.event_name || "photos") +
        (zipIndex > 1 ? `_part_${zipIndex}.zip` : `.zip`);

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
          Permission.delete(Role.user(currentUserId)),
        ]
      );

      // Create download DB record
      const dlDoc = await databases.createDocument(DB, DL_COLLECTION, "unique()", {
        user_id: currentUserId,
        event_id: eventId,
        file_id: uploaded.$id,
        file_name: zipFilename,
        size_mb: (zipBuffer.length / 1024 / 1024).toFixed(2),
        photo_count: currentZipFileCount,
        chunk_index: zipIndex,
      });

      createdDownloads.push(dlDoc.$id);

      zipIndex++;
    };

    // --------------------------------------------
    // Begin first ZIP
    // --------------------------------------------
    startZip();

    // --------------------------------------------
    // Process photos one by one
    // --------------------------------------------
    for (const photo of allPhotos) {
      const fileId = photo.file_id;
      const sizeMB = parseFloat(photo.file_size || 0);

      // If adding this file exceeds 2GB → close current ZIP & start new
      if (currentZipSizeMB + sizeMB > MAX_MB && currentZipFileCount > 0) {
        await finalizeAndUploadZip();
        startZip();
      }

      // Download file (Uint8Array)
      const data = await storage.getFileDownload(PHOTO_BUCKET, fileId);
      const buffer = Buffer.from(data); // convert Uint8Array → Buffer

      const ext = photo.file_type || "jpg";
      archive.append(buffer, {
        name: `photo_${photo.$id}.${ext}`,
      });

      currentZipSizeMB += sizeMB;
      currentZipFileCount++;
    }

    // Final ZIP if it has files
    if (currentZipFileCount > 0) {
      await finalizeAndUploadZip();
    }

    // --------------------------------------------
    // Respond
    // --------------------------------------------
    return context.res.json({
      statusCode: 200,
      message: "Download prepared successfully",
      chunks: createdDownloads.length,
      downloadIds: createdDownloads,
    });
  } catch (err) {
    context.error(err);
    return context.res.json({ statusCode: 500, error: err.message });
  }
}
