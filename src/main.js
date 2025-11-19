import archiver from 'archiver';
import { Writable } from 'stream';
import { Client, Storage, Databases, Query, Permission, Role } from 'node-appwrite';

export default async function prepareDownload(context) {
  context.log('🔹 Starting download preparation function...');

  // -----------------------------
  // Parse request body
  // -----------------------------
  let payload = {};
  try {
    context.log('📥 Parsing request body...');
    if (context.req.bodyRaw) {
      payload = JSON.parse(context.req.bodyRaw);
      context.log('✅ Request body parsed:', payload);
    }
  } catch (err) {
    context.error('❌ Invalid JSON in request body: ' + err.message);
    return context.res.json({ statusCode: 400, error: 'Invalid JSON in request body' });
  }

  const { eventId } = payload;

  if (!eventId) {
    context.error('❌ Missing eventId in request body');
    return context.res.json({ statusCode: 400, error: 'Missing eventId' });
  }

  context.log(`📌 Event ID: ${eventId}`);

  // -----------------------------
  // Init Appwrite
  // -----------------------------
  const client = new Client()
    .setEndpoint(process.env.APPWRITE_ENDPOINT)
    .setProject(process.env.APPWRITE_PROJECT_ID)
    .setKey(process.env.APPWRITE_API_KEY);

  context.log('🔗 Appwrite client initialized');

  const storage = new Storage(client);
  const databases = new Databases(client);

  const databaseId = process.env.APPWRITE_DATABASE_ID;
  const photoCollectionId = process.env.APPWRITE_PHOTO_COLLECTION_ID;
  const eventCollectionId = process.env.APPWRITE_EVENT_COLLECTION_ID;
  const downloadCollectionId = process.env.APPWRITE_DOWNLOAD_COLLECTION_ID;
  const photoBucketId = process.env.APPWRITE_BUCKET_ID;
  const downloadBucketId = process.env.APPWRITE_DOWNLOAD_BUCKET_ID;

  // -----------------------------
  // Verify user
  // -----------------------------
  const headers = context.req.headers;
  const currentUserId = headers['x-appwrite-user-id'];
  context.log(`👤 Current user ID from headers: ${currentUserId}`);

  try {
    // -----------------------------
    // Verify event ownership
    // -----------------------------
    context.log(`🔹 Verifying event ownership for ${eventId}...`);
    const eventDoc = await databases.getDocument(databaseId, eventCollectionId, eventId);

    const eventUserId = String(eventDoc.user_id || '').trim();
    context.log(`🔑 Event owner: ${eventUserId}`);

    if (eventUserId !== currentUserId) {
      context.error(`❌ Ownership mismatch: ${eventUserId} ≠ ${currentUserId}`);
      return context.res.json({
        statusCode: 403,
        error: 'Forbidden – you do not own this event',
      });
    }

    context.log(`🔒 Ownership verified for user ${currentUserId}`);

    // -----------------------------
    // Fetch all photos
    // -----------------------------
    context.log(`🔹 Fetching all photos for event ${eventId}...`);
    const allPhotos = [];
    let offset = 0;
    const batchSize = 100;

    while (true) {
      const result = await databases.listDocuments(databaseId, photoCollectionId, [
        Query.equal('event_id', eventId),
        Query.limit(batchSize),
        Query.offset(offset),
      ]);

      if (result.documents.length === 0) break;

      allPhotos.push(...result.documents);
      offset += result.documents.length;

      context.log(`📄 Fetched ${result.documents.length} photos (total: ${allPhotos.length})`);

      if (result.documents.length < batchSize) break;
    }

    if (allPhotos.length === 0) {
      return context.res.json({ statusCode: 404, error: 'No photos found' });
    }

    context.log(`✅ Total photos: ${allPhotos.length}`);

    // -----------------------------
    // Chunk into 2GB groups
    // -----------------------------
    const MAX_CHUNK_MB = 2048;
    const chunks = [];
    let currentChunk = [];
    let size = 0;

    for (const p of allPhotos) {
      const s = parseFloat(p.file_size || 0);

      if (size + s > MAX_CHUNK_MB && currentChunk.length > 0) {
        chunks.push(currentChunk);
        currentChunk = [];
        size = 0;
      }

      currentChunk.push(p);
      size += s;
    }
    if (currentChunk.length > 0) chunks.push(currentChunk);

    context.log(`📦 Created ${chunks.length} chunk(s)`);

    // -----------------------------
    // Process chunks
    // -----------------------------
    const createdDownloads = [];

    for (let i = 0; i < chunks.length; i++) {
      const chunk = chunks[i];
      const zipFilename =
        chunks.length > 1
          ? `${eventDoc.event_name || 'photos'}_part_${i + 1}.zip`
          : `${eventDoc.event_name || 'photos'}.zip`;

      context.log(`🔹 Processing ZIP chunk ${i + 1}/${chunks.length}`);

      // --- Create ZIP archive ---
      const archive = archiver('zip', { zlib: { level: 0 } });
      const zipChunks = [];

      const zipStream = new Writable({
        write(chunk, enc, cb) {
          zipChunks.push(chunk);
          cb();
        }
      });

      archive.pipe(zipStream);

      archive.on('error', (err) => {
        throw err;
      });

      // --- Add photos ---
      for (const [idx, photo] of chunk.entries()) {
        try {
          const fileId = photo.file_id;
          const type = photo.file_type || 'jpg';
          const filename = `photo_${photo.$id}.${type}`;

          context.log(`📸 Adding photo: ${filename}`);

          const stream = await storage.getFileDownload(photoBucketId, fileId);
          const buffer = Buffer.from(await stream.arrayBuffer());

          archive.append(buffer, { name: filename });
        } catch (err) {
          context.error(`❌ Failed photo ${photo.$id}: ${err.message}`);
        }
      }

      // --- Finalize ZIP ---
      await archive.finalize();
      await new Promise((resolve) => zipStream.on('finish', resolve));

      const zipBuffer = Buffer.concat(zipChunks);
      const zipMB = (zipBuffer.length / 1024 / 1024).toFixed(2);

      context.log(`✅ ZIP created (${zipMB} MB)`);

      // -----------------------------
      // Upload ZIP
      // -----------------------------
      const downloadFileId = `download_${Date.now()}_${i}`;

      const fileObject = {
        name: zipFilename,
        type: 'application/zip',
        size: zipBuffer.length,
        arrayBuffer: async () =>
          zipBuffer.buffer.slice(
            zipBuffer.byteOffset,
            zipBuffer.byteOffset + zipBuffer.byteLength
          ),
        slice: (start, end) =>
          new Blob([zipBuffer.slice(start, end)], { type: 'application/zip' })
      };

      const uploadedFile = await storage.createFile(
        downloadBucketId,
        downloadFileId,
        fileObject,
        [
          Permission.read(Role.user(currentUserId)),
          Permission.delete(Role.user(currentUserId)),
        ]
      );

      context.log(`📤 Uploaded: ${uploadedFile.$id}`);

      // -----------------------------
      // Record in DB
      // -----------------------------
      const downloadDoc = await databases.createDocument(
        databaseId,
        downloadCollectionId,
        'unique()',
        {
          user_id: currentUserId,
          event_id: eventId,
          file_id: uploadedFile.$id,
          file_name: zipFilename,
          size_mb: parseFloat(zipMB),
          photo_count: chunk.length,
          chunk_index: i + 1,
          total_chunks: chunks.length,
        }
      );

      createdDownloads.push(downloadDoc.$id);
      context.log(`📝 Created DB record: ${downloadDoc.$id}`);
    }

    return context.res.json({
      statusCode: 200,
      message: 'Download prepared successfully',
      totalChunks: chunks.length,
      downloadIds: createdDownloads,
    });

  } catch (error) {
    context.error('❌ Error preparing download:', error);
    return context.res.json({ statusCode: 500, error: error.message });
  }
}
