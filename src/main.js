import archiver from 'archiver';
import { Client, Storage, Databases, Query, Permission, Role } from 'node-appwrite';

export default async function prepareDownload(context) {
  context.log('🔹 Starting download preparation function...');

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

  // Initialize Appwrite client
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

  // Verify user ownership
  const headers = context.req.headers;
  const currentUserId = headers['x-appwrite-user-id'];
  context.log(`👤 Current user ID from headers: ${currentUserId}`);

  try {
    // 1️⃣ Verify event ownership
    context.log(`🔹 Verifying event ownership for ${eventId}...`);
    const eventDoc = await databases.getDocument(databaseId, eventCollectionId, eventId);
    
    const eventUserId = String(eventDoc.user_id || '').trim();
    context.log(`🔑 Event owner: ${eventUserId}`);
    
    if (eventUserId !== currentUserId) {
      context.error(`❌ Ownership mismatch: event.user_id=${eventUserId}, user=${currentUserId}`);
      return context.res.json({
        statusCode: 403,
        error: 'Forbidden – you do not own this event',
      });
    }
    context.log(`🔒 Ownership verified for user ${currentUserId}`);

    // 2️⃣ Fetch ALL photos with their sizes
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
      context.log(`📄 Fetched ${result.documents.length} photos (total: ${allPhotos.length})`);
      
      offset += result.documents.length;
      if (result.documents.length < batchSize) break;
    }

    if (allPhotos.length === 0) {
      context.log('⚠️ No photos found');
      return context.res.json({ statusCode: 404, error: 'No photos found' });
    }

    context.log(`✅ Total photos to process: ${allPhotos.length}`);

    // 3️⃣ Split photos into 2GB chunks
    const MAX_CHUNK_SIZE_MB = 2048; // 2GB in MB
    const chunks = [];
    let currentChunk = [];
    let currentChunkSize = 0;

    for (const photo of allPhotos) {
      const photoSize = parseFloat(photo.file_size || 0);
      
      if (currentChunkSize + photoSize > MAX_CHUNK_SIZE_MB && currentChunk.length > 0) {
        // Current chunk would exceed 2GB, start new chunk
        chunks.push([...currentChunk]);
        currentChunk = [];
        currentChunkSize = 0;
      }
      
      currentChunk.push(photo);
      currentChunkSize += photoSize;
    }

    // Add remaining photos
    if (currentChunk.length > 0) {
      chunks.push(currentChunk);
    }

    context.log(`📦 Created ${chunks.length} chunks (max 2GB each)`);

    // 4️⃣ Process each chunk
    const createdDownloads = [];

    for (let chunkIndex = 0; chunkIndex < chunks.length; chunkIndex++) {
      const chunk = chunks[chunkIndex];
      const zipFilename = chunks.length > 1 
        ? `${eventDoc.event_name || 'photos'}_part_${chunkIndex + 1}.zip`
        : `${eventDoc.event_name || 'photos'}.zip`;

      context.log(`🔹 Processing chunk ${chunkIndex + 1}/${chunks.length} (${chunk.length} photos)`);

      // Create ZIP archive
      const archive = archiver('zip', { zlib: { level: 0 } });
      const zipChunks = [];

      archive.on('data', (data) => zipChunks.push(data));
      
      let archiveFinished = false;
      archive.on('end', () => { archiveFinished = true; });
      archive.on('error', (err) => { throw err; });

      // Add photos to archive
      for (const [index, photo] of chunk.entries()) {
        try {
          const fileId = photo.file_id;
          const fileType = photo.file_type || 'jpg';
          const photoId = photo.$id;
          const filename = `photo_${photoId}.${fileType}`;
          
          context.log(`📸 [${index + 1}/${chunk.length}] Adding: ${filename}`);

          const fileData = await storage.getFileDownload(photoBucketId, fileId);
          const fileBuffer = Buffer.isBuffer(fileData) ? fileData : Buffer.from(fileData);
          
          archive.append(fileBuffer, { name: filename });
        } catch (err) {
          context.error(`❌ Failed to add photo ${photo.$id}: ${err.message}`);
        }
      }

      // Finalize archive
      await archive.finalize();
      
      // Wait for archive to finish
      while (!archiveFinished) {
        await new Promise(resolve => setTimeout(resolve, 100));
      }

      const zipBuffer = Buffer.concat(zipChunks);
      const zipSizeMB = (zipBuffer.length / 1024 / 1024).toFixed(2);
      context.log(`✅ ZIP created: ${zipSizeMB} MB`);

      // 5️⃣ Upload to download bucket
      const downloadFileId = `download_${Date.now()}_${chunkIndex}`;
      context.log(`📤 Uploading to download bucket: ${downloadFileId}`);

      const { Readable } = require('stream');
      const readable = new Readable();
      readable.push(zipBuffer);
      readable.push(null);

      const uploadedFile = await storage.createFile(
        downloadBucketId,
        downloadFileId,
        readable,
        [
          Permission.read(Role.user(currentUserId)),
          Permission.delete(Role.user(currentUserId))
        ]
      );

      context.log(`✅ Uploaded: ${uploadedFile.$id}`);

      // 6️⃣ Create download record
      const downloadDoc = await databases.createDocument(
        databaseId,
        downloadCollectionId,
        'unique()',
        {
          user_id: currentUserId,
          event_id: eventId,
          file_id: uploadedFile.$id,
          file_name: zipFilename,
          size_mb: parseFloat(zipSizeMB),
          photo_count: chunk.length,
          chunk_index: chunkIndex + 1,
          total_chunks: chunks.length,
        }
      );

      context.log(`📝 Download record created: ${downloadDoc.$id}`);
      createdDownloads.push(downloadDoc.$id);
    }

    context.log(`✅ All chunks processed successfully`);
    
    return context.res.json({
      statusCode: 200,
      message: 'Download prepared successfully',
      totalChunks: chunks.length,
      downloadIds: createdDownloads,
    });

  } catch (error) {
    context.error('❌ Error preparing download: ' + error.message);
    context.error('Stack trace:', error.stack);
    return context.res.json({ statusCode: 500, error: error.message });
  }
}