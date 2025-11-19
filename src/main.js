import archiver from 'archiver';
import { Client, Storage, Databases, Query } from 'node-appwrite';

export default async function downloadPhotos(context) {
  context.log('🔹 Starting photo download function...');

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

  const { photoIds, chunkIndex = 0, eventId } = payload;
  
  if (!photoIds || !Array.isArray(photoIds) || photoIds.length === 0) {
    context.error('❌ Missing or invalid photoIds in request body');
    return context.res.json({ statusCode: 400, error: 'Missing or invalid photoIds' });
  }
  
  if (!eventId) {
    context.error('❌ Missing eventId in request body');
    return context.res.json({ statusCode: 400, error: 'Missing eventId' });
  }

  context.log(`📌 Event ID: ${eventId}`);
  context.log(`📌 Chunk Index: ${chunkIndex}`);
  context.log(`📌 Total photos requested: ${photoIds.length}`);

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
  const bucketId = process.env.APPWRITE_BUCKET_ID;

  // Verify user ownership
  const headers = context.req.headers;
  const currentUserId = headers['x-appwrite-user-id'];
  context.log(`👤 Current user ID from headers: ${currentUserId}`);

  try {
    // 1️⃣ Verify event ownership
    context.log(`🔹 Verifying event ownership for ${eventId}...`);
    const eventCollectionId = process.env.APPWRITE_EVENT_COLLECTION_ID;
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

    // 2️⃣ Set response headers for streaming ZIP
    const zipFilename = `photos_part_${chunkIndex + 1}.zip`;
    context.log(`📦 Creating ZIP file: ${zipFilename}`);
    
    context.res.send('', 200, {
      'Content-Type': 'application/zip',
      'Content-Disposition': `attachment; filename="${zipFilename}"`,
      'Cache-Control': 'no-cache',
    });

    // 3️⃣ Create archiver instance
    context.log('🔹 Initializing archiver...');
    const archive = archiver('zip', {
      zlib: { level: 0 } // No compression - photos already compressed
    });

    // Handle archiver warnings and errors
    archive.on('warning', (err) => {
      if (err.code === 'ENOENT') {
        context.log('⚠️ Archive warning:', err.message);
      } else {
        context.error('❌ Archive warning:', err);
      }
    });

    archive.on('error', (err) => {
      context.error('❌ Archive error:', err);
      throw err;
    });

    // Pipe archive to response
    archive.pipe(context.res);

    // 4️⃣ Fetch photo documents to get file metadata
    context.log(`🔹 Fetching photo documents for ${photoIds.length} photos...`);
    const photoDocuments = [];
    
    // Fetch in batches to avoid query limits
    const batchSize = 100;
    for (let i = 0; i < photoIds.length; i += batchSize) {
      const batch = photoIds.slice(i, i + batchSize);
      context.log(`📄 Fetching batch ${Math.floor(i / batchSize) + 1}...`);
      
      const result = await databases.listDocuments(databaseId, photoCollectionId, [
        Query.equal('$id', batch),
        Query.equal('event_id', eventId), // Additional security check
      ]);
      
      photoDocuments.push(...result.documents);
    }
    
    context.log(`✅ Fetched ${photoDocuments.length} photo documents`);

    // 5️⃣ Stream each file into the ZIP
    let successCount = 0;
    let errorCount = 0;

    for (const [index, photoDoc] of photoDocuments.entries()) {
      try {
        const fileId = photoDoc.file_id;
        const fileType = photoDoc.file_type || 'jpg';
        const photoId = photoDoc.$id;
        
        // Generate unique filename
        const filename = `photo_${photoId}.${fileType}`;
        
        context.log(`📸 [${index + 1}/${photoDocuments.length}] Processing: ${filename}`);

        // Get file download stream from Appwrite
        const fileBuffer = await storage.getFileDownload(bucketId, fileId);
        
        // Add to archive
        archive.append(fileBuffer, { name: filename });
        
        successCount++;
        context.log(`✅ [${index + 1}/${photoDocuments.length}] Added: ${filename}`);
        
      } catch (err) {
        errorCount++;
        context.error(`❌ Failed to add photo ${photoDoc.$id}: ${err.message}`);
        // Continue with other files
      }
    }

    // 6️⃣ Finalize the archive
    context.log('🔹 Finalizing ZIP archive...');
    await archive.finalize();
    
    context.log(`✅ ZIP creation completed successfully`);
    context.log(`📊 Summary: ${successCount} successful, ${errorCount} failed`);
    context.log(`📦 ZIP file: ${zipFilename}`);

  } catch (error) {
    context.error('❌ Error creating ZIP: ' + error.message);
    context.error('Stack trace:', error.stack);
    return context.res.json({ statusCode: 500, error: error.message });
  }
}