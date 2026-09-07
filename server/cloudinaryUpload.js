// Uploads citizen-submitted photos/videos to Cloudinary (free tier) so
// they persist across restarts — Render's local disk doesn't — and, more
// importantly, so they can actually be *viewed* in the Admin Console.
// Previously the file was written to local disk and never served or
// displayed anywhere: the admin only ever saw a "📷 photo" text badge with
// no way to look at the actual evidence.
//
// If CLOUDINARY_CLOUD_NAME isn't configured, uploadBuffer resolves to null
// instead of throwing — the report still saves fine with hasPhoto/hasVideo
// set correctly, it just won't have a viewable thumbnail. Non-breaking.

const cloudinary = require("cloudinary").v2;
const { Readable } = require("stream");

const configured = !!process.env.CLOUDINARY_CLOUD_NAME;
if (configured) {
  cloudinary.config({
    cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
    api_key: process.env.CLOUDINARY_API_KEY,
    api_secret: process.env.CLOUDINARY_API_SECRET,
  });
}

function uploadBuffer(buffer, resourceType) {
  if (!configured) return Promise.resolve(null);

  return new Promise((resolve) => {
    const uploadStream = cloudinary.uploader.upload_stream(
      { folder: "varshanet", resource_type: resourceType },
      (error, result) => {
        if (error) {
          console.error("Cloudinary upload failed:", error.message);
          return resolve(null);
        }
        resolve(result.secure_url);
      }
    );
    Readable.from(buffer).pipe(uploadStream);
  });
}

module.exports = { uploadBuffer };