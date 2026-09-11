// Verifies a buffer is actually one of the image formats it claims to be,
// by checking magic bytes — not the client-supplied `mimetype` field, which
// is just a header the uploader can set to anything they want.
//
// Why this exists: server/routes/reports.js used to gate photo-only
// processing (perceptual hashing via Jimp, which pulls in the `file-type`
// package internally) purely on `file.mimetype.startsWith("image")`. An
// attacker can set that header to "image/jpeg" while actually uploading any
// bytes at all — including a crafted payload built to exploit a known
// infinite-loop bug in file-type's ASF (WMV/WMA) parser (GHSA-5v7r-6r5c-r473,
// fixed upstream in file-type 21.3.1, which jimp@0.22's old, unmaintained
// file-type dependency does not have). That payload never needs to be a
// real ASF file — file-type sniffs by content, not extension, so a 55-byte
// buffer alone can stall the Node.js event loop for whichever process
// handles the report submission.
//
// Rather than force-upgrading jimp to a major version with a different API
// (real breaking-change risk without a chance to test it here), this simply
// stops anything that isn't a genuine, recognized image from ever reaching
// Jimp/file-type in the first place. Covers the formats citizen reports
// actually use: JPEG, PNG, GIF, WEBP.

function looksLikeImage(buffer) {
  if (!buffer || buffer.length < 12) return false;

  // JPEG: FF D8 FF
  if (buffer[0] === 0xff && buffer[1] === 0xd8 && buffer[2] === 0xff) return true;

  // PNG: 89 50 4E 47 0D 0A 1A 0A
  if (
    buffer[0] === 0x89 &&
    buffer[1] === 0x50 &&
    buffer[2] === 0x4e &&
    buffer[3] === 0x47 &&
    buffer[4] === 0x0d &&
    buffer[5] === 0x0a &&
    buffer[6] === 0x1a &&
    buffer[7] === 0x0a
  ) {
    return true;
  }

  // GIF: "GIF87a" or "GIF89a"
  if (
    buffer[0] === 0x47 &&
    buffer[1] === 0x49 &&
    buffer[2] === 0x46 &&
    buffer[3] === 0x38 &&
    (buffer[4] === 0x37 || buffer[4] === 0x39) &&
    buffer[5] === 0x61
  ) {
    return true;
  }

  // WEBP: "RIFF" .... "WEBP"
  if (
    buffer[0] === 0x52 &&
    buffer[1] === 0x49 &&
    buffer[2] === 0x46 &&
    buffer[3] === 0x46 &&
    buffer[8] === 0x57 &&
    buffer[9] === 0x45 &&
    buffer[10] === 0x42 &&
    buffer[11] === 0x50
  ) {
    return true;
  }

  return false;
}

module.exports = { looksLikeImage };
