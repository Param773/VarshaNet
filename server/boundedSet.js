// A Set that never grows past `maxSize` — oldest entries are evicted first
// (insertion order, same as a plain Set's iteration order). Used by the
// long-running feed-ingestion jobs (sachetIngest.js, imdCapIngest.js) to
// remember which alert GUIDs have already been turned into reports, without
// that memory growing for the entire lifetime of the process.
class BoundedSet {
  constructor(maxSize = 5000) {
    this.maxSize = maxSize;
    this.set = new Set();
  }

  has(value) {
    return this.set.has(value);
  }

  add(value) {
    this.set.add(value);
    while (this.set.size > this.maxSize) {
      const oldest = this.set.values().next().value;
      this.set.delete(oldest);
    }
    return this;
  }
}

module.exports = { BoundedSet };