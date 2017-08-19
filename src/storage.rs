use std::{mem, str};
use std::path::{Path, PathBuf};
use std::io::Write;
use std::sync::Arc;
use rocksdb::{self, Writable};
use byteorder::{BigEndian, WriteBytesExt};
use utils::*;
use nodrop::NoDrop;

struct U16BeSuffixTransform;

impl rocksdb::SliceTransform for U16BeSuffixTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        &key[..2]
    }

    fn in_domain(&mut self, key: &[u8]) -> bool {
        key.len() >= 2
    }
}

pub struct StorageManager {
    path: PathBuf,
    db: Arc<rocksdb::DB>,
}

// TODO: support TTL
// TODO: merge operator would be a huge win
// This implementation goes to a lot of trouble to allow
// safe-ish iterators that don't require a lifetime attached to them.
pub struct Storage {
    db: Arc<rocksdb::DB>,
    iterators_handle: Arc<()>,
    num: u16,
}

pub struct StorageIterator {
    db: Arc<rocksdb::DB>,
    snapshot: NoDrop<Box<rocksdb::rocksdb::Snapshot<'static>>>,
    iterator: NoDrop<Box<rocksdb::rocksdb::DBIterator<'static>>>,
    iterators_handle: Arc<()>,
    key_prefix: [u8; 2],
    first: bool,
}

unsafe impl Send for StorageIterator {}

impl StorageManager {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<StorageManager, GenericError> {
        let mut opts = rocksdb::DBOptions::new();
        opts.create_if_missing(true);
        opts.set_max_background_jobs(4);
        let mut cf_opts = rocksdb::ColumnFamilyOptions::new();
        cf_opts
            .set_prefix_extractor("U16BeSuffixTransform", Box::new(U16BeSuffixTransform))
            .unwrap();
        cf_opts.compression_per_level(
            &[
                rocksdb::DBCompressionType::No,
                rocksdb::DBCompressionType::No,
                rocksdb::DBCompressionType::Lz4,
                rocksdb::DBCompressionType::Lz4,
                rocksdb::DBCompressionType::Lz4,
                rocksdb::DBCompressionType::Lz4,
                rocksdb::DBCompressionType::Lz4,
            ],
        );
        cf_opts.set_write_buffer_size(32 * 1024 * 1024);
        cf_opts.set_max_bytes_for_level_base(4 * 32 * 1024 * 1024);
        cf_opts.set_max_write_buffer_number(4);
        let mut block_opts = rocksdb::BlockBasedOptions::new();
        block_opts.set_bloom_filter(10, false);
        block_opts.set_lru_cache(4 * 32 * 1024 * 1024);
        cf_opts.set_block_based_table_factory(&block_opts);

        // TODO: Rocksdb is complicated, we might want to tune some more options
        let db = rocksdb::DB::open_cf(
            opts,
            path.as_ref().to_str().unwrap(),
            vec!["default"],
            vec![cf_opts],
        ).unwrap();
        Ok(StorageManager {
            path: path.as_ref().into(),
            db: Arc::new(db),
        })
    }

    pub fn open(&self, db_num: u16, _create: bool) -> Result<Storage, GenericError> {
        Ok(Storage {
            db: self.db.clone(),
            num: db_num,
            iterators_handle: Arc::new(()),
        })
    }
}

impl Drop for StorageManager {
    fn drop(&mut self) {
        let sc = Arc::strong_count(&self.db);
        let wc = Arc::weak_count(&self.db);
        assert!(wc == 0);
        assert!(sc == 1, "{} pending databases", sc - 1);
    }
}

impl Storage {
    #[inline]
    fn build_key<'a>(&self, buffer: &'a mut [u8], key: &[u8]) -> &'a [u8] {
        (&mut buffer[..2]).write_u16::<BigEndian>(self.num).unwrap();
        (&mut buffer[2..]).write_all(key).unwrap();
        &buffer[..2 + key.len()]
    }

    pub fn get<R, F: FnOnce(&[u8]) -> R>(&self, key: &[u8], callback: F) -> Option<R> {
        let mut buffer = [0u8; 512];
        let buffer = self.build_key(&mut buffer, key);
        let r = self.db.get(buffer).unwrap();
        trace!(
            "get {:?} ({:?} bytes)",
            str::from_utf8(key),
            r.as_ref().map(|x| x.len())
        );
        r.map(|r| callback(&*r))
    }

    pub fn iterator(&self) -> StorageIterator {
        let mut key_prefix = [0u8; 2];
        (&mut key_prefix[..])
            .write_u16::<BigEndian>(self.num)
            .unwrap();
        unsafe {
            let snapshot = NoDrop::new(Box::new(self.db.snapshot()));
            let mut iterator = NoDrop::new(Box::new(snapshot.iter()));
            iterator.seek(rocksdb::SeekKey::Key(&key_prefix[..]));
            let result = StorageIterator {
                db: self.db.clone(),
                snapshot: mem::transmute_copy(&snapshot),
                iterator: mem::transmute_copy(&iterator),
                iterators_handle: self.iterators_handle.clone(),
                key_prefix: key_prefix,
                first: true,
            };
            result
        }
    }

    pub fn get_vec(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.get(key, |v| v.to_owned())
    }

    pub fn set(&self, key: &[u8], value: &[u8]) {
        trace!("set {:?} ({} bytes)", str::from_utf8(key), value.len());
        let mut buffer = [0u8; 512];
        let buffer = self.build_key(&mut buffer, key);
        self.db.put(buffer, value).unwrap();
    }

    pub fn del(&self, key: &[u8]) {
        trace!("del {:?}", str::from_utf8(key));
        let mut buffer = [0u8; 512];
        let buffer = self.build_key(&mut buffer, key);
        self.db.delete(buffer).unwrap()
    }

    pub fn clear(&self) {
        trace!("clear");
        self.check_pending_iters();
        let mut from = [0u8; 2];
        let mut to = [0u8; 2];
        (&mut from[..]).write_u16::<BigEndian>(self.num).unwrap();
        (&mut to[..]).write_u16::<BigEndian>(self.num + 1).unwrap();
        self.db.delete_file_in_range(&from[..], &to[..]).unwrap();
        for (key, _) in self.iterator().iter() {
            self.del(key);
        }
    }

    pub fn sync(&self) {
        debug!("sync");
        self.db.sync_wal().unwrap();
    }

    fn check_pending_iters(&self) {
        let sc = Arc::strong_count(&self.iterators_handle);
        let wc = Arc::weak_count(&self.iterators_handle);
        assert!(wc == 0);
        assert!(sc == 1, "{} pending databases", sc - 1);
    }
}

impl Drop for Storage {
    fn drop(&mut self) {
        self.check_pending_iters();
    }
}

impl StorageIterator {
    pub fn iter<'a>(&'a mut self) -> StorageIter<'a> {
        StorageIter { it: self }
    }
}

pub struct StorageIter<'a> {
    it: &'a mut StorageIterator,
}

impl<'a> Iterator for StorageIter<'a> {
    type Item = (&'a [u8], &'a [u8]);
    fn next(&mut self) -> Option<Self::Item> {
        if self.it.first {
            self.it.first = false;
        } else {
            self.it.iterator.next();
        }
        if self.it.iterator.valid() && self.it.iterator.key()[..2] == self.it.key_prefix[..] {
            unsafe {
                // safe as slices are valid until the next call to next
                Some((
                    mem::transmute(&self.it.iterator.key()[2..]),
                    mem::transmute(self.it.iterator.value()),
                ))
            }
        } else {
            None
        }
    }
}

impl Drop for StorageIterator {
    fn drop(&mut self) {
        unsafe {
            let iterator: NoDrop<Box<rocksdb::rocksdb::DBIterator>> =
                mem::transmute_copy(&self.iterator);
            let snapshot: NoDrop<Box<rocksdb::rocksdb::Snapshot>> =
                mem::transmute_copy(&self.snapshot);
            drop(iterator.into_inner());
            drop(snapshot.into_inner());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::fs;

    #[test]
    fn test_simple() {
        let _ = fs::remove_dir_all("t/test_simple");
        let sm = StorageManager::new("t/test_simple").unwrap();
        let storage = sm.open(1, true).unwrap();
        assert_eq!(storage.get_vec(b"sample"), None);
        storage.set(b"sample", b"sample_value");
        assert_eq!(storage.get_vec(b"sample").unwrap(), b"sample_value");
        storage.del(b"sample");
        assert_eq!(storage.get_vec(b"sample"), None);
        drop(storage);
    }

    #[test]
    fn test_iter2() {
        let _ = fs::remove_dir_all("t/test_iter2");
        let sm = StorageManager::new("t/test_iter2").unwrap();
        for &i in &[0, 1, 2] {
            let storage = sm.open(i, true).unwrap();
            storage.set(b"1", i.to_string().as_bytes());
            storage.set(b"2", i.to_string().as_bytes());
            storage.set(b"3", i.to_string().as_bytes());
        }
        for &i in &[0, 1, 2] {
            let storage = sm.open(i, true).unwrap();
            let results: Vec<Vec<u8>> = storage.iterator().iter().map(|(k, v)| v.into()).collect();
            assert_eq!(
                results,
                &[i.to_string().as_bytes(), i.to_string().as_bytes(), i.to_string().as_bytes()]
            );
        }
    }

    #[test]
    fn test_clear() {
        let _ = fs::remove_dir_all("t/test_clear");
        let sm = StorageManager::new("t/test_clear").unwrap();
        for &i in &[0, 1, 2] {
            let storage = sm.open(i, true).unwrap();
            storage.set(b"1", i.to_string().as_bytes());
            storage.set(b"2", i.to_string().as_bytes());
            storage.set(b"3", i.to_string().as_bytes());
        }
        for &i in &[0, 1, 2] {
            let storage = sm.open(i, true).unwrap();
            let results: Vec<Vec<u8>> = storage.iterator().iter().map(|(k, v)| v.into()).collect();
            assert_eq!(
                results,
                &[i.to_string().as_bytes(), i.to_string().as_bytes(), i.to_string().as_bytes()]
            );
            storage.clear();
            let results: Vec<Vec<u8>> = storage.iterator().iter().map(|(k, v)| v.into()).collect();
            assert_eq!(results.len(), 0);
        }
    }

    #[test]
    fn test_open_all() {
        let _ = fs::remove_dir_all("t/test_open_all");
        let sm = StorageManager::new("t/test_open_all").unwrap();
        sm.open(1, true).unwrap();
        sm.open(2, true).unwrap();
        sm.open(3, true).unwrap();
        sm.open(1, false).unwrap();
        sm.open(2, false).unwrap();
        sm.open(3, false).unwrap();
    }

}
