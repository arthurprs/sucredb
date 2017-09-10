use std::{mem, str};
use std::mem::ManuallyDrop;
use std::path::{Path, PathBuf};
use std::io::Write;
use std::sync::Arc;
use rocksdb::{self, Writable};
use byteorder::{BigEndian, WriteBytesExt};
use utils::*;

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
pub struct Storage {
    db: Arc<rocksdb::DB>,
    cf: &'static rocksdb::CFHandle,
    iterators_handle: Arc<()>,
    num: u16,
}

unsafe impl Sync for Storage {}
unsafe impl Send for Storage {}

pub struct StorageIterator {
    db: Arc<rocksdb::DB>,
    iterator: Box<rocksdb::rocksdb::DBIterator<'static>>,
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
        let mut def_cf_opts = rocksdb::ColumnFamilyOptions::new();
        def_cf_opts
            .set_prefix_extractor("U16BeSuffixTransform", Box::new(U16BeSuffixTransform))
            .unwrap();
        def_cf_opts.compression_per_level(
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
        def_cf_opts.set_write_buffer_size(32 * 1024 * 1024);
        def_cf_opts.set_max_bytes_for_level_base(4 * 32 * 1024 * 1024);
        def_cf_opts.set_max_write_buffer_number(4);

        let mut block_opts = rocksdb::BlockBasedOptions::new();
        block_opts.set_bloom_filter(10, false);
        block_opts.set_lru_cache(4 * 32 * 1024 * 1024);
        def_cf_opts.set_block_based_table_factory(&block_opts);

        let mut log_cf_opts = rocksdb::ColumnFamilyOptions::new();
        log_cf_opts.compression(rocksdb::DBCompressionType::No);
        // TODO: add options to only keep it for X time
        log_cf_opts.set_compaction_style(rocksdb::DBCompactionStyle::Fifo);
        log_cf_opts.set_write_buffer_size(32 * 1024 * 1024);
        log_cf_opts.set_max_write_buffer_number(4);

        let mut block_opts = rocksdb::BlockBasedOptions::new();
        block_opts.set_bloom_filter(10, false);
        block_opts.set_lru_cache(2 * 32 * 1024 * 1024);
        log_cf_opts.set_block_based_table_factory(&block_opts);

        // TODO: Rocksdb is complicated, we might want to tune some more options

        let db = rocksdb::DB::open_cf(
            opts.clone(),
            path.as_ref().to_str().unwrap(),
            vec!["default", "log"],
            vec![def_cf_opts.clone(), log_cf_opts.clone()],
        ).or_else(|_| -> Result<_, String> {
            let mut db = rocksdb::DB::open_cf(
                opts,
                path.as_ref().to_str().unwrap(),
                vec!["default"],
                vec![def_cf_opts],
            )?;

            db.create_cf("log", log_cf_opts)?;
            Ok(db)
        })?;

        Ok(StorageManager {
            path: path.as_ref().into(),
            db: Arc::new(db),
        })
    }

    pub fn open(&self, db_num: u16) -> Result<Storage, GenericError> {
        Ok(Storage {
            db: self.db.clone(),
            cf: unsafe { mem::transmute(self.db.cf_handle("default")) },
            num: db_num,
            iterators_handle: Arc::new(()),
        })
    }

    pub fn open_log(&self, db_num: u16) -> Result<Storage, GenericError> {
        Ok(Storage {
            db: self.db.clone(),
            cf: unsafe { mem::transmute(self.db.cf_handle("log")) },
            num: db_num,
            iterators_handle: Arc::new(()),
        })
    }
}

impl Drop for StorageManager {
    fn drop(&mut self) {
        let sc = Arc::strong_count(&self.db);
        let wc = Arc::weak_count(&self.db);
        assert_eq!(wc, 0);
        assert_eq!(sc, 1);
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
        let r = self.db.get_cf(self.cf, buffer).unwrap();
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
            let mut iterator = ManuallyDrop::new(Box::new(self.db.iter_cf(self.cf)));
            iterator.seek(rocksdb::SeekKey::Key(&key_prefix[..]));
            let result = StorageIterator {
                db: self.db.clone(),
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
        self.db.put_cf(self.cf, buffer, value).unwrap();
    }

    pub fn del(&self, key: &[u8]) {
        trace!("del {:?}", str::from_utf8(key));
        let mut buffer = [0u8; 512];
        let buffer = self.build_key(&mut buffer, key);
        self.db.delete_cf(self.cf, buffer).unwrap()
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
    pub fn seek(&mut self, key: &[u8]) {
        let mut buffer = [0u8; 512];
        (&mut buffer[0..2]).write(&self.key_prefix[..]).unwrap();
        (&mut buffer[2..]).write(key).unwrap();
        self.iterator.seek(
            rocksdb::SeekKey::Key(&buffer[..2 + key.len()]),
        );
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::fs;

    #[test]
    fn test_simple() {
        let _ = fs::remove_dir_all("t/test_simple");
        let sm = StorageManager::new("t/test_simple").unwrap();
        let storage = sm.open(1).unwrap();
        assert_eq!(storage.get_vec(b"sample"), None);
        storage.set(b"sample", b"sample_value");
        assert_eq!(storage.get_vec(b"sample").unwrap(), b"sample_value");
        storage.del(b"sample");
        assert_eq!(storage.get_vec(b"sample"), None);
    }

    #[test]
    fn test_simple_log() {
        let _ = fs::remove_dir_all("t/test_simple_log");
        let sm = StorageManager::new("t/test_simple_log").unwrap();
        let storage = sm.open_log(1).unwrap();
        assert_eq!(storage.get_vec(b"sample"), None);
        storage.set(b"sample", b"sample_value");
        assert_eq!(storage.get_vec(b"sample").unwrap(), b"sample_value");
        storage.del(b"sample");
        assert_eq!(storage.get_vec(b"sample"), None);
    }

    #[test]
    fn test_iter() {
        let _ = fs::remove_dir_all("t/test_iter");
        let sm = StorageManager::new("t/test_iter").unwrap();
        for &i in &[0, 1, 2] {
            let storage = sm.open(i).unwrap();
            storage.set(b"1", i.to_string().as_bytes());
            storage.set(b"2", i.to_string().as_bytes());
            storage.set(b"3", i.to_string().as_bytes());
        }
        for &i in &[0, 1, 2] {
            let storage = sm.open(i).unwrap();
            let results: Vec<Vec<u8>> = storage.iterator().iter().map(|(k, v)| v.into()).collect();
            assert_eq!(results, vec![i.to_string().as_bytes(); 3]);
        }
    }

    #[test]
    fn test_iter_log() {
        let _ = fs::remove_dir_all("t/test_iter_log");
        let sm = StorageManager::new("t/test_iter_log").unwrap();
        for &i in &[0, 1, 2] {
            let storage = sm.open_log(i).unwrap();
            storage.set(b"1", i.to_string().as_bytes());
            storage.set(b"2", i.to_string().as_bytes());
            storage.set(b"3", i.to_string().as_bytes());
        }
        for &i in &[0, 1, 2] {
            let storage = sm.open_log(i).unwrap();
            let results: Vec<Vec<u8>> = storage.iterator().iter().map(|(k, v)| v.into()).collect();
            assert_eq!(results, vec![i.to_string().as_bytes(); 3]);
        }
    }

    #[test]
    fn test_iter_seek() {
        let _ = fs::remove_dir_all("t/test_iter");
        let sm = StorageManager::new("t/test_iter").unwrap();
        let i = 1;
        let storage = sm.open(i).unwrap();
        storage.set(b"1", b"1");
        storage.set(b"2", b"2");
        storage.set(b"3", b"3");
        let mut iterator = storage.iterator();
        iterator.seek(b"2");
        let results: Vec<Vec<u8>> = iterator.iter().map(|(k, v)| v.into()).collect();
        assert_eq!(results, vec![b"2", b"3"]);
    }

    #[test]
    fn test_clear() {
        let _ = fs::remove_dir_all("t/test_clear");
        let sm = StorageManager::new("t/test_clear").unwrap();
        for &i in &[0, 1, 2] {
            let storage = sm.open(i).unwrap();
            storage.set(b"1", i.to_string().as_bytes());
            storage.set(b"2", i.to_string().as_bytes());
            storage.set(b"3", i.to_string().as_bytes());
        }
        for &i in &[0, 1, 2] {
            let storage = sm.open(i).unwrap();
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
        sm.open(1).unwrap();
        sm.open(2).unwrap();
        sm.open(3).unwrap();
        sm.open(1).unwrap();
        sm.open(2).unwrap();
        sm.open(3).unwrap();
    }

}
