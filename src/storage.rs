use std::path::{Path, PathBuf};
use lmdb_rs;
use utils::*;
use std::str;
use std::sync::{Mutex, Arc};
use nodrop::NoDrop;
use std::collections::HashMap;
use linear_map::LinearMap;

// FIXME: this is ridicullous, but so is lmdb write performance
// FIXME: this module needs to be rewriten to use something like rocksdb
const BUFFER_SOFT_LIM: usize = 32;
const BUFFER_HARD_LIM: usize = 36;

struct Buffer {
    map: HashMap<Vec<u8>, Option<Vec<u8>>>,
    db_h: lmdb_rs::DbHandle,
}

pub struct StorageManager {
    path: PathBuf,
    env: lmdb_rs::Environment,
    storages_handle: Arc<Mutex<LinearMap<i32, Arc<Mutex<Buffer>>>>>,
}

pub struct Storage {
    env: lmdb_rs::Environment,
    db_h: lmdb_rs::DbHandle,
    storages_handle: Arc<Mutex<LinearMap<i32, Arc<Mutex<Buffer>>>>>,
    buffer: Arc<Mutex<Buffer>>,
    num: i32,
    iterators_handle: Arc<()>,
}

pub struct StorageIterator {
    env: NoDrop<Box<lmdb_rs::Environment>>,
    db_h: NoDrop<Box<lmdb_rs::DbHandle>>,
    tx: NoDrop<Box<lmdb_rs::ReadonlyTransaction<'static>>>,
    db: NoDrop<Box<lmdb_rs::Database<'static>>>,
    cursor: NoDrop<lmdb_rs::core::CursorIterator<'static, lmdb_rs::CursorIter>>,
    iterators_handle: Arc<()>,
}

unsafe impl Send for StorageIterator {}
unsafe impl Sync for StorageIterator {}

impl StorageManager {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<StorageManager, GenericError> {
        let mut env = try!(lmdb_rs::Environment::new()
            .map_size(1024 * 1024 * 1024)
            .flags(lmdb_rs::core::EnvCreateNoTls | lmdb_rs::core::EnvCreateNoMemInit |
                   lmdb_rs::core::EnvCreateWriteMap)
            .autocreate_dir(true)
            .max_dbs(256)
            .open(path.as_ref(), 0o777));
        try!(env.set_flags(lmdb_rs::core::EnvNoMetaSync | lmdb_rs::core::EnvNoMemInit, true));
        Ok(StorageManager {
            path: path.as_ref().into(),
            env: env,
            storages_handle: Default::default(),
        })
    }

    pub fn open(&self, db_num: i32, create: bool) -> Result<Storage, GenericError> {
        let mut wlock = self.storages_handle.lock().unwrap();
        let db_name = db_num.to_string();
        let db_h = try!(if create {
            self.env.create_db(&db_name, lmdb_rs::DbFlags::empty())
        } else {
            self.env.get_db(&db_name, lmdb_rs::DbFlags::empty())
        });
        let buffer = Arc::new(Mutex::new(Buffer {
            map: Default::default(),
            db_h: db_h.clone(),
        }));
        wlock.insert(db_num, buffer.clone());

        Ok(Storage {
            num: db_num,
            env: self.env.clone(),
            db_h: db_h,
            storages_handle: self.storages_handle.clone(),
            iterators_handle: Arc::new(()),
            buffer: buffer,
        })
    }
}

impl Storage {
    pub fn get<R, F: FnOnce(&[u8]) -> R>(&self, key: &[u8], callback: F) -> Option<R> {
        let buffer = self.buffer.lock().unwrap();
        let r: Option<&[u8]> = match buffer.map.get(key) {
            Some(&Some(ref v)) => Some(v.as_slice()),
            Some(&None) => None,
            None => self.env.get_reader().unwrap().bind(&self.db_h).get(&key).ok(),
        };
        trace!("get {:?} ({:?} bytes)", str::from_utf8(key), r.map(|x| x.len()));
        r.map(|r| callback(r))
    }

    pub fn iterator(&self) -> StorageIterator {
        self.flush_buffer(true);
        let env = NoDrop::new(Box::new(self.env.clone()));
        let db_h = NoDrop::new(Box::new(self.db_h.clone()));
        let tx = NoDrop::new(Box::new(env.get_reader().unwrap()));
        let db = NoDrop::new(Box::new(tx.bind(&self.db_h)));
        let cursor = NoDrop::new(db.iter().unwrap());
        unsafe {
            use std::mem::transmute_copy;
            StorageIterator {
                db_h: transmute_copy(&db_h),
                env: transmute_copy(&env),
                iterators_handle: self.iterators_handle.clone(),
                db: transmute_copy(&db),
                tx: transmute_copy(&tx),
                cursor: transmute_copy(&cursor),
            }
        }
    }

    pub fn get_vec(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.get(key, |v| v.to_owned())
    }

    pub fn set(&self, key: &[u8], value: &[u8]) {
        trace!("set {:?} ({} bytes)", str::from_utf8(key), value.len());
        let buffer_len = {
            let mut buffer = self.buffer.lock().unwrap();
            buffer.map.insert(key.into(), Some(value.into()));
            buffer.map.len()
        };
        if buffer_len >= BUFFER_SOFT_LIM {
            self.flush_buffer(buffer_len >= BUFFER_HARD_LIM);
        }
    }

    pub fn del(&self, key: &[u8]) {
        trace!("del {:?}", str::from_utf8(key));
        let buffer_len = {
            let mut buffer = self.buffer.lock().unwrap();
            buffer.map.insert(key.into(), None);
            buffer.map.len()
        };
        if buffer_len >= BUFFER_SOFT_LIM {
            self.flush_buffer(buffer_len >= BUFFER_HARD_LIM);
        }
    }

    pub fn clear(&self) {
        self.buffer.lock().unwrap().map.clear();
        let _wlock = self.storages_handle.lock().unwrap();
        let txn = self.env.new_transaction().unwrap();
        txn.bind(&self.db_h).clear().unwrap();
        txn.commit().unwrap();
    }

    fn flush_buffer(&self, force: bool) {
        let wlock = if force {
            self.storages_handle.lock().unwrap()
        } else if let Ok(wlock) = self.storages_handle.try_lock() {
            wlock
        } else {
            return;
        };
        let txn = self.env.new_transaction().unwrap();
        for s in wlock.values() {
            let mut locked = s.lock().unwrap();
            let db = txn.bind(&locked.db_h);
            for (k, v_opt) in locked.map.drain() {
                if let Some(v) = v_opt {
                    db.set(&k, &v).unwrap();
                } else {
                    match db.del(&k) {
                        Ok(_) => (),
                        Err(lmdb_rs::MdbError::NotFound) => (),
                        Err(e) => panic!("del error {:?}", e),
                    }
                }
            }
        }
        txn.commit().unwrap();
    }

    pub fn sync(&self) {
        self.flush_buffer(true);
        let _wlock = self.storages_handle.lock().unwrap();
        self.env.sync(true).unwrap();
    }
}

impl Drop for StorageManager {
    fn drop(&mut self) {
        let c = Arc::strong_count(&self.storages_handle);
        assert!(c == 1, "{} pending databases", c - 1);
    }
}

impl Drop for Storage {
    fn drop(&mut self) {
        let mut wlock = self.storages_handle.lock().unwrap();
        wlock.remove(&self.num);
        let c = Arc::strong_count(&self.iterators_handle);
        assert!(c == 1, "{} pending iterators", c - 1);
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
        self.it.cursor.next().map(|cv| (cv.get_key(), cv.get_value()))
    }
}

impl Drop for StorageIterator {
    fn drop(&mut self) {
        // FIXME: super hack to make sure drops are done in the correct order
        unsafe {
            use std::mem::transmute_copy;
            transmute_copy::<_, NoDrop<lmdb_rs::core::CursorIterator<'static, lmdb_rs::CursorIter>>>(&self.cursor)
                .into_inner();
            transmute_copy::<_, NoDrop<Box<lmdb_rs::Database<'static>>>>(&self.db).into_inner();
            transmute_copy::<_, NoDrop<Box<lmdb_rs::ReadonlyTransaction<'static>>>>(&self.tx)
                .into_inner();
            transmute_copy::<_, NoDrop<Box<lmdb_rs::DbHandle>>>(&self.db_h).into_inner();
            transmute_copy::<_, NoDrop<Box<lmdb_rs::Environment>>>(&self.env).into_inner();
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
        let _ = fs::remove_dir_all("t/test_simple");
        let sm = StorageManager::new("t/test_simple").unwrap();
        let storage = sm.open(1, true).unwrap();
        storage.set(b"1", b"1");
        storage.set(b"2", b"2");
        storage.set(b"3", b"3");
        let results: Vec<Vec<u8>> = storage.iterator().iter().map(|(k, v)| v.into()).collect();
        assert_eq!(results, &[b"1", b"2", b"3"]);
        drop(storage);
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
