use std::path::{Path, PathBuf};
use lmdb_rs;
use utils::*;
use std::str;
use std::sync::{Mutex, Arc};
use nodrop::NoDrop;

pub struct StorageIterator {
    env: NoDrop<Box<lmdb_rs::Environment>>,
    db_h: NoDrop<Box<lmdb_rs::DbHandle>>,
    tx: NoDrop<Box<lmdb_rs::ReadonlyTransaction<'static>>>,
    db: NoDrop<Box<lmdb_rs::Database<'static>>>,
    cursor: NoDrop<lmdb_rs::core::CursorIterator<'static, lmdb_rs::CursorIter>>,
    iterators_handle: Arc<()>,
}

pub struct StorageManager {
    path: PathBuf,
    env: lmdb_rs::Environment,
    storages_handle: Arc<Mutex<()>>,
}

pub struct Storage {
    env: lmdb_rs::Environment,
    db_h: lmdb_rs::DbHandle,
    storages_handle: Arc<Mutex<()>>,
    iterators_handle: Arc<()>,
}

impl StorageManager {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<StorageManager, GenericError> {
        let env = try!(lmdb_rs::Environment::new()
            .map_size(10 * 1024 * 1024)
            .flags(lmdb_rs::core::EnvCreateNoTls)
            .autocreate_dir(true)
            .max_dbs(256)
            .open(path.as_ref(), 0o777));
        Ok(StorageManager {
            path: path.as_ref().into(),
            env: env,
            storages_handle: Default::default(),
        })
    }

    pub fn open(&self, db_num: i32, create: bool) -> Result<Storage, GenericError> {
        let _wlock = self.storages_handle.lock().unwrap();
        let db_name = db_num.to_string();
        let db_h = try!(if create {
            self.env.create_db(&db_name, lmdb_rs::DbFlags::empty())
        } else {
            self.env.get_db(&db_name, lmdb_rs::DbFlags::empty())
        });
        Ok(Storage {
            env: self.env.clone(),
            db_h: db_h,
            storages_handle: self.storages_handle.clone(),
            iterators_handle: Arc::new(()),
        })
    }
}

impl Storage {
    pub fn get<R, F: FnOnce(&[u8]) -> R>(&self, key: &[u8], callback: F) -> Option<R> {
        let r: Option<&[u8]> = self.env.get_reader().unwrap().bind(&self.db_h).get(&key).ok();
        debug!("get {:?} ({:?} bytes)", str::from_utf8(key), r.map(|x| x.len()));
        r.map(|r| callback(r))
    }

    pub fn iter(&self) -> StorageIterator {
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
        debug!("set {:?} ({} bytes)", str::from_utf8(key), value.len());
        let _wlock = self.storages_handle.lock().unwrap();
        let txn = self.env.new_transaction().unwrap();
        txn.bind(&self.db_h).set(&key, &value).unwrap();
        txn.commit().unwrap();
    }

    pub fn del(&self, key: &[u8]) {
        let _wlock = self.storages_handle.lock().unwrap();
        let txn = self.env.new_transaction().unwrap();
        txn.bind(&self.db_h).del(&key).unwrap();
        txn.commit().unwrap();
    }

    pub fn clear(&self) {
        let _wlock = self.storages_handle.lock().unwrap();
        let txn = self.env.new_transaction().unwrap();
        txn.bind(&self.db_h).clear().unwrap();
        txn.commit().unwrap();
    }

    pub fn sync(&self) {
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
        let c = Arc::strong_count(&self.iterators_handle);
        assert!(c == 1, "{} pending iterators", c - 1);
    }
}

unsafe impl Send for StorageIterator {}
unsafe impl Sync for StorageIterator {}

impl StorageIterator {
    pub fn iter<F: FnMut(&[u8], &[u8]) -> bool>(&mut self, mut callback: F) -> bool {
        while let Some(cv) = self.cursor.next() {
            if !callback(cv.get_key(), cv.get_value()) {
                return true;
            }
        }
        false
    }
}

impl Drop for StorageIterator {
    fn drop(&mut self) {
        info!("freeing StorageIterator");
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
    fn test_iter() {
        let _ = fs::remove_dir_all("t/test_simple");
        let sm = StorageManager::new("t/test_simple").unwrap();
        let storage = sm.open(1, true).unwrap();
        storage.set(b"1", b"1");
        storage.set(b"2", b"2");
        storage.set(b"3", b"3");
        let mut results: Vec<Vec<u8>> = Vec::new();
        storage.iter().iter(|k, v| {
            results.push(v.into());
            results.len() < 2
        });
        assert_eq!(results, &[b"1", b"2"]);
        results.clear();
        storage.iter().iter(|k, v| {
            results.push(v.into());
            true
        });
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
