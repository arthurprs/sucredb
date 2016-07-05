use std::path::{Path, PathBuf};
use std::fs;
use lmdb_rs;
use std::collections::HashMap;
use utils::*;
use std::str;
use std::sync::Arc;
use nodrop::NoDrop;

pub struct StorageIterator {
    env: NoDrop<Box<lmdb_rs::Environment>>,
    db_h: NoDrop<Box<lmdb_rs::DbHandle>>,
    tx: NoDrop<Box<lmdb_rs::ReadonlyTransaction<'static>>>,
    db: NoDrop<Box<lmdb_rs::Database<'static>>>,
    cursor: NoDrop<lmdb_rs::core::CursorIterator<'static, lmdb_rs::CursorIter>>,
    iterators_handle: Arc<()>,
}

pub struct Storage {
    path: PathBuf,
    env: lmdb_rs::Environment,
    db_h: lmdb_rs::DbHandle,
    iterators_handle: Arc<()>,
}

impl Storage {
    pub fn open_all<P: AsRef<Path>>(dir_path: P) -> Result<HashMap<i32, Storage>, GenericError> {
        let mut result = HashMap::new();
        for maybe_entry in try!(fs::read_dir(&dir_path)) {
            let path = try!(maybe_entry).path();
            let db_num = try!(path.file_name().unwrap().to_string_lossy().parse::<i32>());
            result.insert(db_num, try!(Self::open(&dir_path, db_num, false)));
        }
        Ok(result)
    }

    pub fn open<P: AsRef<Path>>(path: P, db_num: i32, create: bool) -> Result<Storage, GenericError> {
        let db_path = path.as_ref().to_owned().join(db_num.to_string());
        let env = try!(lmdb_rs::Environment::new()
            .map_size(10 * 1024 * 1024)
            .flags(lmdb_rs::core::EnvCreateNoTls)
            .autocreate_dir(create)
            .open(&db_path, 0o777));
        let db_h = try!(env.get_default_db(lmdb_rs::DbFlags::empty()));
        Ok(Storage {
            path: path.as_ref().into(),
            env: env,
            db_h: db_h,
            iterators_handle: Arc::new(()),
        })
    }

    pub fn get<R, F: FnOnce(Option<&[u8]>) -> R>(&self, key: &[u8], callback: F) -> R {
        let r1 = self.env.get_reader().unwrap().bind(&self.db_h).get(&key).ok();
        let r2 = callback(r1);
        debug!("get {:?} ({:?} bytes)", str::from_utf8(key), r1.map(|x| x.len()));
        r2
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
        self.get(key, |v| v.map(|v| v.to_owned()))
    }

    pub fn set(&self, key: &[u8], value: &[u8]) {
        debug!("set {:?} ({} bytes)", str::from_utf8(key), value.len());
        let txn = self.env.new_transaction().unwrap();
        txn.bind(&self.db_h).set(&key, &value).unwrap();
        txn.commit().unwrap();
    }

    pub fn del(&self, key: &[u8]) {
        let txn = self.env.new_transaction().unwrap();
        txn.bind(&self.db_h).del(&key).unwrap();
        txn.commit().unwrap();
    }

    pub fn clear(&self) {
        let txn = self.env.new_transaction().unwrap();
        txn.bind(&self.db_h).clear().unwrap();
        txn.commit().unwrap();
    }

    pub fn purge(self) {
        self.env.new_transaction().unwrap().bind(&self.db_h).del_db().unwrap();
        let path = self.path.clone();
        drop(self);
        let _ = fs::remove_dir_all(path);
    }

    pub fn sync(&self) {
        self.env.sync(true).unwrap();
    }
}

impl Drop for Storage {
    fn drop(&mut self) {
        assert_eq!(Arc::strong_count(&self.iterators_handle), 1);
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
        let storage = Storage::open(Path::new("t/test_simple"), 1, true).unwrap();
        assert_eq!(storage.get_vec(b"sample"), None);
        storage.set(b"sample", b"sample_value");
        assert_eq!(storage.get_vec(b"sample").unwrap(), b"sample_value");
        storage.del(b"sample");
        assert_eq!(storage.get_vec(b"sample"), None);
        storage.purge();
    }

    #[test]
    fn test_iter() {
        let _ = fs::remove_dir_all("t/test_simple");
        let storage = Storage::open(Path::new("t/test_simple"), 1, true).unwrap();
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
    }

    #[test]
    fn test_open_all() {
        let _ = fs::remove_dir_all("t/test_open_all");
        Storage::open(Path::new("t/test_open_all"), 1, true).unwrap();
        Storage::open(Path::new("t/test_open_all"), 2, true).unwrap();
        Storage::open(Path::new("t/test_open_all"), 3, true).unwrap();
        assert_eq!(Storage::open_all(Path::new("t/test_open_all")).unwrap().len(), 3);
    }

}
