use std::path::{Path, PathBuf};
use std::fs;
use lmdb_rs;
use std::collections::HashMap;
use utils::*;

pub struct Storage {
    path: PathBuf,
    env: lmdb_rs::Environment,
    db: lmdb_rs::DbHandle,
}

impl Storage {
    pub fn open_all(dir_path: &Path) -> Result<HashMap<u32, Storage>, GenericError> {
        let mut result = HashMap::new();
        for maybe_entry in try!(fs::read_dir(dir_path)) {
            let path = try!(maybe_entry).path();
            let vnode = try!(path.file_name().unwrap().to_string_lossy().parse::<u32>());
            result.insert(vnode, try!(Self::open(&path, false)));
        }
        Ok(result)
    }

    pub fn open(path: &Path, create: bool) -> Result<Storage, GenericError> {
        let env = try!(lmdb_rs::Environment::new()
                           .map_size(10 * 1024 * 1024)
                           .autocreate_dir(create)
                           .open(path, 0o777));
        let db = try!(env.get_default_db(lmdb_rs::DbFlags::empty()));
        Ok(Storage {
            path: path.into(),
            env: env,
            db: db,
        })
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.env.get_reader().unwrap().bind(&self.db).get(&key).ok()
    }

    pub fn set(&self, key: &[u8], value: &[u8]) {
        let txn = self.env.new_transaction().unwrap();
        txn.bind(&self.db).set(&key, &value).unwrap();
        txn.commit().unwrap();
    }

    pub fn del(&self, key: &[u8]) {
        let txn = self.env.new_transaction().unwrap();
        txn.bind(&self.db).del(&key).unwrap();
        txn.commit().unwrap();
    }

    pub fn clear(&self) {
        let txn = self.env.new_transaction().unwrap();
        txn.bind(&self.db).clear().unwrap();
        txn.commit().unwrap();
    }

    pub fn purge(self) {
        self.env.new_transaction().unwrap().bind(&self.db).del_db().unwrap();
        let Storage { path, .. } = self;
        let _ = fs::remove_dir_all(path);
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
        let storage = Storage::open(Path::new("t/test_simple"), true).unwrap();
        assert_eq!(storage.get(b"sample"), None);
        storage.set(b"sample", b"sample_value");
        assert_eq!(storage.get(b"sample").unwrap(), b"sample_value");
        storage.del(b"sample");
        assert_eq!(storage.get(b"sample"), None);
        storage.purge();
    }


    #[test]
    fn test_open_all() {
        let _ = fs::remove_dir_all("t/test_open_all");
        Storage::open(Path::new("t/test_open_all/1"), true).unwrap();
        Storage::open(Path::new("t/test_open_all/2"), true).unwrap();
        Storage::open(Path::new("t/test_open_all/3"), true).unwrap();
        assert_eq!(Storage::open_all(Path::new("t/test_open_all")).unwrap().len(),
                   3);
    }

}
