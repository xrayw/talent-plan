
#![allow(missing_docs)]
#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(unused_imports)]

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};
use serde::{Deserialize, Serialize};

use crate::{KvsError, Result};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are persisted to disk in log files. Log files are named after
/// monotonically increasing generation numbers with a `log` extension name.
/// A `BTreeMap` in memory stores the keys and the value locations for fast query.
///
/// ```rust
/// # use kvs::{KvStore, Result};
/// # fn try_main() -> Result<()> {
/// use std::env::current_dir;
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let val = store.get("key".to_owned())?;
/// assert_eq!(val, Some("value".to_owned()));
/// # Ok(())
/// # }
/// ```
///
/// 存储格式,此处忽略掉crc
/// |timestamp|ksize|vsize|key|value|
/// |   u64   |u32  | u32 |   |     |
///
/// 删除该数据时将timestamp置为0
/// 
pub struct KvStore {
    path: PathBuf,
    /// 写到了第几个文件
    nth: u64,
    writer: File,
    readers: HashMap<u64, File>,
    indexes: BTreeMap<String, DataIndex>,
    uncompacted: u64,
}

/// Represents the position and length of a json-serialized command in the log.
struct DataIndex {
    n: u64,
    pos: u64,
    len: u32,
    timestamp: u64,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        // read all log files under path, then init them

        let mut readers = HashMap::new();
        let mut indexes = BTreeMap::new();
        let mut vec: Vec<u64> = Vec::new();
        let mut uncompacted: u64 = 0;
        let mut entries: Vec<_> = fs::read_dir(&path)?
            .flat_map(|v| v.map(|e| e.path()))
            .flat_map(|v| v.file_name().unwrap()
                .to_str().unwrap()
                .trim_end_matches(".log")
                .parse::<u64>()
            )
            .collect();
        entries.sort_unstable();

        for num in entries {
            let (mut f, cpath) = open_file(&path, num);
            if f.metadata().unwrap().len() == 0 {
                fs::remove_file(cpath).unwrap();
                continue;
            }
            loop {
                let item = read_item(num, &mut f);
                if item.is_err() {
                    let err = item.err().unwrap();
                    println!("{:#?}", err);
                    break;
                }

                let (key, data) = item.unwrap();

                // timestamp == 0的代表被删除, 等待compact程序运行
                if data.timestamp == 0 {
                    uncompacted += data.len as u64;
                    continue;
                }

                if let Some(v) = indexes.insert(key, data) {
                    remove_item(readers.get_mut(&v.n).unwrap(), v.pos);
                    uncompacted += v.len as u64;
                }
            }

            readers.insert(num, f);
            vec.push(num);
        }

        vec.sort_unstable();
        let mut maxn: u64 = 0;
        if let Some(n) = vec.last() {
            maxn = *n;
        }
        maxn += 1;

        readers.insert(maxn, open_file(&path, maxn).0);
        let writer = open_file(&path, maxn).0;
        return Ok(KvStore {
            path,
            nth: maxn,
            writer,
            readers,
            indexes,
            uncompacted
        })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let mut file = &self.writer;
        let curpos = file.seek(SeekFrom::Current(0)).unwrap();
        let k = key.as_bytes();
        let v = value.as_bytes();
        let unixtime = unix_time();

        file.write_all(&unixtime.to_le_bytes()[..])?;
        file.write_all(&(k.len() as u32).to_le_bytes()[..])?;
        file.write_all(&(v.len() as u32).to_le_bytes()[..])?;
        file.write_all(k)?;
        file.write_all(v)?;
        file.flush().unwrap();

        // file.write_u64::<LittleEndian>(unixtime).unwrap();
        // file.write_u32::<LittleEndian>(k.len() as _).unwrap();
        // file.write_u32::<LittleEndian>(v.len() as _).unwrap();
        // file.write_all(k).unwrap();
        // file.write_all(v).unwrap();

        let len = 16 + (k.len() + v.len()) as u32;
        if let Some(v) = self.indexes.insert(key, DataIndex {
            n: self.nth,
            pos: curpos,
            len,
            timestamp: unixtime,
        })
        {
            self.uncompacted += v.len as u64;
            if let Some(file) = self.readers.get_mut(&v.n) {
                remove_item(file, v.pos);
            }
        }
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(vv) = self.indexes.get(&key) {
            let s = self.readers.get_mut(&vv.n).map(|f| {
                // to vsize start postion
                f.seek(SeekFrom::Start(vv.pos + 8 + 4)).unwrap();
                let vsize = f.read_u32::<LittleEndian>().unwrap();

                // to vdata start position
                let ksize = vv.len - 16 - vsize;
                f.seek(SeekFrom::Current((ksize) as _)).unwrap();

                let mut s = String::with_capacity(vsize as _);
                let mut take_reader = f.take(vsize as _);
                take_reader.read_to_string(&mut s).unwrap();
                s
            });
            return Ok(s);
        }
        Err(KvsError::KeyNotFound)
    }

    pub fn remove(&mut self, key:String) -> Result<()> {
        if let Some(v) = self.indexes.remove(&key) {
            self.uncompacted += v.len as u64;
            remove_item(self.readers.get_mut(&v.n).unwrap(), v.pos);
            return Ok(());
        }
        Err(KvsError::KeyNotFound)
    }
}

fn fpos(f: &mut File) -> io::Result<u64> {
    f.seek(SeekFrom::Current(0))
}

fn read_item(n: u64, f: &mut File) -> Result<(String, DataIndex)> {
    let pos = fpos(f)?;

    let timestamp: u64 = f.read_u64::<LittleEndian>()?;
    let ksize = f.read_u32::<LittleEndian>()?;
    let vsize = f.read_u32::<LittleEndian>()?;

    // let mut key: Vec<u8> = Vec::with_capacity(ksize as _);
    // f.take(ksize as _).read_to_end(&mut key)?;

    let mut key: Vec<u8> = vec![0; ksize as _];
    f.read_exact(&mut key)?;

    f.seek(SeekFrom::Current(vsize as _)).unwrap();

    Ok((
        String::from_utf8(key).unwrap(),
        DataIndex{
            n,
            pos,
            len: 16 + ksize + vsize,
            timestamp
        }
    ))
}

fn remove_item(f: &mut File, pos: u64) {
    f.seek(SeekFrom::Start(pos)).unwrap();
    f.write_u64::<LittleEndian>(0).unwrap();
}


fn open_file(path: &PathBuf, n: u64) -> (File, PathBuf) {
    let fpath = path.join(format!("{}.log", n));
    (OpenOptions::new().read(true).write(true).create(true).open(&fpath).unwrap(), fpath)
}

fn unix_time() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}


#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};
    use std::path::PathBuf;
    use byteorder::{LittleEndian, WriteBytesExt};

    use crate::{KvsError, KvStore};

    #[test]
    pub fn test_init() {
        let path= PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let logpath = path.join("target/owntest");
        let mut kvs = KvStore::open(logpath).unwrap();
        // let result = kvs.get("hello".to_owned());
        // assert!(result.is_err());

        kvs.set("yongwei".to_string(), "iewgnoy555678".to_string()).unwrap();
        let val = kvs.get("yongwei".to_string());
        let s = val.unwrap().unwrap();
        println!("{}", s);
        assert_eq!(s, "iewgnoy555678".to_string());
    }

    #[test]
    pub fn test_seek_write() {
        let path= PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let logpath = path.join("target/owntest");

        let p = logpath.join("tt.log");
        let mut  file = OpenOptions::new().read(true).write(true).create(true).open(&p).unwrap();
        file.seek(SeekFrom::Start(0));
        file.write_u64::<LittleEndian>(1);

        file.flush().unwrap();
    }
}
