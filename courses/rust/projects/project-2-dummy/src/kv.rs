
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
    oldfiles: HashMap<u64, File>,
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

        let mut oldfiles = HashMap::new();
        let mut indexes = BTreeMap::new();
        let mut vec: Vec<u64> = Vec::new();
        let mut uncompacted: u64 = 0;
        for e in fs::read_dir(&path)? {
            let e = e?;
            let fpath = e.path();
            let n = fpath.to_str().unwrap().trim_end_matches(".log");
            let num: u64 = n.parse().unwrap();

            // read key index
            let mut f = open_file(&path, num);
            loop {
                let item = read_item(num, &mut f);
                if item.is_err() {
                    break;
                }

                let (key, data) = item.unwrap();

                // timestamp == 0的代表被删除, 等待compact程序运行
                if data.timestamp == 0 {
                    uncompacted += data.len as u64;
                    continue;
                }

                indexes.insert(key, data);
            }
            
            oldfiles.insert(num, f);
            vec.push(num);
        }

        vec.sort_unstable();
        let mut maxn: u64 = 0;
        if let Some(n) = vec.last() {
            maxn = *n;
        }

        let writer = open_file(&path, maxn);
        return Ok(KvStore {
            path,
            nth: maxn,
            writer,
            oldfiles,
            indexes,
            uncompacted
        })

    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        if let Some(v) = self.indexes.remove(&key) {
            self.uncompacted += v.len as u64;
            if let Some(file) = self.oldfiles.get_mut(&v.n) {
                remove_item(file, v.pos);
            }
        }

        let mut file = &self.writer;
        let unixtime = unix_time();

        let curpos = file.seek(SeekFrom::Current(0)).unwrap();
        let k = key.as_bytes();
        let v = value.as_bytes();

        file.write_u64::<LittleEndian>(unixtime).unwrap();
        file.write_u64::<LittleEndian>(k.len() as _).unwrap();
        file.write_u64::<LittleEndian>(v.len() as _).unwrap();
        file.write_all(k).unwrap();
        file.write_all(v).unwrap();

        let len = 128 + (k.len() + v.len()) as u32;
        self.indexes.insert(key, DataIndex {
            n: self.nth,
            pos: curpos,
            len,
            timestamp: unixtime
        });
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(vv) = self.indexes.get(&key) {
            let s = self.oldfiles.get_mut(&vv.n).map(|f| {
                f.seek(SeekFrom::Start(vv.pos as _)).unwrap();
                let mut s = String::with_capacity(vv.len as _);
                let mut take_reader = f.take(vv.len as _);
                take_reader.read_to_string(&mut s).unwrap();
                s
            });
            return Ok(s);
        }
        Err(KvsError::KeyNotFound)
    }

    pub fn remove(&mut self, key:String) -> Result<()> {
        todo!()
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
    let mut key: Vec<u8> = Vec::with_capacity(ksize as usize);
    f.read_exact(key.as_mut_slice())?;

    Ok((
        String::from_utf8(key).unwrap(),
        DataIndex{
            n,
            pos,
            len: 128 + ksize + vsize,
            timestamp
        }
    ))
}

fn remove_item(f: &mut File, pos: u64) {
    f.seek(SeekFrom::Start(pos)).unwrap();
    f.write_u64::<LittleEndian>(0).unwrap();
}


fn open_file(path: &PathBuf, n: u64) -> File {
    OpenOptions::new().read(true).write(true).append(true).create(true).open(&path.join(format!("{}.log", n))).unwrap()
}

fn unix_time() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}
