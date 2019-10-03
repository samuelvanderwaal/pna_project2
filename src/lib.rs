// #![deny(missing_docs)]

//! Key Value Storage is a primitive, in-memory database with set, get and remove methods.

#[macro_use]
extern crate failure;

use std::io::{Seek, SeekFrom, Write, BufReader};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions};
use serde::{Serialize, Deserialize};
use failure::Error;
use bson;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Serialize, Deserialize)]
enum KvCommand {
    Set(String, String),
    Rm(String),
}

/// The struct that stores values.
/// Its "values" field is a HashMap of String keys and values.
pub struct KvStore {
    /// Where our in-memory database is stored.
    pub log_name: PathBuf,
    pub log: HashMap<String, u64>,
    pub cursor: u64,
}

impl KvStore {
    /// Create an empty instance of the database.
    /// ```
    /// # use kvs::KvStore;
    /// let mut db = KvStore::new();
    /// ```
    // pub fn new() -> KvStore {
    //     KvStore {
    //         log_name: File::new(),
    //         log: HashMap::new(),
    //     }
    // }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<KvStore> {
        let path = path.as_ref();
        let log_name = path.to_path_buf().join("test_db.log");
        let log = HashMap::new();
        let cursor = 0;

        // let log_name = OpenOptions::new()
        //             .create(true)
        //             .append(true)
        //             .open(log_name)?;

        Ok(KvStore { log_name, log, cursor  })
    }

    /// The getter function for values.
    /// ```
    /// # use kvs::KvStore;
    /// # let mut db = KvStore::new();
    /// db.get("key".to_string());
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if !&self.log_name.exists() {
            return Ok(None)
        }

        // 
        self.read_log_to_memory(self.cursor)?;

        let f = File::open(&self.log_name)?;
        let mut buf = BufReader::new(f);
        let mut value = String::new();

        if self.log.contains_key(&key) {

            // Read from log at key's pointer value.
            // We use expect because we just checked for the key's existence.
            let log_pointer = self.log.get(&key).expect("No key!");
            buf.seek(SeekFrom::Start(*log_pointer))?;

            if let Ok(document) = bson::decode_document(&mut buf) {
                let command: KvCommand = bson::from_bson(bson::Bson::Document(document))?;
                value = match command {
                    KvCommand::Set(_, value) => value,
                    _ => panic!("Invalid log offset!"),
                };
            }
            return Ok(Some(value))

        } else {
            Ok(None)
        }
    }

    /// The setter function.
    /// ```
    /// # use kvs::KvStore;
    /// # let mut db = KvStore::new();
    /// db.set("key1".to_string(), "value1".to_string())
    /// ```
    pub fn set<S: ToString>(&mut self, key: S, value: S) -> Result<()> {
        let mut f = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.log_name)?; 
       
        let command = KvCommand::Set(key.to_string(), value.to_string());
        write_bson_record(command, &mut f)?;
        Ok(())  
    }

    /// Remove values.
    /// ```
    /// # use kvs::KvStore;c
    /// # let mut db = KvStore::new();
    /// db.remove("key1".to_string())
    /// ```
    pub fn remove<S: ToString>(&mut self, key: S) -> Result<()> {
        self.read_log_to_memory(self.cursor)?;

        if !self.log.contains_key(&key.to_string()) {
            bail!("Key not found")
        }

        let command = KvCommand::Rm(key.to_string());
        let mut f = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.log_name)?; 

        write_bson_record(command, &mut f)?;
        Ok(())
    }

    fn read_log_to_memory(&mut self, offset: u64) -> Result<()> {
        let f = File::open(&self.log_name)?;
        let mut buf = BufReader::new(f);
        buf.seek(SeekFrom::Start(offset))?;

        while let Ok(document) = bson::decode_document(&mut buf) {
            let command: KvCommand = bson::from_bson(bson::Bson::Document(document))?;
            match command {
                KvCommand::Set(key, _) => {
                    self.log
                        .insert(key, self.cursor);
                    },
                KvCommand::Rm(key) => {
                    self.log.remove(&key).ok_or(format_err!("Missing key value!"))?;
                }
            }
            self.cursor = buf.seek(SeekFrom::Current(0))?;
        }
        Ok(())
    }
}

fn write_bson_record<T: Serialize, W: Write>(record: T, writer: &mut W) -> Result<()>{
    let ser_record = bson::to_bson(&record)?;
    if let bson::Bson::Document(document) = ser_record {
        bson::encode_document(writer, &document)?;
    } else {
        bail!("Failed to serialize record!");
    }
    Ok(())
}

// fn read_bson_record<'de, T: Deserialize<'de>, R: Read>(mut record: T, mut reader: R) -> Result<T> {
//     if let Ok(decoded) = bson::decode_document(&mut reader) {
//         record = bson::from_bson(bson::Bson::Document(decoded))?;
//     }
//     Ok(record)
// }
