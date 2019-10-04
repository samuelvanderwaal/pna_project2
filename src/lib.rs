// #![deny(missing_docs)]

//! Key Value Storage is a primitive, in-memory database with set, get and remove methods.

#[macro_use]
extern crate failure;

use bson;
use failure::Error;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Seek, SeekFrom, Read, Write};
use std::path::{Path};

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
    pub file_handle: File,
    pub log: HashMap<String, u64>,
    pub cursor: u64,
}

impl KvStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<KvStore> {
        // Convert our generic to a Path and then add database name to it.
        let path = path.as_ref().join("test_db.log");

        // In-memory database
        let log = HashMap::new();
        let cursor = 0;


        let file_handle = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;

        Ok(KvStore {
            file_handle,
            log,
            cursor,
        })
    }

    /// The getter function for values.
    /// ```
    /// # use kvs::KvStore;
    /// # let mut db = KvStore::new();
    /// db.get("key".to_string());
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.read_log_to_memory(self.cursor)?;

        let mut buf = BufReader::new(&mut self.file_handle);
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
            return Ok(Some(value));
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
        let command = KvCommand::Set(key.to_string(), value.to_string());
        write_bson_record(command, &mut self.file_handle)?;
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
        write_bson_record(command, &mut self.file_handle)?;
        Ok(())
    }

    fn read_log_to_memory(&mut self, offset: u64) -> Result<()> {
        let mut buf = BufReader::new(&mut self.file_handle);
        buf.seek(SeekFrom::Start(offset))?;

        while let Ok(document) = bson::decode_document(&mut buf) {
            let command: KvCommand = bson::from_bson(bson::Bson::Document(document))?;
            match command {
                KvCommand::Set(key, _) => {
                    self.log.insert(key, self.cursor);
                }
                KvCommand::Rm(key) => {
                    self.log
                        .remove(&key)
                        .ok_or(format_err!("Missing key value!"))?;
                }
            }
            // Update the cursor to the current location.
            self.cursor = buf.seek(SeekFrom::Current(0))?;
        }
        Ok(())
    }
}

fn write_bson_record<T: Serialize, W: Write>(record: T, writer: &mut W) -> Result<()> {
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
