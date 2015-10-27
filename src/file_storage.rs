use super::{Packable, PackedObject, PackedTransaction, Storage, Transaction};
use error::Error;

use byteorder::{self, ByteOrder, LittleEndian, ReadBytesExt};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

/// A storage implementation that uses the file system to atomically and durably store a packable
/// object.
///
/// The storage will be compacted after every 16 transactions, so the storage file does not grow
/// too large. TODO: Control over compaction.
pub struct FileStorage<T: Packable> {
    base_path: PathBuf,
    temp_path: PathBuf,
    file: Option<File>,
    needs_initial_compact: bool,
    transaction_count: u64,
    marker: PhantomData<T>,
}

impl<T: Packable> FileStorage<T> {
    /// Creates a new storage object linked to the file at `path`.
    ///
    /// If the file at `path` does not exist, it will be created once `read_object` or `read_data`
    /// is called.
    ///
    /// `FileStorage` requires that a special temporary file be writable as well. This special path
    /// will be `path` with a tilde ("~") appended. The temporary file is used to maintain a
    /// durable copy of the stored object during the time that the storage is being compacted.
    ///
    /// Ensure that `path` is in a directory of which the user has write access.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<FileStorage<T>, Error> {
        let base_path = PathBuf::from(path.as_ref());
        let temp_path = PathBuf::from(format!("{}~", path.as_ref().display()));

        let mut result = FileStorage {
            base_path: base_path,
            temp_path: temp_path,
            file: None,
            needs_initial_compact: true,
            transaction_count: 0,
            marker: PhantomData,
        };

        // Hackish. `metadata` returns `Err` if the path does not exist. Change once `PathExt`
        // stabilizes.
        if fs::metadata(&result.base_path).is_err() {
            if fs::metadata(&result.temp_path).is_err() {
                return Ok(result);
            } else {
                try!(fs::rename(&result.temp_path, &result.base_path));
            }
        }

        let file = OpenOptions::new().read(true).write(true).append(true).open(&result.base_path);
        result.file = Some(try!(file));
        Ok(result)
    }

    /// Returns a reference of the path used to serve this storage.
    pub fn path(&self) -> &Path {
        &self.base_path
    }

    fn read_chunk(&mut self) -> Result<Option<Vec<u8>>, Error> {
        let mut file = match self.file {
            Some(ref file) => file,
            None => return Ok(None),
        };

        let length = match file.read_u32::<LittleEndian>() {
            Ok(length) => length,
            Err(byteorder::Error::UnexpectedEOF) => return Ok(None),
            Err(byteorder::Error::Io(err)) => return Err(err.into()),
        } as usize;

        let mut buf = Vec::with_capacity(length);
        let length_read = try!(file.take(length as u64).read_to_end(&mut buf));

        if length == length_read {
            Ok(Some(buf))
        } else {
            Ok(None)
        }
    }

    fn read_object(&mut self) -> Result<Option<PackedObject>, Error> {
        Ok(try!(self.read_chunk()).map(|data| PackedObject(data)))
    }

    fn read_transaction(&mut self) -> Result<Option<PackedTransaction>, Error> {
        let data = match try!(self.read_chunk()) {
            Some(data) => data,
            None => return Ok(None),
        };

        if data.len() < 4 {
            return Ok(None);
        }

        let mut code = data;
        let data = code.split_off(4);
        let code = LittleEndian::read_u32(&code);
        Ok(Some(PackedTransaction(code, data)))
    }
}

impl<T: Packable> Storage<T> for FileStorage<T> {
    fn load(&mut self) -> Result<Option<(PackedObject, Vec<PackedTransaction>)>, Error> {
        let object = match try!(self.read_object()) {
            Some(object) => object,
            None => return Ok(None),
        };

        let mut transactions = vec![];
        self.transaction_count = 0;

        loop {
            match try!(self.read_transaction()) {
                Some(transaction) => {
                    transactions.push(transaction);
                    self.transaction_count += 1;
                },
                None => {
                    return Ok(Some((object, transactions)));
                },
            }
        }
    }

    fn store_object(&mut self, object: &T) -> Result<(), Error> {
        let packed = match object.pack() {
            Ok(packed) => packed,
            Err(()) => return Err(Error::ObjectPack),
        };

        {
            let mut temp = try!(OpenOptions::new().write(true).create(true).open(&self.temp_path));
            let mut buf = [0; 4];
            LittleEndian::write_u32(&mut buf, packed.len() as u32);
            try!(temp.write_all(&buf));
            try!(temp.write_all(&packed));
            try!(temp.flush());
            try!(temp.sync_data());
        }

        self.file.take();

        // Hackish, again.
        if fs::metadata(&self.base_path).is_ok() {
            try!(fs::remove_file(&self.base_path));
        }

        try!(fs::rename(&self.temp_path, &self.base_path));

        self.transaction_count = 0;
        self.needs_initial_compact = false;
        let file = OpenOptions::new().read(true).write(true).append(true).open(&self.base_path);
        self.file = Some(try!(file));

        Ok(())
    }

    fn store_data<R: Transaction<T>>(&mut self, object: &T, transaction: &R)
        -> Result<(), Error>
    {
        if self.file.is_none() || self.needs_initial_compact || self.transaction_count >= 16 {
            return self.store_object(object);
        }

        let packed = match transaction.pack() {
            Ok(packed) => packed,
            Err(()) => return Err(Error::TransactionPack),
        };

        let mut file = self.file.as_mut().unwrap();
        let mut buf = [0; 4];
        LittleEndian::write_u32(&mut buf, (packed.len() + 4) as u32);
        try!(file.write_all(&buf));
        LittleEndian::write_u32(&mut buf, R::key());
        try!(file.write_all(&buf));
        try!(file.write_all(&packed));
        try!(file.flush());
        try!(file.sync_data());
        self.transaction_count += 1;
        Ok(())
    }
}
