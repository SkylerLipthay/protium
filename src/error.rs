use std::io::Error as IoError;
use std::error::Error as StdError;
use std::fmt::Error as FmtError;
use std::fmt::{Display, Formatter};

/// The possible errors that can occur when interacting with storage.
#[derive(Debug)]
pub enum Error {
    /// The object failed to be packed for storage
    ObjectPack,
    /// The object failed to be unpacked from storage
    ObjectUnpack,
    /// The transaction failed to be packed for storage
    TransactionPack,
    /// The transaction failed to be unpacked from storage.
    TransactionUnpack,
    /// The packed transaction's key is invalid.
    TransactionUnregistered,
    /// A generic IO error.
    Io(IoError),
}

impl StdError for Error {
    fn description(&self) -> &str {
        match *self {
            Error::ObjectPack => "The object failed to be packed for storage",
            Error::ObjectUnpack => "The object failed to be unpacked from storage",
            Error::TransactionPack => "The transaction failed to be packed for storage",
            Error::TransactionUnpack => "The transaction failed to be unpacked from storage",
            Error::TransactionUnregistered => "The packed transaction's key is invalid",
            Error::Io(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&StdError> {
        match *self {
            Error::Io(ref err) => err.cause(),
            _ => None,
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        match *self {
            Error::Io(ref err) => Display::fmt(err, f),
            _ => self.description().fmt(f),
        }
    }
}

impl From<IoError> for Error {
    fn from(err: IoError) -> Error {
        Error::Io(err)
    }
}
