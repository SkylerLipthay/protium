extern crate byteorder;

mod error;
mod file_storage;

pub use error::Error;
pub use file_storage::FileStorage;

use std::collections::BTreeMap;
use std::default::Default;
use std::marker::PhantomData;

/// A type that represents a unique key for each corresponding `Transaction` of a `Packable`
/// object.
pub type TransactionKey = u32;

/// A trait that allows its implementer to be packed into and unpacked from a chunk of bytes.
pub trait Packable: Sized {
    /// Converts the object to an encoded chunk of bytes that can later be unpacked.
    ///
    /// Returns `Err(())` if the type could not successfully be packed.
    fn pack(&self) -> Result<Vec<u8>, ()>;

    /// Converts an encoded chunk of bytes into an object.
    ///
    /// Returns `Err(())` if the type could not successfully be unpacked.
    fn unpack(data: &[u8]) -> Result<Self, ()>;
}

/// A trait that represents a single atomic change to be made to a `Packable` object (`T`).
pub trait Transaction<T: Packable>: Packable {
    /// The unique key that represents this transaction that is used by `Storage` implementations
    /// to pack and unpack transactions of this type.
    fn key() -> TransactionKey;

    /// Modifies a given `T` in some way.
    fn apply(&self, &mut T);
}

/// The prominent structure that exposes a packable object linked to durable storange.
pub struct Protium<T: Packable + Default, S: Storage<T>> {
    object: T,
    storage: S,
    transactions: Transactions<T>,
}

impl<T: Packable + Default, S: Storage<T>> Protium<T, S> {
    /// Initialize a durably stored object backed by `storage`.
    ///
    /// All possible `Transaction` types to be supported by this object are passed in as
    /// `transactions`. If the storage is uninitialized, `T::default()` is stored and used.
    ///
    /// Returns `Err` if an IO error occurred during initializing the object from `storage`.
    pub fn new(mut storage: S, transactions: Transactions<T>) -> Result<Protium<T, S>, Error> {
        let object = match try!(storage.load()) {
            Some((object, tx)) => try!(transactions.unpack(object, tx)),
            None => {
                let result = T::default();
                try!(storage.store_object(&result));
                result
            },
        };

        Ok(Protium { object: object, storage: storage, transactions: transactions })
    }

    /// Apply `transaction` to the internal object, storing the data durably.
    ///
    /// # Panics
    ///
    /// Panics if `R` is not a registered transaction type.
    pub fn apply<R: Transaction<T>>(&mut self, transaction: R) -> Result<(), Error> {
        if !self.transactions.is_transaction_registered::<R>() {
            panic!("Unregistered transaction type {}", R::key());
        }

        transaction.apply(&mut self.object);
        try!(self.storage.store_data(&self.object, &transaction));
        Ok(())
    }

    /// Returns an immutable reference to the internal object.
    pub fn object(&self) -> &T {
        &self.object
    }

    /// Returns an immutable reference to the internal storage.
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Returns an immutable reference to the registered transactions.
    pub fn transactions(&self) -> &Transactions<T> {
        &self.transactions
    }
}

/// A collection of acceptable `Transaction` types corresponding to a packable type `T`.
pub struct Transactions<T: Packable> {
    /// A map between transaction keys and closures that apply a corresponding packed transaction
    /// to an object of type `T`.
    transactions: BTreeMap<TransactionKey, Box<Fn(&mut T, &[u8]) -> Result<(), Error>>>,
    marker: PhantomData<T>,
}

impl<T: Packable> Transactions<T> {
    /// Initialize with an empty set of transaction types.
    pub fn new() -> Transactions<T> {
        Transactions { transactions: BTreeMap::new(), marker: PhantomData }
    }

    /// Register a type that implements `Transaction`.
    ///
    /// # Panics
    ///
    /// Panics if a type with the same `Transaction::key()` has already been registered.
    pub fn register<R: Transaction<T>>(mut self) -> Transactions<T> {
        if self.transactions.contains_key(&R::key()) {
            panic!("Duplicated transaction key {}", R::key());
        }

        self.transactions.insert(R::key(), Box::new(|object: &mut T, data: &[u8]| {
            apply_transaction::<_, R>(object, data)
        }));

        self
    }

    /// Returns `true` if the provided transaction type has been registered, `false` otherwise.
    pub fn is_transaction_registered<R: Transaction<T>>(&self) -> bool {
        // TODO: Should this be verified by `TypeId` instead?
        self.transactions.contains_key(&R::key())
    }

    /// Unpacks an object and a list of transactions, applies the list of transactions to the
    /// object, then returns the updated object.
    ///
    /// Returns `Err` if unpacking the object or the transactions fails, or if any of the packed
    /// transactions types were unregistered.
    fn unpack(&self, object: PackedObject, transactions: Vec<PackedTransaction>)
        -> Result<T, Error>
    {
        let mut result = try!(T::unpack(&object.0).map_err(|_| Error::ObjectUnpack));

        for transaction in transactions {
            let key = transaction.0;
            let data = transaction.1;

            if !self.transactions.contains_key(&key) {
                return Err(Error::TransactionUnregistered);
            }

            try!(self.transactions.get(&key).unwrap()(&mut result, &data));
        }

        Ok(result)
    }
}

/// A representation of a packed object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedObject(pub Vec<u8>);

/// A representation of packed transaction data and the appropriate transaction type key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedTransaction(pub TransactionKey, pub Vec<u8>);

pub trait Storage<T: Packable> {
    /// Fetches the packed object and its transactions from the implementation's storage.
    ///
    /// Returns `Ok(None)` if the storage has no object to be retrieved.
    ///
    /// Note that the responsibility of validation of the storage (atomicity) lies with the
    /// implementation. For example, if incomplete or corrupt packed transactions are fetched from
    /// storage, those data are not to be returned by this method.
    fn load(&mut self) -> Result<Option<(PackedObject, Vec<PackedTransaction>)>, Error>;

    /// Durably stores the packable object. This can be called at any point by `Protium`, e.g. when
    /// the client wants to record a new or default object.
    fn store_object(&mut self, object: &T) -> Result<(), Error>;

    /// Durably stores the packable object and/or its newly applied transaction. This is called
    /// called by `Protium::apply()`, whenever a transaction is applied.
    ///
    /// The implementation may choose to ignore `object`, e.g. if it is unnecessary to yet compact
    /// the object's stored transaction log. The implementation may also choose to ignore
    /// `transactions`, e.g. if the storage is capable of storing objects without risk of hardware
    /// failure, so storing transactions is unnecessary.
    fn store_data<R: Transaction<T>>(&mut self, object: &T, transaction: &R)
        -> Result<(), Error>;
}

#[inline]
fn apply_transaction<T: Packable, R: Transaction<T>>(object: &mut T, data: &[u8])
    -> Result<(), Error>
{
    try!(R::unpack(data).map_err(|_| Error::TransactionUnpack)).apply(object);
    Ok(())
}
