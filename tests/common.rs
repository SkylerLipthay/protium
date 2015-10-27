use std::collections::BTreeSet;
use std::marker::PhantomData;

use protium::{
    Error, Packable, PackedObject, PackedTransaction, Storage, Transaction, TransactionKey
};

#[derive(Debug, Default, PartialEq)]
pub struct Object(pub BTreeSet<u8>);

impl Object {
    fn insert(&mut self, value: u8) {
        self.0.insert(value);
    }

    fn remove(&mut self, value: u8) {
        self.0.remove(&value);
    }
}

impl Packable for Object {
    fn pack(&self) -> Result<Vec<u8>, ()> {
        if self.0.contains(&255) {
            return Err(());
        }

        Ok(self.0.iter().cloned().collect())
    }

    // Returns `Err(())` if `data` contains a 255 byte, to test error handling.
    fn unpack(data: &[u8]) -> Result<Self, ()> {
        if data.contains(&255) {
            return Err(());
        }

        Ok(Object(data.iter().cloned().collect()))
    }
}

pub struct TransactionAdd(pub u8);

impl Packable for TransactionAdd {
    // Returns `Err(())` if this is a 255 byte, to test error handling.
    fn pack(&self) -> Result<Vec<u8>, ()> {
        if self.0 == 255 {
            Err(())
        } else {
            Ok(vec![self.0])
        }
    }

    fn unpack(data: &[u8]) -> Result<Self, ()> {
        if data.len() == 1 {
            Ok(TransactionAdd(data[0]))
        } else {
            Err(())
        }
    }
}

impl Transaction<Object> for TransactionAdd {
    fn key() -> TransactionKey {
        1
    }

    fn apply(&self, object: &mut Object) {
        object.insert(self.0);
    }
}

pub struct TransactionRemove(pub u8);

impl Packable for TransactionRemove {
    fn pack(&self) -> Result<Vec<u8>, ()> {
        Ok(vec![self.0])
    }

    fn unpack(data: &[u8]) -> Result<Self, ()> {
        if data.len() == 1 {
            Ok(TransactionRemove(data[0]))
        } else {
            Err(())
        }
    }
}

impl Transaction<Object> for TransactionRemove {
    fn key() -> TransactionKey {
        2
    }

    fn apply(&self, object: &mut Object) {
        object.remove(self.0);
    }
}

#[derive(Debug, PartialEq)]
pub struct SimpleStorage<T: Packable> {
    object: Option<Vec<u8>>,
    transactions: Vec<(TransactionKey, Vec<u8>)>,
    packable: PhantomData<T>,
}

impl<T: Packable> SimpleStorage<T> {
    pub fn new(object: Option<Vec<u8>>, transactions: Vec<(TransactionKey, Vec<u8>)>)
        -> SimpleStorage<T>
    {
        SimpleStorage {
            object: object,
            transactions: transactions,
            packable: PhantomData,
        }
    }
}

impl<T: Packable> Storage<T> for SimpleStorage<T> {
    fn load(&mut self) -> Result<Option<(PackedObject, Vec<PackedTransaction>)>, Error> {
        match self.object {
            Some(ref object) => {
                let object_data = PackedObject(object.clone());
                let tx_data = self.transactions.iter().cloned()
                    .map(|data| PackedTransaction(data.0, data.1))
                    .collect();
                Ok(Some((object_data, tx_data)))
            },
            None => Ok(None),
        }
    }

    fn store_object(&mut self, object: &T) -> Result<(), Error> {
        self.object = match Packable::pack(object) {
            Ok(data) => Some(data),
            Err(()) => return Err(Error::ObjectPack),
        };

        Ok(())
    }

    fn store_data<R: Transaction<T>>(&mut self, object: &T, transaction: &R)
        -> Result<(), Error>
    {
        if self.object.is_none() {
            try!(self.store_object(object));
        } else {
            match Packable::pack(transaction) {
                Ok(data) => self.transactions.push((R::key(), data)),
                Err(()) => return Err(Error::TransactionPack),
            }
        }

        Ok(())
    }
}
