extern crate protium;

use std::collections::BTreeSet;
use std::{env, process};
use protium::{FileStorage, Packable, Protium, Transaction, Transactions, TransactionKey};

fn main() {
    if env::args().len() != 2 {
        println!("usage: file_storage <path>");
        process::exit(1);
    }

    let path = env::args().nth(1).unwrap();
    let storage = FileStorage::<Set>::new(path).unwrap();
    let transactions = Transactions::new().register::<SetAdd>().register::<SetRemove>();
    let mut protium = Protium::new(storage, transactions).unwrap();

    protium.apply(SetAdd(5)).unwrap();
    protium.apply(SetAdd(10)).unwrap();
    protium.apply(SetAdd(15)).unwrap();
    protium.apply(SetRemove(10)).unwrap();
}

#[derive(Default)]
pub struct Set(pub BTreeSet<u8>);

impl Set {
    fn insert(&mut self, value: u8) {
        self.0.insert(value);
    }

    fn remove(&mut self, value: u8) {
        self.0.remove(&value);
    }
}

impl Packable for Set {
    fn pack(&self) -> Result<Vec<u8>, ()> {
        Ok(self.0.iter().cloned().collect())
    }

    fn unpack(data: &[u8]) -> Result<Self, ()> {
        Ok(Set(data.iter().cloned().collect()))
    }
}

pub struct SetAdd(pub u8);

impl Packable for SetAdd {
    fn pack(&self) -> Result<Vec<u8>, ()> {
        Ok(vec![self.0])
    }

    fn unpack(data: &[u8]) -> Result<Self, ()> {
        Ok(SetAdd(data[0]))
    }
}

impl Transaction<Set> for SetAdd {
    fn key() -> TransactionKey {
        1
    }

    fn apply(&self, object: &mut Set) {
        object.insert(self.0);
    }
}

pub struct SetRemove(pub u8);

impl Packable for SetRemove {
    fn pack(&self) -> Result<Vec<u8>, ()> {
        Ok(vec![self.0])
    }

    fn unpack(data: &[u8]) -> Result<Self, ()> {
        Ok(SetRemove(data[0]))
    }
}

impl Transaction<Set> for SetRemove {
    fn key() -> TransactionKey {
        2
    }

    fn apply(&self, object: &mut Set) {
        object.remove(self.0);
    }
}
