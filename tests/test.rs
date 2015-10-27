extern crate protium;
extern crate tempdir;

mod common;
mod file_storage;

use common::{Object, SimpleStorage, TransactionAdd, TransactionRemove};
use protium::{Protium, Storage, Transactions};

#[test]
fn empty_storage_is_default() {
    let protium = Protium::new(empty_storage(), transactions()).unwrap();
    assert_eq!(*protium.object(), Object::default());
}

#[test]
fn add_to_empty_storage() {
    let mut protium = Protium::new(empty_storage(), transactions()).unwrap();
    protium.apply(TransactionAdd(5)).unwrap();
    protium.apply(TransactionAdd(10)).unwrap();
    protium.apply(TransactionAdd(15)).unwrap();
    protium.apply(TransactionRemove(10)).unwrap();
    assert_eq!(*protium.object(), Object(vec![5, 15].iter().cloned().collect()));
    let storage_transactions = vec![(1, vec![5]), (1, vec![10]), (1, vec![15]), (2, vec![10])];
    assert_eq!(*protium.storage(), SimpleStorage::new(Some(vec![]), storage_transactions));
}

#[test]
fn load_from_storage() {
    let storage_transactions = vec![(1, vec![10]), (1, vec![15]), (2, vec![10])];
    let storage = SimpleStorage::new(Some(vec![5]), storage_transactions);
    let protium = Protium::new(storage, transactions()).unwrap();
    assert_eq!(*protium.object(), Object(vec![5, 15].iter().cloned().collect()));
}

#[test]
fn unpacking_unregistered_transaction_keys() {
    let storage_transactions = vec![(1, vec![10]), (1000, vec![15])];
    let storage = SimpleStorage::new(Some(vec![5]), storage_transactions);
    match Protium::new(storage, transactions()) {
        Err(protium::Error::TransactionUnregistered) => (),
        _ => unreachable!(),
    }
}

#[test]
fn packing_invalid_object() {
    match empty_storage().store_object(&Object(vec![255].iter().cloned().collect())) {
        Err(protium::Error::ObjectPack) => (),
        _ => unreachable!(),
    }
}

#[test]
fn unpacking_invalid_object() {
    let storage = SimpleStorage::new(Some(vec![255]), vec![]);
    match Protium::new(storage, transactions()) {
        Err(protium::Error::ObjectUnpack) => (),
        _ => unreachable!(),
    }
}

#[test]
fn packing_invalid_transaction() {
    let object = Object(vec![1].iter().cloned().collect());
    let transaction = TransactionAdd(255);
    let mut storage = empty_storage();
    storage.store_object(&object).unwrap();
    match storage.store_data(&object, &transaction) {
        Err(protium::Error::TransactionPack) => (),
        _ => unreachable!(),
    }
}

#[test]
fn unpacking_invalid_transaction() {
    let storage = SimpleStorage::new(Some(vec![1]), vec![(1, vec![1, 2])]);
    match Protium::new(storage, transactions()) {
        Err(protium::Error::TransactionUnpack) => (),
        _ => unreachable!(),
    }
}

fn empty_storage() -> SimpleStorage<Object> {
    SimpleStorage::new(None, vec![])
}

fn transactions() -> Transactions<Object> {
    Transactions::new().register::<TransactionAdd>().register::<TransactionRemove>()
}
