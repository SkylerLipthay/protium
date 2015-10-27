use common::{Object, TransactionAdd};
use protium::{Error, FileStorage, PackedObject, PackedTransaction, Storage, Transaction};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use tempdir::TempDir;

#[test]
fn loads_pristine_file() {
    let result = write_and_load(&[
        02u8, 00, 00, 00, 03, 04, 05, 00, 00, 00, 01, 00, 00, 00, 05, 05, 00, 00, 00, 02, 00, 00,
        00, 04
    ], false).unwrap().unwrap();
    assert_eq!(result.0, PackedObject(vec![3, 4]));
    assert_eq!(result.1, vec![PackedTransaction(1, vec![5]), PackedTransaction(2, vec![4])]);
}

#[test]
fn ignores_corrupt_object() {
    // Truncated chunk length:
    assert_eq!(write_and_load(&[02u8, 00, 00], false).unwrap(), None);
    // Mismatched chunk length:
    assert_eq!(write_and_load(&[02u8, 00, 00, 00, 03], false).unwrap(), None);
}

#[test]
fn ignores_corrupt_transaction() {
    // Truncated chunk length:
    let result = write_and_load(&[
        02u8, 00, 00, 00, 03, 04, 05, 00, 00, 00, 01, 00, 00, 00, 05, 03, 00, 00, 00, 02, 00, 00
    ], false).unwrap().unwrap();
    assert_eq!(result.1, vec![PackedTransaction(1, vec![5])]);

    // Mismatched chunk length:
    let result = write_and_load(&[
        02u8, 00, 00, 00, 03, 04, 05, 00, 00, 00, 01, 00, 00, 00, 05, 05, 00, 00, 00, 02, 00, 00,
        00
    ], false).unwrap().unwrap();
    assert_eq!(result.1, vec![PackedTransaction(1, vec![5])]);
}

#[test]
fn renames_temp_file_on_load() {
    let result = write_and_load(&[02u8, 00, 00, 00, 03, 04], true).unwrap().unwrap();
    assert_eq!(result.0, PackedObject(vec![3, 4]));
    assert_eq!(result.1, vec![]);
}

#[test]
fn store_object() {
    let temp_dir = temp_dir();
    let mut storage = file_storage(&temp_dir);
    storage.store_object(&Object(vec![1, 2].iter().cloned().collect())).unwrap();
    let mut result = vec![];
    File::open(storage.path()).unwrap().read_to_end(&mut result).unwrap();
    assert_eq!(result, vec![02u8, 00, 00, 00, 01, 02]);
}

#[test]
fn store_data() {
    let temp_dir = temp_dir();
    let mut storage = file_storage(&temp_dir);
    let mut object = Object(vec![1, 2].iter().cloned().collect());
    storage.store_object(&object).unwrap();
    let transaction = TransactionAdd(3);
    transaction.apply(&mut object);
    storage.store_data(&object, &transaction).unwrap();
    let mut result = vec![];
    File::open(storage.path()).unwrap().read_to_end(&mut result).unwrap();
    assert_eq!(result, vec![02u8, 00, 00, 00, 01, 02, 05, 00, 00, 00, 01, 00, 00, 00, 03]);
}

#[test]
fn compact_many_transactions() {
    let temp_dir = temp_dir();
    let mut storage = file_storage(&temp_dir);
    let mut object = Object(vec![].iter().cloned().collect());
    storage.store_object(&object).unwrap();
    for i in 0u8..18 {
        let transaction = TransactionAdd(i);
        transaction.apply(&mut object);
        storage.store_data(&object, &transaction).unwrap();
    }
    let mut result = vec![];
    File::open(storage.path()).unwrap().read_to_end(&mut result).unwrap();
    assert_eq!(result, vec![
        17u8, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 5, 0, 0, 0, 1, 0,
        0, 0, 17
    ]);
}

fn write_and_load(data: &[u8], temp: bool)
    -> Result<Option<(PackedObject, Vec<PackedTransaction>)>, Error>
{
    let temp_dir = temp_dir();
    let path = temp_dir.path().join(match temp {
        false => "test.db",
        true => "test.db~",
    });
    write_bytes(path, data);

    file_storage(&temp_dir).load()
}

fn file_storage(temp_dir: &TempDir) -> FileStorage<Object> {
    FileStorage::<Object>::new(temp_dir.path().join("test.db")).unwrap()
}

fn temp_dir() -> TempDir {
    TempDir::new("protium").unwrap()
}

fn write_bytes(path: PathBuf, data: &[u8]) {
    OpenOptions::new().write(true).create(true).open(path).unwrap().write_all(data).unwrap();
}
