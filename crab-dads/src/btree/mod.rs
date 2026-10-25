mod reader;
mod writer;

pub use reader::*;
pub use writer::*;

#[cfg(test)]
#[allow(dead_code)]
mod test {

    use crate::page::{LayoutU64U64, LayoutU64Var, PageMapMut};
    use crab_dads_traits::{
        test_alloc::{BasicDbReader, BasicDbWriter},
        *,
    };
    use test_alloc::{BasicDbRead, BasicDbWrite};

    use super::*;

    fn new_db() -> (BasicDbReader, BasicDbWriter) {
        let (reader, mut writer) = test_alloc::new_db();
        let txn = writer.write();
        let page = txn.allocate(1).unwrap();
        PageMapMut::<LayoutU64Var>::new(page.0, 1);
        let root = page.1;
        txn.commit(&[root]).unwrap();
        (reader, writer)
    }

    fn cycle_writer(writer: &mut BasicDbWriter) {
        let txn = writer.write();
        let data = txn.root_data().to_vec();
        txn.commit(&data).unwrap();
    }

    struct TestWriteTxn<'a> {
        root: u64,
        txn: BasicDbWrite<'a>,
    }

    impl<'a> TestWriteTxn<'a> {
        fn txn(writer: &'a mut BasicDbWriter) -> Self {
            let txn = writer.write();
            let old_root = txn.root_data()[0];
            // Load the root at least once, then release it back.
            let (_, root) = unsafe {
                BTreeWrite::<LayoutU64U64, LayoutU64Var, BasicDbWrite>::load(&txn, old_root)
                    .unwrap()
            };
            let root = root.unwrap();
            Self { root, txn }
        }

        fn load_mut(&mut self) -> BTreeWrite<'_, LayoutU64U64, LayoutU64Var, BasicDbWrite<'_>> {
            unsafe { BTreeWrite::load(&self.txn, self.root).unwrap().0 }
        }

        fn load(&self) -> BTreeRead<'_, LayoutU64U64, LayoutU64Var, BasicDbWrite<'_>> {
            unsafe { BTreeRead::load(&self.txn, self.root).unwrap() }
        }

        fn commit(self) {
            self.txn.commit(&[self.root]).unwrap();
        }
    }

    struct TestReadTxn<'a> {
        root: u64,
        txn: BasicDbRead<'a>,
    }

    impl<'a> TestReadTxn<'a> {
        fn txn(reader: &'a BasicDbReader) -> Self {
            let txn = reader.read();
            let root = txn.root_data()[0];
            Self { root, txn }
        }

        fn load(&self) -> BTreeRead<'_, LayoutU64U64, LayoutU64Var, BasicDbRead<'_>> {
            unsafe { BTreeRead::load(&self.txn, self.root).unwrap() }
        }
    }

    #[test]
    fn debug_allocator() {
        let (reader, mut writer) = test_alloc::new_db();
        let txn = writer.write();
        let (p, p_num) = txn.allocate(1).unwrap();
        let (p, _) = p.split_at_mut(4);
        p.copy_from_slice(&[5, 6, 7, 8]);
        let root = txn.root_data().to_vec();
        txn.commit(&root).unwrap();
        let txn = reader.read();
        let p = unsafe { txn.load(p_num).unwrap() };
        let (p, _) = p.split_at(4);
        dbg!(p);
        assert_eq!(p, &[5, 6, 7, 8]);
    }

    #[test]
    fn sequential_insert_forward() {
        let (reader, mut writer) = new_db();
        let mut txn = TestWriteTxn::txn(&mut writer);
        let mut tree = txn.load_mut();
        let i_len = 100000;

        // Insertion
        for i in 0..i_len {
            match tree.entry(&i).unwrap() {
                Entry::Occupied(_) => panic!("All entries should be empty right now"),
                Entry::Vacant(v) => {
                    v.insert(i.to_le_bytes().as_slice()).unwrap();
                }
            }
        }
        txn.commit();
        let info = writer.stats();
        println!("Writing complete, {} pages used", info.used_pages);

        // Post-insert check
        let txn = TestReadTxn::txn(&reader);
        let tree = txn.load();
        for i in 0..i_len {
            let val = tree.get(&i).unwrap().unwrap();
            assert_eq!(val, i.to_le_bytes().as_slice());
        }
        let mut iter = tree.range(..).unwrap();
        for i in 0..i_len {
            let (k, v) = iter
                .next()
                .expect("should've gotten a pair")
                .expect("Didn't expect an error");
            assert_eq!(*k, i);
            assert_eq!(v, i.to_le_bytes().as_slice());
        }
        let mut iter = tree.range(1000..50000).unwrap();
        for i in 1000..50000 {
            let (k, v) = iter
                .next()
                .expect("should've gotten a pair")
                .expect("Didn't expect an error");
            assert_eq!(*k, i);
            assert_eq!(v, i.to_le_bytes().as_slice());
        }
        assert!(
            iter.next().is_none(),
            "forward iterator should have ended exactly when we did"
        );
        assert!(
            iter.next_back().is_none(),
            "backward iterator should have ended exactly when we did"
        );
        println!("Reading complete");

        // Deletion
        let mut txn = TestWriteTxn::txn(&mut writer);
        let mut tree = txn.load_mut();
        for i in 0..i_len {
            match tree.entry(&i).unwrap() {
                Entry::Vacant(_) => panic!("All entries should be occupied"),
                Entry::Occupied(o) => {
                    o.delete()
                        .unwrap_or_else(|e| panic!("Entry for {i} should be deletable: {}", e));
                }
            }
        }
        txn.commit();
        cycle_writer(&mut writer);
        let info = writer.stats();
        println!("Deletion complete, {} pages used", info.used_pages);

        // Post-delete check
        let txn = TestReadTxn::txn(&reader);
        let tree = txn.load();
        for i in 0..i_len {
            assert!(tree.get(&i).unwrap().is_none());
        }
    }

    #[test]
    fn sequential_insert_rev() {
        let (reader, mut writer) = new_db();
        let mut txn = TestWriteTxn::txn(&mut writer);
        let mut tree = txn.load_mut();
        let i_len = 100000;

        // Insertion
        for i in (0..i_len).rev() {
            match tree.entry(&i).unwrap() {
                Entry::Occupied(_) => panic!("All entries should be empty right now"),
                Entry::Vacant(v) => {
                    v.insert(i.to_le_bytes().as_slice()).unwrap();
                }
            }
        }
        txn.commit();
        let info = writer.stats();
        println!("Writing complete, {} pages used", info.used_pages);

        // Post-insert check
        let txn = TestReadTxn::txn(&reader);
        let tree = txn.load();
        for i in (0..i_len).rev() {
            let Some(val) = tree.get(&i).expect("no error") else {
                panic!("expected to get a value for {}", i);
            };
            assert_eq!(val, i.to_le_bytes().as_slice());
        }
        let mut iter = tree.range(..).unwrap();
        for i in (0..i_len).rev() {
            let (k, v) = match iter.next_back() {
                Some(Ok(p)) => p,
                Some(Err(e)) => panic!("Didn't expect error for item {i}: {e}"),
                None => panic!("Should've gotten a pair for item {i}"),
            };
            assert_eq!(*k, i);
            assert_eq!(v, i.to_le_bytes().as_slice());
        }
        let mut iter = tree.range(1000..50000).unwrap();
        for i in (1000..50000).rev() {
            let (k, v) = match iter.next_back() {
                Some(Ok(p)) => p,
                Some(Err(e)) => panic!("Didn't expect error for item {i}: {e}"),
                None => panic!("Should've gotten a pair for item {i}"),
            };
            assert_eq!(*k, i);
            assert_eq!(v, i.to_le_bytes().as_slice());
        }
        assert!(
            iter.next_back().is_none(),
            "backward iterator should have ended exactly when we did"
        );
        assert!(
            iter.next().is_none(),
            "forward iterator should have ended exactly when we did"
        );
        println!("Reading complete");

        // Deletion
        let mut txn = TestWriteTxn::txn(&mut writer);
        let mut tree = txn.load_mut();
        for i in (0..i_len).rev() {
            match tree.entry(&i).unwrap() {
                Entry::Vacant(_) => panic!("All entries should be occupied, but {i} is unoccupied"),
                Entry::Occupied(o) => {
                    o.delete()
                        .unwrap_or_else(|e| panic!("Entry for {i} should be deletable: {}", e));
                }
            }
        }
        txn.commit();
        cycle_writer(&mut writer);
        let info = writer.stats();
        println!("Deletion complete, {} pages used", info.used_pages);

        // Post-delete check
        let txn = TestReadTxn::txn(&reader);
        let tree = txn.load();
        for i in (0..i_len).rev() {
            assert!(tree.get(&i).unwrap().is_none());
        }
    }

    #[test]
    fn sequential_var_insert_forward() {
        let (reader, mut writer) = new_db();
        let mut txn = TestWriteTxn::txn(&mut writer);
        let mut tree = txn.load_mut();
        let i_len = 100000;

        fn idx_to_data(i: u64) -> &'static [u8] {
            let len = ((i + (i >> 5)) % 19) as usize;
            let data: &'static [u8] = b"sphinxofblackquartzjudgemyvow";
            &data[..len]
        }

        // Insertion
        for i in 0..i_len {
            match tree.entry(&i).unwrap() {
                Entry::Occupied(_) => panic!("All entries should be empty right now"),
                Entry::Vacant(v) => {
                    v.insert(idx_to_data(i)).unwrap();
                }
            }
        }
        txn.commit();
        let info = writer.stats();
        println!("Writing complete, {} pages used", info.used_pages);

        // Post-insert check
        let txn = TestReadTxn::txn(&reader);
        let tree = txn.load();
        for i in 0..i_len {
            let val = tree.get(&i).unwrap().unwrap();
            assert_eq!(val, idx_to_data(i));
        }
        let mut iter = tree.range(..).unwrap();
        for i in 0..i_len {
            let (k, v) = iter
                .next()
                .expect("should've gotten a pair")
                .expect("Didn't expect an error");
            assert_eq!(*k, i);
            assert_eq!(v, idx_to_data(i));
        }
        let mut iter = tree.range(1000..50000).unwrap();
        for i in 1000..50000 {
            let (k, v) = iter
                .next()
                .expect("should've gotten a pair")
                .expect("Didn't expect an error");
            assert_eq!(*k, i);
            assert_eq!(v, idx_to_data(i));
        }
        assert!(
            iter.next().is_none(),
            "forward iterator should have ended exactly when we did"
        );
        assert!(
            iter.next_back().is_none(),
            "backward iterator should have ended exactly when we did"
        );
        println!("Reading complete");

        // Deletion
        let mut txn = TestWriteTxn::txn(&mut writer);
        let mut tree = txn.load_mut();
        for i in 0..i_len {
            match tree.entry(&i).unwrap() {
                Entry::Vacant(_) => panic!("All entries should be occupied"),
                Entry::Occupied(o) => {
                    o.delete()
                        .unwrap_or_else(|e| panic!("Entry for {i} should be deletable: {}", e));
                }
            }
        }
        txn.commit();
        cycle_writer(&mut writer);
        let info = writer.stats();
        println!("Deletion complete, {} pages used", info.used_pages);

        // Post-delete check
        let txn = TestReadTxn::txn(&reader);
        let tree = txn.load();
        for i in 0..i_len {
            assert!(tree.get(&i).unwrap().is_none());
        }
    }

    #[test]
    fn sequential_var_insert_rev() {
        let (reader, mut writer) = new_db();
        let mut txn = TestWriteTxn::txn(&mut writer);
        let mut tree = txn.load_mut();
        let i_len = 100000;

        // Insertion
        for i in (0..i_len).rev() {
            match tree.entry(&i).unwrap() {
                Entry::Occupied(_) => panic!("All entries should be empty right now"),
                Entry::Vacant(v) => {
                    v.insert(i.to_le_bytes().as_slice()).unwrap();
                }
            }
        }
        txn.commit();
        let info = writer.stats();
        println!("Writing complete, {} pages used", info.used_pages);

        // Post-insert check
        let txn = TestReadTxn::txn(&reader);
        let tree = txn.load();
        for i in (0..i_len).rev() {
            let Some(val) = tree.get(&i).expect("no error") else {
                panic!("expected to get a value for {}", i);
            };
            assert_eq!(val, i.to_le_bytes().as_slice());
        }
        let mut iter = tree.range(..).unwrap();
        for i in (0..i_len).rev() {
            let (k, v) = match iter.next_back() {
                Some(Ok(p)) => p,
                Some(Err(e)) => panic!("Didn't expect error for item {i}: {e}"),
                None => panic!("Should've gotten a pair for item {i}"),
            };
            assert_eq!(*k, i);
            assert_eq!(v, i.to_le_bytes().as_slice());
        }
        let mut iter = tree.range(1000..50000).unwrap();
        for i in (1000..50000).rev() {
            let (k, v) = match iter.next_back() {
                Some(Ok(p)) => p,
                Some(Err(e)) => panic!("Didn't expect error for item {i}: {e}"),
                None => panic!("Should've gotten a pair for item {i}"),
            };
            assert_eq!(*k, i);
            assert_eq!(v, i.to_le_bytes().as_slice());
        }
        assert!(
            iter.next_back().is_none(),
            "backward iterator should have ended exactly when we did"
        );
        assert!(
            iter.next().is_none(),
            "forward iterator should have ended exactly when we did"
        );
        println!("Reading complete");

        // Deletion
        let mut txn = TestWriteTxn::txn(&mut writer);
        let mut tree = txn.load_mut();
        for i in (0..i_len).rev() {
            match tree.entry(&i).unwrap() {
                Entry::Vacant(_) => panic!("All entries should be occupied, but {i} is unoccupied"),
                Entry::Occupied(o) => {
                    o.delete()
                        .unwrap_or_else(|e| panic!("Entry for {i} should be deletable: {}", e));
                }
            }
        }
        txn.commit();
        cycle_writer(&mut writer);
        let info = writer.stats();
        println!("Deletion complete, {} pages used", info.used_pages);

        // Post-delete check
        let txn = TestReadTxn::txn(&reader);
        let tree = txn.load();
        for i in (0..i_len).rev() {
            assert!(tree.get(&i).unwrap().is_none());
        }
    }
}
