mod reader;
mod writer;

pub use reader::*;
pub use writer::*;

#[cfg(test)]
#[allow(dead_code)]
mod test {

    use crate::page::{LayoutU64U64, LayoutU64Var, PageMapMut};
    use crab_dads_traits::*;
    use test_alloc::{BasicDbRead, BasicDbWrite};

    use super::*;

    fn new_db() -> (BasicDbRead, BasicDbWrite) {
        test_alloc::new_db(|page| {
            PageMapMut::<LayoutU64Var>::new(page, 1);
        })
    }

    fn open_tree_mut(
        writer: &BasicDbWrite,
    ) -> BTreeWrite<'_, LayoutU64U64, LayoutU64Var, BasicDbWrite> {
        let root = writer.root();
        let (tree, new_root) = unsafe { BTreeWrite::load(writer, root).unwrap() };
        if let Some(new_root) = new_root {
            writer.update_root(new_root);
        }
        tree
    }

    fn open_tree(
        reader: &BasicDbRead,
    ) -> BTreeRead<'_, LayoutU64U64, LayoutU64Var, BasicDbRead> {
        let root = reader.root();
        unsafe { BTreeRead::load(reader, root).unwrap() }
    }

    #[test]
    fn debug_allocator() {
        let (reader, mut writer) = test_alloc::new_db(|page| {
            PageMapMut::<LayoutU64Var>::new(page, 1);
        });
        let p = writer.allocate_page().unwrap();
        let p_num = p.1;
        let (p, _) = p.0.split_at_mut(4);
        p.copy_from_slice(&[5, 6, 7, 8]);
        writer.commit();
        unsafe {
            let p = reader.load(p_num, 1).unwrap();
            let (p, _) = p.split_at(4);
            dbg!(p);
        }
    }

    #[test]
    fn sequential_insert_forward() {
        let (reader, mut writer) = test_alloc::new_db(|page| {
            PageMapMut::<LayoutU64Var>::new(page, 1);
        });
        let mut tree = open_tree_mut(&writer);
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
        writer.commit();
        println!("Writing complete, {} pages used", writer.page_count());

        // Post-insert check
        let reader = reader.reload();
        let tree = open_tree(&reader);
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
        let mut tree = open_tree_mut(&writer);
        for i in 0..i_len {
            match tree.entry(&i).unwrap() {
                Entry::Vacant(_) => panic!("All entries should be occupied"),
                Entry::Occupied(o) => {
                    o.delete()
                        .unwrap_or_else(|e| panic!("Entry for {i} should be deletable: {}", e));
                }
            }
        }
        writer.commit();
        let reader = reader.reload();
        writer.commit();
        let reader = reader.reload();
        writer.commit();
        println!("Deletion complete, {} pages used", writer.page_count());

        // Post-delete check
        let tree = open_tree(&reader);
        for i in 0..i_len {
            assert!(tree.get(&i).unwrap().is_none());
        }
    }

    #[test]
    fn sequential_insert_rev() {
        let (reader, mut writer) = new_db();
        let mut tree = open_tree_mut(&writer);
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
        writer.commit();
        println!("Writing complete, {} pages used", writer.page_count());

        // Post-insert check
        let reader = reader.reload();
        let tree = open_tree(&reader);
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
        let mut tree = open_tree_mut(&writer);
        for i in (0..i_len).rev() {
            match tree.entry(&i).unwrap() {
                Entry::Vacant(_) => panic!("All entries should be occupied, but {i} is unoccupied"),
                Entry::Occupied(o) => {
                    o.delete()
                        .unwrap_or_else(|e| panic!("Entry for {i} should be deletable: {}", e));
                }
            }
        }
        writer.commit();
        let reader = reader.reload();
        writer.commit();
        let reader = reader.reload();
        writer.commit();
        println!("Deletion complete, {} pages used", writer.page_count());

        // Post-delete check
        let reader = reader.reload();
        let tree = open_tree(&reader);
        for i in (0..i_len).rev() {
            assert!(tree.get(&i).unwrap().is_none());
        }
    }

    #[test]
    fn sequential_var_insert_forward() {
        let (reader, mut writer) = new_db();
        let mut tree = open_tree_mut(&writer);
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
        writer.commit();
        println!("Writing complete, {} pages used", writer.page_count());

        // Post-insert check
        let reader = reader.reload();
        let tree = open_tree(&reader);
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
        let mut tree = open_tree_mut(&writer);
        for i in 0..i_len {
            match tree.entry(&i).unwrap() {
                Entry::Vacant(_) => panic!("All entries should be occupied"),
                Entry::Occupied(o) => {
                    o.delete()
                        .unwrap_or_else(|e| panic!("Entry for {i} should be deletable: {}", e));
                }
            }
        }
        writer.commit();
        let reader = reader.reload();
        writer.commit();
        let reader = reader.reload();
        writer.commit();
        println!("Deletion complete, {} pages used", writer.page_count());

        // Post-delete check
        let tree = open_tree(&reader);
        for i in 0..i_len {
            assert!(tree.get(&i).unwrap().is_none());
        }
    }

    #[test]
    fn sequential_var_insert_rev() {
        let (reader, mut writer) = new_db();
        let mut tree = open_tree_mut(&writer);
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
        writer.commit();
        println!("Writing complete, {} pages used", writer.page_count());

        // Post-insert check
        let reader = reader.reload();
        let tree = open_tree(&reader);
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
        let mut tree = open_tree_mut(&writer);
        for i in (0..i_len).rev() {
            match tree.entry(&i).unwrap() {
                Entry::Vacant(_) => panic!("All entries should be occupied, but {i} is unoccupied"),
                Entry::Occupied(o) => {
                    o.delete()
                        .unwrap_or_else(|e| panic!("Entry for {i} should be deletable: {}", e));
                }
            }
        }
        writer.commit();
        let reader = reader.reload();
        writer.commit();
        let reader = reader.reload();
        writer.commit();
        println!("Deletion complete, {} pages used", writer.page_count());

        // Post-delete check
        let reader = reader.reload();
        let tree = open_tree(&reader);
        for i in (0..i_len).rev() {
            assert!(tree.get(&i).unwrap().is_none());
        }
    }
}
