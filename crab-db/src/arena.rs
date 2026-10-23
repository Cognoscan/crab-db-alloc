use crab_dads_traits::StorageError;

/// Arena tracker using 1 bit per region within an arena.
/// 
/// Always uses an array of `u64` values, which it stores in little-endian form.
/// LSB is region 0, then region 1, and so on. A set bit indicates the region is
/// in the arena (free), and a clear bit indicates it is not (allocated).
pub struct Arena {
    // Internally always stored little-endian, even if processor ops are big-endian.
    data: Vec<u64>,
    size: usize,
}

impl Arena {
    /// Create a new full arena of size `size`.
    pub fn new_full(size: usize) -> Self {
        assert!((1..=512).contains(&size));
        let cnt = size.div_ceil(64);
        let rem = size % 64;
        let mut data = Vec::with_capacity(cnt);
        for _ in 0..cnt {
            data.push(u64::MAX)
        }
        if rem > 0 {
            data.pop();
            let to_push = u64::MAX >> (64 - rem);
            data.push(to_push.to_le());
        }
        Self { data, size }
    }

    /// Create a new empty arena of size `size`.
    pub fn new_empty(size: usize) -> Self {
        assert!((1..=512).contains(&size));
        let cnt = size.div_ceil(64);
        Self {
            data: vec![0u64; cnt],
            size,
        }
    }

    /// Try to load an arena from stored data. Must provide the expected size of the arena.
    pub fn load(data: &[u8], expected_size: usize) -> Result<Self, StorageError> {
        assert!((1..=512).contains(&expected_size));
        let Ok(data) = bytemuck::try_cast_slice::<u8, u64>(data) else {
            return Err(StorageError::Corruption("Arena isn't a set of u64 values"));
        };
        if data.len() != expected_size.div_ceil(64) {
            return Err(StorageError::Corruption(
                "Arena data storage is not the expected size",
            ));
        }
        // The last 1 in the sequence cannot be past the expected size. Find the last 1 and see if it is.
        let final_one = 64
            - (data
                .last()
                .copied()
                .unwrap_or_default()
                .to_le()
                .leading_zeros() as usize);
        if expected_size % 64 < final_one {
            return Err(StorageError::Corruption(
                "Arena has free elements that are located beyond the expected size of the arena",
            ));
        }

        Ok(Self {
            data: data.to_vec(),
            size: expected_size,
        })
    }

    /// Get a byte array for storing to backing memory.
    pub fn as_bytes(&self) -> &[u8] {
        bytemuck::cast_slice(&self.data)
    }

    pub fn allocate(&mut self) -> Option<usize> {
        for (i, b) in self.data.iter_mut().enumerate() {
            let offset = b.to_le().trailing_zeros();
            if offset >= 64 {
                continue;
            }
            *b &= (!(1u64 << offset)).to_le();
            return Some((offset as usize) + 64 * i);
        }
        None
    }

    pub fn is_empty(&self) -> bool {
        self.data.iter().all(|x| *x == 0)
    }

    pub fn free(&mut self, offset: usize) -> Result<(), StorageError> {
        const RANGE_ERR: &str = "Tried to free a region in an arena outside of the arena's range";
        const DOUBLE_FREE: &str =
            "Tried to free a region in an arena that was already marked as free";
        if self.size <= offset {
            return Err(StorageError::Corruption(RANGE_ERR));
        }
        let word_offset = offset / 64;
        let bit_offset = offset % 64;
        let bit = (1u64 << bit_offset).to_le();
        let Some(word) = self.data.get_mut(word_offset) else {
            return Err(StorageError::Corruption(RANGE_ERR));
        };
        if (*word & bit) != 0 {
            return Err(StorageError::Corruption(DOUBLE_FREE));
        }
        *word |= bit;
        Ok(())
    }

    pub fn is_full(&self) -> bool {
        let Some((last, rest)) = self.data.split_last() else {
            return true;
        };
        if rest.iter().any(|x| *x != u64::MAX) {
            return false;
        }
        let mut rem = self.size % 64;
        if rem == 0 { rem = 64; }
        (last.count_ones() as usize) == rem
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_new() {
        assert_eq!(
            Arena::new_full(1).as_bytes(),
            bytemuck::cast_slice::<u64, u8>(&[0x1u64.to_le()]),
            "new 1"
        );
        assert_eq!(
            Arena::new_full(2).as_bytes(),
            bytemuck::cast_slice::<u64, u8>(&[0x3u64.to_le()]),
            "new 2"
        );
        assert_eq!(
            Arena::new_full(62).as_bytes(),
            bytemuck::cast_slice::<u64, u8>(&[0x3FFF_FFFF_FFFF_FFFFu64.to_le()]),
            "new 62"
        );
        assert_eq!(
            Arena::new_full(63).as_bytes(),
            bytemuck::cast_slice::<u64, u8>(&[0x7FFF_FFFF_FFFF_FFFFu64.to_le()]),
            "new 63"
        );
        assert_eq!(
            Arena::new_full(64).as_bytes(),
            bytemuck::cast_slice::<u64, u8>(&[0xFFFF_FFFF_FFFF_FFFFu64.to_le()]),
            "new 64"
        );
        assert_eq!(
            Arena::new_full(65).as_bytes(),
            bytemuck::cast_slice::<u64, u8>(&[0xFFFF_FFFF_FFFF_FFFF, 0x1u64.to_le()]),
            "new 65"
        );
        assert_eq!(
            Arena::new_full(66).as_bytes(),
            bytemuck::cast_slice::<u64, u8>(&[0xFFFF_FFFF_FFFF_FFFF, 0x3u64.to_le()]),
            "new 66"
        );
        assert_eq!(
            Arena::new_full(511).as_bytes(),
            bytemuck::cast_slice::<u64, u8>(&[
                0xFFFF_FFFF_FFFF_FFFF,
                0xFFFF_FFFF_FFFF_FFFF,
                0xFFFF_FFFF_FFFF_FFFF,
                0xFFFF_FFFF_FFFF_FFFF,
                0xFFFF_FFFF_FFFF_FFFF,
                0xFFFF_FFFF_FFFF_FFFF,
                0xFFFF_FFFF_FFFF_FFFF,
                0x7FFF_FFFF_FFFF_FFFFu64.to_le(),
            ]),
            "new 511"
        );
        assert_eq!(
            Arena::new_full(512).as_bytes(),
            bytemuck::cast_slice::<u64, u8>(&[
                0xFFFF_FFFF_FFFF_FFFF,
                0xFFFF_FFFF_FFFF_FFFF,
                0xFFFF_FFFF_FFFF_FFFF,
                0xFFFF_FFFF_FFFF_FFFF,
                0xFFFF_FFFF_FFFF_FFFF,
                0xFFFF_FFFF_FFFF_FFFF,
                0xFFFF_FFFF_FFFF_FFFF,
                0xFFFF_FFFF_FFFF_FFFF,
            ]),
            "new 511"
        );
    }

    #[test]
    fn bad_load() {
        for i in 0..7 {
            let data = vec![0u8; i];
            assert!(
                Arena::load(&data, 4).is_err(),
                "non-u64-size array should produce error"
            );
        }
        let data = bytemuck::cast_slice::<u64, u8>(&[0x7]);
        assert!(
            Arena::load(data, 2).is_err(),
            "2-size arena should not have a 3rd bit set"
        );
        let data = bytemuck::cast_slice::<u64, u8>(&[0, 0x7]);
        assert!(
            Arena::load(data, 64).is_err(),
            "64-size arena should not have a 67th bit set"
        );
        assert!(
            Arena::load(data, 65).is_err(),
            "65-size arena should not have a 67th bit set"
        );
        assert!(
            Arena::load(data, 67).is_ok(),
            "67-size arena should have decoded ok"
        );
    }

    #[test]
    fn try_allocate() {
        for size in 60..=74 {
            let mut arena = Arena::new_full(size);
            assert!(
                arena.is_full(),
                "Arena of size {} should be full, contents are {:x?}",
                size,
                arena.data
            );
            for i in 0..size {
                assert!(
                    !arena.is_empty(),
                    "Arena of size {} should not be empty, contents are {:?}",
                    size,
                    arena.data
                );
                assert_eq!(
                    arena.allocate(),
                    Some(i),
                    "Arena of size {} should be able to allocate #{}, contents are {:?}",
                    size,
                    i,
                    arena.data
                );
                assert!(
                    !arena.is_full(),
                    "Arena of size {} should be not full, contents are {:x?}",
                    size,
                    arena.data
                );
            }
            assert!(
                arena.allocate().is_none(),
                "Arena of size {} should be unable to allocate after {} allocations, contents are {:?}",
                size,
                size,
                arena.data
            );
            assert!(
                arena.is_empty(),
                "Arena of size {} should be empty, contents are {:?}",
                size,
                arena.data
            );
            for i in 0..size {
                assert!(
                    arena.free(i).is_ok(),
                    "Arena of size {} should be able to free {}, contents are {:?}",
                    size,
                    i,
                    arena.data
                );
            }
            assert!(
                arena.is_full(),
                "Arena of size {} should be full, contents are {:?}",
                size,
                arena.data
            );
        }
    }
}
