use std::any::TypeId;

pub type TypeIdMap<V> =
    std::collections::HashMap<TypeId, V, std::hash::BuildHasherDefault<TypeIdHasher>>;

/// A hasher optimized for hashing a single TypeId.
///
/// TypeId is already thoroughly hashed, so there's no reason to hash it again.
/// Just leave the bits unchanged.
#[derive(Default)]
pub struct TypeIdHasher {
    hash: u64,
}

impl std::hash::Hasher for TypeIdHasher {
    fn write_u64(&mut self, n: u64) {
        self.hash = n;
    }

    // Tolerate TypeId being either u64 or u128.
    fn write_u128(&mut self, n: u128) {
        self.hash = n as u64;
    }

    fn write(&mut self, _: &[u8]) {
        unreachable!()
    }

    fn finish(&self) -> u64 {
        self.hash
    }
}
