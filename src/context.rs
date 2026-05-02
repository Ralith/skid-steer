use std::{
    any::{Any, TypeId},
    hash::BuildHasherDefault,
};

use crate::type_id_map::{TypeIdHasher, TypeIdMap};

/// A set of references to at most one of any `Sync + 'static` type
///
/// Enables [`Source`](crate::Source) methods to temporarily borrow arbitrary resources.
///
/// [`Sync`] is necessary to ensure [`Task`](crate::Task)s and their futures are `Send`, making them
/// compatible with work-stealing async runtimes.
#[derive(Default)]
pub struct Context<'a> {
    map: TypeIdMap<&'a (dyn Any + Sync)>,
}

impl<'a> Context<'a> {
    /// Create an empty `Context`
    pub const fn new() -> Self {
        Self {
            map: TypeIdMap::with_hasher(BuildHasherDefault::<TypeIdHasher>::new()),
        }
    }

    /// Convenience method for `from_iter` with less ambiguous types
    pub fn from_slice(elts: &[&'a (dyn Any + Sync)]) -> Self {
        let mut this = Self::with_capacity(elts.len());
        this.extend(elts.into_iter().copied());
        this
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            map: TypeIdMap::with_capacity_and_hasher(
                capacity,
                BuildHasherDefault::<TypeIdHasher>::new(),
            ),
        }
    }

    pub fn reserve(&mut self, additional: usize) {
        self.map.reserve(additional);
    }

    /// Store a reference to a `T` in the map
    pub fn insert<T: Sync + 'static>(&mut self, value: &'a T) {
        self.map.insert(TypeId::of::<T>(), value);
    }

    /// Borrow a `T` whose reference was stored in the map, if any
    pub fn get<T: Sync + 'static>(&self) -> Option<&'a T> {
        self.map
            .get(&TypeId::of::<T>())
            .map(|&x| <dyn Any>::downcast_ref(x).unwrap())
    }

    pub fn clear(&mut self) {
        self.map.clear();
    }
}

impl<'a> Extend<&'a (dyn Any + Sync)> for Context<'a> {
    fn extend<T: IntoIterator<Item = &'a (dyn Any + Sync)>>(&mut self, iter: T) {
        self.map.extend(iter.into_iter().map(|v| (v.type_id(), v)));
    }
}

impl<'a> FromIterator<&'a (dyn Any + Sync)> for Context<'a> {
    fn from_iter<T: IntoIterator<Item = &'a (dyn Any + Sync)>>(iter: T) -> Self {
        let i = iter.into_iter();
        let mut c = Context::new();
        c.extend(i);
        c
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke() {
        let mut c = Context::new();
        let v = 42i32;
        c.insert(&v);
        assert_eq!(c.get::<i32>(), Some(&v));
    }
}
