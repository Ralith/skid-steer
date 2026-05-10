use std::{
    any::{Any, TypeId},
    hash::BuildHasherDefault,
};

use crate::type_id_map::{TypeIdHasher, TypeIdMap};

/// A set of references to at most one of any `'static` type
///
/// Enables [`Source`](crate::Source) methods to temporarily borrow arbitrary resources. Typically
/// created immediately prior to a batch of calls to [`Task::run`](crate::Task::run), and dropped
/// immediately after.
#[derive(Default)]
pub struct Context<'a> {
    map: TypeIdMap<&'a dyn Any>,
}

impl<'a> Context<'a> {
    /// Create an empty `Context`
    pub const fn new() -> Self {
        Self {
            map: TypeIdMap::with_hasher(BuildHasherDefault::<TypeIdHasher>::new()),
        }
    }

    /// Convenience method for `from_iter` with less ambiguous types
    pub fn from_slice(elts: &[&'a dyn Any]) -> Self {
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
    pub fn insert<T: 'static>(&mut self, value: &'a T) {
        self.map.insert(TypeId::of::<T>(), value);
    }

    /// Borrow a `T` whose reference was stored in the map, if any
    pub fn get<T: 'static>(&self) -> Option<&'a T> {
        self.map
            .get(&TypeId::of::<T>())
            .map(|&x| <dyn Any>::downcast_ref(x).unwrap())
    }

    pub fn clear(&mut self) {
        self.map.clear();
    }
}

impl<'a> Extend<&'a dyn Any> for Context<'a> {
    fn extend<T: IntoIterator<Item = &'a dyn Any>>(&mut self, iter: T) {
        self.map.extend(iter.into_iter().map(|v| (v.type_id(), v)));
    }
}

impl<'a> FromIterator<&'a dyn Any> for Context<'a> {
    fn from_iter<T: IntoIterator<Item = &'a dyn Any>>(iter: T) -> Self {
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
