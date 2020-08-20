use rhai::{Array, Dynamic, ImmutableString};
use std::any::TypeId;
use std::fmt;
use std::iter::FromIterator;

// Wrapper around rhai::Map so we can override index functions to return empty arrays to simplify the script logic and prevent runtime errors.
#[derive(Clone)]
pub struct CustomMap(rhai::Map);

impl CustomMap {
    #[cfg(test)]
    pub fn new(map: rhai::Map) -> Self {
        Self(map)
    }

    pub fn insert(&mut self, k: ImmutableString, v: Dynamic) -> Option<Dynamic> {
        self.0.insert(k, v)
    }

    pub fn extend<T: IntoIterator<Item = (ImmutableString, Dynamic)>>(&mut self, iter: T) {
        self.0.extend(iter)
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }

    pub fn get(&self, key: &str) -> Option<&Dynamic> {
        self.0.get(key)
    }

    pub fn keys(&self) -> std::collections::hash_map::Keys<ImmutableString, Dynamic> {
        self.0.keys()
    }

    pub fn elements(self) -> Array {
        self.0
            .into_iter()
            .filter_map(|(_, v)| {
                if TypeId::of::<CustomMap>() == v.type_id() {
                    Some(v)
                } else {
                    None
                }
            })
            .collect()
    }

    fn debug(self) -> rhai::Map {
        fn cast(d: Dynamic) -> Dynamic {
            if TypeId::of::<Array>() == d.type_id() {
                d.cast::<Array>()
                    .into_iter()
                    .map(cast) // recurse.
                    .collect::<Array>()
                    .into()
            } else if TypeId::of::<CustomMap>() == d.type_id() {
                d.cast::<CustomMap>()
                    .0
                    .into_iter()
                    .map(|(k, v)| (k, cast(v))) // recurse.
                    .collect::<rhai::Map>()
                    .into()
            } else {
                d
            }
        };
        self.0.into_iter().map(|(k, v)| (k, cast(v))).collect()
    }
}

impl fmt::Debug for CustomMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("#")?;
        f.debug_map().entries(self.clone().debug()).finish()
    }
}

impl fmt::Display for CustomMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("#")?;
        f.debug_map().entries(self.clone().debug()).finish()
    }
}

impl FromIterator<(ImmutableString, Dynamic)> for CustomMap {
    fn from_iter<T: IntoIterator<Item = (ImmutableString, Dynamic)>>(iter: T) -> Self {
        Self(rhai::Map::from_iter(iter))
    }
}

impl IntoIterator for CustomMap {
    type Item = (ImmutableString, Dynamic);
    type IntoIter = std::collections::hash_map::IntoIter<rhai::ImmutableString, rhai::Dynamic>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
