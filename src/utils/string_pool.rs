use std::{collections::BTreeSet, sync::Arc};

use lazy_static::lazy_static;

use crate::utils::RwLock;

lazy_static! {
    static ref POOL: RwLock<BTreeSet<Arc<str>>> = RwLock::new(Default::default(), "StringPool");
}

pub fn intern_string(s: &str) -> Arc<str> {
    if let Some(owned) = POOL.read().get(s) {
        return owned.clone();
    }
    let owned: Arc<str> = s.into();
    POOL.write().insert(owned.clone());
    owned
}
