use std::{collections::BTreeSet, sync::Arc};

use lazy_static::lazy_static;
use parking_lot::RwLock;

lazy_static! {
    static ref POOL: RwLock<BTreeSet<Arc<str>>> = RwLock::default();
}

pub fn intern_string(s: &str) -> Arc<str> {
    if let Some(owned) = POOL.read().get(s) {
        return owned.clone();
    }
    let owned: Arc<str> = s.into();
    POOL.write().insert(owned.clone());
    owned
}
