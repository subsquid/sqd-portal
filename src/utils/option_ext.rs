pub trait OptionExt<T> {
    #[allow(clippy::wrong_self_convention)]
    fn is_none_or(self, f: impl FnOnce(T) -> bool) -> bool;
}

impl<T> OptionExt<T> for Option<T> {
    fn is_none_or(self, f: impl FnOnce(T) -> bool) -> bool {
        match self {
            Some(v) => f(v),
            None => true,
        }
    }
}
