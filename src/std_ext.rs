pub trait IntoEyre<T> {
    fn into_eyre(self) -> eyre::Result<T>;
}

impl<T, E: Into<eyre::ErrReport>> IntoEyre<T> for Result<T, E> {
    fn into_eyre(self) -> eyre::Result<T> {
        self.map_err(Into::into)
    }
}
