use std::fmt::Display;

pub trait EyreExt<T> {
    fn into_eyre(self) -> eyre::Result<T>;

    fn wrap_err<Msg: Display + Send + Sync + 'static>(self, msg: Msg) -> eyre::Result<T>;
}

impl<T, E: std::error::Error + Into<eyre::ErrReport> + Send + Sync + 'static> EyreExt<T>
    for Result<T, E>
{
    fn into_eyre(self) -> eyre::Result<T> {
        self.map_err(Into::into)
    }

    fn wrap_err<Msg: Display + Send + Sync + 'static>(self, msg: Msg) -> eyre::Result<T> {
        eyre::WrapErr::wrap_err(self, msg)
    }
}
