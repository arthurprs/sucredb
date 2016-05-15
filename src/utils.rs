use std::error::Error;

pub type GenericError = Box<Error + Send + Sync>;
