use bincode::Error as BinCodeError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    BinCodeError(#[from] BinCodeError),
    #[error("File doesn't contain meta data")]
    NoMetaData,
    #[error("Can't generate chunk writer no meta data exists")]
    FailedToGenerateChunkWriter,
    #[error("Lock Error you trying to access something that is locked")]
    PoisonError,
    #[error("File ins't initialized")]
    NotInitialed,
    #[error("Attempting to write data that overlaps the ordinal size")]
    FileOverFlow,
}
