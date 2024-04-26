pub mod error {
    use std::error::Error;

    #[derive(Debug)]
    pub struct CustomError {
        message: String,
    }

    impl CustomError {
        pub fn new(message: &str) -> Self {
            CustomError {
                message: message.to_string(),
            }
        }
    }

    impl Error for CustomError {}

    impl std::fmt::Display for CustomError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.message)
        }
    }

    impl From<sqlx::Error> for CustomError {
        fn from(err: sqlx::Error) -> Self {
            CustomError::new(&format!("SQLx error: {}", err))
        }
    }

    impl From<polars::prelude::PolarsError> for CustomError {
        fn from(err: polars::prelude::PolarsError) -> Self {
            CustomError::new(&format!("Polars error: {}", err))
        }
    }
}