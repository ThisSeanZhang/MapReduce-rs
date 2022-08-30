use std::{fmt::Debug};
pub trait ProcessPlugin: Debug{
    fn map(&self, file_name: String, contents: String) -> Vec<(String, String)>;
    fn reduce(&self, key: String, values: Vec<String>) -> String;
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
