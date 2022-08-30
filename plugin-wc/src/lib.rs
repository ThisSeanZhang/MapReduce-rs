use base::ProcessPlugin;

#[derive(Debug, Default)]
struct WordCount {
}

impl ProcessPlugin for WordCount {
    fn map(&self, _file_name: String, contents: String) -> Vec<(String, String)> {
        contents
        .split(char::is_alphabetic)
        .filter(|s| !s.is_empty())
        .map(|s| (s.to_owned(), "1".to_string()))
        .collect()
    }

    fn reduce(&self, _key: String, values: Vec<String>) -> String {
        values.len().to_string()
    }
}

#[no_mangle]
pub extern "C" fn _build_plugin() -> Box<dyn ProcessPlugin> {
    Box::new(WordCount::default())
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
