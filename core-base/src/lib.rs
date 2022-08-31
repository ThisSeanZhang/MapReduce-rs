use std::{fmt::Debug, path::PathBuf};
use libloading::library_filename;

pub trait ProcessPlugin: Debug{
    fn map(&self, file_name: String, contents: String) -> Vec<(String, String)>;
    fn reduce(&self, key: String, values: Vec<String>) -> String;
}

#[derive(Debug)]
pub struct PluginHolder {
  lib: libloading::Library,
  plugin: Option<Box<dyn ProcessPlugin>>
}

type BuildFn = fn() -> Box<dyn ProcessPlugin>;

impl PluginHolder {
  pub fn new(plugin_path: PathBuf) -> PluginHolder {
    unsafe {
      let lib = libloading::Library::new(library_filename(plugin_path)).unwrap();
      let plugin = lib.get::<BuildFn>(b"_build_plugin").expect("unable to get service");
      let plugin = plugin();
      PluginHolder{
        lib: lib,
        plugin: Some(plugin)
      }
    }
  }
}

impl  ProcessPlugin for PluginHolder{
    fn map(&self, file_name: String, contents: String) -> Vec<(String, String)> {
      if let Some(service) = &self.plugin {
        service.map(file_name, contents)
      } else {
        Vec::new()
      }
    }

    fn reduce(&self, key: String, values: Vec<String>) -> String {
      if let Some(service) = &self.plugin {
        service.reduce(key, values)
      } else {
        "".to_owned()
      }
    }
}

/// fix: segmentation fault
/// [same question with](https://users.rust-lang.org/t/libloading-segfault/56848/4)
/// [demo](https://github.com/jlgerber/pes)
/// 
impl Drop for PluginHolder {
  fn drop(&mut self) {
    let plugin = self.plugin.take();
    if let Some(plugin) = plugin {
      drop(plugin);
    }
    drop(&self.lib)
  }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
