use std::collections::HashMap;
use std::fs::{read_to_string, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use core_base::{PluginHolder, ProcessPlugin};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "seq")]
struct Opt {

  #[structopt(short, long)]
  plugin: PathBuf,
  /// Files to process
  #[structopt(name = "FILE", parse(from_os_str))]
  raw_data: Vec<PathBuf>,
}

fn main() -> Result<(),  Box<dyn std::error::Error>> {
  let opt = Opt::from_args();
  println!("{:#?}", opt);
  let plugin = PluginHolder::new(opt.plugin);
  let intermediate = opt
    .raw_data
    .iter()
    .map(|file_path| (file_path.to_string_lossy().into_owned(), read_to_string(file_path)))
    .filter(|file_info| file_info.1.is_ok())
    .flat_map(|(file_name, contents)| plugin.map(file_name, contents.unwrap()))
    .fold(HashMap::new(), group_by_key);
  // println!("{:?}", intermediate);

  let output_file = File::create(&Path::new("mr-out-0"))?;
  for (key, values) in intermediate {
    let result = plugin.reduce(key.clone(), values);
    writeln!(&output_file, "{} {}", key, result)?;
  }
  
  Ok(())
}


fn group_by_key(mut map: HashMap<String, Vec<String>>, item: (String, String)) -> HashMap<String, Vec<String>> {
  if let Some(value) = map.get_mut(&item.0) {
    value.push(item.1);
  } else {
    map.insert(item.0, Vec::from([item.1]));
  }
  map
}