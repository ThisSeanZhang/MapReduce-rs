use std::sync::Arc;
use std::{path::PathBuf};
use std::error::Error;
use base::{PluginHolder, ProcessPlugin};
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
  let a = plugin.map("aaa".to_string(), "b bb".to_string());
  println!("{:?}", a);
  Ok(())
}
