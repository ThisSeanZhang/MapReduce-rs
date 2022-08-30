use std::{path::PathBuf};
use std::error::Error;
use base::ProcessPlugin;
use libloading::library_filename;
use structopt::StructOpt;

type BuildFn = fn() -> Box<dyn ProcessPlugin>;

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
  let plugin = load_plugin(opt.plugin)?;
  let a = plugin.map("aaa".to_string(), "bbb".to_string());
  println!("{:?}", a);
  Ok(())
}

fn load_plugin(plugin_path: PathBuf) -> Result<Box<dyn ProcessPlugin>, Box<dyn Error>> {
  let plugin = unsafe {
    let lib = libloading::Library::new(library_filename(plugin_path))?;
    let build_plugin = lib.get::<BuildFn>(b"_build_plugin\0")?;
    build_plugin()
  };
  Ok(plugin)
}