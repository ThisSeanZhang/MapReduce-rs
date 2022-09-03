use std::{path::{PathBuf, Path}, fs::{read_to_string, File}, collections::HashMap};
use std::io::Write;

use core_base::{PluginHolder, ProcessPlugin};
use log::{LevelFilter, info};
use structopt::StructOpt;
use uuid::Uuid;

use crate::message::{coordinator_client::CoordinatorClient, TaskReq, TaskResp, TaskType, RunningStatus, TaskDist, TaskSubmit};

pub mod message {
  tonic::include_proto!("io.whileaway.code.example.mapreduce");
}

#[derive(StructOpt, Debug)]
#[structopt(name = "seq")]
struct Opt {
  #[structopt(short="p", long)]
  plugin: PathBuf,

  #[structopt(short="o", long, default_value="data-processed")]
  out_dir: PathBuf,
}

#[tokio::main(worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  env_logger::builder()
    .filter_level(LevelFilter::Info)
    .init();
    
  let opt = Opt::from_args();
  let addr = "http://[::1]:25565".to_string();

  
  let plugin = PluginHolder::new(opt.plugin);
  
  let mut client = CoordinatorClient::connect(addr).await?;

  let worker_id = Uuid::new_v4().simple().to_string();

  for _ in 0..80 {
    let request = tonic::Request::new(TaskReq { work_id: worker_id.clone() });
    let response = client.request_task(request).await?;
    let TaskResp { task, status } = response.into_inner();
    match RunningStatus::from_i32(status) {
      Some(RunningStatus::Running) => {
        if let Some(TaskDist { task_id, work_id, files, out_file_num, status }) = task {
          info!("files: {:?}, status: {:?}, worker_id: {:?}", files, status, work_id);
          // let files = out_file_name(&task_id, &worker_id, out_file_num);
          let files = running(&plugin, TaskType::from_i32(status).unwrap(), &files, out_file_num, &opt.out_dir.to_string_lossy().into_owned(), &task_id, &work_id);
          let request = tonic::Request::new(TaskSubmit {work_id: worker_id.clone(), task_id, files });
          let response = client.submit_task(request).await?;
          info!("RESPONSE={:?}", response.into_inner());
        }
      },
      Some(RunningStatus::Wait) => {},
      _ => return Ok(())
    }
  }


  Ok(())
}

fn running(plugin: &PluginHolder, task_type: TaskType, in_files: &Vec<String>, out_file_num: i32, out_dir:&String, task_id: &String, work_id: &String) -> Vec<String> {
  let mut out_files = Vec::new();
  match task_type {
    TaskType::Map => {
      let in_files_data = in_files.iter()
        .map(|file_path| (file_path, read_to_string(file_path)))
        .filter(|file_info| file_info.1.is_ok())
        .flat_map(|(file_name, contents)| plugin.map(file_name.clone(), contents.unwrap()))
        .collect::<Vec<(String, String)>>();
      let each_file_num = (in_files_data.len() as i32 / out_file_num) as usize + 1;
      let mut file_index = 0;
      let mut output_file = None;
      for (index, (key, result)) in in_files_data.iter().enumerate() {
        if index % each_file_num == 0 {
          let file_save_path = format!("{}/mr-{}_{}-{}", out_dir, task_id, work_id, file_index);
          output_file = Some(File::create(&Path::new(&file_save_path)).expect("crate file error"));
          out_files.push(file_save_path);
          file_index += 1;
        }
        if let Some(out_put) = &mut output_file {
          writeln!(out_put, "{} {}", key, result).expect("write error");
        }
      }
    },
    TaskType::Reduce => {
      let intermediate = in_files.iter().map(|file_path| read_to_string(file_path))
      .filter_map(|result| result.ok())
      .flat_map(|file_data| 
        file_data.lines()
        .map(|line| line.split_whitespace().collect::<Vec<&str>>())
        .map(|splited| (splited[0].to_string(), splited[1].to_string()))
        .filter(|pair| pair.0.len() != 0 || pair.1.len() != 0)
        .collect::<Vec<(String, String)>>()
      )
      .fold(HashMap::new(), group_by_key);
      println!("{:?}", intermediate);
      let out_file_path = in_files.get(0).unwrap().split(|c| c == '-').last();
      let out_file_path = format!("{}/mr-out-{}", out_dir, out_file_path.unwrap());
      let output_file = File::create(&Path::new(&out_file_path)).expect("create reduce write file error");
      out_files.push(out_file_path);
      for (key, values) in intermediate {
        let result = plugin.reduce(key.clone(), values);
        writeln!(&output_file, "{} {}", key, result).expect("reduce write error");
      }
    }
  }
  out_files
}

fn out_file_name(task_id: &String, work_id: &String, num: i32) -> Vec<String> {
  (0..num).map(|index| format!("mr-{}_{}-{}", task_id, work_id, index)).collect()
}

fn group_by_key(mut map: HashMap<String, Vec<String>>, item: (String, String)) -> HashMap<String, Vec<String>> {
  if let Some(value) = map.get_mut(&item.0) {
    value.push(item.1);
  } else {
    map.insert(item.0, Vec::from([item.1]));
  }
  map
}

#[cfg(test)]
mod test {
  #[test]
  fn test_chunk_oriter() {
  }
}