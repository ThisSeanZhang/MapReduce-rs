use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use crossbeam_queue::ArrayQueue;
use message::coordinator_server::{Coordinator, CoordinatorServer};
use message::{TaskType, RunningStatus, TaskReq, TaskResp, TaskDist, TaskSubmit};
use structopt::StructOpt;
use log::{info, LevelFilter};
use tonic::{transport::Server, Request, Response, Status};
use uuid::Uuid;
use dashmap::DashMap;
pub mod message {
  tonic::include_proto!("io.whileaway.code.example.mapreduce");
}

///
/// Coordinator
/// 
#[derive(Debug)]
pub struct CoordinatorMgr {
  n_reduce: i32,

  pending_tasks: Arc<ArrayQueue<Task>>,
  running_tasks: Arc<DashMap<String, Task>>,
  reduce_pending_files: DashMap<usize, Vec<String>>,
}

impl CoordinatorMgr {
  fn new(opt: Opt) -> CoordinatorMgr {
    let task_capacity = (opt.input_files.len() + (opt.n_map as usize)) * 2;
    let pending_tasks = ArrayQueue::new(task_capacity);
    for path in opt.get_str_path().chunks(1) {
      let task = Task::new(path.to_vec(), TaskType::Map, opt.n_reduce);
      pending_tasks.push(task).unwrap();
    }
    CoordinatorMgr {
      n_reduce: opt.n_reduce,
      pending_tasks: Arc::new(pending_tasks),
      running_tasks: Arc::new(DashMap::with_capacity(task_capacity)),
      reduce_pending_files: DashMap::with_capacity(opt.n_map as usize),
    }
  }

  fn is_map_finish(&self) -> bool {
    self.pending_tasks.is_empty() && self.running_tasks.is_empty()
  }

  fn is_finish(&self) -> bool {
    self.is_map_finish() && self.reduce_pending_files.is_empty()
  }

}

#[tonic::async_trait]
impl Coordinator for CoordinatorMgr {
  
  async fn request_task(&self, request: Request<TaskReq>) -> Result<Response<TaskResp>, Status> {
    let work_id = request.into_inner().work_id;
    let resp = match self.pending_tasks.pop() {
      Some(mut task) => {
        let resp = task.to_distribution(work_id);
        self.running_tasks.insert(task.get_running_id(), task);
        TaskResp {
          task: Some(resp),
          status: RunningStatus::Running as i32
        }
      }
      None => {
        let status = if self.is_finish() {
          RunningStatus::Finish
        } else {
          RunningStatus::Wait
        } as i32;
        TaskResp {
          task: None,
          status,
        }
      },
    };

    info!("poll task resp: {:?}", resp);
    Ok(Response::new(resp))
  }

  async fn submit_task(&self, request: Request<TaskSubmit>) -> Result<Response<TaskResp>, Status> {
    let TaskSubmit { task_id, work_id, files } = request.into_inner();
    let running_id = format!("{}_{}", task_id, work_id);
    if let Some((_, task)) = self.running_tasks.remove(&running_id) {
      info!("task done: {:?}, type: {:?}", running_id, task.task_type);
      
      // if task is map task, need add result to reduce_pending_files
      // when push in, need check
      if task.task_type == TaskType::Map && !check_task_repeat(&task_id, &self.reduce_pending_files) {
        for (index, file) in files.iter().enumerate() {
          self.reduce_pending_files.entry(index).or_default().push(file.to_owned());
        }
      }
    }

    if self.is_map_finish() {
      for pair in self.reduce_pending_files.iter() {
        // let files = pair.value().to_vec().iter().map(String::from).filter(distinction_intermediate_file()).collect();
        let task = Task::new(pair.value().to_vec(), TaskType::Reduce, self.n_reduce);
        self.pending_tasks.push(task).unwrap();
      }
      self.reduce_pending_files.clear();
    }
    Ok(Response::new(TaskResp { task: None, status: RunningStatus::Wait as i32}))
  }
}

fn check_task_repeat(task_id: &str, map: &DashMap<usize, Vec<String>>) -> bool {
  if let Some(list) = map.get(&0) {
    list.value().iter().any(|file_path| file_path.contains(task_id))
  } else {
    false
  }
}

fn distinction_intermediate_file() -> impl FnMut(&String) -> bool {
  let mut task_id_diff = HashSet::new();
  move |file| {
    let mut task_id = String::new();
    let _ = file[3..36].clone_into(&mut task_id);
    // let task_id = file.clone().split(|c| c == '-' || c == '_').;
    match task_id_diff.get(&task_id) {
      Some(_) => false,
      None => {
        task_id_diff.insert(task_id);
        true
      }
    }
  }
}

///
/// Task
///
/// 
#[derive(Debug)]
pub struct  Task {
  task_id: String,
  work_id: Option<String>,
  files: Vec<String>,
  task_type: TaskType,
  out_file_num: i32,
  // task_output_files: Vec<String>,
}

impl Task {
  fn new(file_path: Vec<String>, ty: TaskType, out_file_num: i32) -> Task {
    let mut task = Task::default();
    task.files = file_path;
    task.out_file_num = out_file_num;
    task.task_type = ty;
    task
  }

  fn to_distribution(&mut self, work_id: String) -> TaskDist {
    self.work_id = Some(work_id.clone());
    TaskDist {
      task_id: self.task_id.clone(),
      work_id,
      files: self.files.clone(),
      out_file_num: self.out_file_num,
      status: self.task_type as i32
    }
  }

  fn get_running_id(&self) -> String {
    if let Some(work_id) = &self.work_id {
      format!("{}_{}", self.task_id, work_id)
    } else {
      format!("{}_", self.task_id)
    }
  }
}

impl Default for Task {
    fn default() -> Self {
        Self { 
          task_id: Uuid::new_v4().simple().to_string(),
          work_id: None,
          files: Vec::new(),
          task_type: TaskType::Map,
          out_file_num: 1,
        }
    }
}


///
/// Opt
/// 
#[derive(StructOpt, Debug)]
#[structopt(name = "seq")]
struct Opt {
  #[structopt(short="c", long, default_value = "1")]
  n_map: i32,

  #[structopt(short="n", long)]
  n_reduce: i32,
  
  #[structopt(name = "FILE", parse(from_os_str))]
  input_files: Vec<PathBuf>,
}

impl Opt {
  fn get_str_path(&self) -> Vec<String> {
    self.input_files.iter()
    .map(|path| path.to_string_lossy().into_owned())
    .collect::<Vec<String>>()
  }
}

#[tokio::main(worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  env_logger::builder()
    .filter_level(LevelFilter::Info)
    .init();
  
  let opt = Opt::from_args();
  let addr = "[::1]:25565".parse()?;
  let cd = CoordinatorMgr::new(opt);

  Server::builder()
    .add_service(CoordinatorServer::new(cd))
    .serve(addr)
    .await?;
  Ok(())
}

#[cfg(test)]
mod test {
    use dashmap::DashMap;

    use crate::{distinction_intermediate_file, check_task_repeat};

  #[test]
  fn test_file_dist2() {
    let files = [
      "mr-a31b4bbfc48c4f068b1d7e06a217cc70_36751d32ab68433b9bc7aee9da51c587-0".to_string(), 
      "mr-3b11093f152d42a1aaba78da46976573_36751d32ab68433b9bc7aee9da51c587-0".to_string(), 
      "mr-aaff1aa6f6514c3488211af42cc82323_36751d32ab68433b9bc7aee9da51c587-0".to_string(), 
      "mr-931d4d7ed6944f2bbf5e7bbb098905df_36751d32ab68433b9bc7aee9da51c587-0".to_string(), 
      "mr-3ae28425384e4b40a539338e5f78499b_36751d32ab68433b9bc7aee9da51c587-0".to_string(), 
      "mr-754b1a265fa940f8881195e3008b4fae_36751d32ab68433b9bc7aee9da51c587-0".to_string(), 
      "mr-ff34e84a8c844da09c2ec92883ffc88e_36751d32ab68433b9bc7aee9da51c587-0".to_string(), 
    ];
    let map = DashMap::new();
    map.insert(0, Vec::from(files));
    let task_id = "a31b4bbfc48c4f068b1d7e06a217cc70";
    assert_eq!(check_task_repeat(task_id, &map), true);
    let task_id = "a2fc90565c8f40a6944a216ea60ea47a";
    assert_eq!(check_task_repeat(task_id, &map), false);
  }
  #[test]
  fn test_file_dist() {
    let files = [
      "mr-a31b4bbfc48c4f068b1d7e06a217cc70_36751d32ab68433b9bc7aee9da51c587-0".to_string(), 
      "mr-a31b4bbfc48c4f068b1d7e06a217cc70_36751d32ab68433b9bc7aee9da51c588-0".to_string(), 
      "mr-3b11093f152d42a1aaba78da46976573_36751d32ab68433b9bc7aee9da51c587-0".to_string(), 
      "mr-aaff1aa6f6514c3488211af42cc82323_36751d32ab68433b9bc7aee9da51c587-0".to_string(), 
      "mr-931d4d7ed6944f2bbf5e7bbb098905df_36751d32ab68433b9bc7aee9da51c587-0".to_string(), 
      "mr-3ae28425384e4b40a539338e5f78499b_36751d32ab68433b9bc7aee9da51c587-0".to_string(), 
      "mr-754b1a265fa940f8881195e3008b4fae_36751d32ab68433b9bc7aee9da51c587-0".to_string(), 
      "mr-ff34e84a8c844da09c2ec92883ffc88e_36751d32ab68433b9bc7aee9da51c587-0".to_string(), 
      "mr-a2fc90565c8f40a6944a216ea60ea47a_36751d32ab68433b9bc7aee9da51c587-0".to_string()
    ];
    let result = files.iter()
    .map(String::from)
    .filter(distinction_intermediate_file())
    .count();
    
    assert_eq!(result, 8);
  }
}