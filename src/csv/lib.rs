#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate strum_macros;

#[cfg(test)]
#[macro_use]
extern crate maplit;

mod map;
mod object;
mod rows;
mod scripts;
mod utils;
mod xml;

pub use scripts::ScriptError;

use log::info;
use object::ObjectMap;
use rows::{FileRow, MediaRow, NodeRow};
use std::path::{Path, PathBuf};
use std::sync::RwLock;

lazy_static! {
    static ref OBJECTS_DIRECTORY: RwLock<Option<Box<Path>>> = RwLock::new(None);
    static ref DATASTREAMS_DIRECTORY: RwLock<Option<Box<Path>>> = RwLock::new(None);
}

fn set_objects_directory(path: &PathBuf) {
    let mut lock = OBJECTS_DIRECTORY.write().unwrap();
    *lock = Some(path.clone().into_boxed_path());
}

fn set_datastreams_directory(path: &PathBuf) {
    let mut lock = DATASTREAMS_DIRECTORY.write().unwrap();
    *lock = Some(path.clone().into_boxed_path());
}

pub fn valid_source_directory(path: &Path) -> Result<(), String> {
    fn valid_directory(path: &Path) -> Result<(), String> {
        if path.is_dir() {
            Ok(())
        } else {
            Err(format!("The directory '{}' does not exist", path.display()))
        }
    }
    valid_directory(&path)?;
    let objects = path.join("objects");
    valid_directory(&objects)?;
    set_objects_directory(&objects);
    let datastreams = path.join("datastreams");
    valid_directory(&datastreams)?;
    set_datastreams_directory(&datastreams);
    Ok(())
}

pub fn generate_csvs(input: &Path, dest: &Path, pids: Vec<&str>) {
    let objects = ObjectMap::from_path(&input, pids);
    info!("Generating csv files");
    FileRow::csv(&objects, dest);
    MediaRow::csv(&objects, dest);
    MediaRow::revisions_csv(&objects, dest);
    NodeRow::csv(&objects, dest);
}

pub fn execute_scripts(
    input: &Path,
    dest: &Path,
    scripts: Vec<&Path>,
    modules: Vec<&Path>,
    pids: Vec<&str>,
) {
    let objects = ObjectMap::from_path(&input, pids);
    scripts::run_scripts(objects, scripts, modules, dest);
}
