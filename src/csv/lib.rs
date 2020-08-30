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
use std::path::Path;

pub fn valid_source_directory(path: &Path) -> Result<(), String> {
    fn valid_directory(path: &Path) -> Result<(), String> {
        if path.is_dir() {
            Ok(())
        } else {
            Err(format!("The directory '{}' does not exist", path.display()))
        }
    }
    valid_directory(&path)?;
    valid_directory(&path.join("objects"))?;
    valid_directory(&path.join("datastreams"))?;
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

pub fn execute_scripts(input: &Path, dest: &Path, path: &Path, pids: Vec<&str>) {
    let objects = ObjectMap::from_path(&input, pids);

    info!("Executing scripts");
    scripts::run_scripts(objects, path, dest);
}
