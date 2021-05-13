use super::identifiers::*;
use crc32fast::Hasher;
use log::info;
use rayon::prelude::*;
use std::fmt;
use std::fs;
use std::io::prelude::*;
use std::path::Path;
use MigrationResult::*;

#[derive(Eq, PartialEq)]
enum MigrationResult {
    Migrated,
    Updated,
    Skipped,
}

#[derive(Default)]
pub struct MigrationResults {
    total: usize,
    migrated: usize,
    updated: usize,
    skipped: usize,
}

impl MigrationResults {
    fn new(results: &[MigrationResult]) -> Self {
        let mut summary = MigrationResults {
            total: results.len(),
            ..Default::default()
        };
        for result in results {
            match result {
                Migrated => summary.migrated += 1,
                Updated => summary.updated += 1,
                Skipped => summary.skipped += 1,
            }
        }
        summary
    }
}

impl fmt::Display for MigrationResults {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Total: {} (Migrated: {}, Updated: {}, Skipped: {})",
            self.total, self.migrated, self.updated, self.skipped
        )
    }
}

// Checks if the destination does not exist or if the file sizes differ.
fn should_migrate_file(path: &Path, dest: &Path, checksum: bool) -> bool {
    !dest.exists()
        || if checksum {
            let src = {
                let mut hasher = Hasher::new();
                hasher.update(&fs::read(&path).unwrap());
                hasher.finalize()
            };
            let dest = {
                let mut hasher = Hasher::new();
                hasher.update(&fs::read(&dest).unwrap());
                hasher.finalize()
            };
            src != dest
        } else {
            // Check size and modified times.
            let path_metadata = path.metadata().unwrap();
            let dest_metadata = dest.metadata().unwrap();
            let size_differs = path_metadata.len() != dest_metadata.len();
            let modified_time_differs =
                path_metadata.modified().unwrap() != dest_metadata.modified().unwrap();
            size_differs || modified_time_differs
        }
}

fn create_parent_directories(dest: &Path) {
    fs::create_dir_all(&dest.parent().unwrap()).unwrap_or_else(|error| {
        panic!(
            "Failed to create destination directory {}, with error: {}",
            &dest.to_string_lossy(),
            error
        )
    });
}

// Checks if the destination does not exist or if the file sizes differ.
fn should_migrate_content(content: &str, dest: &Path, checksum: bool) -> bool {
    !dest.exists() || {
        if checksum {
            let src = {
                let mut hasher = Hasher::new();
                hasher.update(&content.as_bytes());
                hasher.finalize()
            };
            let dest = {
                let mut hasher = Hasher::new();
                hasher.update(&fs::read(&dest).unwrap());
                hasher.finalize()
            };
            src != dest
        } else {
            // Check size, no modified time can be used.
            (content.len() as u64) != dest.metadata().unwrap().len()
        }
    }
}

// No-op if already exists or not the same size.
// Returns true/false if the file was copied or not.
fn migrate_by_copy(path: &Path, dest: &Path, checksum: bool) -> MigrationResult {
    let existed = dest.exists();
    if should_migrate_file(&path, &dest, checksum) {
        create_parent_directories(&dest);
        fs::copy(&path, &dest).unwrap_or_else(|error| {
            panic!(
                "Failed to copy file {} to {}, with error: {}",
                &path.to_string_lossy(),
                &dest.to_string_lossy(),
                error
            )
        });
        // Set modified times to match source file.
        let metadata = path.metadata().unwrap();
        let mtime = filetime::FileTime::from_last_modification_time(&metadata);
        filetime::set_file_mtime(dest, mtime).unwrap();
        return if existed { Updated } else { Migrated };
    }
    Skipped
}

// No-op if already exists or not the same size.
// Returns true/false if the file was renamed or not.
fn migrate_by_move(path: &Path, dest: &Path, checksum: bool) -> MigrationResult {
    let existed = dest.exists();
    if should_migrate_file(&path, &dest, checksum) {
        create_parent_directories(&dest);
        fs::rename(&path, &dest).unwrap_or_else(|_| {
            // If from and to are on a separate filesystem rename cannot be used
            // so fall back to copying.
            fs::copy(&path, &dest).unwrap_or_else(|error| {
              panic!(
                  "Failed to move/copy file {} to {}, with error: {}",
                  &path.to_string_lossy(),
                  &dest.to_string_lossy(),
                  error
              )
            });
        });
        return if existed { Updated } else { Migrated };
    }
    Skipped
}

fn migrate_content(content: &str, dest: &Path, checksum: bool) -> MigrationResult {
    let existed = dest.exists();
    if should_migrate_content(&content, &dest, checksum) {
        create_parent_directories(&dest);
        let mut file = fs::File::create(&dest).unwrap();
        file.write_all(&content.as_bytes())
            .unwrap_or_else(|_| panic!("Failed to write to file {}", &dest.to_string_lossy()));
        return if existed { Updated } else { Migrated };
    }
    Skipped
}

// Migrates the given files, by either copying or moving.
pub fn migrate_files(files: &PathMap, copy: bool, checksum: bool) -> MigrationResults {
    // Move branch out of loop.
    let action = if copy {
        migrate_by_copy
    } else {
        migrate_by_move
    };
    info!("Migrating {} files.", files.len());
    let progress_bar = logger::progress_bar(files.len() as u64);
    let results: Vec<_> = files
        .par_iter()
        .map(|(src, dest)| {
            progress_bar.inc(1);
            action(&src, &dest, checksum)
        })
        .collect();
    MigrationResults::new(&results)
}

pub fn migrate_inline_content<F>(
    objects: &Vec<Box<Path>>,
    dest: &DatastreamPathMap,
    extract: F,
    checksum: bool,
) -> MigrationResults
where
    F: Fn(&Path) -> DatastreamContentMap + Sync + Send,
{
    let progress_bar = logger::progress_bar(dest.len() as u64);
    let results = objects
        .par_iter()
        .flat_map(|path| {
            let datastreams = extract(&path);
            datastreams
                .iter()
                .map(|(id, content)| {
                    progress_bar.inc(1);
                    migrate_content(content, &dest[id], checksum)
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    MigrationResults::new(&results)
}
