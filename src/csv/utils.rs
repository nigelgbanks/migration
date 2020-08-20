use log::info;
use rayon::prelude::*;
use std::path::Path;
use std::sync::atomic;
use walkdir::WalkDir;

// Find all files recursively in the given folder.
pub fn files(path: &Path) -> Vec<Box<Path>> {
    let spinner = logger::spinner();
    let count = atomic::AtomicUsize::new(0);
    info!("Enumerating files at: {}", path.display());
    WalkDir::new(&path)
        .follow_links(false)
        .into_iter()
        .par_bridge()
        .filter(|entry| {
            entry
                .as_ref()
                .map_or(false, |e| e.metadata().map_or(false, |m| m.is_file()))
        })
        .map(|entry| {
            count.fetch_add(1, atomic::Ordering::Relaxed);
            spinner.set_message(&format!("Found: {}", count.load(atomic::Ordering::Relaxed)));
            Ok(entry?.path().canonicalize()?.into_boxed_path())
        })
        .collect::<Result<Vec<_>, std::io::Error>>()
        .unwrap_or_else(|error| {
            panic!(
                "Failed to find files in path: {}. Error: {}",
                &path.to_string_lossy(),
                error
            )
        })
}
