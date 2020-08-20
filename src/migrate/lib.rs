// Copy / Move the data from the fedora installation into a structure that can
// be copied directly into a Drupal file folder.
#[macro_use]
extern crate lazy_static;

mod extensions;
mod identifiers;
mod inline;
mod migrate;

use crate::migrate::*;
use foxml::FoxmlControlGroup;
use identifiers::*;
use log::*;
use rayon::prelude::*;
use std::collections::HashSet;
use std::path::Path;

static OBJECT_STORE: &str = "data/objectStore";
static DATASTREAM_STORE: &str = "data/datastreamStore";

fn migrate_object_files(
    src: &Path,
    dest: &Path,
    copy: bool,
    checksum: bool,
) -> identifiers::FoxmlPathMap {
    info!("Searching Fedora for object files");
    let object_files: ObjectPathMap = identify_files(&src);

    // Map source files to destination files.
    let identified_files = object_files
        .into_par_iter()
        .map(|(identifier, src)| {
            let file_name = format!("{}.xml", identifier.pid);
            let dest = dest.join(&file_name);
            (src, dest.into_boxed_path())
        })
        .collect::<identifiers::PathMap>();

    let results = migrate_files(&identified_files, copy, checksum);
    info!("Finished migrating object files: {}", results);

    info!("Building list of migrated object files.");
    let object_files = files(&dest);

    // Validate that the migrated files can be deserialized to Foxml object prior to migrating.
    info!("Parsing {} object files.", object_files.len());
    objects(object_files)
}

fn migrate_managed_datastreams(
    objects: &FoxmlPathMap,
    src: &Path,
    dest: &Path,
    copy: bool,
    checksum: bool,
) {
    info!("Searching Fedora datastream store for files.");
    let files: DatastreamPathMap = identify_files(&src);

    // All managed datastreams referenced in object files.
    // May be more/less than files in the datastreamStore folder.
    let managed_datastreams = datastreams(&objects, FoxmlControlGroup::M, &dest);

    info!(
        "Found {} managed datastreams in Fedora, with {} referenced by object files.",
        files.len(),
        managed_datastreams.len()
    );

    // Files that exit but are not referenced by Foxml.
    let unreferenced = {
        let src: HashSet<_> = files.keys().collect();
        let dest: HashSet<_> = managed_datastreams.keys().collect();
        // Source files which a object reference exists.
        src.difference(&dest).cloned().collect::<Vec<_>>()
    };

    if !unreferenced.is_empty() {
        warn!(
            "The following managed datastreams have been orphaned:\n\t{}",
            unreferenced
                .into_iter()
                .map(|identifier| identifier.to_string())
                .collect::<Vec<_>>()
                .join("\n\t")
        )
    }

    // Files to migrate.
    let files = {
        let src: HashSet<_> = files.keys().collect();
        let dest: HashSet<_> = managed_datastreams.keys().collect();
        // Source files which a object reference exists.
        src.intersection(&dest)
            .par_bridge()
            .map(|key| (files[&key].clone(), managed_datastreams[&key].clone()))
            .collect::<PathMap>()
    };

    info!("Migrating {} managed datastreams.", files.len());
    let results = migrate_files(&files, copy, checksum);
    info!("Finished migrating managed datastreams: {}", results);
}

pub fn migrate_data_from_fedora(
    fedora_directory: &Path,
    output_directory: &Path,
    copy: bool,
    checksum: bool,
) {
    info!(
        "Migrating Fedora data from {} to {}.",
        &fedora_directory.to_string_lossy(),
        &output_directory.to_string_lossy()
    );
    let objects = migrate_object_files(
        &fedora_directory.join(OBJECT_STORE),
        &output_directory.join("objects"),
        copy,
        checksum,
    );
    let datastreams_directory = output_directory.join("datastreams");
    migrate_managed_datastreams(
        &objects,
        &fedora_directory.join(DATASTREAM_STORE),
        &datastreams_directory,
        copy,
        checksum,
    );
    inline::migrate_inline_datastreams(&objects, &datastreams_directory, checksum);

    info!("Enumerating all migrated datastreams.");
    info!(
        "In total {} objects, and {} datastreams have been migrated",
        objects.len(),
        identifiers::files(&datastreams_directory).len()
    );
}

pub fn valid_fedora_directory(path: &Path) -> Result<(), String> {
    fn valid_directory(path: &Path) -> Result<(), String> {
        if path.is_dir() {
            Ok(())
        } else {
            Err(format!("The directory '{}' does not exist", path.display()))
        }
    }
    valid_directory(&path)?;
    valid_directory(&path.join(OBJECT_STORE))?;
    valid_directory(&path.join(DATASTREAM_STORE))?;
    Ok(())
}
