extern crate chrono;
extern crate serde;

use super::object::*;
use chrono::{DateTime, FixedOffset};
use indicatif::ProgressBar;
use rayon::prelude::*;
use serde::Serialize;
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::path::{Path, PathBuf};
use strum::AsStaticRef;

lazy_static! {
    #[rustfmt::skip]
    static ref DSID_MAP: HashMap<&'static str, &'static str> = {
        let mut m = HashMap::new();
        m.insert("OCR", "extracted_text");
        m.insert("FULL_TEXT", "extracted_text");
        m.insert("TECHMD", "fits_technical_metadata");
        m
    };
    static ref MIME_TYPE_MAP: HashMap<&'static str, &'static str> = {
        let mut m = HashMap::new();
        m.insert("application/pdf", "document");
        m.insert("application/rdf+xml", "file");
        m.insert("application/xml", "file");
        m.insert("audio/aac", "audio");
        m.insert("audio/mpeg", "audio");
        m.insert("audio/wav", "audio");
        m.insert("image/gif", "image");
        m.insert("image/jp2", "file");
        m.insert("image/tiff", "file");
        m.insert("image/jpeg", "image");
        m.insert("image/jpg", "image");
        m.insert("image/png", "image");
        m.insert("text/plain", "document");
        m.insert("text/xml", "file");
        m.insert("video/mp4", "video");
        m
    };
    static ref MODEL_MAP: HashMap<&'static str, Model> = {
        let mut m = HashMap::new();
        m.insert("islandora:collectionCModel", Model::Collection);
        m.insert("islandora:sp_basic_image", Model::BasicImage);
        m.insert("islandora:sp_large_image_cmodel", Model::LargeImage);
        m.insert("islandora:sp-audioCModel", Model::Audio);
        m.insert("islandora:sp_videoCModel", Model::Video);
        m.insert("islandora:sp_pdf", Model::PDF);
        m.insert("islandora:bookCModel", Model::Book);
        m.insert("islandora:pageCModel", Model::Page);
        m.insert("islandora:newspaperCModel", Model::Newspaper);
        m.insert("islandora:newspaperIssueCModel", Model::NewspaperIssue);
        m.insert("islandora:newspaperPageCModel", Model::NewspaperPage);
        m.insert("islandora:compoundCModel", Model::Compound);
        m.insert("islandora:binaryCModel", Model::Binary);
        m.insert("islandora:binaryObjectCModel", Model::Binary);
        m
    };
}

#[derive(Clone)]
enum Model {
    Audio,
    BasicImage,
    Binary,
    Book,
    Collection,
    Compound,
    LargeImage,
    Newspaper,
    NewspaperIssue,
    NewspaperPage,
    Page,
    PDF,
    Video,
}

impl TryFrom<&str> for Model {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        MODEL_MAP
            .get(value)
            .cloned()
            .ok_or_else(|| format!("Unknown content model {}", value))
    }
}

impl Model {
    fn identifier(&self) -> &'static str {
        match self {
            Model::Audio => "http://purl.org/coar/resource_type/c_18cc",
            Model::BasicImage => "http://purl.org/coar/resource_type/c_c513",
            Model::Binary => "http://purl.org/coar/resource_type/c_1843",
            Model::Book => "https://schema.org/Book",
            Model::Collection => "http://purl.org/dc/dcmitype/Collection",
            Model::Compound => "http://purl.org/dc/dcmitype/Collection",
            Model::LargeImage => "http://purl.org/coar/resource_type/c_c513",
            Model::Newspaper => "https://schema.org/Book",
            Model::NewspaperIssue => "https://schema.org/PublicationIssue",
            Model::NewspaperPage => "http://id.loc.gov/ontologies/bibframe/part",
            Model::Page => "http://id.loc.gov/ontologies/bibframe/part",
            Model::PDF => "https://schema.org/DigitalDocument",
            Model::Video => "http://purl.org/coar/resource_type/c_12ce",
        }
    }
}

#[derive(Serialize)]
pub struct MediaRow<'a> {
    pid: &'a str,
    dsid: &'a str,
    version: &'a str,
    bundle: String,
    created_date: i64,
    file_size: u64,
    label: &'a str,
    mime_type: &'a str,
    name: String,
    user: &'a str,
}

impl<'a> MediaRow<'a> {
    fn new(tuple: (&'a Object, &'a Datastream, &'a DatastreamVersion)) -> Self {
        let (object, datastream, version) = tuple;
        let version_path = version.path();
        let version_exists = version_path.exists();
        MediaRow {
            pid: &object.pid.0,
            dsid: &datastream.id,
            version: &version.id,
            bundle: Self::bundle(&datastream, &version),
            created_date: format_date(&version.created_date),
            // When running locally we may not actually have the files,
            // in which case just do not calculate the file size.
            file_size: if version_exists {
                version_path.metadata().unwrap().len()
            } else {
                0
            },
            label: &version.label,
            mime_type: &version.mime_type,
            name: version
                .path()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            user: &object.owner,
        }
    }

    fn bundle(datastream: &Datastream, version: &DatastreamVersion) -> String {
        if let Some(&bundle) = DSID_MAP.get(&datastream.id.as_str()) {
            bundle.to_string()
        } else if let Some(&bundle) = MIME_TYPE_MAP.get(&version.mime_type.as_str()) {
            bundle.to_string()
        } else {
            "file".to_string() // Default to file for unknown mime-types / datastreams.
        }
    }

    pub fn csv(objects: &ObjectMap, dest: &Path, progress_bar: ProgressBar) {
        progress_bar.set_length(objects.latest_versions().count() as u64);
        let rows = objects
            .latest_versions()
            .map(|row| {
                progress_bar.inc(1);
                MediaRow::new(row)
            })
            .collect::<Vec<_>>();
        create_csv(&rows, &dest.join("media.csv")).expect("Failed to create media.csv");
        progress_bar.finish_with_message("Created media.csv");
    }

    pub fn revisions_csv(objects: &ObjectMap, dest: &Path, progress_bar: ProgressBar) {
        progress_bar.set_length(objects.previous_versions().count() as u64);
        let rows = objects
            .previous_versions()
            .map(|row| {
                progress_bar.inc(1);
                MediaRow::new(row)
            })
            .collect::<Vec<_>>();
        create_csv(&rows, &dest.join("media_revisions.csv"))
            .expect("Failed to create media_revisions.csv");
        progress_bar.finish_with_message("Created media_revisions.csv");
    }
}

#[derive(Serialize)]
pub struct FileRow<'a> {
    pid: &'a str,
    dsid: &'a str,
    version: &'a str,
    created_date: i64,
    mime_type: &'a str,
    name: String,
    path: String,
    user: &'a str,
    sha1: String,
    size: u64,
}

impl<'a> FileRow<'a> {
    fn new(tuple: (&'a Object, &'a Datastream, &'a DatastreamVersion)) -> Self {
        let (object, datastream, version) = tuple;
        let version_path = version.path();
        let version_exists = version_path.exists();
        let relative_path = version_path
            .components()
            .rev()
            .take(5)
            .collect::<Vec<_>>()
            .iter()
            .rev()
            .collect::<PathBuf>()
            .into_boxed_path();
        // Assume all files are in the private://fedora folder for now.
        let mut path = "private://fedora/".to_string();
        path.push_str(&relative_path.to_str().unwrap());
        FileRow {
            pid: &object.pid.0,
            dsid: &datastream.id,
            version: &version.id,
            created_date: format_date(&version.created_date),
            mime_type: &version.mime_type,
            name: version
                .path()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            user: &object.owner,
            path,
            // When running locally we may not actually have the files,
            // in which case just do not generate a sha-1 or calculate the file size.
            sha1: if version_exists {
                Self::sha1(&version_path)
            } else {
                "".to_string()
            },
            size: if version_exists {
                version_path.metadata().unwrap().len()
            } else {
                0
            },
        }
    }

    fn sha1(path: &Path) -> String {
        let mut file = std::fs::File::open(&path).unwrap();
        let mut hasher = Sha1::new();
        std::io::copy(&mut file, &mut hasher).unwrap();
        let hash = hasher.finalize();
        format!("{:x}", hash)
    }

    pub fn csv(objects: &ObjectMap, dest: &Path, progress_bar: ProgressBar) {
        progress_bar.set_length(objects.versions().count() as u64);
        let rows = objects
            .versions()
            .map(|row| {
                progress_bar.inc(1);
                FileRow::new(row)
            })
            .collect::<Vec<_>>();
        create_csv(&rows, &dest.join("files.csv")).expect("Failed to create files.csv");
        progress_bar.finish_with_message("Created files.csv");
    }
}

enum DisplayHint {
    None,
    OpenSeadragon,
    PdfJS,
}

impl DisplayHint {
    pub fn as_str(&self) -> &'static str {
        match *self {
            DisplayHint::None => "",
            DisplayHint::OpenSeadragon => "http://openseadragon.github.io",
            DisplayHint::PdfJS => "http://mozilla.github.io/pdf.js",
        }
    }
}

impl From<Model> for DisplayHint {
    fn from(model: Model) -> Self {
        match model {
            Model::LargeImage => DisplayHint::OpenSeadragon,
            Model::NewspaperPage => DisplayHint::OpenSeadragon,
            Model::Page => DisplayHint::OpenSeadragon,
            Model::PDF => DisplayHint::PdfJS,
            _ => DisplayHint::None,
        }
    }
}

#[derive(Serialize)]
pub struct NodeRow<'a> {
    pid: &'a str,
    created_date: i64,
    label: &'a str,
    weight: String,
    model: &'a str,
    modified_date: i64,
    state: &'a str,
    user: &'a str,
    display_hint: &'a str,
    parents: String,
}

impl<'a> NodeRow<'a> {
    fn new(object: &'a Object) -> Self {
        // Can panic but we shouldn't have any unknown content models in the
        // dataset, so just die here if the unlikely case occurs.
        let model = Model::try_from(object.model.as_str()).unwrap();

        NodeRow {
            pid: &object.pid.0,
            created_date: format_date(&object.created_date),
            label: &object.label,
            weight: object.weight.map_or("".to_string(), |w| w.to_string()),
            model: model.identifier(),
            modified_date: format_date(&object.modified_date),
            user: &object.owner,
            state: &object.state.as_static(),
            display_hint: DisplayHint::from(model).as_str(),
            parents: object.parents.join("|"),
        }
    }

    pub fn csv(objects: &ObjectMap, dest: &Path, progress_bar: ProgressBar) {
        progress_bar.set_length(objects.objects().count() as u64);
        let rows: Vec<_> = objects
            .objects()
            .map(|row| {
                progress_bar.inc(1);
                NodeRow::new(row)
            })
            .collect();
        create_csv(&rows, &dest.join("nodes.csv")).expect("Failed to create media_revisions.csv");
        progress_bar.finish_with_message("Created nodes.csv");
    }
}

pub fn create_csv<S>(rows: &[S], dest: &Path) -> Result<(), std::io::Error>
where
    S: Serialize,
{
    let builder = csv_other::WriterBuilder::new();
    let mut writer = builder.from_path(&dest)?;
    for row in rows {
        writer.serialize(row)?;
    }
    Ok(())
}

fn format_date(date_time: &DateTime<FixedOffset>) -> i64 {
    date_time.timestamp()
}
