extern crate chrono;
extern crate serde;

use super::object::*;
use chrono::{DateTime, FixedOffset};
use log::info;
use serde::Serialize;
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
        m.insert("image/jp2", "image");
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
    created_date: String,
    file_size: u64,
    label: &'a str,
    mime_type: &'a str,
    name: String,
    user: &'a str,
}

impl<'a> MediaRow<'a> {
    fn new(tuple: (&'a Object, &'a Datastream, &'a DatastreamVersion)) -> Self {
        let (object, datastream, version) = tuple;
        MediaRow {
            pid: &object.pid.0,
            dsid: &datastream.id,
            version: &version.id,
            bundle: Self::bundle(&datastream, &version),
            created_date: format_date(&version.created_date),
            file_size: version.path.metadata().unwrap().len(),
            label: &version.label,
            mime_type: &version.mime_type,
            name: version
                .path
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

    pub fn csv(objects: &ObjectMap, dest: &Path) {
        info!("Generating media.csv");
        let rows = objects
            .latest_versions()
            .map(MediaRow::new)
            .collect::<Vec<_>>();
        create_csv(&rows, &dest.join("media.csv")).expect("Failed to create media.csv");
    }

    pub fn revisions_csv(objects: &ObjectMap, dest: &Path) {
        info!("Generating media_revisions.csv");
        let rows = objects
            .previous_versions()
            .map(MediaRow::new)
            .collect::<Vec<_>>();
        create_csv(&rows, &dest.join("media_revisions.csv"))
            .expect("Failed to create media_revisions.csv");
    }
}

#[derive(Serialize)]
pub struct FileRow<'a> {
    pid: &'a str,
    dsid: &'a str,
    version: &'a str,
    created_date: String,
    mime_type: &'a str,
    name: String,
    path: Box<Path>,
    user: &'a str,
}

impl<'a> FileRow<'a> {
    fn new(tuple: (&'a Object, &'a Datastream, &'a DatastreamVersion)) -> Self {
        let (object, datastream, version) = tuple;
        let path = version
            .path
            .components()
            .rev()
            .take(5)
            .collect::<Vec<_>>()
            .iter()
            .rev()
            .collect::<PathBuf>()
            .into_boxed_path();
        FileRow {
            pid: &object.pid.0,
            dsid: &datastream.id,
            version: &version.id,
            created_date: format_date(&version.created_date),
            mime_type: &version.mime_type,
            name: version
                .path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            user: &object.owner,
            path,
        }
    }

    pub fn csv(objects: &ObjectMap, dest: &Path) {
        info!("Generating files.csv");
        let rows = objects.versions().map(FileRow::new).collect::<Vec<_>>();
        create_csv(&rows, &dest.join("files.csv")).expect("Failed to create files.csv");
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
    created_date: String,
    label: &'a str,
    model: &'a str,
    modified_date: String,
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
            model: model.identifier(),
            modified_date: format_date(&object.modified_date),
            user: &object.owner,
            state: &object.state.as_static(),
            display_hint: DisplayHint::from(model).as_str(),
            parents: object.parents.join("|"),
        }
    }

    pub fn csv(objects: &ObjectMap, dest: &Path) {
        info!("Generating nodes.csv");
        let rows: Vec<_> = objects.objects().map(NodeRow::new).collect();
        create_csv(&rows, &dest.join("nodes.csv")).expect("Failed to create media_revisions.csv");
    }
}

fn create_csv<S>(rows: &[S], dest: &Path) -> Result<(), std::io::Error>
where
    S: Serialize,
{
    let builder = csv::WriterBuilder::new();
    let mut writer = builder.from_path(&dest)?;
    for row in rows {
        writer.serialize(row)?;
    }
    Ok(())
}

fn format_date(date_time: &DateTime<FixedOffset>) -> String {
    date_time.format("%Y-%m-%dT%H:%M:%S%:z").to_string()
}
