// Represents identifiers extracted from Fedora datastreamStore and objectStore folders.
// @see https://wiki.lyrasis.org/display/FEDORA35/Fedora+Identifiers
use log::warn;
use rayon::prelude::*;
use regex::Regex;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{atomic, Mutex};
use walkdir::WalkDir;

pub type Paths = Vec<Box<Path>>;
pub type PathMap = HashMap<Box<Path>, Box<Path>>;
pub type IdentifierPathMap<T> = BTreeMap<T, Box<Path>>;
pub type ObjectPathMap = BTreeMap<ObjectIdentifier, Box<Path>>;
pub type DatastreamPathMap = BTreeMap<DatastreamIdentifier, Box<Path>>;
pub type DatastreamContentMap = BTreeMap<DatastreamIdentifier, String>;
pub type FoxmlPathMap = BTreeMap<ObjectIdentifier, (Box<Path>, foxml::Foxml)>;
pub type FoxmlErrors = Vec<(Box<Path>, foxml::FoxmlError)>;

lazy_static! {
    // e.g info%3Afedora%2Farchden%3A13
    static ref OBJECT_FILE_REGEX: Regex = Regex::new(r"info%3Afedora%2F(.*)%3A(.*)").unwrap();
    // e.g info%3Afedora%2Farchden%3A13%2FTECHMD%2FTECHMD.0
    static ref DATASTREAM_FILE_REGEX: Regex = Regex::new(r"info%3Afedora%2F(.*)%3A(.*)%2F(.*)%2F(.*)").unwrap();
    // Map URL encoded strings that can be used in identifiers to their decoded values.
    static ref ENCODING: HashMap<&'static str, &'static str> = {
        let mut m = HashMap::new();
        m.insert("%5F", "_");
        m
    };
}

pub trait Identifier {
    type Item;
    fn from_path(path: &Path) -> Option<Self::Item>;
}

// Find all files recursively in the given folder.
pub fn files(path: &Path) -> Paths {
    let spinner = logger::spinner();
    let count = atomic::AtomicUsize::new(0);
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

// Returns a tuple consisting of a map of identifiers to paths.
pub fn identify_files<T>(path: &Path) -> IdentifierPathMap<T>
where
    T: Identifier<Item = T> + Ord + Sync + Send,
{
    let map = Mutex::new(BTreeMap::new());
    let failed = Mutex::new(Paths::new());
    files(&path)
        .into_par_iter()
        .for_each(|path| match T::from_path(&path) {
            Some(identifier) => {
                map.lock().unwrap().insert(identifier, path);
            }
            None => failed.lock().unwrap().push(path),
        });
    let unknown_files = failed.into_inner().unwrap();
    if !unknown_files.is_empty() {
        warn!(
            "The following files could not be identified:\n\t{}",
            unknown_files
                .iter()
                .map(|path| path.to_string_lossy())
                .collect::<Vec<_>>()
                .join("\n\t")
        )
    }
    map.into_inner().unwrap()
}

pub fn objects(files: Paths) -> FoxmlPathMap {
    let progress_bar = logger::progress_bar(files.len() as u64);
    let map = Mutex::new(FoxmlPathMap::new());
    let failed = Mutex::new(FoxmlErrors::new());
    files.into_par_iter().for_each(|path| {
        match foxml::Foxml::from_path(&path) {
            Ok(foxml) => {
                map.lock().unwrap().insert(
                    ObjectIdentifier {
                        pid: foxml.pid.clone(),
                    },
                    (path.to_owned(), foxml),
                );
            }
            Err(error) => failed.lock().unwrap().push((path.to_owned(), error)),
        }
        progress_bar.inc(1);
    });
    let failed = failed.into_inner().unwrap();
    if !failed.is_empty() {
        warn!(
            "The following Foxml files could not be parsed:\n\t{}",
            failed
                .into_iter()
                .map(|(path, error)| format!("{} => {}", path.to_string_lossy(), error))
                .collect::<Vec<_>>()
                .join("\n\t")
        );
    }
    map.into_inner().unwrap()
}

pub fn datastreams(
    objects: &FoxmlPathMap,
    group: foxml::FoxmlControlGroup,
    dest: &Path,
) -> DatastreamPathMap {
    objects
        .par_iter()
        .flat_map(|(_, (_, object))| {
            object
                .datastreams
                .par_iter()
                .filter(|datastream| datastream.control_group == group)
                .flat_map(|datastream| {
                    datastream
                        .versions
                        .par_iter()
                        .map(|version| {
                            let identifier = DatastreamIdentifier {
                                pid: object.pid.clone(),
                                dsid: datastream.id.clone(),
                                version: version.id.clone(),
                            };
                            // Some datastreams have an appropriate label like '01-01-1942_web.pdf', but
                            // others are things like 'MODS'. So we do a basic check to see if the version
                            // label appears to be a valid name with an known extension if so we use the label
                            // otherwise we generate one based on the the datastream.
                            let file_name = foxml::extensions::version_file_name(
                                &object.pid,
                                &version.id,
                                &version.label,
                                &version.mime_type,
                            );
                            let mut dest = PathBuf::from(dest);
                            dest.push(identifier.as_path());
                            dest.push(file_name);
                            (identifier, dest.into_boxed_path())
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        })
        .collect::<DatastreamPathMap>()
}

fn decode(s: &str) -> Cow<str> {
    ENCODING
        .iter()
        .fold(Cow::from(s), |s, (from, to)| s.replace(from, to).into())
}

#[derive(Eq)]
pub struct ObjectIdentifier {
    pub pid: String,
}

impl Identifier for ObjectIdentifier {
    type Item = ObjectIdentifier;

    fn from_path(path: &Path) -> Option<Self> {
        let file_name = path.file_name()?.to_str()?;
        let capture = OBJECT_FILE_REGEX.captures(file_name)?;
        let pid = format!(
            "{}:{}",
            decode(capture.get(1)?.as_str()),
            decode(capture.get(2)?.as_str())
        );
        Some(Self { pid })
    }
}

impl Hash for ObjectIdentifier {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.pid.hash(state);
    }
}

impl fmt::Display for ObjectIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.pid, f)
    }
}

impl Ord for ObjectIdentifier {
    fn cmp(&self, other: &Self) -> Ordering {
        alphanumeric_sort::compare_str(&self.pid, &other.pid)
    }
}

impl PartialOrd for ObjectIdentifier {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ObjectIdentifier {
    fn eq(&self, other: &Self) -> bool {
        self.pid == other.pid
    }
}

#[derive(Eq)]
pub struct DatastreamIdentifier {
    pub pid: String,
    pub dsid: String,
    pub version: String,
}

impl DatastreamIdentifier {
    fn as_path(&self) -> PathBuf {
        let mut path = PathBuf::new();
        path.push(&self.pid);
        path.push(&self.dsid);
        path.push(&self.version);
        path
    }
}

impl Identifier for DatastreamIdentifier {
    type Item = DatastreamIdentifier;

    fn from_path(path: &Path) -> Option<Self> {
        let file_name = path.file_name()?.to_str()?;
        let capture = DATASTREAM_FILE_REGEX.captures(file_name)?;
        let pid = format!(
            "{}:{}",
            decode(capture.get(1)?.as_str()),
            decode(capture.get(2)?.as_str())
        );
        let dsid = decode(capture.get(3)?.as_str()).into();
        let version = decode(capture.get(4)?.as_str()).into();
        Some(Self { pid, dsid, version })
    }
}

impl Hash for DatastreamIdentifier {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.pid.hash(state);
        self.dsid.hash(state);
        self.version.hash(state);
    }
}

impl<'a> fmt::Display for DatastreamIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.pid, self.dsid, self.version)
    }
}

impl Ord for DatastreamIdentifier {
    fn cmp(&self, other: &Self) -> Ordering {
        let result = alphanumeric_sort::compare_str(&self.pid, &other.pid);
        if result == Ordering::Equal {
            let result = alphanumeric_sort::compare_str(&self.dsid, &other.dsid);
            if result == Ordering::Equal {
                alphanumeric_sort::compare_str(&self.version, &other.version)
            } else {
                result
            }
        } else {
            result
        }
    }
}

impl PartialOrd for DatastreamIdentifier {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for DatastreamIdentifier {
    fn eq(&self, other: &Self) -> bool {
        self.pid == other.pid && self.dsid == other.dsid && self.version == other.version
    }
}
