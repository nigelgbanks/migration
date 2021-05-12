#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use super::utils::*;
use chrono::{DateTime, FixedOffset};
use foxml::*;
use log::info;
use quick_xml::events::attributes::Attribute;
use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;
use rayon::prelude::*;
use regex::Regex;
use std::boxed::Box;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

// Map specific fedora users to Drupal users for the migration.
lazy_static! {
    static ref USER_MAP: HashMap<&'static str, &'static str> = {
        let mut m = HashMap::new();
        m.insert("fedoraAdmin", "admin");
        m
    };
}

#[derive(Clone, Debug, Eq)]
pub struct Pid(pub String);

impl Pid {
    pub fn from_path(path: &Path) -> Pid {
        // Only use for Foxml files expected. eg. 'namespace:123.xml'
        Pid(path.file_stem().unwrap().to_string_lossy().to_string())
    }
}

impl Hash for Pid {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl fmt::Display for Pid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl From<String> for Pid {
    fn from(pid: String) -> Self {
        Pid(pid)
    }
}

impl Ord for Pid {
    fn cmp(&self, other: &Self) -> Ordering {
        alphanumeric_sort::compare_str(&self.0, &other.0)
    }
}

impl PartialOrd for Pid {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Pid {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

#[derive(AsStaticStr, Clone, Debug, Display, Eq, PartialEq)]
pub enum ObjectState {
    Active,
    Inactive,
    Deleted,
}

impl From<FoxmlObjectState> for ObjectState {
    fn from(state: FoxmlObjectState) -> Self {
        match state {
            FoxmlObjectState::Active => ObjectState::Active,
            FoxmlObjectState::Inactive => ObjectState::Inactive,
            FoxmlObjectState::Deleted => ObjectState::Deleted,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DatastreamState {
    Active,
    Inactive,
    Deleted,
}

impl From<FoxmlDatastreamState> for DatastreamState {
    fn from(state: FoxmlDatastreamState) -> Self {
        match state {
            FoxmlDatastreamState::A => DatastreamState::Active,
            FoxmlDatastreamState::I => DatastreamState::Inactive,
            FoxmlDatastreamState::D => DatastreamState::Deleted,
        }
    }
}

#[derive(Clone, Debug, Eq)]
pub struct DatastreamVersion {
    pub pid: String,
    pub dsid: String,
    pub id: String,
    pub label: String,
    pub created_date: DateTime<FixedOffset>,
    pub mime_type: String,
}

impl DatastreamVersion {
    pub fn new(pid: String, dsid: String, version: FoxmlDatastreamVersion) -> Self {
        DatastreamVersion {
            pid,
            dsid,
            id: version.id,
            label: version.label,
            created_date: version.created,
            mime_type: version.mime_type,
        }
    }

    pub fn file_name(&self) -> String {
        foxml::extensions::version_file_name(&self.pid, &self.id, &self.label, &self.mime_type)
    }

    pub fn path(&self) -> PathBuf {
        let lock = super::DATASTREAMS_DIRECTORY.read().unwrap();
        let root = lock.as_ref().unwrap();
        root.join(&self.pid)
            .join(&self.dsid)
            .join(&self.id)
            .join(self.file_name())
    }
}

impl Ord for DatastreamVersion {
    fn cmp(&self, other: &Self) -> Ordering {
        alphanumeric_sort::compare_str(&self.id, &other.id)
    }
}

impl PartialOrd for DatastreamVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Not really guaranteed across objects, but we never compare versions across
// objects as that doesn't really make sense.
impl PartialEq for DatastreamVersion {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

#[derive(Clone, Debug, Eq)]
pub struct Datastream {
    pub id: String,
    pub state: DatastreamState,
    pub versions: Vec<DatastreamVersion>,
}

impl Datastream {
    pub fn latest(&self) -> &DatastreamVersion {
        self.versions.last().unwrap()
    }
}

impl Ord for Datastream {
    fn cmp(&self, other: &Self) -> Ordering {
        alphanumeric_sort::compare_str(&self.id, &other.id)
    }
}

impl PartialOrd for Datastream {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Datastream {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

#[derive(Debug)]
pub enum RelsExtError {
    IOError(std::io::Error),         // Could not read file.
    QuickXMLError(quick_xml::Error), // Wrap QuickXML error.
}

impl From<std::io::Error> for RelsExtError {
    fn from(error: std::io::Error) -> Self {
        RelsExtError::IOError(error)
    }
}

impl From<quick_xml::Error> for RelsExtError {
    fn from(error: quick_xml::Error) -> Self {
        RelsExtError::QuickXMLError(error)
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct RelsExt {
    pub about: String,
    // Fedora Model Rels-Ext Ontology
    // https://github.com/fcrepo3/fcrepo/blob/master/fcrepo-server/src/main/resources/utilities/server/org/fcrepo/server/resources/fedora-system_FedoraObject-3.0.xml#L44-L72
    pub hasModel: Vec<String>,
    // Fedora Rels-Ext Ontology
    // https://github.com/fcrepo3/fcrepo/blob/master/fcrepo-server/src/main/resources/rdfs/fedora_relsext_ontology.rdfs
    pub fedoraRelationship: Vec<String>,
    pub hasAnnotation: Vec<String>,
    pub hasCollectionMember: Vec<String>,
    pub hasConstituent: Vec<String>,
    pub hasDependent: Vec<String>,
    pub hasDerivation: Vec<String>,
    pub hasDescription: Vec<String>,
    pub hasEquivalent: Vec<String>,
    pub hasMember: Vec<String>,
    pub hasMetadata: Vec<String>,
    pub hasPart: Vec<String>,
    pub hasSubset: Vec<String>,
    pub isAnnotationOf: Vec<String>,
    pub isConstituentOf: Vec<String>,
    pub isDependentOf: Vec<String>,
    pub isDerivationOf: Vec<String>,
    pub isDescriptionOf: Vec<String>,
    pub isMemberOf: Vec<String>,
    pub isMemberOfCollection: Vec<String>,
    pub isMetadataFor: Vec<String>,
    pub isPartOf: Vec<String>,
    pub isSubsetOf: Vec<String>,
    // Islandora Rels-Ext Ontology
    pub deferDerivatives: Option<bool>,
    pub generateHOCR: Option<bool>,
    pub generateOCR: Option<bool>,
    pub isPageNumber: Option<isize>,
    pub isPageOf: Option<String>,
    pub isSection: Option<isize>,
    pub isSequenceNumber: Option<isize>,
    pub isSequenceNumberOf: Vec<(String, isize)>,
}

impl RelsExt {
    // Strip the prefix off of applicable values.
    const PREFIX_LENGTH: usize = "info:fedora/".len();

    pub fn from_reader<B>(mut reader: Reader<B>) -> Result<Self, RelsExtError>
    where
        B: BufRead,
    {
        let mut rels_ext = RelsExt::default();
        let mut buffer = Vec::new();
        loop {
            match reader.read_event(&mut buffer)? {
                Event::Start(element) | Event::Empty(element) => {
                    Self::process_element(&mut rels_ext, &mut reader, &element)
                }
                Event::Eof => break,
                // We ignore Comments, CData, XML Declaration,
                // Processing Instructions, and DocType elements.
                _ => (),
            };
            // We have to clone to pass the data to the script so no point in maintaining reference to the string content.
            buffer.clear();
        }
        Ok(rels_ext)
    }

    #[cfg(test)]
    pub fn from_string(xml: &str) -> Result<Self, RelsExtError> {
        let reader = Reader::from_str(&xml);
        Ok(RelsExt::from_reader(reader)?)
    }

    pub fn from_path(path: &Path) -> Result<Self, RelsExtError> {
        let file = File::open(&path)?;
        let reader = Reader::from_reader(BufReader::new(&file));
        Ok(RelsExt::from_reader(reader)?)
    }

    fn process_element<B>(rels_ext: &mut RelsExt, mut reader: &mut Reader<B>, element: &BytesStart)
    where
        B: BufRead,
    {
        match element.name() {
            b"rdf:Description" => {
                rels_ext.about = Self::get_attribute_without_prefix(&element, b"rdf:about");
            }
            // Fedora Model Rels-Ext Ontology
            b"fedora-model:hasModel" => {
                rels_ext
                    .hasModel
                    .push(Self::get_resource_attribute(&element));
            }
            // Fedora Rels-Ext Ontology
            b"fedora:fedoraRelationship" => {
                rels_ext
                    .fedoraRelationship
                    .push(Self::get_resource_attribute(&element));
            }
            b"fedora:isPartOf" => {
                rels_ext
                    .isPartOf
                    .push(Self::get_resource_attribute(&element));
            }
            b"fedora:hasPart" => {
                rels_ext
                    .hasPart
                    .push(Self::get_resource_attribute(&element));
            }
            b"fedora:isConstituentOf" => {
                rels_ext
                    .isConstituentOf
                    .push(Self::get_resource_attribute(&element));
            }
            b"fedora:hasConstituent" => {
                rels_ext
                    .hasConstituent
                    .push(Self::get_resource_attribute(&element));
            }
            b"fedora:isMemberOf" => {
                rels_ext
                    .isMemberOf
                    .push(Self::get_resource_attribute(&element));
            }
            b"fedora:hasMember" => {
                rels_ext
                    .hasMember
                    .push(Self::get_resource_attribute(&element));
            }
            b"fedora:isSubsetOf" => {
                rels_ext
                    .isSubsetOf
                    .push(Self::get_resource_attribute(&element));
            }
            b"fedora:hasSubset" => {
                rels_ext
                    .hasSubset
                    .push(Self::get_resource_attribute(&element));
            }
            b"fedora:isMemberOfCollection" => {
                rels_ext
                    .isMemberOfCollection
                    .push(Self::get_resource_attribute(&element));
            }
            b"fedora:hasCollectionMember" => {
                rels_ext
                    .hasCollectionMember
                    .push(Self::get_resource_attribute(&element));
            }
            b"fedora:isDerivationOf" => {
                rels_ext
                    .isDerivationOf
                    .push(Self::get_resource_attribute(&element));
            }
            b"fedora:hasDerivation" => {
                rels_ext
                    .hasDerivation
                    .push(Self::get_resource_attribute(&element));
            }
            b"fedora:isDependentOf" => {
                rels_ext
                    .isDependentOf
                    .push(Self::get_resource_attribute(&element));
            }
            b"fedora:hasDependent" => {
                rels_ext
                    .hasDependent
                    .push(Self::get_resource_attribute(&element));
            }
            b"fedora:isDescriptionOf" => {
                rels_ext
                    .isDescriptionOf
                    .push(Self::get_resource_attribute(&element));
            }
            b"fedora:hasDescription" => {
                rels_ext
                    .hasDescription
                    .push(Self::get_resource_attribute(&element));
            }
            b"fedora:isMetadataFor" => {
                rels_ext
                    .isMetadataFor
                    .push(Self::get_resource_attribute(&element));
            }
            b"fedora:hasMetadata" => {
                rels_ext
                    .hasMetadata
                    .push(Self::get_resource_attribute(&element));
            }
            b"fedora:isAnnotationOf" => {
                rels_ext
                    .isAnnotationOf
                    .push(Self::get_resource_attribute(&element));
            }
            b"fedora:hasAnnotation" => {
                rels_ext
                    .hasAnnotation
                    .push(Self::get_resource_attribute(&element));
            }
            b"fedora:hasEquivalent" => {
                rels_ext
                    .hasEquivalent
                    .push(Self::get_resource_attribute(&element));
            }
            // Islandora Rels-Ext Ontology
            b"islandora:deferDerivatives" => {
                let text = Self::get_text(&mut reader).to_lowercase();
                rels_ext.deferDerivatives = Some(text.parse().unwrap());
            }
            b"islandora:generate_hocr" => {
                let text = Self::get_text(&mut reader).to_lowercase();
                rels_ext.generateHOCR = Some(text.parse().unwrap());
            }
            b"islandora:generate_ocr" => {
                let text = Self::get_text(&mut reader).to_lowercase();
                rels_ext.generateOCR = Some(text.parse().unwrap());
            }
            b"islandora:isPageNumber" => {
                let text = Self::get_text(&mut reader);
                rels_ext.isPageNumber = Self::parse_integer(text);
            }
            b"islandora:isPageOf" => {
                let attribute = Self::get_resource_attribute(&element);
                rels_ext.isPageOf = Some(attribute);
            }
            b"islandora:isSection" => {
                let text = Self::get_text(&mut reader);
                rels_ext.isSection = Self::parse_integer(text);
            }
            b"islandora:isSequenceNumber" => {
                let text = Self::get_text(&mut reader);
                rels_ext.isSequenceNumber = Self::parse_integer(text);
            }
            _ => {
                // Compounds are weird.
                if let Some(sequence_number) = Self::is_sequence_number_of(&mut reader, &element) {
                    rels_ext.isSequenceNumberOf.push(sequence_number);
                }
            }
        };
    }

    fn parse_integer(text: String) -> Option<isize> {
        let re = Regex::new(r"[^0-9]").unwrap();
        re.replace_all(&text, "").parse().ok()
    }

    // Get an attribute with the given name if it exists.
    fn get_attribute<'a>(element: &'a BytesStart, name: &[u8]) -> Option<Attribute<'a>> {
        let mut attributes = element.attributes().filter_map(|x| x.ok());
        attributes.find(|attribute| attribute.key == name)
    }

    // Get attribute value or panics.
    fn get_attribute_without_prefix(element: &BytesStart, name: &[u8]) -> String {
        let attribute = Self::get_attribute(&element, name).unwrap();
        String::from_utf8(attribute.value.as_ref()[Self::PREFIX_LENGTH..].to_vec()).unwrap()
    }

    fn get_resource_attribute(element: &BytesStart) -> String {
        Self::get_attribute_without_prefix(&element, b"rdf:resource")
    }

    fn get_text<B>(reader: &mut Reader<B>) -> String
    where
        B: BufRead,
    {
        let mut buffer = Vec::new();
        loop {
            let event = reader.read_event(&mut buffer).unwrap();
            if let Event::Text(e) = event {
                let bytes = &e.unescaped().unwrap();
                let s = std::str::from_utf8(bytes).unwrap().to_string();
                if !s.trim().is_empty() {
                    return s;
                }
            } else if let Event::Eof = event {
                panic!("Prevent infinite loop... though this should never be reached with valid RELS-EXT.");
            }
        }
    }

    // Compounds.
    fn is_sequence_number_of<B>(
        mut reader: &mut Reader<B>,
        element: &BytesStart,
    ) -> Option<(String, isize)>
    where
        B: BufRead,
    {
        let name = std::str::from_utf8(element.local_name())
            .unwrap()
            .to_string();
        let predicate = "isSequenceNumberOf";
        if name.starts_with(predicate) {
            let pid = &name[predicate.len()..];
            let pid = pid.replacen("_", ":", 1);
            let text = Self::get_text(&mut reader);
            Some((pid, Self::parse_integer(text).unwrap_or(0)))
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, Eq)]
pub struct Object {
    pub pid: Pid,
    pub state: ObjectState,
    pub owner: String,
    pub label: String,
    pub model: String,
    pub parents: Vec<String>,
    pub created_date: DateTime<FixedOffset>,
    pub modified_date: DateTime<FixedOffset>,
    pub datastreams: Vec<Datastream>,
    pub weight: Option<isize>,
}

impl Object {
    pub fn new(foxml: Foxml) -> Self {
        let pid = foxml.pid.clone();
        let mut object = Object {
            pid: Pid(foxml.pid.to_owned()),
            // Map to the appropriate Drupal user if applicable.
            owner: USER_MAP
                .get(&foxml.properties.owner_id().as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| foxml.properties.owner_id()),
            label: foxml.properties.label(),
            model: "".to_string(),
            parents: vec![],
            weight: None,
            created_date: foxml.properties.created_date(),
            modified_date: foxml.properties.modified_date(),
            state: foxml.properties.state().into(),
            datastreams: {
                let mut datastreams = foxml
                    .datastreams
                    .into_iter()
                    .map(move |datastream| match datastream.control_group {
                        FoxmlControlGroup::E | FoxmlControlGroup::R => unimplemented!(),
                        FoxmlControlGroup::M | FoxmlControlGroup::X => {
                            Object::create_datastream(&pid, datastream)
                        }
                    })
                    .collect::<Vec<Datastream>>();
                datastreams.sort_by(|a, b| a.partial_cmp(b).unwrap());
                datastreams
            },
        };
        if let Some(rels_ext) = object.rels_ext() {
            object.model = Object::model(&rels_ext);
            object.parents = Object::parents(&rels_ext);
            object.weight = Object::weight(&rels_ext);
        } else {
            // No RELS-EXT.
            object.model = String::from("");
            object.parents = vec![];
            object.weight = None;
        }
        object
    }

    pub fn from_path(path: &Path) -> Result<Self, FoxmlError> {
        let foxml = std::fs::read_to_string(&path)
            .unwrap_or_else(|_| panic!("Failed to read file: {}", &path.to_string_lossy()));
        let foxml = Foxml::new(&foxml)?;
        Ok(Object::new(foxml))
    }

    pub fn missing_content_model(&self) -> bool {
        self.model.is_empty()
    }

    pub fn is_system_object(&self) -> bool {
        self.pid.0.starts_with("fedora-system:")
    }

    pub fn is_content_model(&self) -> bool {
        self.model == "fedora-system:ContentModel-3.0"
    }

    fn model(rels_ext: &RelsExt) -> String {
        if rels_ext.hasModel.is_empty() {
            dbg!(&rels_ext);
        }
        rels_ext.hasModel.first().unwrap().into()
    }

    fn parents(rels_ext: &RelsExt) -> Vec<String> {
        // isSequenceNumberOf relationship is covered by isConstituentOf.
        let parents = vec![
            &rels_ext.isPartOf,
            &rels_ext.isConstituentOf,
            &rels_ext.isMemberOf,
            &rels_ext.isSubsetOf,
            &rels_ext.isMemberOfCollection,
            &rels_ext.isDerivationOf,
            &rels_ext.isDependentOf,
            &rels_ext.isDescriptionOf,
            &rels_ext.isMetadataFor,
            &rels_ext.isAnnotationOf,
        ];
        let size = parents.iter().fold(0, |a, b| a + b.len());
        let mut parents = parents
            .into_iter()
            .fold(Vec::with_capacity(size), |mut acc, v| {
                acc.extend(v.clone());
                acc
            })
            .into_iter()
            .map(|parent| parent)
            .collect::<Vec<String>>();
        parents.sort_by(|a, b| alphanumeric_sort::compare_str(&a, &b));
        parents
    }

    // Drupal 8 supports multiple parents but only a single weight!
    fn weight(rels_ext: &RelsExt) -> Option<isize> {
        if rels_ext.isPageNumber.is_some() {
            rels_ext.isPageNumber
        } else if rels_ext.isSequenceNumber.is_some() {
            rels_ext.isSequenceNumber
        } else if let Some((_, weight)) = rels_ext.isSequenceNumberOf.first() {
            Some(*weight)
        } else {
            None
        }
    }

    // Gets the latest version of the request datastream.
    pub fn datastream<'a>(&'a self, datastream_id: &str) -> Option<&'a DatastreamVersion> {
        if let Some(datastream) = self
            .datastreams
            .iter()
            .find(|datastream| datastream.id == datastream_id)
        {
            Some(datastream.latest())
        } else {
            None
        }
    }

    fn rels_ext(&self) -> Option<RelsExt> {
        let rels_ext = self
            .datastreams
            .iter()
            .find(|&datastream| datastream.id == "RELS-EXT");
        if let Some(datastream) = rels_ext {
            let latest_version = datastream.versions.last().unwrap();
            Some(RelsExt::from_path(&latest_version.path()).expect("Failed to parse RELS-EXT"))
        } else {
            None
        }
    }

    fn create_datastream(pid: &str, datastream: FoxmlDatastream) -> Datastream {
        let dsid = datastream.id.clone();
        Datastream {
            id: datastream.id,
            state: datastream.state.into(),
            versions: {
                let mut result = datastream
                    .versions
                    .into_iter()
                    .map(move |version| {
                        DatastreamVersion::new(pid.to_string(), dsid.clone(), version)
                    })
                    .collect::<Vec<DatastreamVersion>>();
                result.sort_by(|a, b| a.partial_cmp(b).unwrap());
                result
            },
        }
    }
}

impl Ord for Object {
    fn cmp(&self, other: &Self) -> Ordering {
        self.pid.cmp(&other.pid)
    }
}

impl PartialOrd for Object {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.pid.partial_cmp(&other.pid)
    }
}

impl PartialEq for Object {
    fn eq(&self, other: &Self) -> bool {
        self.pid == other.pid
    }
}

// Sorted map of pids to objects.
pub type ObjectMapInner = BTreeMap<Pid, Object>;
pub struct ObjectMap(ObjectMapInner);

pub trait VersionIterator<'a>:
    ParallelIterator<Item = (&'a Object, &'a Datastream, &'a DatastreamVersion)>
{
}

impl<'a, T: ParallelIterator<Item = (&'a Object, &'a Datastream, &'a DatastreamVersion)>>
    VersionIterator<'a> for T
{
}

impl ObjectMap {
    pub fn from_path(input: &Path, limit_to_pids: Vec<&str>) -> Self {
        let object_paths = Self::object_files(&input, limit_to_pids);
        info!("Parsing object files");
        let progress_bar = logger::progress_bar(object_paths.len() as u64);
        let inner = object_paths
            .par_iter()
            .map(|path| {
                let object = Object::from_path(&path)?;
                progress_bar.inc(1);
                Ok((object.pid.clone(), object))
            })
            // Ignore system objects & content models, keep any errors to be dealt with later.
            .filter(|result| {
                result
                    .as_ref()
                    .map(|(_, object)| {
                        !(object.is_system_object()
                            || object.is_content_model()
                            || object.missing_content_model())
                    })
                    .map_err(|_| true)
                    .unwrap()
            })
            .collect::<Result<ObjectMapInner, FoxmlError>>()
            .expect("Failed to parse object files.");
        Self(inner)
    }

    pub fn inner(&self) -> &ObjectMapInner {
        &self.0
    }

    pub fn objects(&self) -> impl ParallelIterator<Item = &Object> {
        self.0.par_iter().map(|(_, v)| v)
    }

    fn datastreams(&self) -> impl ParallelIterator<Item = (&Object, &Datastream)> {
        self.objects().flat_map(|object| {
            object
                .datastreams
                .par_iter()
                .map(move |datastream| (object, datastream))
        })
    }

    pub fn versions(&self) -> impl VersionIterator {
        self.datastreams().flat_map(|(object, datastream)| {
            datastream
                .versions
                .par_iter()
                .map(move |version| (object, datastream, version))
        })
    }

    pub fn latest_versions(&self) -> impl VersionIterator {
        self.datastreams().map(|(object, datastream)| {
            let version = datastream.versions.last().unwrap();
            (object, datastream, version)
        })
    }

    pub fn previous_versions(&self) -> impl VersionIterator {
        self.datastreams().flat_map(|(object, datastream)| {
            datastream
                .versions
                .par_iter()
                .rev()
                .skip(1)
                .rev()
                .map(move |version| (object, datastream, version))
        })
    }

    // Enumerate object files, if limit_to_pids is non-empty restrict the files to just those whose PID matches entries in the given list.
    fn object_files(directory: &Path, limit_to_pids: Vec<&str>) -> Vec<Box<Path>> {
        let files = files(&directory.join("objects"));
        if limit_to_pids.is_empty() {
            files
        } else {
            files
                .into_par_iter()
                .filter(|path| limit_to_pids.contains(&Pid::from_path(&path).0.as_str()))
                .collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_rels_ext() {
        let content = r#"
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" 
xmlns:fedora="info:fedora/fedora-system:def/relations-external#" 
xmlns:fedora-model="info:fedora/fedora-system:def/model#" 
xmlns:islandora="http://islandora.ca/ontology/relsext#">
    <rdf:Description rdf:about="info:fedora/namespace:123">
        <fedora-model:hasModel rdf:resource="info:fedora/islandora:pageCModel"></fedora-model:hasModel>
        <fedora:isMemberOfCollection rdf:resource="info:fedora/namespace:456"></fedora:isMemberOfCollection>
        <fedora:isMemberOfCollection rdf:resource="info:fedora/namespace:789"></fedora:isMemberOfCollection>
        <fedora:isMemberOf rdf:resource="info:fedora/namespace:111"></fedora:isMemberOf>
        <islandora:deferDerivatives>true</islandora:deferDerivatives>
        <islandora:isSequenceNumberOfnamespace_100>321</islandora:isSequenceNumberOfnamespace_100>
        <islandora:isSequenceNumberOfnamespace_101>654</islandora:isSequenceNumberOfnamespace_101>
        <islandora:isPageOf rdf:resource="info:fedora/namespace:101"></islandora:isPageOf>
        <islandora:isSequenceNumber>001a</islandora:isSequenceNumber>
        <islandora:isPageNumber>2</islandora:isPageNumber>
        <islandora:isSection>1</islandora:isSection>
        <islandora:generate_ocr>TRUE</islandora:generate_ocr>
        <islandora:generate_hocr>TRUE</islandora:generate_hocr>
    </rdf:Description>
</rdf:RDF>
"#;
        let expected = RelsExt {
            about: "namespace:123".to_string(),
            isMemberOfCollection: vec!["namespace:456".to_string(), "namespace:789".to_string()],
            deferDerivatives: Some(true),
            isMemberOf: vec!["namespace:111".to_string()],
            hasModel: vec!["islandora:pageCModel".to_string()],
            isSequenceNumberOf: vec![
                ("namespace:100".to_string(), 321),
                ("namespace:101".to_string(), 654),
            ], // Compound.
            isPageOf: Some("namespace:101".to_string()),
            isPageNumber: Some(2),
            isSection: Some(1),
            isSequenceNumber: Some(1),
            generateOCR: Some(true),
            generateHOCR: Some(true),
            ..RelsExt::default()
        };
        let result = RelsExt::from_string(&content);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected);
    }
}
