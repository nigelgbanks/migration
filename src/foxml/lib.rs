// @see https://wiki.lyrasis.org/display/FEDORA35/FOXML+Reference+Example
#[macro_use]
extern crate strum_macros;

use chrono::{DateTime, FixedOffset};
use serde::Deserialize;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::str::FromStr;
use strum_macros::{EnumDiscriminants, EnumString};

#[derive(Debug, Display, EnumDiscriminants)]
pub enum FoxmlError {
    DeserializeError(quick_xml::DeError), // Could not deserialize file to Foxml object.
    IOError(std::io::Error),              // Could not read file.
    QuickXMLError(quick_xml::Error),      // Wrap QuickXML error.
    Utf8Error(std::str::Utf8Error),       // Could not decode byte string into utf8.
}

impl From<quick_xml::DeError> for FoxmlError {
    fn from(error: quick_xml::DeError) -> Self {
        FoxmlError::DeserializeError(error)
    }
}

impl From<quick_xml::Error> for FoxmlError {
    fn from(error: quick_xml::Error) -> Self {
        FoxmlError::QuickXMLError(error)
    }
}

impl From<std::io::Error> for FoxmlError {
    fn from(error: std::io::Error) -> Self {
        FoxmlError::IOError(error)
    }
}

impl From<std::str::Utf8Error> for FoxmlError {
    fn from(error: std::str::Utf8Error) -> Self {
        FoxmlError::Utf8Error(error)
    }
}

// The object state can be Active (A), Inactive (I), or Deleted (D)
#[derive(Debug, Deserialize, PartialEq, EnumString)]
pub enum FoxmlObjectState {
    Active,
    Inactive,
    Deleted,
}

// The object state can be Active (A), Inactive (I), or Deleted (D)
#[derive(Debug, Deserialize, PartialEq, EnumString)]
pub enum FoxmlDatastreamState {
    A,
    I,
    D,
}

// Indicates the kind of datastream, either Externally Referenced Content (E),
// Redirected Content (R), Managed Content (M) or Inline XML (X)
#[derive(Debug, Deserialize, PartialEq, EnumString)]
pub enum FoxmlControlGroup {
    E,
    R,
    M,
    X,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct FoxmlProperty {
    #[serde(rename = "NAME")]
    pub name: String,
    #[serde(rename = "VALUE")]
    pub value: String,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct FoxmlObjectProperties {
    #[serde(rename = "property")]
    pub properties: Vec<FoxmlProperty>,
}

impl FoxmlObjectProperties {
    fn property(&self, name: &str) -> String {
        match self.properties.iter().find(|x| x.name == name) {
            Some(property) => property.value.clone(),
            // All public functions refer to required properties in the spec so
            // panicking at runtime is acceptable. As we do not expect to have
            // to deal with invalid FOXML.
            None => panic!("Failed to find required property: {}", name),
        }
    }

    fn date_property(&self, name: &str) -> DateTime<FixedOffset> {
        let date = self.property(&name);
        // It should be acceptable to panic here as we do not expect the FOXML to
        // be invalid.
        DateTime::parse_from_rfc3339(&date).expect("Failed to parse date property of FOXML file.")
    }

    pub fn state(&self) -> FoxmlObjectState {
        let state = self.property("info:fedora/fedora-system:def/model#state");
        FoxmlObjectState::from_str(&state).unwrap()
    }

    pub fn label(&self) -> String {
        self.property("info:fedora/fedora-system:def/model#label")
    }

    pub fn owner_id(&self) -> String {
        self.property("info:fedora/fedora-system:def/model#ownerId")
    }

    pub fn created_date(&self) -> DateTime<FixedOffset> {
        self.date_property("info:fedora/fedora-system:def/model#createdDate")
    }

    pub fn modified_date(&self) -> DateTime<FixedOffset> {
        self.date_property("info:fedora/fedora-system:def/view#lastModifiedDate")
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct FoxmlDatastreamContentLocation {
    #[serde(rename = "TYPE")]
    pub r#type: String,
    #[serde(rename = "REF")]
    pub r#ref: String,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct FoxmlDatastreamContentDigest {
    #[serde(rename = "TYPE")]
    pub r#type: String,
    #[serde(rename = "DIGEST")]
    pub digest: String,
}

#[derive(Debug, Deserialize, PartialEq)]
pub enum FoxmlDatastreamContent {
    #[serde(rename = "foxml:contentLocation")]
    ContentLocation(FoxmlDatastreamContentLocation),
    #[serde(rename = "foxml:contentDigest")]
    ContentDigest(FoxmlDatastreamContentDigest),
    #[serde(rename = "foxml:xmlContent")]
    XmlContent,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct FoxmlDatastreamVersion {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "LABEL")]
    pub label: String,
    #[serde(rename = "CREATED")]
    pub created: DateTime<FixedOffset>,
    #[serde(rename = "MIMETYPE")]
    pub mime_type: String,
    #[serde(rename = "SIZE")]
    pub size: Option<i32>,
    #[serde(rename = "FORMAT_URI")]
    pub format: Option<String>,
    #[serde(rename = "$value")]
    pub content: Vec<FoxmlDatastreamContent>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct FoxmlDatastream {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "STATE")]
    pub state: FoxmlDatastreamState,
    #[serde(rename = "CONTROL_GROUP")]
    pub control_group: FoxmlControlGroup,
    #[serde(rename = "VERSIONABLE")]
    pub versionable: bool,
    #[serde(rename = "datastreamVersion")]
    pub versions: Vec<FoxmlDatastreamVersion>,
}

#[derive(Debug, Deserialize)]
pub struct Foxml {
    #[serde(rename = "PID", default)]
    pub pid: String,
    #[serde(rename = "objectProperties")]
    pub properties: FoxmlObjectProperties,
    #[serde(rename = "datastream")]
    pub datastreams: Vec<FoxmlDatastream>,
}

impl Foxml {
    pub fn new(content: &str) -> Result<Foxml, FoxmlError> {
        Ok(quick_xml::de::from_str::<Foxml>(&content)?)
    }

    pub fn from_path(path: &Path) -> Result<Foxml, FoxmlError> {
        let content = std::fs::read_to_string(path)?;
        Self::new(&content)
    }
}

impl Eq for Foxml {}

impl Hash for Foxml {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.pid.hash(state);
    }
}

impl Ord for Foxml {
    fn cmp(&self, other: &Self) -> Ordering {
        alphanumeric_sort::compare_str(&self.pid, &other.pid)
    }
}

impl PartialOrd for Foxml {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(&other))
    }
}

// Technically there could be different in memory representations of a object
// with the same pid but that should never arise, so we do not account for it.
impl PartialEq for Foxml {
    fn eq(&self, other: &Self) -> bool {
        self.pid == other.pid
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic;
    use std::path::PathBuf;

    // Helper to get the fixtures directory.
    fn fixtures_directory() -> PathBuf {
        let manifest_directory = PathBuf::from_str(&env!("CARGO_MANIFEST_DIR")).unwrap();
        let root_directory = manifest_directory.parent().unwrap().parent().unwrap();
        let mut buf = PathBuf::from(&root_directory);
        buf.push("assets/fixtures");
        buf
    }

    #[test]
    fn invalid_path() {
        let mut path = fixtures_directory();
        path.push("non-existent.foxml.xml");
        let result = Foxml::from_path(path.as_path());
        assert!(result.is_err());
        let err: FoxmlErrorDiscriminants = result.unwrap_err().into();
        assert_eq!(err, FoxmlErrorDiscriminants::IOError);
    }

    #[test]
    fn invalid_content() {
        let mut path = fixtures_directory();
        path.push("invalid.foxml.xml");
        let result = Foxml::from_path(path.as_path());
        assert!(result.is_err());
        let err: FoxmlErrorDiscriminants = result.unwrap_err().into();
        assert_eq!(err, FoxmlErrorDiscriminants::DeserializeError);
    }

    #[test]
    fn valid_content() {
        let mut path = fixtures_directory();
        path.push("valid.foxml.xml");
        let result = Foxml::from_path(path.as_path());
        assert!(result.is_ok());
    }
}
