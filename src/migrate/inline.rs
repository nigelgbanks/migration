extern crate quick_xml;

use super::identifiers::*;
use super::migrate::migrate_inline_content;
use foxml::FoxmlControlGroup;
use log::info;
use quick_xml::events::attributes::Attribute;
use quick_xml::events::{BytesDecl, BytesStart, Event};
use quick_xml::{Reader, Writer};
use std::io::Cursor;
use std::path::Path;

// Checks if the given event applies to the given tag name, handles opening or closing.
fn is_element(event: &Event, name: &[u8]) -> bool {
    match event {
        Event::Start(e) => e.name() == name,
        Event::End(e) => e.name() == name,
        _ => false,
    }
}

// Get an attribute with the given name if it exists.
fn get_attribute<'a>(element: &'a BytesStart, name: &[u8]) -> Option<Attribute<'a>> {
    let mut attributes = element.attributes().filter_map(|x| x.ok());
    attributes.find(|attribute| attribute.key == name)
}

// Get attribute value or panics.
fn get_attribute_value(element: &BytesStart, name: &[u8]) -> String {
    let attribute = get_attribute(element, name)
        .unwrap_or_else(|| panic!("Failed to get attribute {}", String::from_utf8_lossy(name)));
    String::from_utf8(attribute.value.to_vec()).unwrap_or_else(|_| {
        panic!(
            "Failed to get attribute value for {}",
            String::from_utf8_lossy(name)
        )
    })
}

// Checks if the given tag is an datastream version.
fn is_datastream(event: &Event) -> bool {
    is_element(event, b"foxml:datastream")
}

// Checks if the given tag is an datastream version.
fn is_datastream_version(event: &Event) -> bool {
    is_element(event, b"foxml:datastreamVersion")
}

// Checks if the given tag is an inline datastream.
fn is_inline_datastream(event: &Event) -> bool {
    match event {
        Event::Start(e) if is_datastream(event) => {
            if let Some(attribute) = get_attribute(e, b"CONTROL_GROUP") {
                attribute.value.as_ref() == b"X"
            } else {
                false
            }
        }
        _ => false,
    }
}

// Extracts the PID from the foxml.
fn get_pid(reader: &mut Reader<&[u8]>) -> String {
    let mut buf = Vec::new();
    loop {
        // Panic if fails to read.
        match reader.read_event(&mut buf).unwrap() {
            ref event @ Event::Start(_) if is_element(event, b"foxml:digitalObject") => {
                if let Event::Start(ref e) = event {
                    return get_attribute_value(e, b"PID");
                }
            }
            Event::Eof => break, // If we reach the end of the file something has gone horribly wrong.
            _ => (),             // There are several other `Event`s we do not consider here
        }
    }
    panic!("This should not be reachable, but we must appease the compiler.");
}

// Returns the datastream ID for the inline datastream if found.
fn next_inline_datastream(reader: &mut Reader<&[u8]>) -> Option<String> {
    let mut buf = Vec::new();
    loop {
        // Panic if fails to read.
        match reader.read_event(&mut buf).unwrap() {
            ref event @ Event::Start(_) if is_inline_datastream(event) => {
                if let Event::Start(ref e) = event {
                    return Some(get_attribute_value(e, b"ID"));
                }
            }
            Event::Eof => break, // If we reach the end of the file there are no inline datastreams left to find.
            _ => (),             // There are several other `Event`s we do not consider here
        }
    }
    None
}

// Returns the datastream version ID for the datastream if found.
fn next_datastream_version(reader: &mut Reader<&[u8]>) -> Option<String> {
    let mut buf = Vec::new();
    loop {
        match reader.read_event(&mut buf).unwrap() {
            ref event @ Event::Start(_) if is_datastream_version(&event) => {
                if let Event::Start(ref e) = event {
                    return Some(get_attribute_value(e, b"ID"));
                }
            }
            ref event @ Event::End(_) if is_datastream(&event) => break, // Reached the end of the parent datastream tag no more versions to find.
            Event::Eof => break, // If we reach the end of the file there are no inline datastreams left to find.
            _ => (),             // There are several other `Event`s we do not consider here
        }
    }
    None
}

// Creates a writers and populates it with the contents of the inline
// datastream version that the reader currently points to.
fn extract_inline_datastream_version(reader: &mut Reader<&[u8]>) -> Writer<Cursor<Vec<u8>>> {
    let wrapper_element = b"foxml:xmlContent";
    let mut buf = Vec::new();
    let mut writer = Writer::new_with_indent(Cursor::new(Vec::new()), b' ', 2);
    assert!(writer
        .write_event(Event::Decl(BytesDecl::new(b"1.0", Some(b"UTF-8"), None)))
        .is_ok());
    loop {
        match reader.read_event(&mut buf).unwrap() {
            // Skip the parent foxml:xmlContent element.
            ref event @ Event::Start(_) if is_element(event, wrapper_element) => continue,
            // Exit if we have reached the end of the wrapper element foxml:xmlContent.
            ref event @ Event::End(_) if is_element(event, wrapper_element) => break,
            // Remove non-significant whitespace.
            ref event @ Event::Text(_) => {
                if let Event::Text(ref text) = event {
                    let bytes = &text.unescaped().unwrap();
                    let string = std::str::from_utf8(bytes).unwrap().to_string();
                    if !string.trim().is_empty() {
                        // Only copy non whitespace text so that the document is formatted pretty.
                        assert!(writer.write_event(&event).is_ok());
                    }
                }
            }
            // Copy contents by reference.
            event => assert!(writer.write_event(&event).is_ok()),
        }
        // We don't keep a borrow elsewhere, clear the
        // buffer to keep memory usage low.
        buf.clear();
    }
    writer
}

// Extracts all the inline datastreams in the given FOXML document.
fn extract_inline_datastreams(path: &Path) -> DatastreamContentMap {
    let foxml = std::fs::read_to_string(&path)
        .unwrap_or_else(|_| panic!("Failed to read file {}", &path.to_string_lossy()));
    let mut reader = Reader::from_str(&foxml);
    let pid = get_pid(&mut reader);
    let mut results = DatastreamContentMap::new();
    while let Some(dsid) = next_inline_datastream(&mut reader) {
        while let Some(version) = next_datastream_version(&mut reader) {
            // Only write the file if it does not already exist (to save time on multiple runs).
            let writer = extract_inline_datastream_version(&mut reader);
            results.insert(
                DatastreamIdentifier {
                    pid: pid.clone(),
                    dsid: dsid.clone(),
                    version: version.clone(),
                },
                String::from_utf8(writer.into_inner().into_inner()).unwrap(),
            );
        }
    }
    results
}

// Extracts all the inline datastreams to the given destination.
pub fn migrate_inline_datastreams(objects: &Vec<Box<Path>>, dest: &Path, checksum: bool) {
    info!("Migrating inline datastreams in {} object files.",
      objects.len()
    );
    let inline_datastreams = datastreams(&objects, FoxmlControlGroup::X, &dest);
    info!(
        "Found {} inline datastreams in {} object files.",
        inline_datastreams.len(),
        objects.len()
    );

    let results = migrate_inline_content(
        &objects,
        &inline_datastreams,
        extract_inline_datastreams,
        checksum,
    );
    info!("Finished migrating inline datastreams: {}", results);
}
