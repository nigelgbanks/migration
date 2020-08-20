use super::map::CustomMap;
use super::object::*;
use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;
use rhai::{Array, Dynamic, ImmutableString};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

type Element = (ImmutableString, CustomMap);

// Returns optional namespace and local-name portions of the given element.
// If the namespace is not part of the name it will be set to an empty string.
fn name(element: &BytesStart) -> (ImmutableString, ImmutableString) {
    let name = unsafe { std::str::from_utf8_unchecked(element.name()).to_string() };
    let parts: Vec<_> = name.split(':').collect();
    if parts.len() == 2 {
        (parts[0].into(), parts[1].into())
    } else {
        ("".into(), parts[0].into())
    }
}

fn attributes(element: &BytesStart) -> CustomMap {
    element
        .attributes()
        .filter_map(|x| x.ok())
        .map(|attribute| unsafe {
            let key = ImmutableString::from(format!(
                "@{}",
                std::str::from_utf8_unchecked(&attribute.key)
            ));
            let value = Dynamic::from(std::str::from_utf8_unchecked(&attribute.value).to_string());
            (key, value)
        })
        .collect()
}

fn element<B>(reader: &mut Reader<B>, e: &BytesStart) -> Result<Element, quick_xml::Error>
where
    B: BufRead,
{
    let mut properties = attributes(&e);
    let mut children: Vec<Element> = Vec::new();
    let mut text = ImmutableString::from("".to_string());
    let mut buffer = Vec::new();
    loop {
        match reader.read_event(&mut buffer)? {
            // Opening tag of child.
            Event::Start(e) => {
                children.push(element(reader, &e)?); // Recurse.
            }
            // Closing current tag.
            Event::End(_) => break,
            // Tag of childless with no child.
            Event::Empty(e) => {
                let (namespace, local_name) = name(&e);
                let mut properties = attributes(&e);
                properties.insert("#namespace".into(), namespace.into());
                properties.insert("#text".into(), "".to_string().into());
                children.push((local_name, properties));
            }
            // Characters between start and end tags.
            Event::Text(e) => {
                // Remove non-significant whitespace.
                let bytes = &e.unescaped().unwrap();
                unsafe {
                    let string = std::str::from_utf8_unchecked(bytes).to_string();
                    if !string.trim().is_empty() {
                        // Only copy non whitespace text so that the document is formatted pretty.
                        // We don't really handle mixed content at this point.
                        text = ImmutableString::from(string);
                    }
                }
            }
            // End of file has been reached, this should only occur in the `to_map()` function.
            Event::Eof => panic!("Unreachable"),
            // We ignore Comments, CData, XML Declaration, Processing Instructions, and DocType elements.
            _ => (),
        }
        // We have to clone to pass the data to the script so no point in maintaining reference to the string content.
        buffer.clear();
    }
    // Group children by name into vectors.
    let (namespace, local_name) = name(&e);
    let children: CustomMap = {
        let init: HashMap<ImmutableString, Array> = HashMap::new();
        children
            .into_iter()
            .fold(init, |mut acc, (child_name, child_properties)| {
                let list = acc.entry(child_name).or_insert_with(Array::new);
                list.push(Dynamic::from(child_properties));
                acc
            })
            .into_iter()
            .map(|(name, properties)| (name, Dynamic::from(properties)))
            .collect()
    };
    properties.insert("#namespace".into(), namespace.into());
    properties.insert("#text".into(), text.into());
    properties.extend(children);
    Ok((local_name, properties))
}

fn map<B>(mut reader: Reader<B>) -> Result<CustomMap, quick_xml::Error>
where
    B: BufRead,
{
    let mut buffer = Vec::new();
    loop {
        match reader.read_event(&mut buffer)? {
            // Only concerned with the root tag, return a map of it's attributes and children.
            Event::Start(e) => {
                let (_, properties) = element(&mut reader, &e)?;
                return Ok(properties);
            }
            // End of file has been reached.
            Event::Eof => {
                return Err(quick_xml::Error::UnexpectedEof(
                    "Unexpected end of file.".to_string(),
                ))
            }
            // We ignore Comments, CData, XML Declaration, Processing Instructions, and DocType elements, etc.
            _ => (),
        };
        // We have to clone to pass the data to the script so no point in maintaining reference to the string content.
        buffer.clear();
    }
}

pub fn parse(datastream: &DatastreamVersion) -> Option<Result<CustomMap, quick_xml::Error>> {
    let valid_mime_types = vec!["application/rdf+xml", "application/xml", "text/xml"];
    if valid_mime_types.contains(&datastream.mime_type.as_str()) {
        let file = File::open(&datastream.path).unwrap();
        let reader = Reader::from_reader(BufReader::new(&file));
        Some(map(reader))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::super::map::CustomMap;
    use super::*;
    use rhai::{Array, Dynamic, ImmutableString};
    use std::any::TypeId;

    #[test]
    fn valid_content() {
        let content = r#"
<oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
    <dc:title>Denver Catholic Register November 18, 1954</dc:title>
    <dc:subject>Carmel of the Holy Spirit</dc:subject>
    <dc:subject>Catholic News</dc:subject>
    <dc:subject></dc:subject>
</oai_dc:dc>
"#;
        let expected = CustomMap::new(hashmap! {
            ImmutableString::from("@xmlns:dc") => Dynamic::from("http://purl.org/dc/elements/1.1/"),
            ImmutableString::from("@xmlns:oai_dc") => Dynamic::from("http://www.openarchives.org/OAI/2.0/oai_dc/"),
            ImmutableString::from("@xmlns:xsi") => Dynamic::from("http://www.w3.org/2001/XMLSchema-instance"),
            ImmutableString::from("@xsi:schemaLocation") => Dynamic::from("http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd"),
            ImmutableString::from("title") => Dynamic::from(vec![
                Dynamic::from(CustomMap::new(hashmap! {
                    ImmutableString::from("#namespace") => Dynamic::from("dc"),
                    ImmutableString::from("#text") => Dynamic::from("Denver Catholic Register November 18, 1954")
                }))
            ]),
            ImmutableString::from("subject") => Dynamic::from(vec![
                Dynamic::from(CustomMap::new(hashmap! {
                    ImmutableString::from("#namespace") => Dynamic::from("dc"),
                    ImmutableString::from("#text") => Dynamic::from("Carmel of the Holy Spirit"),
                })),
                Dynamic::from(CustomMap::new(hashmap! {
                    ImmutableString::from("#namespace") => Dynamic::from("dc"),
                    ImmutableString::from("#text") => Dynamic::from("Catholic News"),
                })),
                Dynamic::from(CustomMap::new(hashmap! {
                    ImmutableString::from("#namespace") => Dynamic::from("dc"),
                    ImmutableString::from("#text") => Dynamic::from(""),
                }))
            ]),
            ImmutableString::from("#namespace") => Dynamic::from("oai_dc"),
            ImmutableString::from("#text") => Dynamic::from("")
        });
        let reader = Reader::from_str(&content);
        let result = map(reader);
        assert!(result.is_ok());
        valid_map_equals_expected(&result.unwrap(), &expected);
    }

    fn valid_map_equals_expected(result: &CustomMap, expected: &CustomMap) {
        // Check keys match.
        let result_keys = {
            let mut keys = result.keys().collect::<Vec<_>>();
            keys.sort();
            keys
        };
        let expected_keys = {
            let mut keys = expected.keys().collect::<Vec<_>>();
            keys.sort();
            keys
        };
        assert_eq!(result_keys, expected_keys);
        for key in result_keys {
            let result_value = result.get(key).unwrap();
            let expected_value = expected.get(key).unwrap();
            if TypeId::of::<ImmutableString>() == result_value.type_id() {
                let result_value = result_value.read_lock::<ImmutableString>().unwrap();
                let expected_value = expected_value.read_lock::<&str>().unwrap();
                assert_eq!(*result_value, *expected_value);
            }
            if TypeId::of::<CustomMap>() == result_value.type_id() {
                let result = result_value.read_lock::<CustomMap>().unwrap();
                let expected = expected_value.read_lock::<CustomMap>().unwrap();
                valid_map_equals_expected(&(*result), &(*expected));
            }
            if TypeId::of::<Array>() == result_value.type_id() {
                let result = result_value.read_lock::<Array>().unwrap();
                let expected = expected_value.read_lock::<Vec<Dynamic>>().unwrap();
                (*result)
                    .iter()
                    .zip((*expected).iter())
                    .for_each(|(result, expected)| {
                        let result = result.read_lock::<CustomMap>().unwrap();
                        let expected = expected.read_lock::<CustomMap>().unwrap();
                        valid_map_equals_expected(&(*result), &(*expected));
                    });
            }
        }
    }
}
