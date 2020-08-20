// Set the headers, order is important when adding new rows make sure the order matches what is given here.
fn headers() {
    return [
        "pid",
        "field_access",
        "field_alternative_title",
        "field_classification",
        "field_coordinates",
        "field_coordinates_text",
        "field_edtf_date",
        "field_edtf_date_created",
        "field_edtf_date_issued",
        "field_description",
        "field_dewey_classification",
        "field_edition",
        "field_extent",
        "field_genre",
        "field_geographic_subject",
        "field_identifier",
        "field_isbn",
        "field_language",
        "field_lcc_classification",
        "field_linked_agent",
        "field_local_identifier",
        "field_note",
        "field_oclc_number",
        "field_physical_form",
        "field_place_published",
        "field_resource_type",
        "field_rights",
        "field_subject",
        "field_subject_name",
        "field_table_of_contents",
        "field_temporal_subject",
    ];
}

// Look up appropriate 
fn resource_type(resource_type) {
    let resource_types = #{
        "still image": "http://purl.org/dc/dcmitype/StillImage",
        "text": "http://purl.org/dc/dcmitype/Text",
        "cartographic": "http://purl.org/dc/dcmitype/Image",
        "notated music": "http://purl.org/dc/dcmitype/Text",
        "sound recording": "http://purl.org/dc/dcmitype/Sound",
        "sound recording-musical": "http://purl.org/dc/dcmitype/Sound",
        "sound recording-nonmusical": "http://purl.org/dc/dcmitype/Sound",
        "moving image": "http://purl.org/dc/dcmitype/MovingImage",
        "three dimensional object": "http://purl.org/dc/dcmitype/Image",
        "software, multimedia": "http://purl.org/dc/dcmitype/Software",
    };
    return resource_types[resource_type];
}

// Flatten list with the given delimiter into a string.
// Removes empty results.
fn flatten(delimiter) {
    let result = "";
    for i in range(0, this.len()) {
        let item = this[i];
        item.trim();
        if item != "" {
            result += if i > 0 {
                delimiter + item
            }
            else {
                item
            };
        }
    }
    return result;
}

// Extract the '#text' fields from the sub path of elements.
fn text() {
    let results = [];
    for item in this {
        let text = item["#text"];
        text.trim();
        if text != "" {
            results.push(text);
        }        
    }
    return results;
}

// Assumes the op evaluates to bool.
fn filter(op) {
    let results = [];
    for element in this {
        if op.call(element) {
            results.push(element);
        }
    }
    return results;
} 

// Recursively descend and enumerate the values at the given path.
fn enumerate(children) {
    let child = children.shift();
    if children.len != 0 {
        let results = [];
        for element in this[child] {
            let extracted = element.enumerate(children);
            if type_of(extracted) == "array" {
                for item in extracted {
                    results.push(item);
                }
            }
            else {
                results.push(extracted);
            }
        }
        return results;
    }
    return this[child];
}

// Returns list of rows, values should match order of the headers() function.
fn rows(pid) {
    let rows = [];

    let mods = object(pid).datastream("MODS");
    if mods != () {
        // The '|' character is used to specify multiple values for the given field.
        let field_access = "";
        let field_alternative_title = mods.enumerate(["titleInfo", "subTitle"]).text().flatten(" ");
        let field_classification = mods["classification"].text().flatten("|");
        let field_coordinates = "";
        let field_coordinates_text = mods.enumerate(["subject", "cartographics", "coordinates"]).text().flatten("|");
        let field_edtf_date = mods.enumerate(["part", "date"]).text().flatten("|");
        let field_edtf_date_created = mods.enumerate(["originInfo", "dateCreated"]).text().flatten("|");
        let field_edtf_date_issued =  mods.enumerate(["originInfo", "dateIssued"]).text().flatten("|");
        let field_description = mods["abstract"].text().flatten("\n");
        let field_dewey_classification = "";
        let field_edition = mods.enumerate(["originInfo", "edition"]).text().flatten("|");
        let field_extent = mods.enumerate(["physicalDescription", "extent"]).text().flatten("|");
        // From top-level and subject.
        let field_genre = mods.enumerate(["genre"]).text();
        for genre in mods.enumerate(["subject", "genre"]).text() {           
            field_genre.push(genre);
        }
        let field_genre = field_genre.flatten("|");
        // From many elements.
        let field_geographic_subject = [];
        for geographic in mods.enumerate(["subject", "geographic", "#text"])  + mods.enumerate(["originInfo", "place", "placeTerm", "#text"]) {
            geographic.trim();
            if geographic != "" {
                field_geographic_subject.push(geographic);
            }   
        }
        for subject in mods["subject"] {
            for hierarchicalGeographic in subject["hierarchicalGeographic"] {
                // Capture all children.
                for child in hierarchicalGeographic.elements() {
                    let text = child["#text"];
                    text.trim();
                    if text != "" {
                        field_geographic_subject.push(text);
                    }   
                }
            }
        }
        let field_geographic_subject = field_geographic_subject.flatten("|");
        let field_identifier = mods["identifier"].filter(|e| e["@type"] != "local").text().flatten("|");
        let field_isbn = "";
        let field_language = mods.enumerate(["language", "languageTerm"]).text().flatten("|");
        let field_lcc_classification = "";
        // Typed relations currently not handled, need to group two values per field, might do as a separate CSV.
        let field_linked_agent = "";
        let field_local_identifier = mods["identifier"].filter(|e| e["@type"] == "local").text().flatten("|");
        let field_note = mods.enumerate(["note"]).text().flatten("|");;
        let field_oclc_number = "";
        let field_physical_form = mods.enumerate(["physicaldescription", "form"]).text().flatten("|");
        let field_place_published = mods.enumerate(["originInfo", "publisher"]).text().flatten("|");
        let field_resource_type = mods.enumerate(["typeOfResource"]).text().flatten("|");
        let field_rights = mods.enumerate(["accessCondition"]).text().flatten("|");
        let field_subject = mods.enumerate(["subject", "topic"]).text().flatten("|");
        // Name is composed of the three taxonomies "Corporate/Family/Person".
        let field_subject_name = [];
        for name in mods["name"].filter(|e| e["@type"] == "corporate" || e["@type"] == "personal" || e["@type"] == "family") {
            let text = name.enumerate(["namePart"]).text().flatten(" ");
            if text != "" {
                field_subject_name.push(text);
            }            
        }
        let field_subject_name = field_subject_name.flatten("|");
        let field_table_of_contents = mods.enumerate(["tableOfContents"]).text().flatten("|");
        let field_temporal_subject = mods.enumerate(["subject", "temporal"]).text().flatten("|");

        // Appending to rows performs automated trimming of fields.
        // Order of fields must match the output of the `header()` 
        // function.
        rows += [
            pid,
            field_access,
            field_alternative_title,
            field_classification,
            field_coordinates,
            field_coordinates_text,
            field_edtf_date,
            field_edtf_date_created,
            field_edtf_date_issued,
            field_description,
            field_dewey_classification,
            field_edition,
            field_extent,
            field_genre,
            field_geographic_subject,
            field_identifier,
            field_isbn,
            field_language,
            field_lcc_classification,
            field_linked_agent,
            field_local_identifier,
            field_note,
            field_oclc_number,
            field_physical_form,
            field_place_published,
            field_resource_type,
            field_rights,
            field_subject,
            field_subject_name,
            field_table_of_contents,
            field_temporal_subject,
        ];
    }
    return rows;
}
