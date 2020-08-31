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
    import "utilities" as utils;
    
    let rows = [];
    let mods = utils::mods(pid);
    if mods != () {
        // The '|' character is used to specify multiple values for the given field.
        let field_access = "";
        let field_alternative_title = utils::flatten(mods.find(["titleInfo", "subTitle", "#text"]), " ");
        let field_classification = utils::flatten(mods.find(["classification", "#text"]), "|");
        let field_coordinates = "";
        let field_coordinates_text = utils::flatten(mods.find(["subject", "cartographics", "coordinates", "#text"]), "|");
        let field_edtf_date = utils::flatten(mods.find(["part", "date", "#text"]), "|");
        let field_edtf_date_created = utils::flatten(mods.find(["originInfo", "dateCreated", "#text"]), "|");
        let field_edtf_date_issued =  utils::flatten(mods.find(["originInfo", "dateIssued", "#text"]), "|");
        let field_description =  utils::flatten(mods.find(["abstract", "#text"]), "\n");
        let field_dewey_classification = "";
        let field_edition = utils::flatten(mods.find(["originInfo", "edition", "#text"]), "|");
        let field_extent = utils::flatten(mods.find(["physicalDescription", "extent", "#text"]), "|");
        // From top-level and subject.
        let field_genre = mods.find(["genre", "#text"]);
        for genre in mods.find(["subject", "genre", "#text"]) {           
            field_genre.push(genre);
        }
        field_genre = utils::flatten(field_genre, "|");
        // From subject, originInfo, and hierarchicalGeographic.
        let field_geographic_subject = [];
        for geographic in mods.find(["subject", "geographic", "#text"]) {
            field_geographic_subject.push(geographic);
        }    
        for geographic in mods.find(["originInfo", "place", "placeTerm", "#text"]) {
            field_geographic_subject.push(geographic);
        }
        for subject in mods["subject"] {
            for hierarchicalGeographic in subject["hierarchicalGeographic"] {
                // Capture all children.
                for child in hierarchicalGeographic.elements() {
                    field_geographic_subject.push(child["#text"]);
                }
            }
        }
        field_geographic_subject = utils::flatten(field_geographic_subject, "|");
        let field_identifier = [];
        for identifier in mods["identifier"] {
            if identifier["@type"] != "local" {
                field_identifier.push(identifier["#text"]);
            }
        }
        field_identifier = utils::flatten(field_identifier, "|");
        let field_isbn = "";
        let field_language = utils::flatten(mods.find(["language", "languageTerm", "#text"]), "|");
        let field_lcc_classification = "";
        // Typed relations currently not handled, need to group two values per field, might do as a separate CSV.
        let field_linked_agent = "";
        let field_local_identifier = [];
        for identifier in mods["identifier"] {
            if identifier["@type"] == "local" {
                field_local_identifier.push(identifier["#text"]);
            }
        }
        field_local_identifier = utils::flatten(field_local_identifier, "|");
        let field_note = utils::flatten(mods.find(["note", "#text"]), "|");
        let field_oclc_number = "";
        let field_physical_form = utils::flatten(mods.find(["physicalDescription", "form", "#text"]), "|");
        let field_place_published = utils::flatten(mods.find(["originInfo", "publisher", "#text"]), "|");
        let field_resource_type = utils::flatten(mods.find(["typeOfResource", "#text"]), "|");
        let field_rights = utils::flatten(mods.find(["accessCondition", "#text"]), "|");
        let field_subject = utils::flatten(mods.find(["subject", "topic", "#text"]), "|");
        // Name is composed of the three taxonomies "Corporate/Family/Person".
        let field_subject_name = [];
        for name in mods["name"] {
            if (name["@type"] == "corporate" || name["@type"] == "personal" || name["@type"] == "family") {
                let parts = utils::flatten(name.find(["namePart", "#text"]), " ");
                field_subject_name.push(parts);
            }
        }
        field_subject_name = utils::flatten(field_subject_name, "|");
        let field_table_of_contents = utils::flatten(mods.find(["tableOfContents", "#text"]), "|");
        let field_temporal_subject = utils::flatten(mods.find(["subject", "temporal", "#text"]), "|");

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
