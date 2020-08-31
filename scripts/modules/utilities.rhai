// Gets the mods datastream if it exists returns null otherwise.
fn mods(pid) {
    object(pid).datastream("MODS")
}

// Map the resource type to the the appropriate URI as understood by Drupal.
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
    // Can panic if a unknown resource type is encountered.
    return resource_types[resource_type];
}

// Flatten list with the given delimiter into a string.
// Removes empty results.
fn flatten(list, delimiter) {
    let result = "";
    for i in range(0, list.len()) {
        let item = list[i];
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
fn text(element) {
    let results = [];
    for child in element {
        let text = child["#text"];
        text.trim();
        if text != "" {
            results.push(text);
        }        
    }
    return results;
}

// Assumes the op is a function that evaluates to bool.
fn filter(element, op) {
    let results = [];
    for item in element {
        if op.call(item) {
            results.push(item);
        }
    }
    return results;
} 
