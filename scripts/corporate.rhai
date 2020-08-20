// Set the headers, order is important when adding new rows make sure the order matches what is given here.
fn headers() {
    return ["name"];
}

// Collapses name-parts into single string.
fn name_parts() {
    let values = [];
    for part in this["namePart"] {
        values.push(part["#text"])
    }
    values.join(" ");
}

// Returns list of rows, values should match order of the headers() function.
fn rows(pid) {
    let rows = [];
    let mods = object(pid).datastream("MODS");
    if mods != () {
        for name in mods["name"] {
            if name["@type"] == "corporate" {
                rows += [name.name_parts()]
            }
        }
    }
    return rows;
}