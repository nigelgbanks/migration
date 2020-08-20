// Set the headers, order is important when adding new rows make sure the order matches what is given here.
fn headers() {
    return ["name"];
}

// Returns list of rows, values should match order of the headers() function.
fn rows(pid) {
    let rows = [];
    let mods = object(pid).datastream("MODS");
    if mods != () {
        for language in mods["subject"] {
            for term in mods["temporal"] {
                rows += [term["#text"]];
            }
        }
    }
    return rows;
}