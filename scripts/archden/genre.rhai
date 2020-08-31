// Set the headers, order is important when adding new rows make sure the order matches what is given here.
fn headers() {
    return ["name"];
}

// Returns list of rows, values should match order of the headers() function.
fn rows(pid) {
    let rows = [];
    let mods = object(pid).datastream("MODS");
    if mods != () {
        for genre in mods["genre"] {
            rows += [genre["#text"]];
        }
        for subject in mods["subject"] {
            for genre in subject["genre"] {
                rows += [genre["#text"]];
            }
        }
    }
    return rows;
}