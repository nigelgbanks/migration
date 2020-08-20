// Set the headers, order is important when adding new rows make sure the order matches what is given here.
fn headers() {
    return ["name"];
}

// Returns list of rows, values should match order of the headers() function.
fn rows(pid) {
    let rows = [];
    let mods = object(pid).datastream("MODS");
    if mods != () {
        for subject in mods["subject"] {
            for geographic in subject["geographic"] {
                rows += [geographic["#text"]];
            }
            for hierarchicalGeographic in subject["hierarchicalGeographic"] {
                // Capture all children.
                for child in hierarchicalGeographic.elements() {
                    rows += [child["#text"]];
                }
            }
        }
        for originInfo in mods["originInfo"] {
            for place in originInfo["place"] {
                for placeTerm in place["placeTerm"] {
                    rows += [placeTerm["#text"]];
                }
            }
        }
    }
    return rows;
}