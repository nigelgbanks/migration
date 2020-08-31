// Set the headers, order is important when adding new rows make sure the order matches what is given here.
fn headers() {
    return ["hash", "name"];
}

// Returns list of rows, values should match order of the headers() function.
fn rows(pid) {
    import "utilities" as utils;
    import "taxonomy" as taxonomy;
    let rows = [];
    let mods = utils::mods(pid);
    if mods != () {
        for name in mods["name"] {
            if name["@type"] == "corporate" {
                let parts = taxonomy::name_parts(name, " ");
                let term = taxonomy::term(parts);
                if term != () {
                    rows.push(term);
                }
            }
        }
    }
    return rows;
}