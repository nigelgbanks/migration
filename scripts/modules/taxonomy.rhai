// Creates a row for the given value if the value is not an empty string. Otherwise returns null.
fn term(value) {
    value.trim();
    if (value != "") {
      // Identity columns for Drupal migrate must be less than 255 characters
      // which isn't always the case for taxonomy terms. Hence we generate a 
      // hash from the value to act as an identifier.
      [hash(value), value]
    }
    else {
      ()
    }
}

// Collapses name-parts into single string separated by the given delimiter.
fn name_parts(name, delimiter) {
    import "utilities" as utils;
    utils::flatten(name.find(["namePart", "#text"]), delimiter)
}