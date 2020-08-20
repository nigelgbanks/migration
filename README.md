# Migration <!-- omit in toc -->

[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](./LICENSE)
![CI](https://github.com/nigelgbanks/migration/workflows/CI/badge.svg)

- [Introduction](#introduction)
- [Downloading](#downloading)
- [Building Locally](#building-locally)
  - [Development](#development)
- [Usage](#usage)
- [Migrate Command](#migrate-command)
- [CSV Command](#csv-command)
- [Customization via Scripting](#customization-via-scripting)
  - [Expectations](#expectations)
  - [Working with Objects](#working-with-objects)
  - [Existing Documentation](#existing-documentation)
  - [Debugging](#debugging)
  - [Understanding Errors](#understanding-errors)

## Introduction

Processes an existing Fedora 3 repository and generates CSV files that can be
used to migrate to Drupal 8. Exits non-zero if not successful.

CSV files are expected to be used with [islandora_migrate_fedora_feature].

**Only Linux is supported at this time**.

## Downloading

You do not need to build this tool locally, you can download the latest version
[here](https://github.com/nigelgbanks/migration/releases/download/latest/migration).

## Building Locally

Building the tool requires a local installation of [Rust]. Instructions for
installing [Rust] can be found
[here](https://www.rust-lang.org/learn/get-started).

To build locally simply use the following `cargo` commands.

**Debug Build**:

```bash
cargo build
```

**Release Build**:

```bash
cargo build --release
```

**Execute Tests**:

```bash
cargo test
```

**Run Debug**:

```bash
cargo run
```

**Run Release**:

```bash
cargo run --release
```

### Development

For a free editor with decent [Rust] support try [Visual Studio Code]. In
addition the following extensions for [Visual Studio Code] could be useful.

- [Rust Extension](https://marketplace.visualstudio.com/items?itemName=rust-lang.rust)
- [Rust Analyzer](https://marketplace.visualstudio.com/items?itemName=matklad.rust-analyzer)

In addition there are number of packages tools you can install via `cargo` that
help with linting etc.

[Rust Language Server (RLS)](https://github.com/rust-lang/rls):

```bash
rustup component add rls rust-analysis rust-src
```

[Rust Clippy](https://github.com/rust-lang/rust-clippy)

```bash
rustup component add clippy
```

## Usage

```bash
Processes an existing Fedora 3 repository and generates CSV files that can be used to migrate to Drupal 8.
Exits non-zero if not successful.

USAGE:
    migration [SUBCOMMAND]

FLAGS:
    -h, --help
            Prints help information

    -V, --version
            Prints version information


SUBCOMMANDS:
    csv        Generate CSV files from migrated Fedora data.
    help       Prints this message or the help of the given subcommand(s)
    migrate    Copy/Move Fedora data to layout required for migration
```

## Migrate Command

```bash
Copy/Move Fedora data to layout required for migration

USAGE:
    migration migrate [FLAGS] --input <FILE> --output <FILE>

FLAGS:
        --checksum    Generate a checksum to determine if a source file has changed and should be migrated again (by default only checks file size & modified timestamp).
    -h, --help        Prints help information
        --move        Move the files instead of copying (DESTRUCTIVE)
    -V, --version     Prints version information
```

## CSV Command

```bash
Generate CSV files from migrated Fedora data.

USAGE:
    migration csv [OPTIONS] --input <FILE> --output <FILE> --scripts <FILE>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
        --input <FILE>      Input directory to process, this should be the same as the output directory of the `migrate` command.
        --output <FILE>     The directory to move Fedora content to
    -p, --pids <PID>...     Limit the objects processed to the PIDs listed (useful for testing small migrations)
        --scripts <FILE>    The directory containing scripts to customize csv generation.
```

## Customization via Scripting

When using the [CSV Command](#csv-command) the `--script` argument should point
to a folder that contains [RHAI] scripts. For some examples see the
[scripts](./scripts) folder in this repository.

### Expectations

For each script provided in the `--scripts` folder a corresponding `csv` file
will be generated with a matching name.

The scripts must define at a minimum two functions: `headers`, and `rows`.

The `headers` function is expected to return an array of strings denoting the
header of the `csv` file to be generated.

*For example:*

```rust
fn headers() {
    return ["name"];
}
```

In this case it denotes there there will only be a single column and that
columns is `name`.

The `rows` function is expected to be called **once per object** for every
object in the repository. `rows` takes a single string argument `pid` which
denotes the identifier of object being processed.

The `rows` function must return a array of zero or more `rows`, where each row
has the same number of items and in the same order as stipulated by the `header`
function.

```rust
fn rows(pid) {
    let rows = [];
    let mods = object(pid).datastream("MODS"); // Get a map of all the data in the MODS datastream.
    if mods != () {
        for subject in mods["subject"] {
            for topic in subject["topic"] {
                rows += [topic["#text"]]; // Append a single row to the list of rows.
            }
        }
    }
    return rows; // Return zero or more rows.
}
```

### Working with Objects

There are a number of functions which have been added to the scripting language
to make looking up object information possible.

```rust
let obj = object("namespace:123"); // `object` takes a string pid and returns a Fedora Object.

// Fedora Objects have a number of properties.
print(obj.pid);     // Get the `pid` of the object. Prints "namespace:123".
print(obj.state);   // Gets the `state` of the object. Prints either: "Active", "Inactive", or "Deleted".
print(obj.label);   // Gets the `label` of the object.
print(obj.model);   // Gets the `model` of the object e.g "info:fedora/islandora:sp_large_image_cmodel".
print(obj.parents); // Gets a list of PIDs corresponding to the objects parents e.g ["namespace:root"].

// There is also a function which takes a DSID and returns the corresponding datastream.
// It grabs the latest version of the datastream and will only work on datastreams that are XML.
let mods = obj.datastream("MODS");

// The returned object is a `map` representing the XML.
debug(mods);

// Which would display something like:
#{
    "#namespace": "mods",
    "@xmlns:dcterms": "http://purl.org/dc/terms/",
    "@xmlns:dc": "http:://purl.org/elements/1.1/",
    "@xmlns:edm": "http://pro.europeana.eu/edm-documentation",
    "accessCondition": [
        #{
            "#text": "The object has been digitized for non-commercial use. You are responsible for your own use. You may need to obtain other permissions for your intended use. For example other rights such as publicity, privacy or moral rights may limit how you may use the material. For any intended commercial reproduction please contact the Archdiocese of Denver",
            "#namespace": "mods",
        },
    ],
    "@xmlns:xlink": "http://www.w3.org/1999/xlink",
    "extension": [
        #{
            "admin": [
                #{
                    "#text": "",
                    "accessConditions": [
                        #{
                            "#text": "original available by appointment only",
                            "#namespace": "drs",
                        },
                    ],
                    "#namespace": "drs",
                },
            ],
            "#text": "",
            "#namespace": "mods",
        },
        // ...
    ],
    // ...
}

// All attributes are prefixed with `@`, and included their namespaces.
//
// Elements are indexed by their `local-name`, but their namespace can be
// checked by looking at the `#namespace` entry on the corresponding element.
//
// Every element has `#text` field that corresponds to the text within the
// element, it is trimmed and may be an empty string.
//
// If one knew there would always be a single `/mods:mods/mods:titleInfo/mods:title`
// element in all of their data. They could access the single `mods:title` text like so:
let title = mods["titleInfo"][0]["title"]["#text"];

// Though that typically isn't the case and elements may or may not be there or
// their may be multiple elements. Which is why you will typically have to iterate.
let rows = []
for subject in mods["subject"] {
    for topic in subject["topic"] {
        rows += [topic["#text"]]; // Append a single row to the list of rows.
    }
}

// If doing this often it might be useful to define a function that does the same behavior.
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

rows += mods.enumerate(["subject", "topic", "#text"]);
```

### Existing Documentation

The [RHAI] scripting language is fairly well documented
[documented](https://schungx.github.io/rhai/language/index.html). With examples
for many features. Here is just a very brief overview of the syntax.

Variables are defined with the `let` statements like so:

```rust
let foo = "bar";
```

There are a number of primitives / data types which are available.

```rust
let a = 13; // All numbers are 64 bit signed integers, floating point is not supported.
let b = "test"; // Strings
let c = ["a","b"]; // Arrays
let d = #{ // Maps
    "one": 1,
    "two": 2,
};

// Most operations are what you'd typically expect.
let a = 10 * 10; // a = 100;
let c = "a" + "b"; // c = "ab";
let d = ["a"] + ["b"]; // d = ["a", "b"];
```

You can define functions which take one or more arguments. Functions with the
same name but a different number of arguments are overloaded, if a function with
the same name and number of arguments as another is added it will override the
previous implementation.

```rust
fn hello(s) {
    print("Hello " + s + "!");
}

// Overload existing function.
fn hello(a, b) {
    print("Overloaded Hello " + a + " " + b + "!");
}

hello("World");
hello("New", "York");

// Override existing function.
fn hello(s) {
    print("Goodbye " + s + "!");
}

hello("World");
```

*Prints the following*:

```bash
Hello World!
Overloaded Hello New York!
Goodbye World!
```

Functions can be called `method` style on any variable provided that the
function uses the `this` keyword somewhere in the body to refer to the object
for whom the method is called upon.

```rust
fn hello() {
    print("Hello " + this);
}

let world = "world";

world.hello();
```

*Prints the following*:

```bash
Hello World!
```

**Important Concepts:**

- All functions in [RHAI] are pure so there are no closures, they cannot
  manipulate variables outside of the function definition.
- Any functions that use the `this` keyword can be called like an object method.

### Debugging

There is a building function `debug` which will pretty print the variable to
standard out while the script is running for example.

```rust
let foo = #{
    "one": 1,
    "two": 2,
};
debug(foo);
```

*Will print:*

```rust
#{"two": 2, "one": 1}
```

### Understanding Errors

There are broadly two types of errors you'll encounter when writing [RHAI] scripts.

1. Failure to parse
2. Runtime errors

**Failure to parse:**

*In the following example:*

```bash
thread 'main' panicked at 'Failed to parse script /home/nbanks/Projects/islandora/migration/scripts/corporate.rhai.
Error: Syntax error: Expecting ';' to terminate this statement (line 8, position 5)', src/scripts/lib.rs:139:33
```

The full path to the script with the problem will be printed, in this case
`/home/nbanks/Projects/islandora/migration/scripts/corporate.rhai`.

The exact error that will be printed as well prefixed by `Error:`.

In this case the error is
`Syntax error: Expecting ';' to terminate this statement (line 8, position 5)'`.

This can be fixed by looking at `line 8` `position 5` (here position means
column), and adding the missing `;` semicolon.

The section `src/scripts/lib.rs:139:33` indicates the location in the Rust code
where the error caused the program to exit.

**Runtime errors:**

Some error do not occur until the script is actually run.

*As shown in the following example:*

```bash
thread '<unnamed>' panicked at 'Runtime error in script /home/nbanks/Projects/islandora/migration/scripts/corporate.rhai.
Error: Error in call to function 'rows' : Function not found: 'text (())' (line 12, position 7)', src/scripts/lib.rs:172:45
```

The full path to the script with the problem will be printed, in this case
`/home/nbanks/Projects/islandora/migration/scripts/corporate.rhai`.

The exact error that will be printed as well prefixed by `Error:`.

In this case the error is
`Error in call to function 'rows' : Function not found: 'text (())'`.

So in the function `rows` there was a call to another function `text` on a
`null` value (aka `()`), so in this case we'd have to revisit the logic to see
how the `null` arose.

The section `src/scripts/lib.rs:172:45` indicates the location in the Rust code
where the error caused the program to exit.

[islandora_migrate_fedora_feature]: https://github.com/nigelgbanks/islandora_migrate_fedora_feature
[RHAI]: https://schungx.github.io/rhai
[Rust]: https://www.rust-lang.org/
[Visual Studio Code]: https://code.visualstudio.com/