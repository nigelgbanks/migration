use super::map::CustomMap;
use super::object::{Object, ObjectMap};
use super::utils::*;
use super::xml;
use chrono::{DateTime, NaiveDate};
use indicatif::ProgressBar;
use log::info;
use rayon::prelude::*;
use regex::Regex;
use rhai::module_resolvers::{FileModuleResolver, ModuleResolversCollection};
use rhai::*;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::{Arc, RwLock};

#[derive(Debug)]
pub struct ScriptError(Box<Path>, Box<EvalAltResult>);

impl fmt::Display for ScriptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let EvalAltResult::ErrorParsing(_, _) = *self.1 {
            write!(
                f,
                "Failed to parse script {}.\nError: {}",
                self.0.display(),
                self.1
            )
        } else {
            write!(
                f,
                "Runtime error in script {}.\nError: {}",
                self.0.display(),
                self.1
            )
        }
    }
}

type Script = (Box<Path>, AST);
type Scripts = HashMap<Box<Path>, AST>;
type Row = Vec<String>;
type Header = Vec<String>;
type Rows = Vec<Row>;
type ProgressBars = HashMap<Box<Path>, ProgressBar>;

fn edtf(value: ImmutableString) -> String {
    if let Ok(date) = DateTime::parse_from_rfc2822(&value) {
        return date.to_rfc3339();
    } else if let Ok(date) = DateTime::parse_from_rfc3339(&value) {
        return date.to_rfc3339();
    }
    let re = Regex::new(r"\d{4}-\d{2}-\d{2}").unwrap();
    if let Some(found) = re.find(&value) {
        if let Ok(date) = NaiveDate::parse_from_str(&found.as_str(), "%Y-%m-%d") {
            return date.format("%Y-%m-%d").to_string();
        }
    }
    "".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edtf() {
        assert_eq!(edtf("1900-01-01".into()), "1900-01-01".to_string(), "Dates equal");
    }
}

fn create_engine(objects: Arc<RwLock<ObjectMap>>, modules: Vec<&Path>) -> Engine {
    let mut engine = Engine::new();

    // Custom types.
    engine.register_type::<Object>();
    engine.register_type::<CustomMap>();

    // Custom functions.
    engine.register_result_fn(
        "object",
        move |pid: ImmutableString| -> Result<Dynamic, Box<EvalAltResult>> {
            match objects.read() {
                Ok(objects) => match (*objects)
                    .inner()
                    .get(&super::object::Pid(pid.clone().into()))
                {
                    Some(object) => Ok(Dynamic::from(object.clone())), // Have to clone cannot return references.
                    None => Err(format!("Failed to find object: {}", &pid).into()),
                },
                Err(_) => Err(format!("Failed to find object: {}", &pid).into()),
            }
        },
    );

    engine.register_result_fn(
        "datastream",
        |object: &mut Object, dsid: &str| -> Result<Dynamic, Box<EvalAltResult>> {
            match object.datastream(dsid) {
                Some(datastream) => match xml::parse(datastream) {
                    Some(result) => match result {
                        Ok(map) => Ok(Dynamic::from(map)),
                        Err(e) => Err(e.to_string().into()),
                    },
                    None => Ok(().into()),
                },
                None => Ok(().into()),
            }
        },
    );

    engine.register_fn("hash", |value: ImmutableString| -> String {
        let mut s = DefaultHasher::new();
        value.hash(&mut s);
        format!("{:X}", s.finish())
    });

    engine.register_fn(
        "join",
        |array: &mut Array, delimiter: &str| -> ImmutableString {
            array
                .iter()
                .map(|e| e.to_string())
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
                .join(delimiter)
                .into()
        },
    );

    engine.register_fn("edtf", edtf);

    // Object properties.
    engine.register_get("pid", |object: &mut Object| object.pid.0.clone());
    engine.register_get("state", |object: &mut Object| object.state.to_string());
    engine.register_get("label", |object: &mut Object| object.label.clone());
    engine.register_get("model", |object: &mut Object| object.model.clone());
    engine.register_get("parents", |object: &mut Object| object.parents.clone());

    // CustomMap functions (custom type is required to override indexing behavior on maps).
    engine.register_fn("print", |map: &mut CustomMap| -> ImmutableString {
        map.to_string().into()
    });

    engine.register_fn("debug", |map: &mut CustomMap| -> ImmutableString {
        format!("{:#?}", map).into()
    });

    engine.register_fn("push", |list: &mut Array, item: CustomMap| {
        list.push(Dynamic::from(item));
    });

    engine.register_fn("keys", |map: &mut CustomMap| -> Array {
        map.keys().cloned().map(|k| k.into()).collect()
    });

    engine.register_fn("elements", |map: &mut CustomMap| -> Array {
        map.clone().elements()
    });

    engine.register_fn(
        "find",
        |map: &mut CustomMap, mut children: Array| -> Array {
            // Must reverse for the function to work correctly otherwise we'd have to adopt a dequeue or something.
            children.reverse();
            let children: Vec<ImmutableString> =
                children.into_iter().map(|child| child.cast()).collect();
            map.find(children)
        },
    );

    // Returns empty array if element is not found to simplify script logic.
    engine.register_indexer_get(
        |map: &mut CustomMap, index: ImmutableString| -> rhai::Dynamic {
            if map.contains_key(&index) {
                map.get(&index).unwrap().clone()
            } else {
                Array::new().into()
            }
        },
    );

    // Override default to trim empty rows from the results.
    engine.register_fn("+=", |array: &mut Array, other: Array| {
        let mut trimmed = Array::with_capacity(other.len());
        let mut empty = true;
        for item in other {
            let value = item.take_string().unwrap();
            let value = value.trim();
            if !value.is_empty() {
                empty = false;
            }
            trimmed.push(value.into());
        }
        if !empty {
            array.push(trimmed.into());
        }
    });

    // Allow multiple modules directories to be registered.
    let mut collection = ModuleResolversCollection::new();
    for directory in modules {
        let resolver = FileModuleResolver::new_with_path(directory.canonicalize().unwrap());
        collection.push(resolver);
    }
    engine.set_module_resolver(Some(collection));

    engine
}

fn is_rhai_file(path: &Path) -> bool {
    match path.extension() {
        Some(extension) => extension.to_string_lossy() == "rhai",        
        None => false,
    }    
}

fn is_script(path: &Path) -> bool {
    is_rhai_file(&path) && !is_module(&path)
}

fn is_module(path: &Path) -> bool {
    is_rhai_file(&path)
        && path
            .file_stem()
            .unwrap()
            .to_string_lossy()
            .ends_with(".module")
}

fn parse_script(path: Box<Path>, engine: &Engine) -> Result<Script, ScriptError> {
    let ast = engine
        .compile_file(path.to_path_buf())
        .map_err(|error| ScriptError(path.clone(), error))?;
    Ok((path, ast))
}

// Parse the script files in the script folder.
fn parse_scripts(paths: Vec<&Path>, engine: &Engine) -> Scripts {
    info!("Parsing Scripts");
    paths
        .into_par_iter()
        .flat_map(|path| files(&path))
        .into_par_iter()
        .filter(|path| is_script(&path))
        .map(|path| parse_script(path, engine))
        .collect::<Result<Scripts, ScriptError>>()
        .unwrap()
}

// Call `headers()` function in the given script.
fn call_headers(engine: &Engine, script: &Script) -> (Header, usize) {
    let (path, ast) = script;
    let mut scope = Scope::new();
    let mut result: Map = engine
        .call_fn(&mut scope, &ast, "headers", ())
        .unwrap_or_else(|error| {
            panic!(
                "Failed to find 'fn headers()' in {} with error: {}",
                path.display(),
                error
            )
        });
    // Consume results and convert to a list of strings.
    let columns: Header = {
        let columns: Array = result.remove("columns").unwrap().cast();
        columns
            .into_iter()
            .map(|d| d.take_string().unwrap())
            .collect()
    };
    let sort_by_column: usize = {
        let sort_by: String = result.remove("sort_by").unwrap().cast();
        columns.iter().position(|r| r.eq(&sort_by)).unwrap()
    };
    (columns, sort_by_column)
}

fn call_rows(
    engine: &Engine,
    script: &Script,
    object: &Object,
    progress_bars: &ProgressBars,
) -> Rows {
    // Serially in alphanumeric order.
    let (path, ast) = script;
    let mut scope = Scope::new();
    let result: Array = engine
        .call_fn(&mut scope, &ast, "rows", (object.pid.to_string(),))
        .map_err(|error| ScriptError(path.clone(), error))
        .unwrap();
    // Update progress.
    let progress_bar = progress_bars.get(path).unwrap();
    progress_bar.inc(1);
    if progress_bar.position() == progress_bar.length() {
        progress_bar.finish_with_message("Done");
    }
    // Consume result and convert to a list of lists of strings.
    result
        .into_iter()
        .map(|d| d.cast::<rhai::Array>())
        .map(|a| a.into_iter().map(|v| v.to_string()).collect())
        .collect()
}

fn aggregate_rows(
    engine: &Engine,
    script: &Script,
    objects: &ObjectMap,
    progress_bars: &ProgressBars,
    sort_by_column: usize,
) -> Rows {
    // Execute scripts and aggregate the results.
    let rows: Rows = objects
        .inner()
        .values()
        .flat_map(|object| call_rows(&engine, &script, &object, &progress_bars))
        .collect();
    // Filter identical rows / collect into
    let mut rows: Rows = rows
        .into_iter()
        .collect::<BTreeSet<Row>>()
        .into_iter()
        .collect();
    // Sort alphanumerically on the first column only.
    rows.sort_by(|a, b| alphanumeric_sort::compare_str(&a[sort_by_column], &b[sort_by_column]));

    rows
}

fn execute_script(
    engine: &Engine,
    script: &Script,
    objects: &ObjectMap,
    progress_bars: &ProgressBars,
) -> (Header, Rows) {
    let header = call_headers(&engine, &script);
    (
        header.0,
        aggregate_rows(&engine, &script, &objects, &progress_bars, header.1),
    )
}

fn csv_destination(script: &Script, dest: &Path) -> Box<Path> {
    let (path, _) = script;
    dest.join(format!(
        "{}.{}",
        path.file_stem().unwrap().to_string_lossy(),
        "csv"
    ))
    .into_boxed_path()
}

fn create_csv(header: Header, rows: Rows, dest: Box<Path>) {
    let mut wtr = csv_other::WriterBuilder::new()
        .from_path(&dest)
        .expect("Failed to create CSV");

    wtr.write_record(header)
        .expect("Failed to write header to csv");

    for row in rows {
        wtr.write_record(row).expect("Failed to row header to csv");
    }
}

pub fn run_scripts(objects: ObjectMap, scripts: Vec<&Path>, modules: Vec<&Path>, dest: &Path) {
    // Track our progress per script, against the total number of objects.
    let count = objects.inner().len() as u64;

    // Wrap such that it can be shared across script invocations.
    // RHAI assumes ownership so we need a type that can be cloned.
    // Should be fairly fast as it will only increment a counter per clone,
    // and allows for concurrent reads.
    let arc = Arc::new(RwLock::new(objects));
    let engine = create_engine(arc.clone(), modules);

    let scripts = parse_scripts(scripts, &engine);

    let (multi, bars) = logger::progress_bars(count, scripts.keys().cloned());

    // Create a thread to run the scripts in the background so we can update the
    // progress bars in this thread.
    let dest = dest.to_path_buf();
    let thread = std::thread::spawn(move || {
        info!("Executing scripts");
        let results: Vec<_> = scripts
            .into_par_iter()
            .map(|script| match arc.read() {
                Ok(objects) => (
                    script.clone(),
                    execute_script(&engine, &script, &objects, &bars),
                ),
                Err(_) => panic!("Failed to get read access to objects"),
            })
            .collect();
        // Create CSV files.
        info!("Writing CSV files");
        results
            .into_par_iter()
            .for_each(|(script, (header, rows))| {
                create_csv(header, rows, csv_destination(&script, &dest));
            });
    });

    // Wait for progress to finish and update the progress bar display.
    multi.join_and_clear().unwrap();
    // Process can still continue after the progress bars have finished, make sure the thread is joined.
    thread.join().unwrap();
}
