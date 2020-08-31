extern crate clap;

use clap::{App, Arg, ArgMatches, SubCommand};
use std::env;
use std::ffi::OsStr;
use std::path::Path;

type ArgResult = std::result::Result<(), String>;

fn valid_directory(s: String) -> ArgResult {
    let path = Path::new(OsStr::new(&s));
    if path.is_dir() {
        Ok(())
    } else {
        Err(format!("The directory '{}' does not exist", path.display()))
    }
}

fn valid_fedora_directory(s: String) -> ArgResult {
    let path = Path::new(OsStr::new(&s));
    migrate::valid_fedora_directory(&path)?;
    Ok(())
}

fn valid_source_directory(s: String) -> ArgResult {
    let path = Path::new(OsStr::new(&s));
    csv::valid_source_directory(&path)?;
    Ok(())
}

pub fn get_migrate_subcommand_args<'a>(args: &'a ArgMatches) -> (&'a Path, &'a Path, bool, bool) {
    let home_arg = args
        .value_of("input")
        .expect("Failed to get argument --input");
    let fedora_directory = Path::new(OsStr::new(home_arg));

    let output_arg = args
        .value_of("output")
        .expect("Failed to get argument --output");
    let output_directory = Path::new(OsStr::new(output_arg));

    let copy = !args.is_present("move");

    let checksum = args.is_present("checksum");

    (fedora_directory, output_directory, copy, checksum)
}

pub fn get_csv_subcommand_args<'a>(args: &'a ArgMatches) -> (&'a Path, &'a Path, Vec<&'a str>) {
    let input_arg = args
        .value_of("input")
        .expect("Failed to get argument --input");
    let input_directory = Path::new(OsStr::new(input_arg));

    let output_arg = args
        .value_of("output")
        .expect("Failed to get argument --output");
    let output_directory = Path::new(OsStr::new(output_arg));

    let limit_to_pids = match args.values_of("pids") {
        Some(pids) => pids.collect(),
        None => Vec::new(),
    };

    (input_directory, output_directory, limit_to_pids)
}

pub fn get_scripts_subcommand_args<'a>(
    args: &'a ArgMatches,
) -> (&'a Path, &'a Path, &'a Path, Option<&'a Path>, Vec<&'a str>) {
    let input_arg = args
        .value_of("input")
        .expect("Failed to get argument --input");
    let input_directory = Path::new(OsStr::new(input_arg));

    let output_arg = args
        .value_of("output")
        .expect("Failed to get argument --output");
    let output_directory = Path::new(OsStr::new(output_arg));

    let scripts_arg = args.value_of("scripts").unwrap();
    let scripts_directory = Path::new(OsStr::new(scripts_arg));

    let modules_arg = args.value_of("modules");
    let modules_directory = modules_arg.map(|s| Path::new(OsStr::new(s)));

    let limit_to_pids = match args.values_of("pids") {
        Some(pids) => pids.collect(),
        None => Vec::new(),
    };

    (
        input_directory,
        output_directory,
        scripts_directory,
        modules_directory,
        limit_to_pids,
    )
}

pub fn args<'a, 'b>() -> App<'a, 'b> {
    let args: Vec<String> = env::args().collect();
    let program_name = Path::new(OsStr::new(&args[0]))
        .file_name()
        .expect("Failed to get program name.");
    let program_name = program_name.to_string_lossy();
    App::new(program_name)
    .version("0.1")
    .author("Nigel Banks <nigel.g.banks@gmail.com>")
    .about("\nProcesses an existing Fedora 3 repository and generates CSV files that can be used to migrate to Drupal 8. \nExits non-zero if not successful.")
    .subcommand(SubCommand::with_name("migrate")
                .about("Copy/Move Fedora data to layout required for migration")
                .arg(
                  Arg::with_name("move")
                  .long("move")
                  .help("Move the files instead of copying")
                  .required(false)
                )
                .arg(
                  Arg::with_name("checksum")
                  .long("checksum")
                  .help("Generate a checksum to determine if a source file has changed and should be migrated again (by default only checks file size & modified timestamp).")
                  .required(false)
                )
                .arg(
                  Arg::with_name("input")
                  .long("input")
                  .value_name("FILE")
                  .help("FEDORA_HOME directory to process")
                  .required(true)
                  .takes_value(true)
                  .validator(valid_fedora_directory)
                )
                .arg(
                  Arg::with_name("output")
                  .long("output")
                  .value_name("FILE")
                  .help("The directory to move Fedora content to")
                  .required(true)
                  .takes_value(true)
                  .validator(valid_directory)
                )
    )
    .subcommand(SubCommand::with_name("csv")
                .about("Generate CSV files from migrated Fedora data.")
                .arg(
                  Arg::with_name("input")
                  .long("input")
                  .value_name("FILE")
                  .help("Input directory to process, this should be the same as the output directory of the `migrate` sub-command.")
                  .required(true)
                  .takes_value(true)
                  .validator(valid_source_directory)
                )
                .arg(
                  Arg::with_name("output")
                  .long("output")
                  .value_name("FILE")
                  .help("The directory to move Fedora content to")
                  .required(true)
                  .takes_value(true)
                  .validator(valid_directory)
                )
                .arg(
                  Arg::with_name("pids")
                  .short("p")
                  .long("pids")
                  .value_name("PID")
                  .help("Limit the objects processed to the PIDs listed (useful for testing small migrations)")
                  .multiple(true)
                  .require_delimiter(true)
                  .required(false)
                  .takes_value(true)
                )
    )
    .subcommand(SubCommand::with_name("scripts")
                .about("Generate CSV files from migrated Fedora data.")
                .arg(
                  Arg::with_name("input")
                  .long("input")
                  .value_name("FILE")
                  .help("Input directory to process, this should be the same as the output directory of the `migrate` sub-command.")
                  .required(true)
                  .takes_value(true)
                  .validator(valid_source_directory)
                )
                .arg(
                  Arg::with_name("output")
                  .long("output")
                  .value_name("FILE")
                  .help("The directory to move Fedora content to")
                  .required(true)
                  .takes_value(true)
                  .validator(valid_directory)
                )
                .arg(
                  Arg::with_name("scripts")
                  .long("scripts")
                  .value_name("FILE")
                  .help("The directory containing scripts to customize csv generation.")
                  .required(true)
                  .takes_value(true)
                  .validator(valid_directory)
                )
                .arg(
                  Arg::with_name("modules")
                  .long("modules")
                  .value_name("FILE")
                  .help("The directory containing modules scripts to share functionality across script files.")
                  .required(false)
                  .takes_value(true)
                  .validator(valid_directory)
                )
                .arg(
                  Arg::with_name("pids")
                  .short("p")
                  .long("pids")
                  .value_name("PID")
                  .help("Limit the objects processed to the PIDs listed (useful for testing small migrations)")
                  .multiple(true)
                  .require_delimiter(true)
                  .required(false)
                  .takes_value(true)
                )
    )
}
