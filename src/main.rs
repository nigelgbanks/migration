mod args;

use args::*;
use log::*;
use logger::Logger;

static LOGGER: Logger = Logger;

fn main() {
    // Force exit if panics on thread.
    let original_panic_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        // Use custom logger.
        if let Some(error) = panic_info.payload().downcast_ref::<String>() {
            if let Some(location) = panic_info.location() {
                Logger::error(&format!(
                    "Panic (File: {}, Line: {}, Column: {}): {}",
                    location.file(),
                    location.line(),
                    location.column(),
                    error
                ));
            } else {
                Logger::error(&format!("Panic: {}", error));
            }
        } else {
            // Invoke the default handler as a fallback.
            original_panic_hook(panic_info);
        }
        std::process::exit(1);
    }));

    // Configure logger.
    if let Ok(()) = log::set_logger(&LOGGER) {
        log::set_max_level(LevelFilter::Info)
    }

    // Process arguments and execute the given command.
    let mut args = args();
    match args.clone().get_matches().subcommand() {
        ("migrate", Some(matches)) => {
            let (fedora_directory, output_directory, copy, checksum) =
                get_migrate_subcommand_args(matches);
            migrate::migrate_data_from_fedora(fedora_directory, output_directory, copy, checksum);
        }
        ("csv", Some(matches)) => {
            // Source directory should be the output directory of the "fedora" sub command.
            let (source_directory, output_directory, pids) = get_csv_subcommand_args(matches);
            csv::generate_csvs(source_directory, output_directory, pids);
        }
        ("scripts", Some(matches)) => {
            // Source directory should be the output directory of the "fedora" sub command.
            let (source_directory, output_directory, script_directories, module_directories, pids) =
                get_scripts_subcommand_args(matches);
            csv::execute_scripts(
                source_directory,
                output_directory,
                script_directories,
                module_directories,
                pids,
            );
        }
        _ => {
            args.print_long_help().unwrap();
        }
    }
}
