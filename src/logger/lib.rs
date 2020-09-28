use chrono::offset::Local;
use colored::*;
use core::fmt::Arguments;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{Level, Metadata, Record};
use std::collections::HashMap;
use std::hash::Hash;

pub struct Logger;

impl Logger {
    pub fn println(level: Level, args: &Arguments) {
        let local = Local::now();
        print!(
            "{}{}{} {}{}{} ",
            "[".blue().bold(),
            match &level {
                Level::Error => level.to_string().red().bold(),
                Level::Warn => level.to_string().yellow().bold(),
                Level::Info => level.to_string().green().bold(),
                _ => level.to_string().white().bold(),
            },
            "]".blue().bold(),
            "[".blue().bold(),
            local.format("%T").to_string().magenta(),
            "]".blue().bold(),
        );
        println!("{}", args);
    }

    pub fn error(msg: &str) {
        Self::println(Level::Error, &format_args!("{}", msg));
    }
}

impl log::Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Info
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            Logger::println(record.level(), record.args());
        }
    }

    fn flush(&self) {}
}

pub fn multi_progress() -> MultiProgress {
    MultiProgress::new()
}

pub fn progress_bar(total: u64) -> ProgressBar {
    let progress_bar = ProgressBar::new(total);
    let style = ProgressStyle::default_bar()
        .template(
            "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>7}/{len:7} ({eta})",
        )
        .progress_chars("#>-");
    progress_bar.set_style(style);
    progress_bar
}

pub fn spinner() -> ProgressBar {
    let spinner = ProgressBar::new(1);
    let style = ProgressStyle::default_spinner()
        .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ")
        .template("{prefix:.bold.dim} {spinner} {wide_msg}");
    spinner.set_style(style);
    spinner
}

pub fn progress_bars<T, I>(total: u64, keys: I) -> (MultiProgress, HashMap<T, ProgressBar>)
where
    T: Eq + Hash,
    I: IntoIterator<Item = T>,
{
    let multi = MultiProgress::new();
    let bars = keys
        .into_iter()
        .map(|key| {
            let pb = multi.add(progress_bar(total));
            (key, pb)
        })
        .collect();
    (multi, bars)
}
