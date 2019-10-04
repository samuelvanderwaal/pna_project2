#[macro_use]
extern crate failure;

use clap::{App, Arg, SubCommand};
use kvs::{KvStore, Result};
use std::path::PathBuf;

fn main() -> Result<()> {
    let mut db = KvStore::open(PathBuf::from("./"))?;
    let matches = App::new("kvs")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(
            SubCommand::with_name("get")
                .about("get value from provided key")
                .arg(Arg::with_name("key").value_name("KEY").takes_value(true)),
        )
        .subcommand(
            SubCommand::with_name("set")
                .about("set key-value pair")
                .arg(
                    Arg::with_name("key")
                        .value_name("KEY")
                        .takes_value(true)
                        .required(true)
                        .index(1),
                )
                .arg(
                    Arg::with_name("value")
                        .value_name("VALUE")
                        .takes_value(true)
                        .required(true)
                        .index(2),
                ),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("remove key-value pair")
                .arg(Arg::with_name("key").value_name("KEY").takes_value(true)),
        )
        .get_matches();

    match matches.subcommand_name() {
        Some("get") => {
            let matches = matches
                .subcommand_matches("get")
                .expect("failed to unwrap get values");
            let key = matches
                .value_of("key")
                .ok_or(format_err!("Missing key value!"))?;

            match db.get(key.to_string())? {
                Some(value) => {
                    println!("{}", value);
                }
                None => {
                    println!("Key not found");
                }
            }
            std::process::exit(0);
        }
        Some("set") => {
            let matches = matches
                .subcommand_matches("set")
                .expect("failed to unwrap set values");
            // These error messages should be unreachable because of Clap? Consider "unwrap()"
            let key = matches
                .value_of("key")
                .ok_or(format_err!("Missing key value!"))?;
            let value = matches
                .value_of("value")
                .ok_or(format_err!("Missing value!"))?;
            db.set(key, value)?;
            std::process::exit(0);
        }
        Some("rm") => {
            let matches = matches
                .subcommand_matches("rm")
                .expect("failed to unwrap rm values");
            let key = matches
                .value_of("key")
                .ok_or(format_err!("Missing key value!"))?;
            match db.remove(key) {
                Ok(_) => {
                    std::process::exit(0);
                }
                Err(_) => {
                    println!("Key not found");
                    std::process::exit(1);
                }
            }
        }
        _ => {
            std::process::exit(1);
        }
    }
}
