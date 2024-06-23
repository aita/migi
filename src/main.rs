use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Init,
    Inspect {},
    Generate {},
}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Init) => {
            todo!()
        }
        Some(Commands::Inspect {}) => {
            inspect();
        }
        Some(Commands::Generate {}) => {}
        None => {
            todo!()
        }
    }
}

fn inspect() {
    todo!()
}
