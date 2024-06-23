use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use config::Config;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Inspect {},
    Generate {},
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let config_path = cli.config.unwrap_or(PathBuf::from("migi.toml"));
    let settings = Config::builder()
        .add_source(config::File::from(config_path))
        .build()?;
    let options = settings.try_deserialize::<migi::Config>()?.to_options()?;

    match &cli.command {
        Commands::Inspect {} => inspect(options)?,
        Commands::Generate {} => {}
    }

    Ok(())
}

fn inspect(options: migi::Options) -> Result<()> {
    todo!()
}
