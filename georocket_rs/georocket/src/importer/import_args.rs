use crate::importer::{ImporterBuilder, SourceType, StoreType};
use anyhow::Context;
use clap::Args;

#[derive(Args, Debug)]
pub struct ImportArgs {
    /// the source to import from.
    source: String,
    /// the type of the source. GeoRocket will attempt to infer the type if not specified
    #[arg(long)]
    source_type: Option<SourceType>,
    /// the destination to import to.
    #[arg(long, short)]
    destination: Option<String>,
    /// the type of the destination.
    #[arg(long)]
    destination_type: Option<StoreType>,
}

pub async fn run_import(args: ImportArgs) -> anyhow::Result<()> {
    let builder: ImporterBuilder = args.try_into()?;
    let importer = builder.build().await?;
    importer.run().await
}

impl TryFrom<ImportArgs> for ImporterBuilder {
    type Error = anyhow::Error;

    fn try_from(args: ImportArgs) -> Result<Self, Self::Error> {
        let source = args.source;
        let destination = args
            .destination
            .context("No storage destination specified")?;
        let source_type = match args.source_type {
            None => unimplemented!("inferring source is not yet implemented"),
            Some(source_type) => source_type,
        };
        let store_type = match args.destination_type {
            None => unimplemented!("Get store information from environment/config-file"),
            Some(store_type) => store_type,
        };
        let builder = ImporterBuilder::new()
            .with_source_config(source)
            .with_source_type(source_type)
            .with_store_config(destination)
            .with_store_type(store_type);

        Ok(builder)
    }
}
