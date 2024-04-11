use crate::builder::ImporterBuilder;
use crate::import_config::ImportConfig;
use crate::{SourceType, StoreType};
use clap::Args;

#[derive(Args, Debug)]
pub struct ImportArgs {
    /// the source to import from.
    pub(super) source: String,
    /// the type of the source. GeoRocket will attempt to infer the type if not specified
    #[arg(long)]
    pub(super) source_type: Option<SourceType>,
    /// the destination to import to.
    #[arg(long, short)]
    pub(super) destination: Option<String>,
    /// the type of the destination.
    #[arg(long)]
    pub(super) destination_type: Option<StoreType>,
}

pub async fn run_import(args: ImportArgs) -> anyhow::Result<()> {
    let config = ImportConfig::from_import_args_with_fallback(args)?;
    let builder: ImporterBuilder = config.into();
    let importer = builder.build().await?;
    importer.run().await
}
