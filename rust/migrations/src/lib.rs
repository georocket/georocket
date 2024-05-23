#[cfg(feature = "postgis")]
pub mod postgis {
    refinery::embed_migrations!("./migrations/postgis");
}
