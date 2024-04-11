use testcontainers::core::WaitFor;
use testcontainers::Image;

use std::collections::HashMap;

/// name of the official postgis image on DockerHub
const NAME: &str = if cfg!(feature = "postgis-experimental-arm64") {
    "imresamu/postgis"
} else {
    "postgis/postgis"
};

/// Alpine version for minimal testing image
const TAG: &str = "16-3.4-alpine";
/// Default environment variables for the image
const ENV_VARS: &[(&str, &str)] = &[
    ("POSTGRES_DB", "postgres"),
    ("POSTGRES_USER", "postgres"),
    ("POSTGRES_PASSWORD", "postgres"),
];

/// PostGIS Image for testing
#[derive(Debug)]
pub struct PostGIS {
    env_vars: HashMap<String, String>,
}

impl PostGIS {
    /// Creates a new instance of a `PostGIS` Image.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable access from host, without authentication.
    pub fn with_host_auth(mut self) -> Self {
        self.env_vars
            .insert("POSTGRES_HOST_AUTH_METHOD".to_owned(), "trust".to_owned());
        self
    }
}

impl Default for PostGIS {
    fn default() -> Self {
        Self {
            env_vars: ENV_VARS
                .iter()
                .map(|(key, val)| (key.to_string(), val.to_string()))
                .collect(),
        }
    }
}

impl Image for PostGIS {
    type Args = ();

    fn name(&self) -> String {
        NAME.to_string()
    }

    fn tag(&self) -> String {
        TAG.to_string()
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        )]
    }

    fn env_vars(&self) -> Box<dyn Iterator<Item = (&String, &String)> + '_> {
        Box::new(self.env_vars.iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use testcontainers::clients;

    /// Checks if the container can be set up and if the postgis extension is correctly set.
    #[test]
    fn postgres_check_postgis_and_version() {
        let docker = clients::Cli::default();
        let postgis_image = PostGIS::default().with_host_auth();
        let node = docker.run(postgis_image);

        let connection_string = format!(
            "postgres://postgres@127.0.0.1:{}/postgres",
            node.get_host_port_ipv4(5432)
        );

        let mut conn = postgres::Client::connect(&connection_string, postgres::NoTls).unwrap();

        let rows = conn.query("SELECT * FROM pg_extension;", &[]).unwrap();
        //Check if an entry for postgis can be found in the extensions of the postgres database.
        rows.iter()
            .find(|row| {
                let extension: &str = row.get("extname");
                let version: &str = row.get("extversion");
                extension == "postgis" && version.starts_with("3.4")
            })
            .unwrap();
    }
}
