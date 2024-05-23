use testcontainers::core::WaitFor;
use testcontainers::Image;
use testcontainers_modules::postgres::Postgres;

const NAME: &str = "postgis/postgis";
const TAG: &str = "16-3.4-alpine";

#[derive(Debug, Default)]
pub struct PostGIS {
    postgres: Postgres,
}

impl PostGIS {
    pub fn with_host_auth(mut self) -> Self {
        self.postgres = self.postgres.with_host_auth();
        self
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
        self.postgres.ready_conditions()
    }

    fn env_vars(&self) -> Box<dyn Iterator<Item = (&String, &String)> + '_> {
        self.postgres.env_vars()
    }
}
