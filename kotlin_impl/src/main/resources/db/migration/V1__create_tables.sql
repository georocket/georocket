CREATE TABLE chunks (
  id VARCHAR,
  data bytea NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE documents (
  id VARCHAR,
  data jsonb NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE chunkMeta (
  id VARCHAR,
  data jsonb NOT NULL UNIQUE,
  PRIMARY KEY(id)
);
