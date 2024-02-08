CREATE SCHEMA IF NOT EXISTS georocket;

CREATE TABLE georocket.feature (
    id UUID,
    raw_feature TEXT
);

CREATE TABLE georocket.bounding_box (
    id UUID,
    bounding_box GEOMETRY
);

CREATE TABLE georocket.property (
    id UUID,
    key TEXT,
    value TEXT
);