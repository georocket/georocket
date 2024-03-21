insert into georocket.feature (id, raw_feature)
values
('a1e0c442-b1c5-49cf-b776-7f25e38ea477', 'feature_a'),
('1bb4a804-cbea-4949-bae4-d49def8bce12', 'feature_b'),
('d10e4694-af31-4499-9be1-73b48179b2d3', 'feature_c');

insert into georocket.bounding_box (id, bounding_box)
values 
('a1e0c442-b1c5-49cf-b776-7f25e38ea477', st_makeenvelope(1, 1, 3, 3, 4326)),
('1bb4a804-cbea-4949-bae4-d49def8bce12', st_makeenvelope(3, 6, 5, 8, 4326)),
('d10e4694-af31-4499-9be1-73b48179b2d3', st_makeenvelope(6, 2, 8, 4, 4326));

insert into georocket.property (id, key, value_s)
values
('a1e0c442-b1c5-49cf-b776-7f25e38ea477', 'key_a', 'value_a'),
('1bb4a804-cbea-4949-bae4-d49def8bce12', 'key_b', 'value_b'),
('d10e4694-af31-4499-9be1-73b48179b2d3', 'key_c', 'value_c');

insert into georocket.property (id, key, value_i)
values 
('a1e0c442-b1c5-49cf-b776-7f25e38ea477', 'key_int', 1);

insert into georocket.property (id, key, value_f)
values 
('1bb4a804-cbea-4949-bae4-d49def8bce12', 'key_float', 1.0);

insert into georocket.property (id, key, value_s)
values 
('d10e4694-af31-4499-9be1-73b48179b2d3', 'key_string', '1');