-- Create temp prelim event table
DROP TABLE IF EXISTS temp.fishdistrib_events_temp;
CREATE TABLE temp.fishdistrib_events_temp (
  fishdistrib_event_id       serial primary key,
  linear_feature_id        integer,
  blue_line_key            integer,
  downstream_route_measure numeric,
  upstream_route_measure   numeric,
  species_codes text[]
);

-- Add a unique constraint on blue line key and measures
ALTER TABLE temp.fishdistrib_events_temp
ADD CONSTRAINT fishdistrib_events_temp_rt
UNIQUE (blue_line_key, downstream_route_measure, upstream_route_measure);


CREATE INDEX ON temp.fishdistrib_events_temp (blue_line_key);
CREATE INDEX ON temp.fishdistrib_events_temp (linear_feature_id);






