-- create load output event table by aggregating the initial traces


-- Create output event table
DROP TABLE IF EXISTS temp.fishdistrib_events;
CREATE TABLE temp.fishdistrib_events (
  fishdistrib_event_id       serial primary key,
  blue_line_key            integer,
  downstream_route_measure numeric,
  upstream_route_measure   numeric,
  species_codes text[]
);

-- Add a unique constraint on blue line key and measures
ALTER TABLE temp.fishdistrib_events
ADD CONSTRAINT fishdistrib_events_rt
UNIQUE (blue_line_key, downstream_route_measure, upstream_route_measure);

-- index the blkey
CREATE INDEX ON temp.fishdistrib_events (blue_line_key);

-- now load the data
-- first, unnest the arrays and aggregate duplicate species on the same blue line
WITH popped AS (
SELECT
  blue_line_key,
  downstream_route_measure,
  MAX(upstream_route_measure) as upstream_route_measure,
  unnest(species_codes) as species_code
FROM temp.fishdistrib_events_temp e
GROUP BY blue_line_key, downstream_route_measure, species_code
ORDER BY blue_line_key, downstream_route_measure, species_code
)

-- next, re-aggregate by species code.
INSERT INTO temp.fishdistrib_events
(  blue_line_key,
   downstream_route_measure,
   upstream_route_measure,
   species_codes
)
SELECT
  blue_line_key,
  downstream_route_measure,
  upstream_route_measure,
  array_agg(DISTINCT species_code) as species_codes
FROM popped
GROUP BY blue_line_key, downstream_route_measure, upstream_route_measure
ORDER BY blue_line_key, downstream_route_measure, upstream_route_measure;