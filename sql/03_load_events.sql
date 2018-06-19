-- create load output event table by aggregating the initial traces, then
-- removing any overlapping segments (where the different upstream_route_measure of
-- a maximal observation - where a line gets split - doesn't allow for easy aggregation)


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
),

-- next, re-aggregate by species code. This is almost enough to create the output,
-- but overlapping events remain where streams are getting split lower in the system
agg AS
(SELECT
  blue_line_key,
  downstream_route_measure,
  upstream_route_measure,
  array_agg(DISTINCT species_code) as species_codes
FROM popped
GROUP BY blue_line_key, downstream_route_measure, upstream_route_measure
ORDER BY blue_line_key, downstream_route_measure, upstream_route_measure
),

-- remove overlaps, created at max extent of given species that is covered by others.
-- This is a bit awkward - find them by doing self join on dnstr measure
agg_overlap AS (
SELECT
  l.blue_line_key,
  l.downstream_route_measure as l_dnstrmeas,
  l.upstream_route_measure as l_upstrmeas,
  l.species_codes as l_speciescodes,
  u.downstream_route_measure as u_dnstrmeas,
  u.upstream_route_measure as u_upstrmeas,
  u.species_codes as u_speciescodes,
  l.species_codes||u.species_codes as species_codes
FROM agg l
INNER JOIN agg u ON l.blue_line_key = u.blue_line_key
AND l.downstream_route_measure = u.downstream_route_measure
AND l.upstream_route_measure < u.upstream_route_measure
),

-- with the overlaps found, fix the measures to remove overlap
fixed_overlaps AS (
SELECT
  blue_line_key,
  l_dnstrmeas AS downstream_route_measure,
  l_upstrmeas AS upstream_route_measure,
  species_codes
FROM agg_overlap
UNION ALL
SELECT
  blue_line_key,
  l_upstrmeas AS downstream_route_measure,
  u_upstrmeas AS upstream_route_measure,
  u_speciescodes AS species_codes
FROM agg_overlap
ORDER BY downstream_route_measure
)

-- now put things back together, replacing the original records with the fixed ones
INSERT INTO temp.fishdistrib_events
(  blue_line_key,
   downstream_route_measure,
   upstream_route_measure,
   species_codes
)
SELECT a.* FROM agg a
LEFT OUTER JOIN agg_overlap o
ON a.blue_line_key = o.blue_line_key
AND a.downstream_route_measure = o.l_dnstrmeas
WHERE o.blue_line_key IS NULL
UNION ALL
SELECT * FROM fixed_overlaps
ORDER BY blue_line_key, downstream_route_measure



