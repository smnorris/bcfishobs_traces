-- create output geometry table
DROP TABLE IF EXISTS temp.fishdistrib;
CREATE TABLE temp.fishdistrib (
  fishdistrib_id       serial primary key,
  blue_line_key            integer,
  downstream_route_measure numeric,
  upstream_route_measure   numeric,
  species_codes text[][],
  geom geometry(MultiLinestring, 3005)
);

-- add constraint to output table as well just to make sure things get written correctly
ALTER TABLE temp.fishdistrib
ADD CONSTRAINT fishdistrib_rt
UNIQUE (blue_line_key, downstream_route_measure, upstream_route_measure);

-- index the blkey & geom
CREATE INDEX ON temp.fishdistrib (blue_line_key);
CREATE INDEX ON temp.fishdistrib USING GIST (geom);

-- join events to streams based on blkey and measures, returning all un-cut segments
-- (everything except the top)
WITH unbroken_segments AS (
  SELECT
    e.blue_line_key,
    e.downstream_route_measure,
    e.upstream_route_measure,
    e.species_codes,
    s.linear_feature_id,
    s.geom
  FROM temp.fishdistrib_events e
  INNER JOIN  whse_basemapping.fwa_stream_networks_sp s
  ON e.blue_line_key = s.blue_line_key
  AND abs(e.downstream_route_measure - round(s.downstream_route_measure::numeric, 3)) < .01
  AND abs(e.upstream_route_measure - round(s.upstream_route_measure::numeric, 3)) < .01
  --WHERE e.species_codes @> '{"CH"}'::text[]
),

-- create ends of lines, don't bother with lines < 1m
end_segments AS (
  SELECT * FROM (
    SELECT
      e.blue_line_key,
      e.downstream_route_measure,
      e.upstream_route_measure,
      e.species_codes,
      s.linear_feature_id,
      ST_LineSubstring(
        s.geom,
        0,
        -- there are a very small number of records that return very small negative
        -- values - an invalid input for st_linesubstring - set to zero with GREATEST
        GREATEST(ROUND(
                   CAST((e.upstream_route_measure  - s.downstream_route_measure)
                          / s.length_metre AS NUMERIC),
                   5
                ),
                0)
      ) AS geom
    FROM temp.fishdistrib_events e
    INNER JOIN  whse_basemapping.fwa_stream_networks_sp s
    ON e.blue_line_key = s.blue_line_key
    AND abs(e.downstream_route_measure - round(s.downstream_route_measure::numeric, 3)) < .01
    AND e.upstream_route_measure < round(s.upstream_route_measure::numeric, 3)
    -- double check that this line hasn't already been added as an unbroken segment
    -- by joining back
    LEFT OUTER JOIN unbroken_segments u
    ON s.linear_feature_id = u.linear_feature_id
    --WHERE e.species_codes @> '{"CH"}'::text[]
    AND u.linear_feature_id IS NULL
    ) AS lines
  WHERE ST_Length(geom) > 1
),

-- lump everything together
all_segments AS
(
  SELECT * FROM unbroken_segments
  UNION ALL
  SELECT * from end_segments
)

-- merge the lines where possible
INSERT INTO temp.fishdistrib
(
  blue_line_key,
  species_codes,
  geom
)
SELECT
  blue_line_key,
  species_codes,
  ST_Multi(ST_Union(ST_Force2D(geom))) as geom
FROM all_segments
GROUP BY
  blue_line_key,
  species_codes
