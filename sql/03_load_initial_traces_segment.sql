-- next, load segment on which event lies
INSERT INTO temp.fishdistrib_events_temp
 (blue_line_key,
  linear_feature_id,
  downstream_route_measure,
  upstream_route_measure,
  species_codes)

SELECT DISTINCT * FROM
(
  SELECT
    b.blue_line_key,
    b.linear_feature_id,
    round(b.downstream_route_measure::numeric, 3) as downstream_route_measure,
    round(a.downstream_route_measure::numeric, 3) as upstream_route_measure,
    ARRAY[%s] AS species_codes
  FROM whse_fish.fiss_fish_obsrvtn_events a
  INNER JOIN whse_basemapping.fwa_stream_networks_sp b
  ON b.linear_feature_id = a.linear_feature_id
  WHERE a.maximal_species @> ARRAY[%s]
) as obs
ON CONFLICT (blue_line_key, downstream_route_measure, upstream_route_measure)
DO UPDATE SET species_codes = fishdistrib_events_temp.species_codes||ARRAY[%s];

