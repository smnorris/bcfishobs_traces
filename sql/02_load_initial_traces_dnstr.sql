-- first, load all downstream segments

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
    round(b.upstream_route_measure::numeric, 3) as upstream_route_measure,
    ARRAY[%s] AS species_codes
  FROM whse_fish.fiss_fish_obsrvtn_events a
  INNER JOIN whse_basemapping.fwa_stream_networks_sp b
  ON b.linear_feature_id != a.linear_feature_id AND
        (
          -- donwstream criteria 1 - same blue line, lower measure
          (b.blue_line_key = a.blue_line_key AND
           b.downstream_route_measure <= a.downstream_route_measure)
          OR
          -- criteria 2 - watershed code a is a child of watershed code b,
          -- (but not equal, that has to be handled by the blue line)
          (b.wscode_ltree @> a.wscode_ltree
              AND b.wscode_ltree != a.wscode_ltree
              AND (
                   -- local code is lower or wscode and localcode are equivalent
                   (
                    b.localcode_ltree < subltree(a.localcode_ltree, 0, nlevel(b.localcode_ltree))
                    OR b.wscode_ltree = b.localcode_ltree
                   )
                   -- OR any missed side channels on the same watershed code
                   OR (b.wscode_ltree = a.wscode_ltree AND
                       b.blue_line_key != a.blue_line_key AND
                       b.localcode_ltree < a.localcode_ltree)
                   )
          )
      )
  WHERE a.maximal_species @> ARRAY[%s]
) as obs
ON CONFLICT (blue_line_key, downstream_route_measure, upstream_route_measure)
DO UPDATE SET species_codes = fishdistrib_events_temp.species_codes||ARRAY[%s];

