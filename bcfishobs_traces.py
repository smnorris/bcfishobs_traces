import fwakit as fwa

species_list = ['ACT', 'BT', 'CH', 'CM', 'CO', 'CT', 'DV', 'GSG', 'NDC', 'PK', 'RB',
                'SK', 'SSU', 'ST', 'WSG']

db = fwa.util.connect()

# create output tables
db.execute(db.queries['01_create_trace_table'])

# load trace query and process each species separately
q_a = db.queries['02_load_initial_traces_dnstr']
q_b = db.queries['03_load_initial_traces_segment']
for species in species_list:
    print(species+' - query a')
    db.execute(q_a, (species, species, species))
    print(species+' - query b')
    db.execute(q_b, (species, species, species))


# with initial traces done, aggregate the results to create event table
print('loading events')
db.execute(db.queries['04_load_events_test'])
print('loading geometries')
db.execute(db.queries['05_load_geoms'])

# dump each species to shapefile
sql = """
           SELECT
             blue_line_key as bllnk,
             'ALL SPECIES' as species,
             geom
           FROM temp.fishdistrib
          """
db.pg2ogr(sql, 'ESRI Shapefile', 'fishtraces_all.shp')
for species in species_list:
    sql = """
           SELECT
             blue_line_key as bllnk,
             '{}' as species,
             ST_Union(geom) as geom
           FROM temp.fishdistrib
           WHERE species_codes @> ARRAY['{}']
           GROUP BY blue_line_key, species
          """.format(species, species)
    print(sql)
    db.pg2ogr(sql, 'ESRI Shapefile', 'fishtraces_{}.shp'.format(species))


# dump observation events to file as well
sql = """SELECT fish_observation_point_id,
        linear_feature_id        ,
        blue_line_key            ,
        waterbody_key            ,
        downstream_route_measure ,
        distance_to_stream       ,
        match_type               ,
        watershed_group_code     ,
        geom                     ,
        species_code             ,
        agency_id                ,
        observation_date         ,
        agency_name              ,
        source                   ,
        source_ref
        FROM whse_fish.fiss_fish_obsrvtn_events_vw"""
db.pg2ogr(sql, 'ESRI Shapefile', 'observation_events.shp')
