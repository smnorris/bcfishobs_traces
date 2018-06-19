from sqlalchemy.sql import text

import fwakit as fwa

species_list = ['ACT', 'BT', 'CH', 'CM', 'CO', 'CT', 'DV', 'GSG', 'NDC', 'PK', 'RB',
                'SK', 'SSU', 'ST', 'WSG']

db = fwa.util.connect()

# create output tables
db.execute(db.queries['01_create_trace_table'])

# load trace query and process each species separately
sql = text(db.queries['02_load_initial_traces'])
for species in species_list:
    db.engine.execute(sql, species=species)

# with initial traces done, aggregate the results to create event table
#db.execute(db.queries['03_load_events'])
#db.execute(db.queries['04_load_geoms'])

# dump each species to shapefile
sql = """
           SELECT
             fishdistrib_id as id,
             blue_line_key as bllnk,
             array_to_string(species_codes, ', ') as species_codes,
             geom
           FROM temp.fishdistrib
          """
#db.pg2ogr(sql, 'ESRI Shapefile', 'fishdistrib.shp')
