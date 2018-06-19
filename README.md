# bcfishobs_traces

Create traces of BC Fish Observation events downstream from observation point to the ocean via the FWA Stream Network.

## Requirements
- Python / Postgresql / PostGIS / GDAL
- [fwakit](https://github.com/smnorris/fwakit)
- [bcfishobs](https://github.com/smnorris/bcfishobs)


## Usage
Run `bcfishobs` to download observations and reference the points to streams.  

With that complete, modify the species list in `bcfishobs_traces.py` as required then run to create downstream traces and dump to shapefile. 

```
python bcfishobs_traces.py
```