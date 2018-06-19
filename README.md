# bcfishobs_traces

Create traces of BC Fish Observation events downstream from observation point to the ocean via the FWA Stream Network.

## Requirements
- Python / Postgresql / PostGIS / GDAL
- [fwakit](https://github.com/smnorris/fwakit)
- [bcfishobs](https://github.com/smnorris/bcfishobs)


## Usage
Run `bcfishobs` to download observations and reference the points to streams.  

With that complete, create downstream traces and dump to shapefile:

```
python process.py
```