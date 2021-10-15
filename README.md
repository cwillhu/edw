# Exposome Data Warehouse

The Exposome Data Warehouse (EDW) contains data from the 
[American Community Survey](https://www.census.gov/programs-surveys/acs/data.html),
EPA [air quality monitoring system](https://www.epa.gov/outdoor-air-quality-data), 
and NOAA's [ISD-Lite weather measurements](https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database).

## ACS Tables

ACS tables in EDW correspond to 1-year, 3-year and 5-year [ACS summary files](https://assets.nhgis.org/original-data/acs/acs_summary-file_handbook_2019.pdf).

An overview can be found in the 
[ACS General Handbook](https://www.census.gov/content/dam/Census/library/publications/2020/acs/acs_general_handbook_2020.pdf).

The type of geography (county, zipcode, etc) that a particular table row pertains to is identified by the summary level. A key to summary levels can be found [here](https://www.census.gov/programs-surveys/geography/technical-documentation/naming-convention/cartographic-boundary-file/carto-boundary-summary-level.html).
## EPA Tables

- epa.annual
  - For a given year, the mean, standard deviation, etc for every monitor in the US
- epa.daily
  - Daily measurement (if only one exists) or summary of measurements for each day for each monitor
- epa.eight_hour
  - Eight-hour average calculated for every clock hour, for every monitor that had at least six valid hourly samples in the 8-hour block
- epa.sites
  - Geographic locations of sites
  - Provides a mapping to site zip codes (latitude/longitude/geometry columns are already in EPA data tables)

Detailed documention on EPA annual, daily, eight_hour and sites tables can be found
[here](https://aqs.epa.gov/aqsweb/airdata/FileFormats.html). 

The tables in EDW differ slightly from the EPA documentation as two addtional columns have been added: 
- geoid, which uniquely identifies each and is a concatenation of the state code, county code, and site number columns
- geom, which is a geometry column (SRID: 4326) generated from the latitude and longitude columns

## ISD-Lite Tables

- isd.isd_lite
  - Weather measurements (temperature, preciptation, etc) from stations across the US
  - Some stations report hourly measurements, others a few per day
- isd.stations
  - Geographic locations of stations, station names, elevation. (Geographic locations are also in isd.isd_lite table)

Documentation on ISD tables can be found in isd-lite-technical-document.pdf and isd-lite-format.pdf at the following site:
- ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-lite/

The columns time_local, date_local, geom and geoid have been added. geom is a geometry column (SRID: 4326) generated from the 
latitude and longitude columns. The geoid column is a concatenation of USAF and WBAN station IDs, and is primary key in the 
isd.stations table.
