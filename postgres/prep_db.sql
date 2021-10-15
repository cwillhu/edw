
-- Prepare postgres DB for data import

create database census;
create role census;  # role 'census' is required by ACS sql dumps
grant census to postsa;

--Enable PostGIS extensions.
--  Note: This extensions list is at https://postgis.net/install
\c census;
create extension postgis;
create extension postgis_topology;
create extension postgis_sfcgal;
create extension fuzzystrmatch;
create extension address_standardizer;
create extension address_standardizer_data_us;
create extension postgis_tiger_geocoder;
--create extension postgis_raster; --not supported by Azure Database for PostgreSQL

