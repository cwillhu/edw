
# ISD-Lite table attributes

class prop: pass
prop.colTypeMap = {
    'year'                   : 'integer',
    'month'                  : 'integer',
    'day'                    : 'integer',
    'hour'                   : 'integer',
    'temperature'            : 'integer',
    'dew_point'              : 'integer',
    'sea_level_pressure'     : 'integer',
    'wind_direction'         : 'integer',
    'wind_speed'             : 'integer',
    'cloud_coverage_code'    : 'integer',
    'precipitation_one_hour' : 'integer',
    'precipitation_six_hour' : 'integer',
    'usaf_station_id'        : 'varchar',
    'wban_station_id'        : 'varchar',
    'station_name'           : 'varchar',
    'country'                : 'varchar',
    'state'                  : 'varchar',
    'icao'                   : 'varchar',
    'latitude'               : 'numeric',
    'longitude'              : 'numeric',
    'elevation'              : 'numeric',
    'begin_date'             : 'date',
    'end_date'               : 'date'
}

class stations: pass
prop.stations = stations
prop.stations.colNames_orig = [
    'usaf_station_id',
    'wban_station_id',
    'station_name',
    'country',
    'state',
    'icao',
    'latitude',
    'longitude',
    'elevation',
    'begin_date',
    'end_date'
]

class isd_lite: pass
prop.isd_lite = isd_lite
prop.isd_lite.colNames_orig = [
    'year',
    'month',
    'day',
    'hour',
    'temperature',
    'dew_point',
    'sea_level_pressure',
    'wind_direction',
    'wind_speed',
    'cloud_coverage_code',
    'precipitation_one_hour',
    'precipitation_six_hour',
    'usaf_station_id',
    'wban_station_id'
]

