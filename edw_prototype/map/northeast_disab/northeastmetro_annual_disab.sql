

----------------------------------------------------------------------------------------------
-- Select number impaired/non-impaired, aged 18 to 64, in Northwest metro areas, for each year
----------------------------------------------------------------------------------------------

--First remove duplicate rows in tiger2019.census_name_lookup
drop table if exists tiger2019.census_name_lookup_unique;
create table tiger2019.census_name_lookup_unique as
    select distinct display_name, simple_name, prefix_match_name, sumlevel, geoid, full_geoid, priority, population, aland, awater, geom
	from tiger2019.census_name_lookup;


drop table if exists yearly_disability_northeast_metros;
create table yearly_disability_northeast_metros as
with counts as (
  select 
    c19.full_geoid as c19_full_geoid, 
    c19.display_name as c19_display_name, 
    c19.geom as c19_geom,
    st_astext(c19.geom) as c19_geom_wkt,
	
    b19.b18101010 + b19.b18101029 + b19.b18101013 + b19.b18101032 + b19.b18101016 + b19.b18101035 + b19.b18101019 + b19.b18101038 as acs19_disab_18to64_num,
    b19.b18101011 + b19.b18101030 + b19.b18101014 + b19.b18101033 + b19.b18101017 + b19.b18101036 + b19.b18101020 + b19.b18101039 as acs19_nodisab_18to64_num,	

    b18.b18101010 + b18.b18101029 + b18.b18101013 + b18.b18101032 + b18.b18101016 + b18.b18101035 + b18.b18101019 + b18.b18101038 as acs18_disab_18to64_num,
    b18.b18101011 + b18.b18101030 + b18.b18101014 + b18.b18101033 + b18.b18101017 + b18.b18101036 + b18.b18101020 + b18.b18101039 as acs18_nodisab_18to64_num,	

    b17.b18101010 + b17.b18101029 + b17.b18101013 + b17.b18101032 + b17.b18101016 + b17.b18101035 + b17.b18101019 + b17.b18101038 as acs17_disab_18to64_num,
    b17.b18101011 + b17.b18101030 + b17.b18101014 + b17.b18101033 + b17.b18101017 + b17.b18101036 + b17.b18101020 + b17.b18101039 as acs17_nodisab_18to64_num,
	
    b16.b18101010 + b16.b18101029 + b16.b18101013 + b16.b18101032 + b16.b18101016 + b16.b18101035 + b16.b18101019 + b16.b18101038 as acs16_disab_18to64_num,
    b16.b18101011 + b16.b18101030 + b16.b18101014 + b16.b18101033 + b16.b18101017 + b16.b18101036 + b16.b18101020 + b16.b18101039 as acs16_nodisab_18to64_num,
	
    b15.b18101010 + b15.b18101029 + b15.b18101013 + b15.b18101032 + b15.b18101016 + b15.b18101035 + b15.b18101019 + b15.b18101038 as acs15_disab_18to64_num,
    b15.b18101011 + b15.b18101030 + b15.b18101014 + b15.b18101033 + b15.b18101017 + b15.b18101036 + b15.b18101020 + b15.b18101039 as acs15_nodisab_18to64_num,
	
    b14.b18101010 + b14.b18101029 + b14.b18101013 + b14.b18101032 + b14.b18101016 + b14.b18101035 + b14.b18101019 + b14.b18101038 as acs14_disab_18to64_num,
    b14.b18101011 + b14.b18101030 + b14.b18101014 + b14.b18101033 + b14.b18101017 + b14.b18101036 + b14.b18101020 + b14.b18101039 as acs14_nodisab_18to64_num,

    b13.b18101010 + b13.b18101029 + b13.b18101013 + b13.b18101032 + b13.b18101016 + b13.b18101035 + b13.b18101019 + b13.b18101038 as acs13_disab_18to64_num,
    b13.b18101011 + b13.b18101030 + b13.b18101014 + b13.b18101033 + b13.b18101017 + b13.b18101036 + b13.b18101020 + b13.b18101039 as acs13_nodisab_18to64_num,	

    b12.b18101010 + b12.b18101029 + b12.b18101013 + b12.b18101032 + b12.b18101016 + b12.b18101035 + b12.b18101019 + b12.b18101038 as acs12_disab_18to64_num,
    b12.b18101011 + b12.b18101030 + b12.b18101014 + b12.b18101033 + b12.b18101017 + b12.b18101036 + b12.b18101020 + b12.b18101039 as acs12_nodisab_18to64_num,

    b11.b18101010 + b11.b18101029 + b11.b18101013 + b11.b18101032 + b11.b18101016 + b11.b18101035 + b11.b18101019 + b11.b18101038 as acs11_disab_18to64_num,
    b11.b18101011 + b11.b18101030 + b11.b18101014 + b11.b18101033 + b11.b18101017 + b11.b18101036 + b11.b18101020 + b11.b18101039 as acs11_nodisab_18to64_num,	

    b10.b18101010 + b10.b18101029 + b10.b18101013 + b10.b18101032 + b10.b18101016 + b10.b18101035 + b10.b18101019 + b10.b18101038 as acs10_disab_18to64_num,
    b10.b18101011 + b10.b18101030 + b10.b18101014 + b10.b18101033 + b10.b18101017 + b10.b18101036 + b10.b18101020 + b10.b18101039 as acs10_nodisab_18to64_num

    from acs2019_1yr.b18101 b19
      inner join acs2018_1yr.b18101 b18
        on b19.geoid = b18.geoid
      inner join acs2017_1yr.b18101 b17
        on b19.geoid = b17.geoid
      inner join acs2016_1yr.b18101 b16
        on b19.geoid = b16.geoid
      inner join acs2015_1yr.b18101 b15
        on b19.geoid = b15.geoid
      inner join acs2014_1yr.b18101 b14
        on b19.geoid = b14.geoid
      inner join acs2013_1yr.b18101 b13
        on b19.geoid = b13.geoid
      inner join acs2012_1yr.b18101 b12
        on b19.geoid = b12.geoid
      inner join acs2011_1yr.b18101 b11
        on b19.geoid = b11.geoid
      inner join acs2010_1yr.b18101 b10
        on b19.geoid = b10.geoid
      inner join tiger2019.census_name_lookup_unique c19
        on c19.full_geoid = b19.geoid
    where c19.sumlevel = '350'  -- summary level 350: northeast metro areas
)
select *,
    100.0 * acs19_disab_18to64_num::float / (acs19_disab_18to64_num + acs19_nodisab_18to64_num) as perc_18to64_disab_2019,
    100.0 * acs18_disab_18to64_num::float / (acs18_disab_18to64_num + acs18_nodisab_18to64_num) as perc_18to64_disab_2018,
    100.0 * acs17_disab_18to64_num::float / (acs17_disab_18to64_num + acs17_nodisab_18to64_num) as perc_18to64_disab_2017,
    100.0 * acs16_disab_18to64_num::float / (acs16_disab_18to64_num + acs16_nodisab_18to64_num) as perc_18to64_disab_2016,
    100.0 * acs15_disab_18to64_num::float / (acs15_disab_18to64_num + acs15_nodisab_18to64_num) as perc_18to64_disab_2015,
    100.0 * acs14_disab_18to64_num::float / (acs14_disab_18to64_num + acs14_nodisab_18to64_num) as perc_18to64_disab_2014,
    100.0 * acs13_disab_18to64_num::float / (acs13_disab_18to64_num + acs13_nodisab_18to64_num) as perc_18to64_disab_2013,
    100.0 * acs12_disab_18to64_num::float / (acs12_disab_18to64_num + acs12_nodisab_18to64_num) as perc_18to64_disab_2012,
    100.0 * acs11_disab_18to64_num::float / (acs11_disab_18to64_num + acs11_nodisab_18to64_num) as perc_18to64_disab_2011,
    100.0 * acs10_disab_18to64_num::float / (acs10_disab_18to64_num + acs10_nodisab_18to64_num) as perc_18to64_disab_2010
from counts;

-- Write to csv
-- Note: Without "FORCE_QUOTE *", Postgres will only quote fields containing a comma, and unless all fields are quoted, SQL Server will treat
-- quotes as ordinary string characters (even if FIELD_QUOTE is set). Using "FORCE_QUOTE *" will generate a fully-quoted csv.
-- \copy yearly_disability_northeast_metros TO '/mnt/scratch/temp/yearly_disability_northeast_metros.csv' WITH (FORMAT CSV, HEADER TRUE, FORCE_QUOTE *)

