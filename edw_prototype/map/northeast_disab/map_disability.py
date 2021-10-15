from PIL import Image
import json, time, os, io
import os.path as p
import geopandas as gpd
import psycopg2, yaml, folium, shapely
from folium import GeoJson
from folium.features import DivIcon
import branca.colormap

# Fetch data from postgres
creds = yaml.load(open('cred.yaml.secret'), Loader=yaml.Loader)
conn = psycopg2.connect(**creds)
sql = "select * from yearly_disability_northeast_metros"
df = gpd.GeoDataFrame.from_postgis(sql, conn, geom_col='c19_geom' )

# Find min, max values for colorscale
perc_colnames = [f'perc_18to64_disab_{x}' for x in range(2010,2020)]
dfmin = df[[*perc_colnames]].min().min()
dfmax = df[[*perc_colnames]].max().max()

# Set colorscale
colorscale = branca.colormap.linear.Reds_09.scale(dfmin, dfmax)
colorscale.caption = 'Percent of 18- to 64-year-olds with physical or cognitive impairment'

for frame_index, year in enumerate(range(2010,2020)):
    
    # Create map
    m = folium.Map(location=[43.55, -70.994557], zoom_start = 6.5, tiles='CartoDB positron', width='100%')

    # Add legend
    colorscale.add_to(m)    
    
    perc_colname = f'perc_18to64_disab_{year}'  #name of column containing data value to display
    
    def style_function(feature):
        ''' Set color which corresponds to this polygon's data value '''
        val = df[perc_colname].get(int(feature["id"][-5:]))
        return {
            "fillOpacity": 0.9,
            "weight": 0,
            "fillColor": colorscale.rgba_hex_str(val)
        }
    
    # Add polygons
    folium.GeoJson(
        data=df[['c19_geom', 'c19_display_name']],
        name="Percent Disabled Ages 18-64",
        style_function=style_function,
        smooth_factor=.08
    ).add_to(m)    
    
    # Add year label
    folium.map.Marker(
        [43.157220, -69.478542],
        icon=DivIcon(
            icon_size=(300,36),
            icon_anchor=(0,0),
            html=f'<div style="font-size: 20pt">{year}</div>',
            )
    ).add_to(m)
    
    # Export to png
    img_data = m._to_png(5)
    img = Image.open(io.BytesIO(img_data))
    img.save(p.join(os.getcwd(), f'perc_disab_{str(frame_index).zfill(2)}.png'))
