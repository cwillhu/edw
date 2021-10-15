
# install libraries and python packages required by edw_etl_aws scripts

sudo apt-get install libpq-dev
sudo apt install python3-pip
python3 -m pip install --upgrade pip 

pip3 install sqlalchemy
pip3 install Cython
pip3 install pandas fiona shapely pyproj rtree
pip3 install geopandas
pip3 install geoalchemy2
pip3 install psycopg2
pip3 install bs4
pip3 install pyyaml
pip3 install "dask[dataframe]"
pip3 install distributed
pip3 install xlrd
pip3 install openpyxl
