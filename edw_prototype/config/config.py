import yaml
from pathlib import Path

# Read config file
config_filepath = Path(__file__).parent.joinpath('config.yaml')
with open(config_filepath) as f:
    config = yaml.load(f, Loader=yaml.Loader)

##
# Logging settings
##

logDir = config['logging']['logDirectory']

##
# URL and path settings
##

epa_downloadDir = config['EPA']['downloadDirectory']
epa_extractDir = config['EPA']['extractDirectory']

isd_extractDir = config['NOAA']['ISDExtractDir']
