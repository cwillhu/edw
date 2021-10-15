#!/bin/bash

# Download Census Reporter sql dumps of American Community Survey data.
#   See https://censusreporter.tumblr.com/post/73727555158/easier-access-to-acs-data

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
gzfiles=$(cat $SCRIPT_DIR/gzfiles.txt)
logfile=download_"$(date +"%Y%m%d_%H_%M")".log

: > $SCRIPT_DIR/log/$logfile  #clear log file

for gzfile in $gzfiles; do
    echo $(date) "Downloading $gzfile..."              | tee -a $SCRIPT_DIR/log/$logfile
    wget 'https://census-extracts.b-cdn.net/'"$gzfile" | tee -a $SCRIPT_DIR/log/$logfile    
done
