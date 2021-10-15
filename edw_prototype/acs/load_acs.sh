#!/bin/bash

# Import ACS sql.gz files to Postgres DB

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

gzfiles=$(cat $SCRIPT_DIR/gzfiles.txt)
conn_string=$(cat $SCRIPT_DIR/conn_string.secret)
logfile=import_"$(date +"%Y%m%d_%H_%M")".log

#clear log file
: > $SCRIPT_DIR/log/$logfile  

#load sql dumps into postgres
for gzfile in $gzfiles; do
    echo $(date) "Importing $gzfile..."                        | tee -a $SCRIPT_DIR/log/$logfile
    zcat /mnt/scratch/acs/"$gzfile" | psql "$conn_string" 2>&1 | tee -a $SCRIPT_DIR/log/$logfile
done

#log completion time
echo $(date) "Done" | tee -a $SCRIPT_DIR/log/$logfile
