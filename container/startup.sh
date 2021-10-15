#!/usr/bin/env bash
set -e

## Install local packages for EDW
for package in edw_prototype pypostgres
do
    if [ -d "/home/$USER/$package" ]; then 
        python3 -m pip install -e "/home/$USER/$package"
    else
        echo "Directory /home/$USER/$package does not exist."
	exit 1
    fi
done

## Run passed command
if [[ "$1" ]]; then
    eval "$@"
fi
