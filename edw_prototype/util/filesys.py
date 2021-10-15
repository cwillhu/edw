import os
import os.path as p
from glob import glob
from zipfile import ZipFile

# List directory contents
def list_dir(directory, pattern='*', returnFiles=True, returnDirectories=False):  #pattern is a glob pattern (not RE)
    itemList = []
    searchPat = p.join(directory, pattern)
    if returnFiles:
        items = [x for x in glob(searchPat) if p.isfile(x)]
        itemList.extend(items)
    if returnDirectories:
        items = [x for x in glob(searchPat) if p.isdir(x)]
        itemList.extend(items)
    return itemList

# Extract contents of zip file
def extract_zip(zipfile, destinationDir):
    os.makedirs(destinationDir, exist_ok=True)
    with ZipFile(zipfile, 'r') as f:
        f.extractall(destinationDir)
