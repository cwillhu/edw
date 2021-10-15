import yaml, sys, ftplib, os, time, errno, requests, traceback
from os.path import join, normpath, realpath, dirname, basename
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.types import TIMESTAMP
from edw_prototype.util.ts_print import ts_print as base_print

def ts_print(mystr, indent=0): #add script name
    base_print("{}: {}".format(basename(__file__), mystr), indent)
    
def set_status(frec, status, error = None): #set download status of file in DB
    frec.status = status
    frec.error = error
    session.commit()
    
#------------------------------
# DB settings
#------------------------------

credentialsPath = normpath(join(dirname(realpath(__file__)), '../credentials.yaml'))
credentials = yaml.load(open(credentialsPath))

userName = credentials['database']['username']
password = credentials['database']['password']

databaseURI = credentials['database']['hostname'] + ':' + str(credentials['database']['port']) + '/' + credentials['database']['databasename']
postgresURL = 'postgresql+psycopg2://' + userName + ':' + password +'@'+ databaseURI

#-------------------------------
# DB table model and connection
#-------------------------------

engine = create_engine(postgresURL, echo=False)
conn = engine.connect()

Base = declarative_base()
Base.metadata.schema = 'etl'  #set current schema

class EDW_Download(Base):   # create table model
    __tablename__ = 'edw_download'

    id = Column(Integer, primary_key=True)
    filename = Column(String)
    status = Column(String)    
    localDir = Column(String)
    remoteDir = Column(String)
    date = Column(TIMESTAMP, default=datetime.now, onupdate=datetime.now)
    scriptPath = Column(String)
    error = Column(String)

Base.metadata.create_all(engine) #register table model

Session = sessionmaker(bind=engine)
session = Session() #connect to db

#------------------------------------------------------
# Functions to download remote file and set status in DB
#------------------------------------------------------

def query_file(filename, localDir, remoteDir, scriptPath, skipFailed=True):  # Return file record

    filepath = join(localDir, filename)
    ts_print('{}: querying DB'.format(filepath))
    found = session.query(EDW_Download).filter_by(filename=filename, localDir=localDir).all()

    if len(found) == 0: 
        ts_print('{}: Not in DB'.format(filepath))
        frec = EDW_Download(filename=filename, localDir=localDir, remoteDir=remoteDir, scriptPath=scriptPath)
        session.add(frec)
        return frec
    
    if len(found) == 1:  # Found existing record
        frec = found[0]
        ts_print('{}: Found 1 record with status: {}'.format(filepath, frec.status))
        return frec
    
    elif len(found) >= 2:
        raise SystemExit('Multiple records found for ({}, {}, {})'.format(filename, localDir, remoteDir))

    
def ftp_download(ftpConn, remoteDir, localDir, filename, scriptPath, skipFailed=True):  # Download file via ftp

    filepath = join(localDir, filename)

    frec = query_file(filename, localDir, remoteDir, scriptPath, skipFailed)  # get file record
    if frec.status == 'SUCCESS' or (frec.status == 'FAILED' and skipFailed == True):  # skip record
        ts_print('{}: Skipping file with status {}'.format(filepath, frec.status))
        return

    # Update status and begin file download
    set_status(frec, 'DOWNLOADING')
    ts_print('{}: Downloading'.format(filepath))

    try:
        ftpConn.cwd(remoteDir)
        with open(filepath, 'wb') as f:
            ftpConn.retrbinary('RETR ' + filename, f.write)

    except ftplib.all_errors as e:
        set_status(frec, 'FAILED', error=str(e))

    else:
        set_status(frec, 'SUCCESS')
        ts_print('{}: Done'.format(filepath))


def http_download(url, localDir, scriptPath, skipFailed, timeout_tuple=(3,30)):  # Download file via HTTP

    filename = basename(url)
    filepath = join(localDir, filename)

    frec = query_file(filename, localDir, remoteDir=basename(url), scriptPath=scriptPath, skipFailed=skipFailed)  # get file record
    if frec.status == 'SUCCESS' or (frec.status == 'FAILED' and skipFailed == True):  # skip record
        ts_print('{}: Skipping file with status {}'.format(filepath, frec.status))
        return
    
    # Update status and begin file download
    set_status(frec, 'DOWNLOADING')
    ts_print('Downloading ' + url)

    try:
        req = requests.get(url, timeout=timeout_tuple) # timeouts:  https://docs.python-requests.org/en/master/user/advanced/#timeouts
        req.raise_for_status()

    except requests.exceptions.RequestException as e:
        ts_print("DOWNLOAD ERROR: " + str(e))        
        set_status(frec, 'FAILED', error=str(e))
        return

    # Write content to file
    ts_print('Writing ' + filepath)
    set_status(frec, 'WRITING')    

    try:
        os.makedirs(localDir, exist_ok=True)
        with open(filepath, 'wb') as f:
            f.write(req.content)

    except Exception as e:
        ts_print("WRITE ERROR: " + str(e))
        set_status(frec, 'FAILED', error=str(e))

    else:
        set_status(frec, 'SUCCESS')

