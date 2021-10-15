#
# psql, python3 and python packages required for EDW ETL
#

FROM ubuntu:20.04

#------------------------------------------------------------------------------
# Basic initial system configuration
#------------------------------------------------------------------------------

RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections

RUN yes | unminimize

RUN apt-get update \
	&& apt-get -y install apt-utils 

# we're going to create a non-root user at runtime and give the user sudo
RUN apt-get update \
	&& apt-get -y install sudo \
	&& echo "Set disable_coredump false" >> /etc/sudo.conf

# set locale info
RUN echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen \
	&& apt-get update && apt-get -y install locales \
	&& locale-gen en_US.utf8 \
	&& /usr/sbin/update-locale LANG=en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LANG en_US.UTF-8
ENV TZ=America/New_York

WORKDIR /tmp

#------------------------------------------------------------------------------
# Install system tools and libraries via apt
#------------------------------------------------------------------------------

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
	&& apt-get -y install \
		ca-certificates \
		wget \
		curl \
		tmux \
		git \
		libgit2-dev \
		libpq-dev \
		libssl-dev \
		postgis \
		emacs \
		vim \
		man-db \
		python3-dev \
		python3 \ 
		python3-pip \
		alien \
		pkg-config \
		unzip \
		iputils-ping \
		net-tools \
		ssh \
	&& rm -rf /var/lib/apt/lists/*

# Install version 11 of psql client:
RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" >> /etc/apt/sources.list.d/pgdg.list' \
    && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
RUN apt-get update \
    && apt-get install -y postgresql-11 \
    && apt-get install -y postgresql-client-11

#------------------------------------------------------------------------------
# Configure python
#------------------------------------------------------------------------------

#upgrade to latest version of pip (otherwise pyproj will not install), and install packages
RUN python3 -m pip install --upgrade pip \
     && python3 -m pip install \
          pandas \
		  numpy \
		  folium \
          sqlalchemy \
          fiona \
          shapely \
          pyproj \
          rtree \
          geopandas \
          geoalchemy2 \
          psycopg2-binary \
          bs4 \
          pyyaml \
          dask[dataframe] \
          xlrd \
          openpyxl \
          pytest \
          importlib_resources  #backport

#------------------------------------------------------------------------------
# Final odds and ends
#------------------------------------------------------------------------------

RUN echo 'export PS1="[\u@docker] \W # "' >> /etc/bash.bashrc

COPY startup.sh /tmp/startup.sh
RUN chmod +x /tmp/startup.sh
ENTRYPOINT ["/tmp/startup.sh"]