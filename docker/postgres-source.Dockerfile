# Use an appropriate base image
FROM postgres:13

# initial data file from the host machine to the container
COPY data/uncleaned_data.csv /data/

# create table at entrypoint
COPY ./docker/setup.sql /docker-entrypoint-initdb.d/

# RUN chmod a+r /docker-entrypoint-initdb.d/*