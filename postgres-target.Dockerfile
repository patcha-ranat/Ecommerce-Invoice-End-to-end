# Use an appropriate base image
FROM postgres

# create table for the database with public schema
COPY target.sql /docker-entrypoint-initdb.d/

# RUN chmod a+r /docker-entrypoint-initdb.d/*