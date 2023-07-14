# Use an appropriate base image
FROM postgres

# Copy the necessary files from the host machine to the container
COPY data/cleaned_data.csv /data/

COPY setup.sql /docker-entrypoint-initdb.d/

# RUN chmod a+r /docker-entrypoint-initdb.d/*