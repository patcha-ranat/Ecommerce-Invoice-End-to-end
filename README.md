# Development Note
End-to-end fullstack data project practice.

tools:
- Postgres Database in Docker
    - source
    - data warehouse
- MinIO
    - data lake
- airflow
    - orchestration (ETLs process)
- Bigquery
    - data warehouse
- PowerBI
    - Dashboard for EDA
- Python
    - Model Development
        - jupyter-notebook
    - Deploy model
        - FastAPI

## 1. Set up Postgres source (docker compose)
---
### **Step 1**
Run with docker compose
```bash
docker compose build
```
To execute bash command in `Dockerfile`.
- Copying csv file (`cleaned_data.csv`) to docker container's local
- Copying sql file (`setup.sql`) to `docker-entrypoint-initdb.d` to be executed when we initialize container.

### **Step 2**
Initialize docker container(s) and run process in background (Detach mode)
```bash
docker compose up -d
```

***Note:** some services need time to start, check container's logs from `docker desktop` to see if services are ready to work with.*

To check status of running containers.
```bash
docker ps
```

### **Step 3**
Get into command-line or bash of container we specified.
```bash
docker exec -it <container-name-or-id> bash
```
Note: Get container's name or id from `docker-compose.yml` or from `docker ps` command.

At this step, we can check if csv file we meant to execute in Dockerfile is executed successfully by:
```bash
ls
ls data/
ls docker-entrypoint-initdb.d/
```

### **Step 4**
Access to database to check if csv file copied into database.
```bash
psql -U postgres -d mydatabase
```
Then we will be mounted into postgres' bash

Then we will check table, and schema we executed by `setup.sql` file
```bash
\dt or \d
\dn or \z
```
if we see table and schema are corrected and showed, then importing csv to the Postgres database part is done.

if not, these can be issues
- check if `setup.sql` is executed successfully, by inspecting logs in docker desktop
- check if data csv file and `setup.sql` are copied into docker container's local by using container's bash and check path in `Dockerfile` and `setup.sql`
- we need to set search_path by
```bash
SET search_path TO <myschema>;
```
To set only in current session.
```bash
ALTER DATABASE <mydatabase> SET search_path TO <myschema>; 
```
To set permanently at database level.

Then exit from all bash
```bash
\q
exit
```
### **Step 5**
Don't forget to remove all image and containers
```bash
docker compose down -v
```

## 2. Set up Data Lake (MinIO)
