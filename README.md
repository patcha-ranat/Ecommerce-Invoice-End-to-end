# Ecommerce-Invoice Development Note
*Patcharanat P.*

End-to-end data project for practice. ETLs (batch), Visualization, Machine Learning development for ecommerce invoice analysis.


*The project is in development . . .*

Tools:
- Sources
    - Postgres Database (Data warehouse)
    - REST API (raw file url)
    - API (with token)
- Data Lake
    - MinIO
    - Google Cloud Storage
- Data Warehouse
    - Postgres Database
    - Bigquery
- Orchestrator
    - Airflow
- Virtualization
    - Docker compose (for local demo environment)
- Visualization
    - PowerBI (Dashboard)
- Machine Learning Model
    - Jupyter Notebook (Model development)
    - FastAPI (Model Deployment)

Prerequisite:
- Docker and Docker compose installed.
- Postgresql installed (just in case).
- Get a credentials file from kaggle and activate the token for API.


## 1. Setting up environment
---
Firstly, clone this repository to obtain all neccessary files, then use it as working directory.
```bash
git clone https://github.com/Patcharanat/ecommerce-invoice
```
We need to set up environment to demo ETLs process locally, which including:
- Postgres database
- Minio
- Airflow

All of the tools need to be run in different environment to simulate real use-case. So, Docker and Docker compose become important for demo.

***Note:** the hardest part is setting up environment for demo*

### **Step 1: Setting up overall services (containers)**
Run with docker compose to execute bash command in `Dockerfile` by:
```bash
docker compose build
```
- Copying csv file (`cleaned_data.csv`) to postgres container's local
- Copying sql file (`setup.sql`) to `docker-entrypoint-initdb.d` to be executed when we initialize container.
- Installing `requirements.txt` for airflow's container to run libraries we needed in DAGs.
- Add a credentials file: `kaggle.json` (in this case we use Kaggle API) to make API usable.

### **Step 2: Intiating all containers**
Initialize docker container(s) and run process in background (Detach mode)
```bash
docker compose up -d
```

***Note:** some services need time to start, check container's logs from `docker desktop` to see if the services are ready to work with.*

To check status of running containers:
```bash
docker ps
```

### **Step 3: checking if all Dockerfiles correctly executed**
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
What you should check more is that credentials file: `kaggle.json` correctly imported in airflow's scheduler and webservice containers.

### **Step 4: Checking data in database**
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
- check if data csv file and `setup.sql` are copied into docker container's local by using container's bash and check if path in `Dockerfile` and `setup.sql` were set correctly.
- we need to set search_path by
```bash
SET search_path TO <myschema>;
```
to set only in current session.
```bash
ALTER DATABASE <mydatabase> SET search_path TO <myschema>; 
```
to set permanently at database level.

Then exit from all bash
```bash
\q
exit
```
### **Step 5: Exiting**
Don't forget to remove all image and containers when you're done.
```bash
docker compose down -v
```

### **Step 6: Setting up Data Lake (MinIO)**
After running `docker compose up -d`, check if MinIO is accessible via web browser with URL "http://localhost:9001" or "http://localhost:9000" using `username` and `password` we specified in `docker-compose.yml`.

### **Step 7: Setting up Airflow Web UI**

To set up airflow, we need to define more 4 services that refer to [official's .yml file template](https://airflow.apache.org/docs/apache-airflow/2.6.1/docker-compose.yaml) including `airflow-postgres` to be backendDB, `airflow-scheduler` to make scheduler, `airflow-webserver` to make airflow accessible via web UI, and `airflow-init` to initiate airflow session.

Trying to understand how every components in services work make much more easier to comprehend and debug issues that occur, such as depends-on, environment, healthcheck, and storing object in `&variable`.

***Note:*** In `docker-compose.yml` file, Identation is very important.

**For this project**, we create 2 postgres containers, so we need to check carefully if airflow connect to its own backendDB or the right database.

<details><summary>Issue debugged</summary>
<p>
use this template from official's document in `.env` file:

```python
postgresql+psycopg2://<user>:<password>@<host>/<db>

#or

[dialect]+[driver]://[username:password]@[host:port]/[database]

# which results in

AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@airflow-postgres/airflow
```

</p>
</details>

***Note:*** In .env file, airflow core need *FERNET* key which can be obtained from fernet.py (random generated)

## 2. Writing DAGs
---
In my case (this project), I used a dataset from kaggle which loaded to postgres database, uploaded to this repo github and I wrote DAGs to use Kaggle API to obtain the dataset directly. If you want to change data, you have to write your own DAGs which I highly recommend for steeper learning curve.

### **Step 1: Setting connections between Data Lake, and source**
... In development