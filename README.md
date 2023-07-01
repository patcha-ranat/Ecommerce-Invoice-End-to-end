# End-to-end E-commerce Demand Forecasting
*Patcharanat P.*

End-to-end data project in the e-commerce and retail domain for Demand Forecasting, Customer Segmentation, and Market Basket Analysis.

***The project is in development . . .***

## **Context**
*"Typically e-commerce datasets are proprietary and consequently hard to find among publicly available data. However, The UCI Machine Learning Repository has made this dataset containing actual transactions from 2010 and 2011. The dataset is maintained on their site, where it can be found by the title "Online Retail".*

**Anyway**, it's crucial in nowadays to emphasize data existing and make the most use of it. The project was created to practice and demonstrate the full process of data exploitation, including Data Engineering skills, Data Science skills, and Data Analytic skills, and also how to implement them in the real world utilizing Business and Marketing knowledge.

## **Processes**:
1. [Setting up environment](#1-setting-up-environment)
2. [ETL (Extract, Transform, Load): Writing DAGs and managing cloud services](#2-etl-process-writing-dags-and-managing-cloud-services)
3. Web Scraping
4. EDA and Visualization
5. Machine Learning development
6. Model Deployment and Monitoring

## **Project Overview**

![project-overview](./src/Picture/project-overview.png)
*Note: this project will also include developing strategy and marketing campagin based on data.*

## **Tools**:
- Sources
    - Postgres Database (Data warehouse)
    - REST API (raw file url)
    - API (with token)
- Data Lake
    - Google Cloud Storage
    
    *(Extend to Amazon S3 and Azure Blob Storage in the future)*
- Data Warehouse
    - Postgres Database
    - Bigquery (External and Native Tables)
- Orchestrator
    - Airflow
- Virtualization and Infrastucture management
    - Docker compose
    - Terraform
- Visualization
    - PowerBI (Desktop and Service)
- Machine Learning Model
    - Jupyter Notebook (Model development)
    - FastAPI (Model Deployment)
    - Streamlit (Monitoring)

Dataset: [E-Commerce Data - Kaggle](https://www.kaggle.com/datasets/carrie1/ecommerce-data)

## Prerequisites:
- Docker and Docker compose installed.
- Get a credentials file from kaggle and activate the token for API.
- Have Google Account being able to use google cloud services

*The credentials are hidden in this project*

## 1. Setting up environment

Firstly, clone this repository to obtain all neccessary files, then use it as working directory.
```bash
git clone https://github.com/Patcharanat/ecommerce-invoice
```
We need to set up environment to demo ETL process, which including:
- Postgres database
- Airflow

All of the tools need to be run in different environment to simulate the real use-case. Hence, Docker compose become important for demo.

***Note1:** the hardest part is setting up environments.*

***Note2:** if you want to reproduce the project by yourself, just create a folder for your working directory. And, the process you need to do in the next parts will be remarked with **`DIY`***

### **Step 1.1: Setting up overall services (containers)**
Open your docker desktop and execute bash command (in terminal) within your working directory (in clone file) by:
```bash
docker compose build
```
This command will build all containers we specified in `docker-compose.yml` file, especially in `build` and `context` parts which do follwing tasks:
- Copying sql file (`setup.sql`) to `docker-entrypoint-initdb.d` to be executed when we initialize container.
- Copying `cleaned_data.csv` file to the postgres container.
- Creating schema and table with `cleaned_data.csv` by executing `setup.sql` file.
- Installing `requirements.txt` for airflow's container to be able to use libraries we needed in DAGs.
- Add a Kaggle credentials file: `kaggle.json` (in this case we use Kaggle API) to make API usable.

**DIY**: Setting up overall services (containers)
<details>
<p>

First, you need to simulate postgres database by creating a container with postgres image. you will need to copy `cleaned_data.csv` file into the postgres container. Then, you need to create a database and a schema, and a table with [`setup.sql`](setup.sql) file, and also configure [`.env`](.env) file, like username, password, and database. The file that will do the task is [`postgres.Dockerfile`](postgres.Dockerfile)

Then, you need to create a container with airflow image. You will need to copy `kaggle.json` file into the container (webserver, and scheduler). Then, you need to install libraries we needed in DAGs by **"pip install"**[`requirements.txt`](requirements.txt) file within the containers. The file that will do the task is [`airflow.Dockerfile`](airflow.Dockerfile)

To easily run multiple docker containers, you will need docker compose. The file that will do the task is [`docker-compose.yml`](docker-compose.yml), which will build all containers we specified in `build` and `context` parts which run different dockerfile for different containers.

In airflow official site, you will find template to run airflow as `docker-compose.yml` you can use it as reference and change it to fit your needs, like add postgres section, and remove unnecessary part that can causes running out of memory making you unable to run docker containers successfully.

If you're new to container, you will be confused a little with using path. please be careful where you mount the files to, unless it will bug.
</p>
</details>

### **Step 1.2: Intiating all containers**
Initialize docker container(s) and run process in background (Detach mode)
```bash
docker compose up -d
```

***Note:** some services need time to start, check container's logs from `docker desktop` to see if the services are ready to work with.*

To check status of running containers:
```bash
docker ps
```

### **Step 1.3: checking if all Dockerfiles correctly executed**
What to be checked are
- Is the data table in postgres database created correctly?
- Is the `kaggle.json` credentials file imported?

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

you should see the data csv file and `setup.sql` file in the directory.

What you should check more is that credentials file: `kaggle.json` correctly imported in airflow's scheduler and webservice containers.

### **Step 1.4: Checking data in database**
Access to postgres container, and then access database to check if csv file copied into table.
```bash
psql -U postgres -d mydatabase
```
Then we will be mounted into postgres' bash

Then we will check table, and schema we executed by `setup.sql` file
```bash
\dt or \d -- to see tables list
\dn or \z -- to see schemas list
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

In postgres bash, we will be able to see only the table that match the schema we created. Hence, we have to change the schema to see the table in the database.

Then exit from all bash
```bash
\q
exit
```
### **Step 1.5: Exiting**
Don't forget to remove all image and containers when you're done.
```bash
docker compose down -v
```

and remove all images via `docker desktop`, we will initiate `docker compose build` and `docker compose up -d` again, when we want to test ETL process (test our airflow DAGs).


### **Step 1.6: Setting up Airflow Web UI**

To set up airflow, we need to define more 4 services that refer to [official's .yml file template](https://airflow.apache.org/docs/apache-airflow/2.6.1/docker-compose.yaml) including `airflow-postgres` to be backendDB, `airflow-scheduler` to make scheduler, `airflow-webserver` to make airflow accessible via web UI, and `airflow-init` to initiate airflow session.

Understanding how every components in services work make much more easier to comprehend and debug issues that occur, such as `depends-on`, `environment`, `healthcheck`, `context`, `build`, and storing object in `&variable`.

***Note:*** In `docker-compose.yml` file, Identation is very important.

**For this project**, we create 2 postgres containers, so we need to check carefully if airflow connected to its own backendDB or the right database.

<details><summary>Issue debugged: for being unable to connect to airflow backendDB</summary>
<p>
Use this template from official's document in `.env` file:

```python
postgresql+psycopg2://<user>:<password>@<host>/<db>

#or

[dialect]+[driver]://[username:password]@[host:port]/[database]

# which results in

AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@airflow-postgres/airflow
```

</p>
</details>

***Note:*** In `.env` file, airflow core need *FERNET* key which can be obtained from fernet.py (random generated)

## 2. ETL process: Writing DAGs and managing cloud services

In my case (this project), I used a dataset from kaggle which was:
- loaded to postgres database
- uploaded to this repo github as csv format
- and I wrote DAGs to use Kaggle API to obtain the dataset directly from the Kaggle website.

If you want to change data, you have to write your own DAGs what match your specific use-case.

### **Step 2.1: Setting Data Lake, and Data Warehouse**
As we will use GCP (Google Cloud Platform) as our Data Lake and Data Warehouse, we need to make our airflow script, which run in docker containers in local, able to connect to GCP using **google credentials** as known as `service account` or `IAM`. I recommend to manually get the credential from GCP console for the safety aspect.

Do the following:

1. Go to your GCP console **within your project** (I assume you have free credit), and go to navigation menu (3-bar icon at top left), then go to `IAM & Admin` > `Service Accounts` > `Create Service Account` > Create your Service Account 
2. In Service accounts section, click 3 dots at your new created service account > `Manage keys` > `Add key` > `Create new key` > `JSON` > `Create` > `Download JSON` > `Close`, keep your credentials (this json file) in safe place. (if you want to add your project to github, make sure you are working in private repo, or add it to `.gitignore` file)

Until now, you've finished getting service account credentials.

The next step is creating your Google cloud storage bucket. Go to `Cloud Storage` > `Create` > `Name your bucket` (which *globally unique*)

Then, choose the options that match you specific needs, the recommend are:
- `Location type`: Region
- `Storage Class`: Standard
- Activate `Enforce public access prevention`
- `Access control`: Uniform
- `Protection tools`: None

Click `Create`, and now you have your own data lake bucket.

*Data Warehouse part is in development . . .*

***Note:** Actually, we can achieve creating the bucket by **"Terraform"**, which is an alternative way to create cloud resources. you can see the code in `terraform` folder, consisting of [main.tf](terraform/main.tf) and [variables.tf](terraform/variables.tf). Terraform make it easier to create and delete or managing the resources in this demonstration with a few bash commands*

**Terraform**

The [`main.tf`](./terraform/main.tf) file, using some variables from [`variables.tf`](./terraform/variables.tf) file, will produce the following resources:
- 1 data lake bucket
- 1 Bigquery dataset
- 1 Bigquery table

To use terraform, you need to install terraform in your local machine (+add to PATH), and have your google credentials for the service account in your local machine as well. Then, you can run terraform commands in your terminal.

```bash
terraform init

terraform plan

terraform apply

terraform destroy
```

you can use `terraform init` to initialize terraform (where `main.tf` located) in your local machine, `terraform plan` to see what resources will be created, `terraform apply` to create the resources, and `terraform destroy` to delete the resources. After all, you can see the result in your GCP console.

***Note**: The written script make us to easily create and delete the resources which proper for testing purpose not on production.*


### **Step 2.2: Setting up DAGs**
In this project, I wrote [`ecomm_invoice_etl_dag.py`](src/dags/ecomm_invoice_etl_dag.py) to create 1 DAG of **8 tasks**, which are:
1. Reading data from raw url from github that I uploaded myself. Then, upload it to GCP bucket as uncleaned data.
2. Fetching data from the postgres database that we simulate in docker containers as a data warehouse source. Then, upload it to GCP bucket as cleaned data.
3. Downloading from the Kaggle website using Kaggle API. Then, upload it to GCP bucket as uncleaned data. 
4. Data Transformation: reformat to parquet file, and cleaning data to be ready for data analyst and data scientist to use.
5. Loading to data warehouse (Bigquery) as cleaned data with a Native table way.
6. Loading to data warehouse (Bigquery) as cleaned data with an External table way.
7. Loading to another Postgres database as cleaned data.
8. Clearing data in staging area (GCP bucket).

But before triggering the DAG, we need to set up the connection between Airflow and our containerized Postgres database in Airflow web UI:

go to `Admin` > `Connections` > `Create` 
- `Connection Type:` **Postgres**
- `Host:` {*service name of postgres in docker-compose.yml*}
- `Schema:` {*schema name we used in `setup.sql`*}
- `Login:` {*username of postgres in docker-compose.yml*}
- `Password:` {*username of postgres in docker-compose.yml*}
- `Port:` {*username of postgres in docker-compose.yml*}

And then, `Save`

**Note:** we used **`database name`** that we specified in `docker-compose.yml` in DAG script where we need to connect to the database, PostgresHook with `conn_id` as Postgres Host name, and `schema` as **`database name`**.

After this step, we're gonna test our DAG by initating docker compose again, and go to `localhost:8080` in web browser, trigger DAG and see if our DAG worked successfully.

<details><summary>The issues I encountered during developed DAG</summary>
<p>

- It was kinda lost when trying to write my own DAGs with my own concepts, like how to get, read, fetch, write the file, or how to connect and upload to the cloud. Reading directly from official documentation was a big help, such as Google Cloud Documentation, and Kaggle API Documentation.
- Reading Logs in airflow web UI was very helpful to debug the issues.
- When we have google credentials as a json file, it make us less concerned about how to authenticate, like gcloud, gsutil etc (or even connection in Airflow). We just need to mount the credentials into the airflow container, put the path of the json file in the code and use the right API or library provided by provider which mostly can be found as templates in official documentation.
- The libraries are able to be used or not, depends on requirements.txt file. If you want to use a library, you have to install it in `requirements.txt` file, and rebuild the image.
    - I encountered that `pandas` version and `pyarrow` are not compatible with python version of airflow image, so I decide not to use pandas and pyarrow to transform data to parquet file, but use `csv` instead.
 
</p>
</details>

*The dependencies management will be changed to **poetry** in further development. Since, it's not worked until now and not the focus of this project*

*The project is in development . . .*

---
*Development Note*

- At the moment, I am at writing DAGs to upload to Data Warehouse (Biquery and Postgres), and learning what external and native table are in Bigquery (and also staging area).

- I am also doing an EDA for the dataset that was loaded into data lake, and try to investigate more about characteristic of the dataset to see if I can clean the data better.

- When I'm done with loading to data warehouse and cleaning the data. I will skip to model development and then comeback to create the dashboard with powerBI later.

- During model development, I will also learn **dbt** and see if I can include it in this project. This may take a while to develop project to the next part.

***Star the project would be a big help for me. Thank you for your support and reading. happy learning.***