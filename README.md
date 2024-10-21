# End-to-end E-commerce Data Project - Machine Learning-Driven Dynamic Personalized Marketing Management
*Patcharanat P.*
```text
Click "⋮≡" at top right to show the table of contents.
```
**End-to-end Data project** in the e-commerce and retail industries covering the full process of data exploitation, including Data Engineering skills, Data Science skills, and Data Analytic skills, and how to automate ML lifecycle management (MLOps).

## **Context**

It's crucial in nowadays to emphasize data existing and make the most use of it. **The project was created to practice and demonstrate the full process of data exploitation** covering setting up environments, ETL process, Web Scraping, Data Visualization, Machine Learning Model Development, and Model Deployment using E-commerce data.

## **Table of Contents**:
1. [Setting up Environment](#1-setting-up-environment)
    - 1.1 [Setting up Overall Services (containers)](#11-setting-up-overall-services-containers)
    - 1.2 [Intializing all Containers](#12-intializing-all-containers)
    - 1.3 [Checking if all Dockerfiles correctly executed](#13-checking-if-all-dockerfiles-correctly-executed)
    - 1.4 [Checking Data in a Database](#14-checking-data-in-a-database)
    - 1.5 [Exiting](#15-exiting)
    - 1.6 [Setting up Airflow Web UI](#16-setting-up-airflow-web-ui)
2. [ETL (Extract, Transform, Load): Writing DAGs and Managing Cloud Services](#2-etl-process-writing-dags-and-managing-cloud-services)
    - 2.1 [Setting up Data Lake, and Data Warehouse](#21-setting-up-data-lake-and-data-warehouse)
    - 2.2 [Setting up DAG and Connections](#22-setting-up-dag-and-connections)
    - 2.3 [Triggering DAG and Monitoring](#23-triggering-dag-and-monitoring)
    - [**Step to Reproduce Virtualization for Testing**](#step-to-reproduce-virtualization-for-testing)
    - 2.4 [Extend to AWS](#24-extend-to-aws)
    - 2.5 [Detail of the ETL Code](#25-detail-of-the-etl-code)
<!-- 3. [Web Scraping](#3-web-scraping) -->
3. Web Scraping
4. [EDA and Data Visualization](#4-eda-and-data-visualization)
    - 4.1 [EDA](#41-eda)
    - 4.2 [PowerBI Dashboard](#42-powerbi-dashboard)
5. [Machine Learning Model Development](#5-machine-learning-model-development)
    - 5.1 [Customer Segmentation By RFM, KMeans, and Tree-based Model](#51-customer-segmentation-by-rfm-kmeans-and-tree)
    - 5.2 [Market Basket Analysis](#52-market-basket-analysis)
    - 5.3 [Demand Forecasting](#53-demand-forecasting)
    - 5.4 Recommendation System
    - 5.5 Customer Churn Prediction
    - 5.6 Price Analysis and Optimization
    <!-- - 5.4 [Recommendation System](#54-recommendation-system) -->
    <!-- - 5.5 [Customer Churn Prediction](#55-customer-churn-prediction) -->
    <!-- - 5.6 [Price Analysis and Optimization](#56-price-analysis-and-optimization) -->
6. [Model Deployment and Monitoring](#6-model-deployment-and-monitoring)
    - 6.1 [Exporting the Models](#61-exporting-the-models)
    - 6.2 [Designing and Creating an API Web Service](#62-designing-and-creating-an-api-web-service)
    - 6.3 [Docker Containerization](#63-docker-containerization)
    - 6.4 [Deploying the Model to the Cloud Environment](#64-deploying-the-model-to-the-cloud-environment)
    - 6.3 [*(Optional)* Automating the Deployment Process](#65-automating-the-deployment-process-optional)
7. [Conclusion](#7-conclusion)

*Disclaimer: The project is not fully finished, but covered all the parts available as links above.*

## **Project Overview**

![project-overview](./src/Picture/project-overview.png)

## **Tools**:
- Sources
    - Postgres Database (Data warehouse)
    - REST API (raw file url)
    - API (with token)
- Data Lake & Staging Area
    - Google Cloud Storage
    - AWS S3
    *(Extend to Azure Blob Storage in the future)*
- Data Warehouse
    - Postgres Database
    - Bigquery (External and Native Tables)
    - Redshift
    *(Extend to Azure Synapse in the future)*
- Orchestrator
    - Airflow
- Virtualization and Infrastucture management
    - Docker compose
    - Terraform
- EDA & Visualization
    - PowerBI (Desktop and Service)
    - Python (Jupyter Notebook)
- Machine Learning Model Development
    - Jupyter Notebook
- Model Deployment and Monitoring
    - FastAPI (Model Deployment)
    - ~~Streamlit (Monitoring)~~
    - Artifact Registry
    - Cloud Run
    - Github Actions (CI/CD)

Dataset: [E-Commerce Data - Kaggle](https://www.kaggle.com/datasets/carrie1/ecommerce-data)

## Prerequisites:
- Get a credentials file from kaggle and activate the token for API.
- Have Google Account being able to use google cloud services.
- Docker Desktop
- Python

*Although `.env` file is push to the repository, sensitive data and the credentials are hidden in by `.gitignore`*

## 1. Setting up Environment

![setting-overview](./src/Picture/setting-overview.png)

Firstly, clone this repository to obtain all neccessary files, then use it as root working directory.
```bash
git clone https://github.com/Patcharanat/ecommerce-invoice
```
We need to set up local environment with docker to perform ETL process, which basically including:
- Postgres database
- Airflow

Since the pipelines might need to be run in different environment not just on your machine, it's essential to make sure that your code can be packaged and run in anywhere or orchestrated by different tools for further scaling up. Developing and testing your code to perform ETL/ELT processes in a **Docker** container is essential. **Docker Compose** make it possible to do it, orchestraing multiple containers (and maybe built by different images) with a lightweight approach simulating running on different environment. 

Usually, we don't use docker compose in production, but it is lightweight and easy enough to enable you to run and test your code on local machine. However, packing your code as a docker image is still the way to go for production scale pipeline, specifically for *Kubernetes* as an example.

### 1.1 Setting up Overall Services (containers)

Open your docker desktop and execute bash command in terminal with your root working directory (in a clone repo) by:

```bash
docker compose build
```

This command will build all containers we specified in [docker-compose.yml](./docker-compose.yml) file, especially in `build` and `context` parts which do following tasks:
- Copying [setup.sql](./setup.sql) script to `docker-entrypoint-initdb.d` path in a container to be executed when we initialize the session.
- Copying [cleaned_data.csv](./data/cleaned_data.csv) file to the postgres container as an mock-up source database.
- Creating schema and table with `cleaned_data.csv` by executing `setup.sql` within the container.
- Replicating postgres container to simulate another database for data warehouse with empty table using `postgres-target.Dockerfile`, and `target.sql` file.
- Installing `requirements.txt` for airflow's container to be able to use libraries we needed in DAGs.
- Add a Kaggle credentials file: `kaggle.json` (in this case we use Kaggle API) to make API usable.

**Note**
- `docker compose build` is creating container(s), following specification within `Dockerfile` file type specified in `docker-compose.yml` of how it should be built, in your local machine, but it's still not spinning up.

**Debugging Note**
- you may fail to run the command because you may not have google cloud credentials as a json file in your local. You can skip to [Step 2.1: Setting up Data Lake, and Data Warehouse](#step-21-setting-data-lake-and-data-warehouse) in ***Service account*** part to get your own google crendentials json file and put it in `credentials` folder. After that you can try running `docker compose build` again.
- Don't forget to get `Kaggle.json` credentials, and add to `credentials` folder also.

**Reproducing Note**
- First, you need to simulate postgres database by creating a container with postgres image. you will need to copy `cleaned_data.csv` file into the postgres container. Then, you need to create a database and a schema, and a table with [`setup.sql`](setup.sql) file, and also configure [`.env`](.env) file, like username, password, and database. The file that will do the copying file task is [`postgres.Dockerfile`](postgres.Dockerfile)
- We added another postgres container to simulate target database. The files that are relevant to this task are [`postgres-target.Dockerfile`](postgres-target.Dockerfile) to build image for the container and [`target.sql`](target.sql) to setup empty table. In this database, we will use different database name and different schema for testing how to handle with multiple databases.
- Then, you need to create a container with airflow image. You will need to copy `kaggle.json` file into the container (webserver, and scheduler). Then, you need to install libraries we needed in DAGs by **"pip install"**[`requirements.txt`](requirements.txt) file within the containers. The file that will do the task is [`airflow.Dockerfile`](airflow.Dockerfile)
- Then, you need to create a container with airflow image. You will need to copy `kaggle.json` file into the container (webserver, and scheduler). Then, you need to install python dependencies we needed in DAGs by `pip install -r`[`requirements.txt`](requirements.txt) file within the containers. The file that will do the task is [`airflow.Dockerfile`](airflow.Dockerfile)
- To easily run multiple docker containers or running microservices, you will need docker compose. The file that will do the task is [`docker-compose.yml`](docker-compose.yml), which will `build` all the images for containers we specified in `build` and `context` parts resulting in running different `.Dockerfile` for different containers.

In airflow official site, you will find [`docker-compose.yml`](https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml) template to run airflow you can use it as reference and change it to fit your needs, like adding postgres section, and remove unnecessary part that can causes running out of memory making you unable to run docker containers successfully.

If you're new to container, you will be confused a little with using path. please be careful with paths where you mount the files to.

### 1.2 Intializing all Containers
Initialize docker container(s) and run process in background (Detach mode)

```bash
docker compose up -d
```

***Note:** some services need time to start, check container's logs from **docker desktop UI** or `docker ps` to see if the services are ready to work with.*

<img src="./src/Picture/docker-ps.jpg">

### 1.3 Checking if all Dockerfiles correctly executed
What's needed to be checked are
- Is the data table in postgres database as a source created correctly?
    - data loaded from `cleaned_data.csv` and using the right schema?
- Is all the credentials file imported?
    - `kaggle.json`
    - `gcs_credentials.json`
    - `ecomm-invoice-kde-aws-iam_accessKeys.csv`
- Is the data table in postgres database as a target created correctly?
    - empty table with the right schema?

Getting into terminal of the container we specified by:
```bash
docker exec -it <container-name-or-id> bash
```

*Note: You can get container's name or id from `docker-compose.yml` or from `docker ps` command.*

At this step, we can check if csv file we meant to execute in Dockerfile is executed successfully by:
```bash
ls
ls data/
ls docker-entrypoint-initdb.d/
```

you should see the data csv file and `setup.sql` file in the directory.

<img src="./src/Picture/dockerfile-executed.jpg">

What you should check more is that credentials file: `kaggle.json` correctly imported in airflow's scheduler and webservice containers.

### 1.4 Checking Data in a Database
Access to both postgres containers, and then access database to check if csv file copied into table.
```bash
psql -U postgres -d mydatabase
```
Then we will be mounted into postgres' bash

Then we will check table, and schema we executed by `setup.sql` file
```bash
\dt or \d -- to see tables list
\dn or \z -- to see schemas list
```
if we see table and schema are corrected and shown, then importing csv to the Postgres database part is done.

<img src="./src/Picture/check-postgres.jpg">

if not, these can be issues
- check if `setup.sql` is executed successfully, by inspecting logs in docker desktop
- check if data csv file and `setup.sql` are copied into docker container's local by using container's bash and check if path in `Dockerfile` and `setup.sql` were set correctly.
- we need to set search_path by
```bash
SET search_path TO <myschema>;
```
to set only in current session. *(reccomended)*
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

***Note:** In my lastest update adding another postgres databse to simulate data warehouse, I found that specifying image name in `docker-compose.ymal` file is crucial when we pulling the same image but using in different container, because it will `build` with the wrong `Dockerfile` and cause the some issues, like build postgres database target with `postgres.Dockerfile` instead of `postgres-target.Dockerfile` which is not what we want.*

### 1.5 Exiting
Don't forget to remove all image and containers when you're done.
```bash
docker compose down -v
```

and remove all images via `docker desktop`, we will initiate `docker compose build` and `docker compose up -d` again, when we want to test developed ETL code (test our airflow DAGs).

<img src="./src/Picture/docker-desktop.jpg">

### 1.6 Setting up Airflow Web UI

To set up airflow, we need to define more 4 services that refer to [official's .yml file template](https://airflow.apache.org/docs/apache-airflow/2.6.1/docker-compose.yaml) including `airflow-postgres` to be backendDB, `airflow-scheduler` to make scheduler, `airflow-webserver` to make airflow accessible via web UI, and `airflow-init` to initiate airflow session.

<img src="./src/Picture/airflow-ui.jpg" width="75%">

Understanding how every components in `docker-compose.yml` work make much more easier to comprehend and debug issues that occur, such as `depends-on`, `environment`, `healthcheck`, `context`, `build`, and storing object in `&variable`.

***Note:*** In `yaml` file, identation is very important.

**For this project**, we create 3 postgres containers, so we need to check carefully if airflow connected to its own backendDB or the right database.

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

***Note:*** In `.env` file, airflow core need *FERNET* key which can be obtained from fernet.py (randomly generated)

## 2. ETL process: Writing DAGs and Managing Cloud Services

In this project, I used a dataset from kaggle which was:
- loaded to postgres database
- uploaded to this repo github as csv format
- and I wrote DAGs to use Kaggle API to obtain the dataset directly from the Kaggle website.

<img src="./src/Picture/etl-overview.png">

If you use different dataset, you might have to write your own DAGs what match your specific use cases.

### 2.1 Setting up Data Lake, and Data Warehouse
As we will use GCP (Google Cloud Platform) for  Data Lake and Data Warehouse, we need to make our airflow script, which run in docker containers locally, being able to connect to GCP by using **google credentials** as known as a `service account` got from `IAM` section.

**Service Account**

Please follow this guideline:
1. Go to your GCP project console with available access to manage cloud resources, and go to navigation menu (3-bar icon at top left), then go to `IAM & Admin` > `Service Accounts` > `Create Service Account` > Create your Service Account 
2. In Service accounts section, click 3 dots at your newly created service account > `Manage keys` > `Add key` > `Create new key` > `JSON` > `Create` > `Download JSON` > `Close`, please keep your credentials (this json file) in safe place (must not be uploaded to anywhere public).
3. You have to to specify the permission that you allow for that service account, like how much it can manage resources. What you need to do is:
- Go to `IAM` page in `IAM & Admin` section > Edit principal for your created service account > Add Roles
- Add the following roles:
    - BigQuery Admin
    - Storage Admin
    - Storage Object Admin
- And then, save your changes.

***Caution**: if you want to add your project to github, make sure you are working in private repo, or add it to `.gitignore` file*

Until now, you've finished getting service account credentials.

<img src="./src/Picture/gcp-iam.jpg">

**Data Lake**

The next step is creating your Google cloud storage bucket. Go to `Cloud Storage` > `Create` > `Name your bucket` (which is *globally unique*)

Then, choose the options that match you specific needs, the recommend are:
- `Location type`: Region
- `Storage Class`: Standard
- Activate `Enforce public access prevention`
- `Access control`: Uniform
- `Protection tools`: None

Click `Create`, and now you have your own data lake bucket.

<img src="./src/Picture/gcs-ui.jpg" width="75%">

**Data Warehouse**

The last step is creating your Bigquery dataset and table. In Bigquery (Google Data warehouse) you could have many projects, each project might have many datasets (most called *schema* for other OLAP databases), and each dataset might have many tables.

Do the following to create your dataset and table:
- Go to `Bigquery` > Click on 3-dot after your project name > `Create dataset` > `Name your dataset` (which is *unique* within the project) > `Create dataset`

*(Recommend to choose location type that suit your region)*

- Click on 3-dot after your created dataset > `Create table` > `Select your data source` (In this case, select empty table) > `name your table` > define schema > `Create table`

Until now, you've finished creating your data warehouse that's ready to load our data in.

<img src="./src/Picture/bigquery-ui.jpg" width="75%">

As you can see it's quite inconvenient that we have to create all of these resources manually via Google UI. So, we will use **Terraform** to create these resources in the next step.

**Terraform**

We can achieve creating the bucket and the warehouse by **"Terraform"**, which is a better way to create and manage cloud resources reducing error-prone when reproducing the process and proper for production stage. you can see the code in `terraform` folder, consists of [main.tf](terraform/main.tf) and [variables.tf](terraform/variables.tf). Terraform make it easier to create and delete or managing the resources in this demonstration with a few bash commands.

The [`main.tf`](./terraform/main.tf) file, using some variables from [`variables.tf`](./terraform/variables.tf) file, will produce the following resources:
- 1 data lake bucket
- 1 Bigquery dataset
- 1 Bigquery table

To use terraform, you need to install Terraform in your local machine (+add to PATH), and have your google credentials (service account credentials) as a json file within `"credentials"` directory located in the same level of your root working directory. Then, you can run terraform commands in your terminal **in your terraform working directory**.

```bash
terraform init

terraform plan

terraform apply

terraform destroy
```

- `terraform init` initialize Terraform (where `main.tf` located) in your local machine (mount your working directory to terraform folder first).
- `terraform plan` to see what resources will be created and syntax checking.
- `terraform apply` to create the resources.
- `terraform destroy` to delete the resources. 

After all, you can see the result in your GCP console, in Google cloud storage, and Bigquery that it's already created bucket, dataset and an empty table.

***Note**: The written script made us easily create and **delete** the resources which proper for testing purpose not on production.*


### 2.2 Setting up DAG and Connections

<img src="./src/Picture/airflow-dag-graph.jpg">

In this project, I wrote main script: [`ecomm_invoice_etl_dag.py`](src/dags/ecomm_invoice_etl_dag.py) to create 1 DAG of **(8+1) tasks**, which are:
1. Reading data from raw url from github that I uploaded myself. Then, upload it to GCP bucket as uncleaned data.
2. Fetching (Unloading) data from the postgres database that we simulate in docker containers as a data warehouse source. Then, upload it to GCP bucket as cleaned data.
3. Downloading from the Kaggle website using Kaggle API. Then, upload it to GCP bucket (GCS) as uncleaned data. 
4. Data Transformation: reformat to parquet file, and cleaning data to be ready for data analyst and data scientist to use, then load to staging area.
5. Loading to data warehouse (Bigquery) as cleaned data with a Native table way from staging area.
6. Loading to data warehouse (Bigquery) as cleaned data with an External table way from staging area.
7. Loading to another Postgres database as cleaned data.
8. Clearing data in staging area (GCP bucket). **(this will not be used since we will implement an external table that requires the source file to be exists)**

Additionally, I also wrote [**transform_load.py**](./src/dags/transform_load.py) and [**alternative_cloud_etl.py**](./src/dags/alternative_cloud_etl.py) to demonstrate inheritance of DAGs and how to use different cloud services, respectively.

After we wrote the DAG script, we're gonna test our DAG by initating docker compose again, and go to `localhost:8080` in web browser, get ready to trigger DAG and see if our DAG worked successfully.

But before triggering the DAG, we need to set up the connection between Airflow and our applications in Airflow web UI:

**Postgres connection**: Go to `Admin` > `Connections` > `Create` 
- `Connection Type:` **Postgres**
- `Host` *service name of postgres in docker-compose.yml*
- `Schema` *schema name we used in `setup.sql`*
- `Login` *username of postgres in docker-compose.yml*
- `Password` *username of postgres in docker-compose.yml*
- `Port` *username of postgres in docker-compose.yml*

<img src="./src/Picture/airflow-connections-postgres.jpg" width="75%">

And then, `Save`

***Note1:** we used **`database name`** that we specified in `docker-compose.yml` in DAG script where we need to connect to the database, PostgresHook with `conn_id` as Postgres Host name, and `schema` as **`database name`***.

***Note2:** we can omit `schema` argument in PostgresHook and Airflow connection, if we use `public` schema, or specify SELECT `myschema.table_name` FROM ... in `setup.sql`*

**Bigquery connection**: Go to `Admin` > `Connections` > `Create` 
- `Connection Id`: *your own defined name (will be use in DAG)*
- `Connection Type`: **Google Cloud** 
- `Project Id`: Your Project Id
- `Keyfile Path`: *absolute path to your service account credentials file*

<img src="./src/Picture/airflow-connections-google.jpg" width="75%">

And then, `Save` again.

**Note:** you have to change the code in [`ecomm_invoice_etl_dag.py`](src/dags/ecomm_invoice_etl_dag.py) to match your connection id that you created in Airflow UI, and your project id also.

For details of airflow connection configuring, we only create connections of Postgres and Bigquery for fetching data from containerized Postgres database, and creating **external table** for Bigquery respectively because it's required, unless airflow could not connect to the applications and cause the bug. Most of time, you can tell if it requires connection in Airflow UI by existing of `conn_id` arguments in the Operators. But, the other connection, Google Cloud Storage, is not required to be created in Airflow UI, because we use the credentials file that mounted into Airflow container to authenticate the connection in the code.

<img src="./src/Picture/airflow-connections.jpg" width="75%">

<details><summary>The issues I encountered during developed DAG</summary>
<p>

- It was kinda lost when trying to write my own DAGs with my own concepts, like how to get, read, fetch, write the file, or how to connect and upload to the cloud. Reading directly from official documentation was a big help, such as Google Cloud Documentation, and Kaggle API Documentation.
- Reading Logs after triggering DAG in airflow web UI was very helpful to debug the issues.
- When we have google credentials as a json file, it make us less concerned about how to authenticate, like gcloud, gsutil etc. (or even connection in Airflow). We just need to mount the credentials into the airflow container, put the path of the json file in the code and use the right API or library provided by provider which mostly can be found as templates in official documentation.
- The libraries are able to be used in the DAG script or not, depending on `requirements.txt` file. If you want to use a library, you have to install it in `requirements.txt` file, and rebuild the image *(docker compose down -v, docker compose build, docker compose up -d)* again.
    - At first, I encountered that `pandas` version and `pyarrow` are not compatible with python version of default latest airflow image, so I decide not to use pandas and pyarrow to transform data to parquet file, but use `csv` instead. But, after a while, I found out that I can pull an image with own desired python version, so I can use pandas to transform data, and change into parquet file format using this line: 
        ```dockerfile
        FROM apache/airflow:slim-2.6.2-python3.10
        ```
    in [airflow.Dockerfile](airflow.Dockerfile)
- I used `gcsfs` or `GCSFileSystem` in DAG script to run transformation task, because in case of transforming the data from data lake using pandas, it's required the library, which currently in beta, instead of using `goole.cloud.storage`.
    - And you have to install it in `requirements.txt` file, and also use the authentication via `GCSFileSystem` in the code instead of `google.cloud.storage.Client`.
- In the DAG code, I used **overwriting** option for Native table in Bigquery. The append method is not covered in the project scope, but in production, append method is more preferred to preserve the old data.
    - External table always refer to the source file, so it's not possible to be appended, but manually append the data by itself with the code.
- When I first load the parquet file to pre-defined schema in Bigquery, I encountered the error that the schema is not matched. I found out that the schema of parquet file is not the same as the schema in Bigquery with extra column "index level 0". So, the solution is to drop the column before saving to parquet file in Google Cloud Storage by using `df.to_parquet(..., index=False)`, just like `to_csv('filename.csv', index=False)`.

    *(Even you download the `index=True` parquet file to check in pandas, it will not show the extra index column)*
- *(Update)* Meanwhile I was developing other part of the project, a new airflow version(2.7) was launched, and the new version of airflow image is not compatible with the old version by airflow backendDB which caused a serious bug making `airflow-init` initialized unsuccessfully, `airflow-scheduler`, and `airflow-webserver` not work as expected.
    - the solution is to remove all containers, images, and existing volumes of airflow backendDB, and then intialize again with the fix image version.
        - remove local volumes by
        ```bash
        docker compose down --volumes --rmi all

        docker system prune --all --volumes
        ```
        - remove local airflow backendDB `postgres-db-volume`, and also `logs`, `plugins`, and `config` files in `src` folder.
    - I changed the airflow image version to `apache/airflow:2.6.2-python3.10` in [airflow.Dockerfile](airflow.Dockerfile) and `apache/airflow:2.6.2` in [docker-compose.yml](./docker-compose.yml) to the version of image.
    - Don't use the `latest` version in your script, if you want to make you work reproduceable.
</p>
</details>

*The dependencies management will be changed to **poetry** in further development. Since, it's not worked until now and not the focus of this project*

**Bigquery: Native tables vs External tables**

<img src="./src/Picture/native-vs-external.png" width="75%">

In the DAG script, we have 2 tasks to load data to Bigquery, one is using Native table, and another is using External table. the differences between them are their performance and how it works.

External tables are suitable when you want to query data without loading it into BigQuery, optimizing costs and leveraging existing data sources. Native tables, on the other hand, offer enhanced performance and advanced features within BigQuery, like partitioning and clustering.

*What to note is: if you're using external table, you can't delete the source in data lake, because it require the source to query directly from. And you couldn't see the preview of data table in the Bigquery UI, with Extenal table approach.*

### 2.3 Triggering DAG and Monitoring

After finishing the writing DAG part, we can go to `localhost:8080` via web browser and login with username, and password we defined in [docker-compose.yml](docker-compose.yml) file. Then, we can see the DAGs we created in the UI. We can trigger the DAG by clicking on the `Trigger DAG` button, and monitor the progress of the DAG by clicking on the DAG name.

You can also see the logs of each task by clicking on the task name in the DAG graph and see how the tasks flow is working via Graph.

<img src="./src/Picture/airflow-triggered.jpg">

Most of time, you don't write the DAGs in one time and test once it's done, you have come to this UI, triggering and monitoring to see if it works or not, and then fix the bugs. So, you have to trigger the DAGs many times, and see the logs to debug the code. It's very helpful to debug the code by reading the logs and checking which tasks are failed.

**Once the ETL DAG worked successfully, the data engineering part is finished.**

### **Step to Reproduce Virtualization for testing**
1. Apply Terraform to create the infrastructure
    ```hcl
    terraform plan

    terraform apply
    ```
2. Start microservices with docker-compose
    ```bash
    docker compose build

    docker compose up -d
    ```
3. Trigger the DAG in Airflow UI
    - go to `localhost:8080` via web browser
4. Check the data in Data Lake and Data Warehouse
    - login to Cloud Console
    - (If you want to test model deployment, you can do it here)
5. Stop microservices with docker-compose
    ```bash
    docker compose down -v
    ```
6. Destroy the infrastructure with Terraform
    ```hcl
    terraform destroy
    ```

### 2.4 Extend to AWS
In the previous part, I used GCP as a cloud service provider for data lake and data warehouse, but we can also use AWS (or Azure) as a cloud service provider. The process is quite similar to GCP, but it will have some differences in the code and architecture which we can adapt to it easily if we understand the concept of ETL.

We will use AWS **S3** as a data lake, and AWS **Redshift** as a data warehouse. As before, we need to create IAM user (which equal to service account in GCP) and get the credentials file as `.csv` extension, then use it to create S3 bucket and Redshift by **terraform**.

We will create an IAM user, and get the credentials file manually regarding the security aspect. Access for AWS is quite more complex than GCP, composed of IAM user, IAM Role, and Policy which will not be described in detail in this project.

**IAM User and Policies**

To get the IAM user, we must have root user which is the first user we created when we created AWS account. Then,
- We need to go to `IAM` service in AWS console.
- And go to `Users` > `Add users` > Name an unique `User name` > `Attach existing policies directly`.
- Type in search box `AmazonS3FullAccess` and `AmazonRedshiftAllCommandsFullAccess` > check the box > `Create user`.
- Adding custom policies for Redshift Serverless by clicking the created user and then `Add permission` > `Create inline policy`,
    - use json option and type in this:
    ```json
    {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "",
            "Effect": "Allow",
            "Action": [
                "iam:*",
                "ec2:*",
                "redshift-serverless:*"
            ],
            "Resource": "*"
        }
    ]
    }
    ```
    - `Next` > `Create policy`
- Get a csv credential file by clicking on the created user > `Security credentials` > `Create access key` > choose `Local Code` for our purpose > name an unique access key > `Download .csv file`

After getting the credentials file, mount it to your `credentials` folder. Now, we can use it in **terraform** to create S3 bucket and Redshift resources.

**Terraform for AWS**

In this part is quite complex due to **"static credentials"** aspect, since we don't need to hard-coded or type in the key derectly to the terraform file. So in general, we will use **"terraform.tfvars"** to pass the hard-coded credentials to terraform file and add `terraform.tfvars` to `.gitignore`.

The concept is simple: we create resources in `main.tf` where some part of it use `var.` to refer to the variable in `variables.tf` file. In `variables.tf` file, we specify the variable name, type, and default value, if the default value is not specified, we have to pass the value interactively after `terraform apply` as inputs **OR** pass it automatically by creating `terraform.tfvars` file and type in the variable name and value. This is where we will copy credentials from csv to put it in **(and again don't forget to add both files to `.gitignore`)**.

All you need to do is creating `terraform.tfvars` (must be this name) in your `terraform` folder, and type in the following:
```hcl
# aws credentials
aws_access_key = "your-access-key"
aws_secret_key = "your-secret-key"

# optional for serverless redshift

# Application Definition
app_environment   = "dev" # Dev, Test, Staging, Prod, etc

# Network Configuration
redshift_serverless_vpc_cidr      = "10.0.0.0/16"
redshift_serverless_subnet_1_cidr = "10.0.1.0/24"
redshift_serverless_subnet_2_cidr = "10.0.2.0/24"
redshift_serverless_subnet_3_cidr = "10.0.3.0/24"
```

Then you good to go with `terraform apply`.

In Addition, configuring Redshift Serverless is quite complex, so I will not go into detail, but you can check the code in [main.tf](./terraform/main.tf). Basically we need to create the following:
- Data "aws_availability_zones" to get the availability zone.
- VPC.
- Redshift subnet, and also config your available IP address in `terraform.tfvars`.
- IAM role for Redshift Serverless.
- Grant some access and attach some policy for the role.
- Workgroup
- Namespace.

Unlike S3, which is much more easier to create, we just need to specify the name of the bucket, and the region.

*Note: Redshift Serverless let us create a data warehouse without managing the cluster ourselves, it can **scale down to zero** or **pay as you use**, but it's still in preview.*

### 2.5 Detail of the ETL Code

I intentionally separate the code for AWS and GCP, so we can easily find between them. The code for AWS is in [alternative_cloud_etl.py](./src/dags/alternative_cloud_etl.py) file. The code is quite similar to GCP, but there are some differences, such as:
- using `boto3` instead of `google.cloud.storage` to connect to S3.
- how we use credentials file to connect to the cloud.
- using `psycopg2` instead of `bigquery` to load to Redshift.
- uploading clean data to S3 bucket using `awswrangler`, which implemented on `pandas` library, instead of using `gcsfs`.
- how to fetch the data from postgres database to gcp and aws, which is different in the code, but the concept is the same.

**Note: Since we use the same file that is downloaded or fetched, so the deleting file from local airflow docker container will be deprecated avoiding conflict between clouds.** *Unless, it will cause a bug by `shutil` in the first run, because the file is not exists in the local container.*

*Note: Loading to Redshift part will be described more in the future*

## 3. Web Scraping

This part is not currently in development. I will update the progress later. But, you can check the concept and the old written code in [web-scraping](https://github.com/Patcharanat/ecommerce-invoice/tree/master/web-scraping) folder.

## 4. EDA and Data Visualization

Once we have the data in the data warehouse, we can do the EDA and data visualization connecting data from the data warehouse. I will use **PowerBI** to create the dashboard, and Python for some ad-hoc analysis.

### 4.1 EDA

Actually, we have to do the **EDA** (Exploratory Data Analysis) before loading data to data warehouse for cleaning and transformation, but in the analysis part, we can do in different objectives. In this part, I will do an EDA to see the characteristic of the data, and see if there is any insight that can be used to improve the business.

<details><summary>Data Characteristics</summary>
<p>

This part is mostly done with Python by **Pandas** and **Matplotlib**, because of its flexible.

- `CustomerID` contains null values
    - fill null values with int 0 → for RFM, CustomerID 0 will be removed
- `Description` contains null values
    - fill null values with “No Description”
- `Description` contains different descriptions for the same stock code
    - replace the most frequent description → for market basket analysis
- `UnitPrice` contains negative values
    - StockCode "B": adjust bad dept
- `Quantity` contains negative values means the refund
- `Quantity` and `UnitPrice` contain outliers
    - need to be removed for model development
    - should be kept for revenue analysis
- `Quantity` < 0 & `UnitPrice` = 0 → Anomaly 
    - should be removed for model development
    - should be kept for revenue analysis
    - should be filtered out for market basket analysis
    - it can be something, like damagae, manually filled, etc.
- `InvoiceNo` contains "C" → refund
- Inconsistent `StockCode`
    - "D" → discount
    - "M" → manual (there's also "m")
    - "S" → sample
    - "BANK CHARGES" → bank charges
    - "POST" → postage
    - "DOT" → dotcom postage
    - "CRUK" → charity
    - "PADS" → pads to match all cushions
    - "C2" → carriage
    - the digits StockCode with a letter → the same product but different colors
- More from random data description: `UnitPrice` is in "Pound sterling" 
- `Country` mostly contains "United Kingdom" → more than 90% of the data
- `InvoiceDate` includes time, but not in the same timezone, for different countries

</p>
</details>

<img src="./src/Picture/boxplot_outlier.png" width="75%">

You can check the code in [**ecomm_eda.ipynb**](ecomm_eda.ipynb) file.

### 4.2 PowerBI Dashboard

Since, the current version of PowerBI has a bug of connecting to data warehouse, I will connect the data from local parquet file that loaded from data warehouse instead.

We can connect parquet file locally to Power BI:
- Go to `Get Data` > Type in `"Parquet"` > `Connect` > use Basic option, and type in URL box by this: `file:///C:/path/to/data.parquet` > Then, click OK

*Note: you have to change the path to your local path*

Now we can see the data in PowerBI, and do the data visualization.

![ecomm_pbi](./src/Picture/ecomm_pbi.jpg)

I won't go into detail of how to create the dashboard or each component, but you can download and check it yourself in [**ecomm_bi.pbix**](ecomm_bi.pbix) file.

What worth to mention are:
- I used **DAX** to calculate Growth Rate as a measure that would be quite complex for some people who are not familiar with PowerBI or DAX, but it used just regular formula as: `(Sales in the Current Context month - Sales in the Previous month) / Sales in the Previous month`. I also used `CALCULATE` function to calculate the previous month value together with `ALLSELECTED`, and `DIVIDE` function to calculate the growth rate.
- I linked variables between reports to be able to **Drill Through** to other reports, making the dashboard more interactive, and more in-depth to analyze.

A little note for future myself:
- **Dashboard is about Storytelling**, so it's better to have a story in mind before creating the dashboard. It's not just about the data, but how to arrange the story from the data.
- **It's crucial to know who are the audiences of the dashboard, and what the objective of the dashboard is**. So, we can select the right metrics, right data, and right visualization.
- **Data model is very important**, it's the foundation of the dashboard. If the data model is incorrected, the dashboard will be incorrected also. If the data model come in a good shape, the dashboard will be easier to create, and the data will be easier to analyze. (especially in aspect of **Time Intelligence**)

*Note: I will update the dashboard in the future, since it's not fully finished.*

## 5. Machine Learning Model Development

The Model Development part is not fully finished yet, but some part of it are done and ready to be presented.

This is what I have planned so far:
1. [Customer Segmentation By RFM, KMeans, and Tree](#52-customer-segmentation-by-rfm-kmeans-and-tree) ***(Done)***
2. [Market Basket Analysis](#53-market-basket-analysis) ***(Done)***
3. Recommendation System *(Not started)*
4. [Demand Forecasting](#54-demand-forecasting) ***(In progress)***
5. Customer Churn Prediction *(Not started)*
6. Price Analysis and Optimization *(Not started)*

You can see the code in [**model_dev.ipynb**](model_dev.ipynb)

### 5.1 Customer Segmentation By RFM, KMeans, and Tree-based Model

**Introduction**

**RFM (Recency, Frequency, Monetary)** is a well known method to segment customers based on their behavior. **But recency, fequency, and monetary alone, are not effective enough to segment the diversity of customers**. We should use other available features or characteristics of customers to segment them more effectively that come to the harder to score with traditional RFM method.

As a result, **KMeans emerged as a crucial machine learning technique for effectively clustering clients** into more precise customer segments. **But, the Kmeans is not interpretable**, we can't explain the result or criteria to the business that how clusters are formed or how the customers are segmented, but only show what features we use in KMeans model (we don't even know which features are more influence).

**So, we can use Decision Tree to find the criteria of the clusters**, and explain the result to the business, leading to proper campaigns and strategies that suit to customer segments to be launched.

Moreover, we can use **XGBoost to find the most important features that influence the customer segments (or each segment)**, and further concern and develop strategies regarding that features.

**RFM and Features Details**

First, we will form the RFM table based on customer transactions and add some more features (Feature Engineering) that we think it might be useful for segmentation which include:
- Recency
- Frequency
- Monetary
- is one time purchase
- mean time between purchase
- mean ticket size (average total price of each purchase)
- mean of number of unique items of each purchase
- mean quantity items of each purchase
- mean spending per month
- frequency of purchase per month
- (refund rate)

Eventually, we will get a new table that represents customers profile and their behavior, this table is a `rfm` variable in the code in [model.dev](model_dev.ipynb).

*Note: The features we used to decribe the customer behavior is not limited to the features above, we can add more features that we think it might be useful for segmentation, but also should be interpretable for initiaing the campaigns and strategies, so some features, like standard deviation, might not be appropriate for this case.*

**KMeans**

Before we put the customer profile data into KMeans, we should scale the data first , in my case I used **Robust Scaler**, to make the data have the same scale, since KMeans algorithm use distance to calculate the clusters, so it's sensitive to the different scale of the data.

Then, we will use KMeans to segment customers into clusters. We will use **Elbow Method** and **Silhouette Plot** to find the optimal number of clusters which eventually is 10 clusters.

<img src="./src/Picture/kmeans-elbow.png" alt="kmeans-elbow" width="75%">

![kmeans-silhouette](./src/Picture/kmeans-silhouette.png)

Then, we label the clusters to customer profile table which not scaled yet, so we can see the customer profile of each cluster with the original scale to make it more interpretable. We will use this table to find the criteria of the clusters as an input and validation set for the Decision Tree.

References:
- [Selecting the number of clusters with silhouette analysis on KMeans clustering](https://scikit-learn.org/stable/auto_examples/cluster/plot_kmeans_silhouette_analysis.html)
- [Segmenting Customers using K-Means, RFM and Transaction Records](https://towardsdatascience.com/segmenting-customers-using-k-means-and-transaction-records-76f4055d856a)

**Decision Tree**

After that, we will use **Decision Tree** to find the criteria of the clusters to make the clustering result from KMeans become interpretable. I used **Random Search** with **Cross Validation** to find the best hyperparameters of the Decision Tree to make it the most accurate **resulting in accuracy 91.41% on test set, 94.11% on train set, macro averaged of F1 score 0.75, to clarify criteria of each cluster.**

![kmeans-tree](./src/Picture/kmeans-tree.png)

***Note: The 91% accuracy doesn't mean we segmented the customers correctly by 91%**, because we can't be sure that Kmeans correctly 100% segmented the customers (and definitely, it didn't), but we can tell that the Decision Tree can explain the criteria of the clusters correctly by 91% in generalization, So we can take account of the criteria from the Decision Tree to cluster customer segmented to follow Kmeans result.*

**XGBoost**

Finally, we will use **XGBoost** to find the most important features that influence the customer segments or important factors to develop the strategies. I also used **Random Search** with **Cross Validation** to find the best hyperparameters of XGBoost resulting in accuracy 97.76% on test set, 100% on train set, macro averaged of F1 score 0.77, to find the most important features (seem overfitting, but it's not a main focused).

You may heard of feature importance from XGBoost that can describe which features are more influence to the model, but it's not effective enough due to its limitation, like how it treats the correlated features, how differently treats the categorical features and numerical features, and how it's not generalized to other models. So, I used **Permutation Feature Importance** instead, which is more generalized for choosing the most influence features to each cluster. Moreover, it can be used with any model, not just XGBoost.

References:
- [Permutation feature importance - sklearn](https://scikit-learn.org/stable/modules/permutation_importance.html#permutation-importance)
- [Permutation Importance vs Random Forest Feature Importance (MDI)](https://scikit-learn.org/stable/auto_examples/inspection/plot_permutation_importance.html#sphx-glr-auto-examples-inspection-plot-permutation-importance-py)

The most important features for each cluster can be shown in the plot below:

<img src="./src/Picture/permutation_feature_importance.png" alt="permutation_feature_importance" width="75%">

Finally we can classify the customers into 10 clusters and explain the criteria of each cluster to the business.
- **Cluster 0** : Bought less, not often, and small ticket size, I think this is a cluster of casual customers or one-time purchase customers.
- **Cluster 1** : Bought much in amount, but low quantity.
- **Cluster 3** : Bought less in amount, but bought often, and small ticket size, I think this is a cluster of regular customers who are likely to active and easily attracted by marketing campaign.
- **Cluster 4** : Big spenders, and bought a lot of items both quantity, and diverse items, I guess this is a cluster of wholesale customers.
- **Cluster 5** : Also Big spenders, and bought a lot of items both quantity, and diverse items, I also think this is a cluster of wholesaler but bigger than cluster 4, because they bought in more amount and more quantity.
- **Cluster 7** : Big spenders, but not much as cluster 5, spent 50 - 155£ per month and bought once a month approximately, and also bought in low quantity. I guess this is a cluster of valuable regular customers that we should keep. Moreover, they're also the largest group among all clusters.

... and so on.

*Note: the more cluster and the more depth you have, the more complex and specific the criteria of each cluster will be, leading to harder to classify.*
### 5.2 Market Basket Analysis

The Market Basket Analysis is a technique to find the association between items that customers purchase together. It can be used to find the relationship between items, and use the result to develop strategies such as cross-selling, and product bundling.

**The important metrics** of Market Basket Analysis are:
- **Support**
    - how frequent the itemset appears in the dataset
    - can be calculated by: *support(itemset) = count_basket(itemset) / total_count_basket*
    - support score range from 0 to 1, "minimum support" can be used to filter out itemset that are not frequent (threshold depends on user and dataset)
- **Confidence**
    - how likely item B is purchased when item A is purchased
    - confidence(A→B) = count_basket(itemset (A and B)) / count_basket(A)
    - confidence score range from 0 to 1, "minimum confidence threshold" can be used to filter out itemset that are not meet the requirement (threshold depends on user and dataset)
- **Lift**
    - how likely item B is purchased when item A is purchased, how many times probability increase or decrease compared with normal B purchased
    - lift(A→B) = confidence(A→B) / support(B)
    - lift score range from 0 to infinity *(in times unit)*
    - lift > 1 → B is likely to be purchased when A is purchased
    - lift < 1 → B is unlikely to be purchased when A is purchased
    - lift = 1 → B is independent from A

The Market Basket Analysis can be done by using **Apriori Algorithm** which is a popular and effective algorithm to find the association between items. one can use **mlxtend** library to implement Apriori Algorithm following these steps:
- **Step 1**: Find frequent itemset (itemset that meet the minimum support threshold)
- **Step 2**: Generate association rules from frequent itemset (itemset that meet the minimum confidence threshold)
- **Step 3**: Sort the rules by lift
- **Step 4**: Visualize the rules as a table and interpret the result

What challenges in market basket analysis are data preparation, specifying threshold of each metric, and interpret the result to the business. It's can be done easily and useful for the retail business.

for example of the result of market basket analysis can be shown in the table below:

Antecedents | Consequents | Support | Confidence | Lift
:---: | :---: | :---: | :---: | :---:
PINK REGENCY TEACUP AND SAUCER | GREEN REGENCY TEACUP AND SAUCER | 0.034121 | 0.828530 | 15.045681
GREEN REGENCY TEACUP AND SAUCER | PINK REGENCY TEACUP AND SAUCER | 0.034121 | 0.619612 | 15.045681
PINK REGENCY TEACUP AND SAUCER | ROSES REGENCY TEACUP AND SAUCER | 0.031806 | 0.772334 | 13.714834
... | ... | ... | ... | ...
LUNCH BAG BLACK SKULL. | LUNCH BAG PINK POLKADOT | 0.030560 | 0.425620 | 6.896678

*Note: The table is snippet from the result of market basket analysis*

We can interpret the result as:
- *PINK REGENCY TEACUP AND SAUCER* and *GREEN REGENCY TEACUP AND SAUCER* are **frequently purchased together**, the itemset appeared by 3.41% of total transactions **(considered by support score)**

    *Note: In our case, we filter out single item purchase transaction, hence the 3.14% is the percentage of transactions that contain both 2 or more items which is `basket_encoded_filter` variable in the [notebook](model_dev.ipynb) resulting in 15660 x 0.034121 = 534 transactions out of 16811 transactions.*

- **the probability** of *GREEN REGENCY TEACUP AND SAUCER* **purchased when** *PINK REGENCY TEACUP AND SAUCER* **purchased** is 82.85% **(high confidence)**
- **the probability** of *GREEN REGENCY TEACUP AND SAUCER* **purchased** when *PINK REGENCY TEACUP AND SAUCER* **purchased** is 15.04 **times higher than alone** *GREEN REGENCY TEACUP AND SAUCER* **purchases (high lift)**
- the second row (or rule) is the same as the first row, but the consequent and antecedent are swapped, so we can watch the result from both sides
- the third rule and so on, show the bundle of item that are frequently purchased together, these rules can be used to develop strategies such as cross-selling, product bundling.

References:
- [How To Perform Market Basket Analysis in Python - Jihargifari - Medium](https://medium.com/@jihargifari/how-to-perform-market-basket-analysis-in-python-bd00b745b106)
- [Association Rule Mining using Market Basket Analysis - Sarit Maitra - Towards Data Science](https://towardsdatascience.com/market-basket-analysis-knowledge-discovery-in-database-simplistic-approach-dc41659e1558)

### 5.3 Demand Forecasting

In this section, we will use **Time Series Forecasting** technique to predict future values based on the past values of the data. we will use some input from the past as features to predict the future sales.

In general, we use current features or features that already happened fed into the model to predict the sales as a target. But, the problem is, if we want to predict the sales of the next month, we don't have the records of the next month yet. So, we have to use the records of the past to predict the sales of the next month.

Therefore, we have to perform feature engineering, transform data, create features to obtain the appropriate and effective features to predict the future sales. So, we will use **Lag Features** and **Rolling Window Statistics**.

But how can we know how lag of the features and how many rolling window statistics should we use? first we can use **Auto-correlation** to find the optimal lag value or candidates to be used as lag features. Then, we can use **Cross-Validation** to find the optimal number of rolling window and the best candidate of lag features.

<img src="./src/Picture/autocorrelation.png">

Additionally, I used **Fast Fourier Transform** to find seasonality of the data, which is the pattern that repeats itself at regular intervals of time. The seasonality can be used to create features to predict the future sales.

<img src="./src/Picture/fourier.png" width="75%">

Personally, I thought using fast fourier transform to find seasonality is quite more quantitative than using autocorrelation. But, we can use both of them to find the seasonality of the data to ensure the result.

![](./src/Picture/predict-demand.png)

I think the most challenging part of timeseries forecasting is to find the appropriate features to predict the future sales. The features that we use to predict the future sales should be the features that already happened in the past, and we can't use the features that will happen in the future. So, checking how much lagged values of the features can be significant to predict the future sales is very important.

Model | RMSE | MAPE
:---: | :---: | :---:
Baseline (Mean) | 3170.143 | 27.16%
LightGBM | 4884.230 | 32.29%

*(Current Result)*

Even we can see that the prediction result can a bit capture the trend of the data, but the result is not good enough compared with **mean** of the target.

I intended to **decompose** the data into trend, seasonality, and residual, then use them as features to predict the future sales to make it stationary **and also add moving average types** such as EMA (Exponential Moving Average) and LWMA (Linear Weighted Moving Average) to the model, to weight the recent data more than the old data. Moreover, I want to test traditional statistical model such as ARIMA, and SARIMA. But, I think it's enough for now, I will update the model later.

*In development . . .*

References:
- [How to Calculate Autocorrelation in Python?](https://www.geeksforgeeks.org/how-to-calculate-autocorrelation-in-python/)
- [How to detect seasonality, forecast and fill gaps in time series using Fast Fourier Transform](https://fischerbach.medium.com/introduction-to-fourier-analysis-of-time-series-42151703524a)
- [How To Apply Machine Learning To Demand Forecasting (Concept)](https://mobidev.biz/blog/machine-learning-methods-demand-forecasting-retail)
- [All Moving Averages (SMA, EMA, SMMA, and LWMA)](https://srading.com/all-moving-averages-sma-ema-smma-and-lwma/)
- [Finding Seasonal Trends in Time-Series Data with Python](https://towardsdatascience.com/finding-seasonal-trends-in-time-series-data-with-python-ce10c37aa861)
- [Various Techniques to Detect and Isolate Time Series Components Using Python (Technical)](https://www.analyticsvidhya.com/blog/2023/02/various-techniques-to-detect-and-isolate-time-series-components-using-python/)

### 5.4 Recommendation System

*in development . . .*

### 5.5 Customer Churn Prediction

*In development . . .*

### 5.6 Price Analysis and Optimization

*In development . . .*

## 6. Model Deployment and Monitoring

After we developed and evaluated the model, we can deploy the model to production to leverage the business, bringing the model out from the Python notebook or your lab, and not only making it available to the data scientist.

We can deploy the model to production in many approaches, such as:
- **Batch Inference**: the model is deployed to production as a batch process, the model is run periodically to predict the target variable, the result is stored in the database, and the business can use the result to leverage the business.
- **Real-Time Inference**: the model is deployed to production as a real-time process.
- **Model as a Service**: the model is deployed to production as a web service utilizing APIs, the model is run only when the business needs to predict the target variable, and the business can use the result to leverage the business or use the API to integrate with other applications.

In this project, we will deploy the model to the cloud environment **(GCP)**. The model will be deployed as a web service using **FastAPI** as a web framework for building RESTful APIs, and **Docker** as a containerization platform to make the model portable and scalable.

![deploy-overview](./src/Picture/deploy-overview.png)

The steps to deploy the model to production are:
1. [Exporting the model(s)](#61-exporting-the-models)
2. [Designing and creating an API web service using FastAPI](#62-designing-and-creating-an-api-web-service)
3. [Making it portable and scalable using Docker Containerization](#63-docker-containerization)
4. [Deploying the Codel to the Cloud Environment (GCP)](#64-deploying-the-model-to-the-cloud-environment)
5. [*(Optional)* Automating the deployment process using CI/CD pipeline by Github Actions](#65-automating-the-deployment-process-optional)

For more resource for deployment overview, I recommended this [ML Deployment Chapter](https://github.com/alexeygrigorev/mlbookcamp-code/tree/master/course-zoomcamp/05-deployment) from [*Machine Learning Zoomcamp*](https://github.com/alexeygrigorev/mlbookcamp-code/tree/master).

### 6.1 Exporting the Model(s)

In this section, we will export the model and extract the pipeline, pre-processing and transformation code, which we meant to use in the API web service.

- For ML model, we will use **Pickle** to save the model as a file.
    ```python
    import pickle

    # model.fit(X_train, y_train)
    with open("./deployment/model_name.pkl", "wb") as f:
        pickle.dump(model, f)
    ```
    Note: save in the same directory as [api_app.py](./deployment/api_app.py) file, so we can easily use it in `api_app.py` later.
- For **pre-processing and transformation code**, we will extract the **production code** into Python script to be ready to put it in the FastAPI script. When we develop the model, it's usually in the phase of the experiment, consisting of validation and many trials and errors. But, when we deploy to production, we have to select the part of the code that's consistent and will work the best.

### 6.2 Designing and Creating an API Web Service

In this project, we will use **FastAPI** which is a high-performance, web framework for building APIs with Python. FastAPI official documentation is very well written, easy to understand and follow even you're not familiar with web development, just knowing a basic understanding of Python. Check it out here: [FastAPI Documentation](https://fastapi.tiangolo.com/)

What I recommended are:
- To understand its structure and how it works:
    - [FastAPI: First Steps](https://fastapi.tiangolo.com/tutorial/first-steps/)
    - [FastAPI: Path Parameters](https://fastapi.tiangolo.com/tutorial/path-params/)
    - [FastAPI: Query Parameters](https://fastapi.tiangolo.com/tutorial/query-params/)
    - [FastAPI: Request Body](https://fastapi.tiangolo.com/tutorial/body/)
- To understand how to deploy it to production:
    - [FastAPI: FastAPI in Containers - Docker](https://fastapi.tiangolo.com/deployment/docker/)

Mostly, your API should have these endpoints:
- **GET**: to get the path and query parameters from the client and return the result.
- **POST**: to get the **"Request Body"** from the client and return the result.

**Conceptually,**

In my opinion, **GET** method should be used when the client wants to get some data from the server not using the request body to exploit the ML model.

And, **POST** method should be used when the client wants to send the data to the server and get the result from the server which is more proper for utilizing ML model scenario.

We will put pre-processing, transformation code, and write some logic to handle the request from the client in the FastAPI script. 

*In development . . .*

### 6.3 Docker Containerization

After we designed and created the FastAPI script: [api_app.py](./deployment/api_app.py), we will pack it as a Docker image built by: [deployment.Dockerfile](./deployment/deployment.Dockerfile) and run it as a container when we want to use it.

If your API use any libraries that are not included in the base image, you have to install it additionally in the Dockerfile with the second `requirements.txt` that is used by the FastAPI script.

In the [deployment.Dockerfile](./deployment/deployment.Dockerfile), we will specify the base image, copy the FastAPI script, the model file, and dependencies for the script to the container, making it ready to be portable.

**Note: What worth to mention is that the path specifying in the Dockerfile is relative to working directory, or where we execute the `docker build` command. So, we have to be aware of the path we specify in the Dockerfile and how to execute the file via CLI.**

For example, in the [deployment.Dockerfile](./deployment/deployment.Dockerfile), we specify the path to copy the files as:

```dockerfile
COPY ./deployment .
```

Because the command meant to be executed via CLI having working directory in the root directory of the project, not the within `deployment` directory (and with [a few research](https://stackoverflow.com/questions/24537340/docker-adding-a-file-from-a-parent-directory), I found that it's not possible to specify the path to copy the files from the parent directory, so we can tell that this is the right and the only way to be done).

And when we execute the command, we have to be in the root directory of the project with CLI and use this command pattern:

```bash
docker build -t <image-name> -f <path-to-dockerfile> <path-to-working-directory>
# -t: tag the image with the name
# -f: specify the path to Dockerfile that we want to execute

# which can lead to this command
docker build -t ecomm-invoice-api-local -f deployment\deployment.Dockerfile .

```

### 6.4 Deploying the Model to the Cloud Environment

After we have everything ready, we will deploy the model to the cloud environment (GCP). We will use **Google Cloud Run** which is a serverless environment to deploy the containerized application. It's easy to use and manage, and it's also scalable. But, it needs to be connected to the **Artifact Registry** to use the Docker image we built.

So, what we need to do is:
- Build the Docker image locally and push it to the Artifact Registry.
- Deploy the Docker image from the Artifact Registry to the Cloud Run.

That's it! What we need to worry about is how to authenticate the GCP account to use the GCP CLI ***(gcloud)*** and how to connect the Artifact Registry to the Cloud Run which is very well documented in the GCP documentation:
- [*Creating standard repositories in Artifact Registry*](https://cloud.google.com/artifact-registry/docs/repositories/create-repos)
- [*Setting up authentication for Docker*](https://cloud.google.com/artifact-registry/docs/docker/authentication)
- [*Pushing and pulling images*](https://cloud.google.com/artifact-registry/docs/docker/pushing-and-pulling)
- [*Deploying to Cloud Run*](https://cloud.google.com/run/docs/deploying#command-line)
- [*Configuring containers*](https://cloud.google.com/run/docs/configuring/services/containers)

First, you need to install Google Cloud CLI (gcloud/Cloud SDK) and then enable the Artifact Registry API and Cloud Run API (Read Documentation above for more detail). Second, you can run `gcloud --version` in Google Cloud SDK to check if it is able to be used. Third, add the following roles to the service account that you use to authenticate the gcloud CLI in IAM & Admin page:

- Service Account User
- Artifact Registry Writer
- Artifact Registry Reader
- Cloud Run Admin

*Note: Add ***"Artifact Registry Administrator"*** role if you want to create a repository in the Artifact Registry via Terraform.*

And the last step is manually creating a repository in the Artifact Registry.

Then, you have to run the following commands:
```bash
# Google Cloud SDK Shell

# use absolute path to the service account key file to avoid any error
gcloud auth activate-service-account --key-file=path/to/your/service-account-key.json

# replace placeholder with your desired region
gcloud auth configure-docker "$REGION-docker.pkg.dev"

# replace placeholder with your desired variables
# this will build an image locally
docker build -t "$REGION-docker.pkg.dev/$PROJECT_ID/$IMAGE_NAME:latest" -f deployment\deployment.Dockerfile .

# this will tag the image for the Artifact Registry (This step is essential)
docker tag "$REGION-docker.pkg.dev/$PROJECT_ID/$IMAGE_NAME:latest" $REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/$IMAGE_NAME:latest

# this will push the image to the Artifact Registry
docker push "$REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/$IMAGE_NAME:latest"

# this will deploy the image in Artifact Registry to the Cloud Run
gcloud run deploy $SERVICE_NAME --image $REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/$IMAGE_NAME:latest --region $REGION --port $PORT --project $PROJECT_ID --allow-unauthenticated
```

*Note1: All commands above are well documented in the GCP documentation, so you can read it above for more detail.*

*Note2: You can export the environment variables to avoid typing the same variables over and over again, reducing the chance of making a mistake.*

Then, you can go to **Cloud Run Console** and check **URL or an endpoint** where you application is deployed. You can also check the logs in the **Cloud Run Console** to see if there is any error. **Done!** You can test your application with the endpoints.

### 6.5 Automating the Deployment Process *(Optional)*

When we have developed the model and the API, we surely need to deploy it to the cloud environment with newer version of it. But, we don't want to do it manually every time we update the model or the API. So, this come to the important of automating the deployment process.

Github Actions is one of many CI/CD tools that we can use to automate the deployment process. It's quite easy to use and manage, and it's also free for public repositories. It can automate tasks when we trigger it with the events we specify. For example, we can automate the deployment process when we push or pull request the code with a certain branch to the repository.

What you need to do is upload your project to your own github repository, and create a sample branch that can be used to trigger the workflow. Then, create a workflow file in the `.github/workflows` directory. You can name it whatever you want, but it has to be in the `.yml` format. For example, I name it [**deploy.yml**](./.github/workflows/deploy.yml).

I won't go into detail about how to write the workflow file, but you can check it out in [deploy.yml](./.github/workflows/deploy.yml). The process is similar to what we did in the previous section, but we have to specify the environment variables in the workflow file and add additionally steps for runner to check out the repository, set up the gcloud CLI, and authenticate the service account.

**Github Secrets Variable**

**The most important step** is to add the `GOOGLE_CREDENTIALS` to `Secrets` variables in Github. **Remember that we must not push or expose the service account key file to the public repository**, so we have to add it to the `Secrets` variable so that our workflow can use it to authenticate with google cloud services.

In the [deploy.yml](./.github/workflows/deploy.yml), you should see something like, `${{ secrets.variable_name }}`. This indicate that the workflow call the variable from the `Secrets` variable we specified. You have to do the following steps to do it correctly:
- Open [**"Git Bash"**](https://git-scm.com/downloads) and run the command to get the content of the service account key file in base64 encoded format.
    ```bash
    # Git Bash

    # Use absolute path to the service account key file to avoid any error
    cat path/to/your/local/service-account-key.json | base64
    ```
- Copy the output and go to your github repository.
- Go to **Settings** > **Secrets and variables** > **New repository secret**.
- Add the `GOOGLE_CREDENTIALS` as the name of the secret variable and paste the output of the command above to the value of the secret variable.
- Click **Add secret** and Type in your password to confirm.
- If you want any additional environment variables, you can add it to the `Secrets` variable as well, like `PROJECT_ID`, etc.

*For the original detail of encoding the service account key as base64, check it [here](https://stackoverflow.com/a/75127891/22191119)*

When we use it in the workflow, we will decode it back to the original format to put into the service account key file used to authenticate with google cloud services. And, after all the processes are done, we will delete the service account key file from the github runner to avoid any security issue. Check the code detail in the [deploy.yml](./.github/workflows/deploy.yml).

Done! Now, you can push or pull request the code to the repository (with your specific branch) to trigger the workflow, and you can check the workflow status in the **Actions** tab in your github repository. You can also check the logs in the **Cloud Run Console** to see if there is any error.

*Note1: for my personal method, I use `deploy` branch which is an arbitrary name and content in the branch is different from the `main` branch. So, I can pull request the code to the `deploy` branch to trigger the workflow. And if the workflow is failed, I fix the code in `main` branch and use `git reset` to undo the pushed code repeatedly. If you don't close the pull request, the workflow will be triggered again and again when you push the new version of workflow code to the `main` branch.*

*Note2: **`git reset` then force push is dangerous when you're working with other collaborators**, please be aware of it. Check how to reset or undo the committed without traces [here](https://stackoverflow.com/a/31937298/22191119)*

This solution of encoding the service account key file as base64 is not the best practice regarding security aspect. There's another better approach I found which is called ["Keyless authentication"](https://cloud.google.com/blog/products/identity-security/enabling-keyless-authentication-from-github-actions), but it's much more complicated and requires more services invloved and many more steps to be done. So, as we are in the learning phase, I think this approach is enough for now.

**References:**
- [Deploy a Dockerized FastAPI App to Google Cloud Platform](https://towardsdatascience.com/deploy-a-dockerized-fastapi-app-to-google-cloud-platform-24f72266c7ef)
- [Discover what Google Cloud Run is, how it works and how to get started with Cloud Run.](https://www.d3vtech.com/insights/the-ultimate-guide-to-google-cloud-run)
- [How To Deploy and Test Your Models Using FastAPI and Google Cloud Run](https://towardsdatascience.com/how-to-deploy-and-test-your-models-using-fastapi-and-google-cloud-run-82981a44c4fe)
- [Step-by-step Approach to Build Your Machine Learning API Using Fast API](https://towardsdatascience.com/step-by-step-approach-to-build-your-machine-learning-api-using-fast-api-21bd32f2bbdb)
- [How to Build MLOps Pipelines with GitHub Actions [Step by Step Guide]](https://neptune.ai/blog/build-mlops-pipelines-with-github-actions-guide)
## 7. Conclusion

From this project, we learned how to:
- **Design data architecture, and select tools for each process** to be used in the project.
- **Set up environment for virtualization** to simulate the local environment such as database, and web server.
- **Create an ETL pipeline** to extract data from various sources, transform the data, and load the data to the target database.
- **Utilize cloud services** to extend the datalake, data warehouse to production.
- **Create a dashboard** to visualize the data and analyze the business.
- **Develop machine learning models** to predict the future sales, customer segments, and customer churn.
- **Deploy the model to production** to leverage the business using API web service, and cloud services.

This is the biggest personal project I've ever done so far, and I learned a lot from it. I hope it can be useful for anyone reading this as well.  Although this project is not fully finished yet, but I will keep working on it and update it continuously as my knowledge and experience grow.

***Thank you for your reading, happy learning.***

---
*What's coming next?*

- Productionalization of FastAPI together with ML solution as ML System enabling MLOps.
- Extending to Redshift cluster is not fully finished, so I will continue to work on it and then extend to Microsoft Azure.
- The ETL pipeline in the part of loading data to the local target Postgres database is not fully finished.
- Recommendation System, Churn prediction Model, and Price Analysis.

---