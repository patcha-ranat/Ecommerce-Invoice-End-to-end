# End-to-end E-commerce Demand Forecasting
*Patcharanat P.*
```text
Always click "⋮≡" adjacent to `README.md` at top left to show the table of contents if you're lost.
```
**End-to-end Data project** in the e-commerce and retail domain covering the full process of data exploitation, including Data Engineering skills, Data Science skills, and Data Analytic skills, and also how to implement them in the real world utilizing Business and Marketing knowledge.

## **Context**

It's crucial in nowadays to emphasize data existing and make the most use of it. **The project was created to practice and demonstrate the full process of data exploitation** covering setting up environments, ETL process, Web Scraping, Data Visualization, Machine Learning Model Development, Model Deployment and Monitoring using E-commerce data, and also extend to develop strategy and marketing campagin based on the data.

## **Processes**:
1. [Setting up Environment](#1-setting-up-environment)
2. [ETL (Extract, Transform, Load): Writing DAGs and Managing Cloud Services](#2-etl-process-writing-dags-and-managing-cloud-services)
3. Web Scraping
4. [EDA and Data Visualization](#4-eda-and-data-visualization)
5. [Machine Learning Model Development](#5-machine-learning-model-development)
6. Model Deployment and Monitoring

## **Project Overview**

![project-overview](./src/Picture/project-overview.png)
*Note: this project will also include developing strategy and marketing campagin based on the data.*

## **Tools**:
- Sources
    - Postgres Database (Data warehouse)
    - REST API (raw file url)
    - API (with token)
- Data Lake & Staging Area
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
- EDA & Visualization
    - PowerBI (Desktop and Service)
    - Python (Jupyter Notebook)
- Machine Learning Model
    - Jupyter Notebook (Model development)
    - FastAPI (Model Deployment)
    - Streamlit (Monitoring)

Dataset: [E-Commerce Data - Kaggle](https://www.kaggle.com/datasets/carrie1/ecommerce-data)

## Prerequisites:
- Get a credentials file from kaggle and activate the token for API.
- Have Google Account being able to use google cloud services.

*The credentials are hidden in this project*

## 1. Setting up Environment

![setting-overview](./src/Picture/setting-overview.png)

Firstly, clone this repository to obtain all neccessary files, then use it as working directory.
```bash
git clone https://github.com/Patcharanat/ecommerce-invoice
```
We need to set up environment to demo ETL process, which including:
- Postgres database
- Airflow

All of the tools need to be run in different environment to simulate the real use-case. Hence, Docker compose become important for demo.

***Note:** if you want to reproduce the project by yourself, just create a folder for your working directory. And, the process you need to do will be remarked with **`DIY`***.

### **Step 1.1: Setting up Overall Services (containers)**
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

**Note1**: you may fail to run `docker compose build` since, you may not have google cloud credentials as a json file in your local. You can skip to [Step 2.1: Setting up Data Lake, and Data Warehouse](#step-21-setting-data-lake-and-data-warehouse) in ***Service account*** part to get your own google crendentials json file and put it in your own created `credentials` folder in the current working directory and try running `docker compose build` again.

*Note2: Don't forget to get `Kaggle.json` credentials, and add to `credentials` folder also.*

**DIY**: Setting up Overall Services (containers)
<details>
<p>

First, you need to simulate postgres database by creating a container with postgres image. you will need to copy `cleaned_data.csv` file into the postgres container. Then, you need to create a database and a schema, and a table with [`setup.sql`](setup.sql) file, and also configure [`.env`](.env) file, like username, password, and database. The file that will do the task is [`postgres.Dockerfile`](postgres.Dockerfile)

Then, you need to create a container with airflow image. You will need to copy `kaggle.json` file into the container (webserver, and scheduler). Then, you need to install libraries we needed in DAGs by **"pip install"**[`requirements.txt`](requirements.txt) file within the containers. The file that will do the task is [`airflow.Dockerfile`](airflow.Dockerfile)

To easily run multiple docker containers, you will need docker compose. The file that will do the task is [`docker-compose.yml`](docker-compose.yml), which will build all containers we specified in `build` and `context` parts which run different dockerfile for different containers.

In airflow official site, you will find template to run airflow as `docker-compose.yml` you can use it as reference and change it to fit your needs, like add postgres section, and remove unnecessary part that can causes running out of memory making you unable to run docker containers successfully.

If you're new to container, you will be confused a little with using path. please be careful where you mount the files to, unless it will bug.
</p>
</details>

### **Step 1.2: Intiating all Containers**
Initialize docker container(s) and run process in background (Detach mode)
```bash
docker compose up -d
```

***Note:** some services need time to start, check container's logs from `docker desktop` to see if the services are ready to work with.*

To check status of running containers:
```bash
docker ps
```

<img src="./src/Picture/docker-ps.jpg">

### **Step 1.3: Checking if all Dockerfiles correctly executed**
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

<img src="./src/Picture/dockerfile-executed.jpg">

What you should check more is that credentials file: `kaggle.json` correctly imported in airflow's scheduler and webservice containers.

### **Step 1.4: Checking Data in a Database**
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
if we see table and schema are corrected and shown, then importing csv to the Postgres database part is done.

<img src="./src/Picture/check-postgres.jpg">

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

<img src="./src/Picture/docker-desktop.jpg">

### **Step 1.6: Setting up Airflow Web UI**

To set up airflow, we need to define more 4 services that refer to [official's .yml file template](https://airflow.apache.org/docs/apache-airflow/2.6.1/docker-compose.yaml) including `airflow-postgres` to be backendDB, `airflow-scheduler` to make scheduler, `airflow-webserver` to make airflow accessible via web UI, and `airflow-init` to initiate airflow session.

<img src="./src/Picture/airflow-ui.jpg" width="75%">

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

## 2. ETL process: Writing DAGs and Managing Cloud Services

In my case (this project), I used a dataset from kaggle which was:
- loaded to postgres database
- uploaded to this repo github as csv format
- and I wrote DAGs to use Kaggle API to obtain the dataset directly from the Kaggle website.

<img src="./src/Picture/etl-overview.png">

If you want to change data, you have to write your own DAGs what match your specific use-case.

### **Step 2.1: Setting up Data Lake, and Data Warehouse**
As we will use GCP (Google Cloud Platform) as our Data Lake and Data Warehouse, we need to make our airflow script, which run in docker containers in local, being able to connect to GCP by using **google credentials** as known as `service account` or `IAM`. I recommend to manually get the credential from GCP console for the safety aspect.

**Service Account**

Do the following:

1. Go to your GCP console **within your project** (I assume you have free credit), and go to navigation menu (3-bar icon at top left), then go to `IAM & Admin` > `Service Accounts` > `Create Service Account` > Create your Service Account 
2. In Service accounts section, click 3 dots at your new created service account > `Manage keys` > `Add key` > `Create new key` > `JSON` > `Create` > `Download JSON` > `Close`, keep your credentials (this json file) in safe place.
3. You need to specify the permission that you allow for that service account, like how much it can access to the resources. What you need to do is:
- Going to `IAM` page in `IAM & Admin` section > Edit principal for your created service account > Add Roles
- Add the following roles:
    - BigQuery Admin
    - BigQuery Data Editor
    - BigQuery Data Owner
    - BigQuery Job User
    - BigQuery User
    - Storage Admin
    - Storage Object Admin
    - Storage Object Creator
    - Storage Object Viewer
- And then, save your changes.

***Note**: if you want to add your project to github, make sure you are working in private repo, or add it to `.gitignore` file*

Until now, you've finished getting service account credentials.

<img src="./src/Picture/gcp-iam.jpg">

**Data Lake**

The next step is creating your Google cloud storage bucket. Go to `Cloud Storage` > `Create` > `Name your bucket` (which *globally unique*)

Then, choose the options that match you specific needs, the recommend are:
- `Location type`: Region
- `Storage Class`: Standard
- Activate `Enforce public access prevention`
- `Access control`: Uniform
- `Protection tools`: None

Click `Create`, and now you have your own data lake bucket.

<img src="./src/Picture/gcs-ui.jpg" width="75%">

**Data Warehouse**

The last step is creating your Bigquery dataset and table. In Bigquery (Google Data warehouse) you could have many projects, each project might have many datasets, and each dataset might have many tables. Understanding of this hierarchy, we can easily know what we should create for a data table.

Do the following to create your dataset and table:
- Go to `Bigquery` > Click on 3-dot after your project name > `Create dataset` > `Name your dataset` (which *globally unique*) > `Create dataset`

*(Recommend to choose location type that suit your region, and table expiration is up to you)*

- Click on 3-dot after your created dataset > `Create table` > `Select your data source` (In this case, select empty table) > `name your table` > define schema > `Create table`

Until now, you've finished creating your data warehouse that's ready to load our data in.

<img src="./src/Picture/bigquery-ui.jpg" width="75%">

As you can see it's quite inconvenient that we have to create all of these resources manually via Google UI. So, we will use **Terraform** to create these resources in the next step.

**Terraform**

We can achieve creating the bucket and the warehouse by **"Terraform"**, which is a better way to create and manage cloud resources. you can see the code in `terraform` folder, consists of [main.tf](terraform/main.tf) and [variables.tf](terraform/variables.tf). Terraform make it easier to create and delete or managing the resources in this demonstration with a few bash commands.

The [`main.tf`](./terraform/main.tf) file, using some variables from [`variables.tf`](./terraform/variables.tf) file, will produce the following resources:
- 1 data lake bucket
- 1 Bigquery dataset
- 1 Bigquery table

To use terraform, you need to install Terraform in your local machine (+add to PATH), and have your google credentials (service account credentials) as a json file within `"credentials"` directory in the same level of your working directory. Then, you can run terraform commands in your terminal **in your terraform working directory**.

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


### **Step 2.2: Setting up DAG and Connections**

<img src="./src/Picture/airflow-dag-graph.jpg">

In this project, I wrote [`ecomm_invoice_etl_dag.py`](src/dags/ecomm_invoice_etl_dag.py) to create 1 DAG of **(7+1) tasks**, which are:
1. Reading data from raw url from github that I uploaded myself. Then, upload it to GCP bucket as uncleaned data.
2. Fetching data from the postgres database that we simulate in docker containers as a data warehouse source. Then, upload it to GCP bucket as cleaned data.
3. Downloading from the Kaggle website using Kaggle API. Then, upload it to GCP bucket as uncleaned data. 
4. Data Transformation: reformat to parquet file, and cleaning data to be ready for data analyst and data scientist to use, then load to staging area.
5. Loading to data warehouse (Bigquery) as cleaned data with a Native table way from staging area.
6. Loading to data warehouse (Bigquery) as cleaned data with an External table way from staging area.
7. Loading to another Postgres database as cleaned data.
8. Clearing data in staging area (GCP bucket). **(this will not be used since we will implement an external table that requires the source file to be exists)**

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

***Note:** we used **`database name`** that we specified in `docker-compose.yml` in DAG script where we need to connect to the database, PostgresHook with `conn_id` as Postgres Host name, and `schema` as **`database name`***.

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
</p>
</details>

*The dependencies management will be changed to **poetry** in further development. Since, it's not worked until now and not the focus of this project*

**Bigquery: Native tables vs External tables**

<img src="./src/Picture/native-vs-external.png" width="75%">

In the DAG script, we have 2 tasks to load data to Bigquery, one is using Native table, and another is using External table. the differences between them are their performance and how it works.

External tables are suitable when you want to query data without loading it into BigQuery, optimizing costs and leveraging existing data sources. Native tables, on the other hand, offer enhanced performance and advanced features within BigQuery, like partitioning and clustering.

*What to note is: if you're using external table, you can't delete the source in data lake, because it require the source to query directly from. And you couldn't see the preview of data table in the Bigquery UI, with Extenal table approach.*

### **Step 2.3: Triggering DAG and Monitoring**

After finishing the writing DAG part, we can go to `localhost:8080` via web browser and login with username, and password we defined in [docker-compose.yml](docker-compose.yml) file. Then, we can see the DAGs we created in the UI. We can trigger the DAG by clicking on the `Trigger DAG` button, and monitor the progress of the DAG by clicking on the DAG name.

You can also see the logs of each task by clicking on the task name in the DAG graph and see how the tasks flow is working via Graph.

<img src="./src/Picture/airflow-triggered.jpg">

Most of time, you don't write the DAGs in one time and test once it's done, you have come to this UI, triggering and monitoring to see if it works or not, and then fix the bugs. So, you have to trigger the DAGs many times, and see the logs to debug the code. It's very helpful to debug the code by reading the logs and checking which tasks are failed.

**Once the ETL DAG worked successfully, the data engineering part is finished.**

## 3. Web Scraping

This part is not currently in development. I will update the progress later. But, you can check the concept and the old written code in [web-scraping](https://github.com/Patcharanat/ecommerce-invoice/tree/master/web-scraping) folder.

## 4. EDA and Data Visualization

Once we have the data in the data warehouse, we can do the EDA and data visualization connecting data from the data warehouse. I will use **PowerBI** to create the dashboard, and Python for some ad-hoc analysis.

### 4.1 EDA

Actually, we have to do the **EDA** (Exploratory Data Analysis) before loading data to data warehouse for cleaning and transformation, but in the analysis part, we can do in different objectives. In this part, I will do an EDA to see the characteristic of the data, and see if there is any insight that can be used to improve the business.

<details><summary>Data Characteristics</summary>
<p>

This part is mostly done with Python, since it's more flexible to do ad-hoc analysis. I used **Pandas** and **Matplotlib** to do the analysis.

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
- **It's crucial to know who are the audiences of the dashboard, and what is the objective of the dashboard**. So, we can select the right metrics, right data, and right visualization.
- **Data model is very important**, it's the foundation of the dashboard. If the data model is not correct, the dashboard will not be correct. If the data model come in a good shape, the dashboard will be easier to create, and the data will be easier to analyze. (especially in aspect of **Time Intelligence**)

*Note: I will update the dashboard in the future, since it's not fully finished.*

## 5. Machine Learning Model Development

The Model Development part is not fully finished yet, but some part of it are done and ready to be presented.

This is what I have planned so far:
1. Anomaly Detection ***(Partially done, wait for improvement)***
2. [Customer Segmentation By RFM, KMeans, and Tree](#52-customer-segmentation-by-rfm-kmeans-and-tree) ***(Done)***
3. [Market Basket Analysis](#53-market-basket-analysis) ***(Done)***
4. Recommendation System ***(In progress)***
5. Demand Forecasting ***(In progress)***
6. Customer Churn Prediction *(Not started)*
7. Customer Lifetime Value Prediction *(Not started)*
8. Price Analysis and Optimization *(Not started)*

You can see the code in [**model_dev.ipynb**](model_dev.ipynb)

### 5.1 Anomaly Detection

*in development . . .*

### 5.2 Customer Segmentation By RFM, KMeans, and Tree

**Introduction**

**RFM (Recency, Frequency, Monetary)** is a well known method to segment customers based on their behavior. **But recency, fequency, and monetary alone, are not effective enough to segment the diversity of customers**. We should use other available features or characteristics of customers to segment them more effectively that come to the harder to score with traditional RFM method.

As a result, **KMeans emerged as a crucial machine learning technique for effectively clustering clients** into more precise customer segments. **But, Kmeans is not interpretable**, we can't explain the result or criteria to the business that how clusters are formed or how the customers are segmented, but only show what features we use in KMeans model (we don't even know which features are more influence).

**So, we can use Decision Tree to find the criteria of the clusters**, and explain the result to the business leading to proper campaigns and strategies that suit to customer segments.

Moreover, we can use **XGBoost to find the most important features** that influence the customer segments, and further concern and develop strategies regarding that features, or even use them to segment customers with KMeans again.

**RFM and Features Details**

First, we will form the RFM table based on customer trasactions and add some more features (Feature Engineering) that we think it might be useful for segmentation which include:
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
- refund rate

Eventually, we will get a new table that represents customers profile and their behavior.

**KMeans**

Before we put the customer profile data into KMeans, we should scale the data first , in my case I used **Robust Scaler**, to make the data have the same scale, since KMeans algorithm use distance to calculate the clusters, so it's sensitive to the different scale of the data.

Then, we will use KMeans to segment customers into clusters. We will use **Elbow Method** and **Silhouette Plot** to find the optimal number of clusters which eventually is 10 clusters.

<img src="./src/Picture/kmeans-elbow.png" alt="kmeans-elbow" width="75%">

![kmeans-silhouette](./src/Picture/kmeans-silhouette.png)

Then, we label the clusters to customer profile table which not scaled yet, so we can see the customer profile of each cluster with the original scale to make it more interpretable. We will use this table to find the criteria of the clusters as an input and validation set for the Decision Tree.

**Decision Tree**

After that, we will use **Decision Tree** to find the criteria of the clusters to make the clustering result from KMeans become interpretable. I used **Random Search** with **Cross Validation** to find the best hyperparameters of the Decision Tree to make it the most accurate **resulting in accuracy 93.29% on test set, 93.97% on train set, macro averaged of F1 score 0.86, to clarify criteria of each cluster.**

![kmeans-tree](./src/Picture/kmeans-tree.png)

***Note: The 93% accuracy doesn't mean we segmented the customers correctly by 93%**, because we can't be sure that Kmeans correctly 100% segmented the customers (and definitely, it didn't), but we can tell that the Decision Tree can explain the criteria of the clusters correctly by 93%, So we can quite believe to use the criteria from the Decision Tree to cluster customer segmented to follow Kmeans result*

**XGBoost**

Finally, we will use **XGBoost** to find the most important features that influence the customer segments or important factors to develop the strategies. I also used **Random Search** with **Cross Validation** to find the best hyperparameters of XGBoost resulting in accuracy 97.65% on test set, 100% on train set, macro averaged of F1 score 0.97, to find the most important features (seem overfitting, but it's not a main focused).

The most important features can be shown in the graph below:

<img src="./src/Picture/xgb-importance-features.png" alt="xgb-importance-features" width="75%">

Finally we can classify the customers into 10 clusters and explain the criteria of each cluster to the business.
- **Cluster 0** : Bought less, not often, and small ticket size, I think this is a cluster of casual customers or one-time purchase customers.
- **Cluster 1** : Bought much in amount, but low quantity.
- **Cluster 3** : Bought less in amount, but bought often, and small ticket size, I think this is a cluster of regular customers who are likely to active and easily attracted by marketing campaign.
- **Cluster 4** : Big spenders, and bought a lot of items both quantity, and diverse items, I guess this is a cluster of wholesale customers.
- **Cluster 5** : Also Big spenders, and bought a lot of items both quantity, and diverse items, I also think this is a cluster of wholesaler but bigger than cluster 4, because they bought in more amount and more quantity.
- **Cluster 7** : Big spenders, but not much as cluster 5, spent 50 - 155£ per month and bought once a month approximately, and also bought in low quantity. I guess this is a cluster of valuable regular customers that we should keep. Moreover, they're also the largest group among all clusters.

... and so on.

*Note: the more cluster you have, the more specific the criteria of each cluster will be, leading to harder to classify.*
### 5.3 Market Basket Analysis

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

### 5.4 Recommendation System

*in development . . .*

### 5.5 Demand Forecasting

*in development . . .*

### 5.6 Customer Churn Prediction

*in development . . .*

### 5.7 Customer Lifetime Value Prediction

*in development . . .*

### 5.8 Price Analysis and Optimization

*in development . . .*

## 6. Model Deployment and Monitoring

*The project is in development . . .*

---
*Development Note*

- Currently, I'm working on developing Power BI Dashboard and researching for Model Development (not the algorithms of the ML model, but how to implement in business use cases).

- Model Deployment and Extending Data Lake and Data Warehouse to Microsoft Azure and AWS likely to be more focused than Model Development, but I'm waiting for a proper model to be a prototype.

***Thank you for your reading. happy learning.***