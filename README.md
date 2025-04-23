# End-to-end E-commerce Data Project - AI-Driven Interpretable Dynamic Customer Segmentation

*Patcharanat P.*

*If you use or adapt this project, please credit and link to the original. Thank you!*

```text
Click "⋮≡" at top right to show the table of contents.
```

**End-to-end Data project** in the e-commerce and retail industries covering the full process of data exploitation, including Data Engineering, Data Science, Data Analytic, and how to automate ML deployment (MLOps).

## **Context**

It's crucial in nowadays to emphasize data existing and make the most use of it. **The project was created to practice and demonstrate the full process of data exploitation** covering setting up local environments, cloud infrastructure, ETL pipelines, Data Visualization, Machine Learning Model Development, and Model Deployment using E-commerce data.

## **Table of Contents**:

*(latest revised: July 2023, Aug 2023, Oct 2024, 14 Nov 2024, 25 Nov 2024, 15 Dec 2024)*
1. [Setting up Local Environment](#1-setting-up-local-environment)
    - 1.1 [Setting up Overall Services (containers)](#11-setting-up-overall-services-containers)
    - 1.2 [Checking Docker Results](#12-checking-docker-results)
    - 1.3 [Setting up Airflow Properties](#13-setting-up-airflow-properties)
2. [**Setting up Cloud Infrastructure and Authentication**](#2-setting-up-cloud-infrastructure-and-authentication)
    - 2.1 [Setting IAM and Airflow Connections](#21-setting-iam-and-airflow-connections)
        - 2.1.1 [Setting up IAM and Service Account](#211-setting-up-iam-and-service-account)
        - 2.1.2 [Account Impersonation](#212-account-impersonation)
        - 2.1.3 [Setting up Airflow Connections and Variables](#213-setting-up-airflow-connections-and-variables)
        - 2.1.4 [Airflow Entrypoint](#214-airflow-entrypoint)
    - 2.2 [Authentication](#22-authentication)
        - 2.2.1 [Long-lived credentials method vs others](#221-long-lived-credentials-method-vs-others)
        - 2.2.2 [GCP ADC for Terraform](#222-gcp-adc-for-terraform)
        - 2.2.3 [GCP ADC for pipeline code](#223-gcp-adc-for-pipeline-code)
    - 2.3 [Terraform](#23-terraform)
    - 2.4 [Extending Terraform to AWS](#24-extending-terraform-to-aws)
3. [DAGs and ETL Pipelines](#3-dags-and-etl-pipelines)
    - 3.1 [**Airflow DAGs and Data warehouse in Production**](#31-airflow-dags-and-data-warehouse-in-production)
    - 3.2 [**Data Modeling and Optimization in Production**](#32-data-modeling-and-optimization-in-production)
    - 3.3 [**How to Optimize Data Models**](#33-how-to-optimize-data-models)
4. [EDA and Data Visualization](#4-eda-and-data-visualization)
    - 4.1 [EDA](#41-eda)
    - 4.2 [PowerBI Dashboard](#42-powerbi-dashboard)
5. [Machine Learning Model Development](#5-machine-learning-model-development)
    - 5.1 [Customer Segmentation By RFM, KMeans, and Tree-based Model](#51-customer-segmentation-by-rfm-kmeans-and-tree-based-model)
    - 5.2 [Market Basket Analysis](#52-market-basket-analysis)
    - 5.3 [Demand Forecasting](#53-demand-forecasting)
6. [**ML Code Productionization and Deployment**](#6-ml-code-productionization-and-deployment)
    - 6.1 [Exporting from Notebook](#61-exporting-from-notebook)
    - 6.2 [Docker Containerization](#62-docker-containerization)
    - 6.3 [Deploying the Model to the Cloud Environment](#63-deploying-the-model-to-the-cloud-environment)
    - 6.4 [Unit Test, Pre-commit, and Linting](#64-unit-test-pre-commit-and-linting)
        - 6.4.1 [Pre-commit and Linting](#641-pre-commit-and-linting)
        - 6.4.2 [Unit Test](#642-unit-test)
    - 6.5 [CI/CD Workflow - Automated Deployment Process](#65-cicd-workflow---automated-deployment-process)
        - 6.5.1 [Github Actions - Secrets Variable and Authentication](#651-github-actions---secrets-variable-and-authentication)
        - 6.5.2 [Workload Identity Federation Debugging](#652-workload-identity-federation-debugging)
    - 6.6 [ML Service Integration Test](#66-ml-service-integration-test)
7. [Conclusion](#7-conclusion)
8. [Appendix](#8-appendix)

## **Project Overview**

![project-overview](./docs/project-overview.png)

Dataset: [E-Commerce Data - Kaggle](https://www.kaggle.com/datasets/carrie1/ecommerce-data)

## Prerequisites:

- Python
- Docker Desktop
- Terraform (+Add to `$PATH`)
- Available GCP account to enable google cloud services.
- [gcloud (GCP SDK)](https://cloud.google.com/sdk/docs/install)
- Credential File for AWS as `.csv`
- Credential file from kaggle with activated token for API.

*The credentials are hidden from repo in by `.gitignore`*

## 1. Setting up Local Environment

Clone this repository to obtain all neccessary files, then use it as root working directory.

```bash
git clone https://github.com/patcha-ranat/Ecommerce-Invoice-End-to-end.git
```

Create all hidden files from repo in order to reproduce:
- `credentials/<aws-credentials>.csv`
- `credentials/kaggle.json`
- `src/config/variables.json`, please change as needed referred to [variables-masked.json](./src/config/variables-masked.json)
- `.env` using this template
    ```shell
    # GCP
    GOOGLE_CLOUD_PROJECT=<your-gcp-project>
    GCP_REGION=<your-gcp-region>

    # AWS
    AWS_CREDENTIALS_FILENAME=<aws-credentials-filename>.csv
    AWS_BUCKET=<your-aws-bucket-name>
    ```
- `terraform/terraform.tfvars`
    ```bash
    # aws credentials
    aws_access_key = "XXX"
    aws_secret_key = "XXX"

    # serverless redshift
    ##########################
    # Application Definition # 
    ##########################
    # app_name        = "XXX" # Do NOT enter any spaces
    app_environment = "dev" # Dev, Test, Staging, Prod, etc

    #########################
    # Network Configuration #
    #########################
    redshift_serverless_vpc_cidr      = "10.0.0.0/16"
    redshift_serverless_subnet_1_cidr = "10.0.1.0/24"
    redshift_serverless_subnet_2_cidr = "10.0.2.0/24"
    redshift_serverless_subnet_3_cidr = "10.0.3.0/24"

    ###################################
    ## Redshift Serverless Variables ##
    ###################################
    # redshift_serverless_namespace_name      = "xxx-namespace"
    # redshift_serverless_database_name       = "xxxdb"
    # redshift_serverless_admin_username      = "XXX"
    # redshift_serverless_admin_password      = "XXX"
    # redshift_serverless_workgroup_name      = "XXX-workgroup"
    # redshift_serverless_base_capacity       = 32
    # redshift_serverless_publicly_accessible = false
    ```
- `code/models/terraform/terraform.tfvars`
    ```bash
    project_id = "<GCP-Project-ID>"
    service_account_email = "<Service-Account-Email>"
    ```
- `code/models/terraform/auth/terraform.tfvars`
    ```bash
    project_id = "<GCP-Project-ID>"
    service_account_email = "<Service-Account-Email>"
    ```

Run the following commands:
```bash
# workdir: .

# docker compose build

# docker compose up

docker compose up --build

# check localhost:8080 via web browser

# Ctrl+C

# docker compose down -v

# delete all local docker images via Docker UI
```

Since the pipelines might be needed to be run in different environment not just on local machine, it's essential to make sure that your code can be packaged and run in anywhere or be able to orchestrated by different tools such as *Kubernetes* for further scaling up. So, developing and testing your code to perform ETL/ELT processes in a **Docker** container is essential. And with **Docker Compose**, it make it more convenience to orchestrate multiple containers, in which most of time are built by different images, with a lightweight approach.

Usually, we don't use docker compose in production, but it is lightweight and easy enough to allow you to run and test your code on local machine. However, packaging your code as a docker images is still the way to go for production scale pipeline, specifically for *Kubernetes* as an example.

### 1.1 Setting up Overall Services (containers)

![setting-overview](./docs/set-env-overview.png)

From **"Setting Up Local Environment Overview"** image,
1. Docker compose will copy raw data [data/uncleaned.csv](./data/uncleaned_data.csv), and [setup.sql](./setup.sql) to source postgres database container to mockup data source as DBMS. With the `setup.sql` as an entrypoint of the container, source postgres db will initiate table and load data into in. All the execution steps defined in [postgres.Dockerfile](./postgres.Dockerfile)
2. Docker compose will copy [target.sql](./target.sql) to target postgres database container to mockup target database as DBMS. With the *target.sql* as an entrypoint of the container, target postgres db will initiate empty table waiting for ETL processes. All the execution steps defined in [postgres-target.Dockerfile](./postgres-target.Dockerfile)
3. Docker compose will do the following:
    1. Mount Volume of credentials (GCP-ADC, AWS, and Kaggle) to airflow scheduler and webserver containers. And it also mount necessary components for airflow such as `logs` folder, [src/config/variables.json](./src/config/variables-masked.json), and [src/config/connections.json](./src/config/connections.json) which will be clarified more on Topic [*2.1.3 Setting up Airflow Connections and Variables*](#213-setting-up-airflow-connections-and-variables)
    2. Mount [airflow-entrypoint.sh](./airflow-entrypoint.sh) to airflow scheduler and webserver containers and use it as entrypoint to import  mounted [variables.json](./src/config/variables-masked.json) and [connections.json](./src/config/connections.json) at runtime when first start. The step that related to authentication also will be explained in Topic [*2.1.4 Airflow Entrypoint*](#214-airflow-entrypoint).
    3. Use [airflow.properties](./airflow.properties) to configure Airflow Containers' environment variables (act as `.env` file in airflow containers)
    4. install airflow python dependencies specified in [requirements.txt](./requirements.txt) executed by [airflow.Dockerfile](./airflow.Dockerfile). Setting up gcloud also will be explained in Topic 2.
     
#### Explaination Note

- Docker Compose file [docker-compose.yml](./docker-compose.yml) orchestrated all the processes above. It can indicate how images should be built by specifying different `Dockerfile`(s) in `build` section and `context` section that's relative to `docker-compose.yml` file.
- To execute entrypoint commands, you can use `entrypoint` section in `docker-compose.yml` file using shell script. However, it's different for `.sql` script. To execute `.sql` script as entrypoint in postgres database containers, you have to copy (or mount) the script to `/docker-entrypoint-initdb.d` directory, and it will execute automatically while the container spinning up (initializing docker session).
- You may notice that in `docker-compose.yml` file using `${VARIABLE_NAME}` that referred to variables specified in `.env` file. The `.env` file export environment variables ***(env var)*** specified in the file for local machine during docker compose runtime, operating separately from `airflow.properties` which export *env var* in airflow containers during runtime.

In airflow official site, you will find [`docker-compose.yml`](https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml) template to run airflow you can use it as reference and change it to fit your needs, like adding postgres section, and remove unnecessary part that can causes running out of memory making you unable to run docker containers successfully.

### 1.2 Checking Docker Results

***Note:** some services need time to start, check container's logs from **docker desktop UI** or `docker ps` to see if the services are ready to work with.*

![docker-ps](./docs/docker-ps.png)

What needed to be checked are
- Are the files are mounted/copied to target path correctly?
    - `kaggle.json`
    - `ecomm-invoice-kde-aws-iam_accessKeys.csv`
    - `setup.sql`
    - `cleaned_data.csv`
    - `target.sql`
- Is the data table in postgres database as a source created correctly?
    - data loaded from `cleaned_data.csv` and using the right schema?

- Is the data table in postgres database as a target created correctly?
    - empty table with the right schema?

Getting into terminal of the container we specified by:
```bash
docker exec -it <container-name-or-id> bash
```

*Note: You can get container's name or id from `docker-compose.yml` or from `docker ps` command.*

![dockerfile-executed](./docs/dockerfile-executed.png)

#### Checking Data in a Database

Access to both postgres containers, and then access database to check if csv file copied into table.
```bash
# docker exec -it postgres-container-source bash

psql -U postgres -d mydatabase
```

```bash
# to see tables list
\dt or \d

# to see schemas list
\dn or \z
```
if we see table and schema are corrected and shown, then we successfully importing csv to the Postgres database.

<img src="./docs/check-postgres.jpg">

if not, these can be issues
- check if `setup.sql` is executed successfully, by inspecting logs in docker desktop
- check if data csv file and `setup.sql` are copied into docker container's local by using container's bash and check if path in `Dockerfile` and `setup.sql` were set correctly.
- we need to set search_path by
```bash
# to set only in current session. (reccomended)
SET search_path TO <myschema>;

# to set permanently at database level
ALTER DATABASE <mydatabase> SET search_path TO <myschema>; 
```

In postgres bash, we will be able to see only the table that match the schema we created. Hence, we have to change the schema to see the table in the database.

Then exit from all bash
```bash
\q
exit
```

***Note:** Specifying image name or container name in `docker-compose.ymal` is crucial to not be the same between services if it are meant to be created with different steps, because it could `build` with the wrong `Dockerfile` and cause the some unexpected issues, such as create the image build postgres database target with source Dockerfile: `postgres.Dockerfile`, instead of `postgres-target.Dockerfile`*.

### 1.3 Setting up Airflow Properties

To set up airflow locally, we need to define at least 4 services referred from [official's .yml file template](https://airflow.apache.org/docs/apache-airflow/2.6.1/docker-compose.yaml) including `airflow-postgres` to be airflow backend database, `airflow-scheduler` to execute task for `LocalExecutor`, `airflow-webserver` to make airflow accessible via web UI, and `airflow-init` to initiate overall airflow session.

Understanding how every components in `docker-compose.yml` work make much more easier to comprehend and debug issues that occur, such as `depends-on`, `environment`, `healthcheck`, `context`, `build`, and storing object in `&variable`.

**For this project**, we create 3 postgres containers, so we need to check carefully if airflow connected to its own backendDB or the right database.

#### Issue debugged: for being unable to connect to airflow backendDB

Use this template from official's document in `airflow.properties` file:

```python
postgresql+psycopg2://<user>:<password>@<host>/<db>

#or

[dialect]+[driver]://[username:password]@[host:port]/[database]

# which results in

AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@airflow-postgres/airflow
```

***Note:*** In `airflow.properties` file, airflow core need *FERNET* key which can be obtained from [fernet.py](./fernet.py) (randomly generated)

## 2. Setting up Cloud Infrastructure and Authentication

To me, This part is quite complex compared with other topic considered by security-risk and utilizing different cloud services architecture. We will talk about these following topics:

1. [Setting IAM and Airflow Connections](#21-setting-iam-and-airflow-connections)
    1. [Setting up IAM and Service Account](#211-setting-up-iam-and-service-account)
    2. [Account Impersonation](#212-account-impersonation)
    3. [Setting up Airflow Connections and Varibles](#213-setting-up-airflow-connections-and-variables)
    4. [Airflow Entrypoint](#214-airflow-entrypoint)
2. [Authentication](#22-authentication)
    1. [Long-lived credentials method vs others](#221-long-lived-credentials-method-vs-others)
    2. [GCP ADC for Terraform](#222-gcp-adc-for-terraform)
    3. [GCP ADC for pipeline code](#223-gcp-adc-for-pipeline-code)
3. [Terraform](#23-terraform)
4. [Extending Terraform to AWS](#24-extending-terraform-to-aws)

### 2.1 Setting IAM and Airflow Connections

### 2.1.1 Setting up IAM and Service Account

Actually, it's available to retrieve ***Service Account (SA)*** with accesses via `gcloud` CLI or ***Terraform***, but I'd prefer to create it via GCP console (Web UI) manually to reduce complexity for utilizing SA in further steps. So, we don't have to switch account email back and forth.

Please, follow this guideline:
1. Go to your GCP project console with available access to manage cloud resources, and go to navigation menu, then go to `IAM & Admin` > `Service Accounts` > `Create Service Account` > Create your SA and keep the email.
2. You have to specify the permissions that you allow for that SA, like how much it can manage resources. What you need to do is:
    - Go to `IAM` page in `IAM & Admin` section > Edit principal for your created service account > Add Roles
    - For the sake of this topic and the next, please add these following roles for sufficient permissions:
        - **Artifact Registry Administrator**: *For creating/deleting Artifact Registry object*
        - **BigQuery Admin** : *To be able to manage BigQuery Resources*
        - **IAM Workload Identity Pool Admin**: *For creating/deleting Workload Identity Federation (WIF) Pool*
        - **Service Account Token Creator**: *For account impersonation*
        - **Storage Admin** : *To be able to manage Cloud Storage (GCS) Resources*
        - **Workload Identity User**: *To be able to authenticate via WIF*
    - And then, save your changes.
3. Also add **Service Account Token Creator** role to your owner account

![gcp-iam](./docs/gcp-iam.png)

### 2.1.2 Account Impersonation

- For GCP, you can use your own account to interact with cloud resources directly, but in enterprise-scale project, there's will be some restriction to limit access of how someone can access or manage the resources due to security purpose.
- Using **Impersonation** to SA allow your email to act as that SA email to interact with cloud resources with SA's access or permission. By this methodology, team can unify everyone's access to this SA or separate group access by multiple SAs. This can enhance security of cloud management and improve data governance of the company.

```bash
gcloud auth application-default login --impersonate-service-account <your-service-account-email>
```

### 2.1.3 Setting up Airflow Connections and Variables

After we have proper permissions for the SA, we have to configure airflow to allow it authenticating with cloud through code. There's multiple ways to do it, like by configuring ADC, Secret as URI, or specifying long-lived credentials path. In this project, I will use the ADC method which will be elaborated on Topic 2.2.1: *Long-lived credentials method vs others*.
- In many tutorials, you may find instructor to tell you how to configure airflow connections on Airflow Web UI, but in reality, we should keep it as code for versioning and reducing manual process as much as possible. this can be done by using *pre-defined connections* [connections.json](./src/config/connections.json)
- Airflow Variables also operate in the same behavior to allow the code act differently between different stage of environments (dev/stg/prod) by specifying different variables for each stage. [variables.json](./src/config/variables-masked.json)
- You can import/update connections and variables during airflow runtime by:
    ```bash
    docker exec -it <airflow-scheduler-container-name> bash

    airflow variables import config/variables.json

    airflow connections import config/connections.json

    # You can also export both of it from manual configuration via Web UI
    # airflow variables export config/variables-export.json
    # airflow connections import config/connections-export.json
    ```
    - But as mentioned before in Topic [1.1](#11-setting-up-overall-services-containers), it's already executed first time when containers start by `airflow-entrypoint.sh`, but we can execute these commands to update connections and variables at runtime.

![airflow-connections](./docs/airflow-connections.png)
![airflow-variables](./docs/airflow-variables.png)

#### **Note:**

- Path of `config/variables.json` and `config/connections.json` depends on mounted path from local machine or path we specified to copied it to in `Dockerfile`
- If you used copying approach through `Dockerfile`, please note that the image would contain files that contain sensitive information, especially in `connections.json`, and can be dangerous to leak to public.
- `google_cloud_default` is leave blank to use conenction from ADC, this method was explained by Airflow Official in reference link below.

References:
- [Configuring Airflow Connections - Airflow Official](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html)
- [Configuring Airflow-GCP Connections - Airflow Official](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html)
- [Importing Airflow Connections - Airflow Official](https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html#import)
- [Importing Airflow Variables - Stack Overflow](https://stackoverflow.com/a/65580854)

Here's some additional information of my old written guideline to configure **postgres connections**:
- `Connection Type:` **Postgres**
- `Host` *service name of postgres in docker-compose.yml*
- `Schema` *schema name we used in `setup.sql`*
- `Login` *username of postgres in docker-compose.yml*
- `Password` *username of postgres in docker-compose.yml*
- `Port` *username of postgres in docker-compose.yml*

***Note1:** we used **`database name`** that we specified in `docker-compose.yml` in DAG script where we need to connect to the database, PostgresHook with `conn_id` as Postgres Host name, and `schema` as **`database name`***.

***Note2:** we can omit `schema` argument in PostgresHook and Airflow connection, if we use `public` schema, or specify SELECT `myschema.table_name` FROM ... in `setup.sql`*

### 2.1.4 Airflow Entrypoint

However, importing airflow connections and variables through CLI is still a manual process and we don't expect this steps in production. So, using airflow entrypoint become important. [airflow-entrypoint.sh](./airflow-entrypoint.sh) is specified in `docker-compose.yml` file in `entrypoint` section to be executed everytime when airflow containers created and started. `airflow-entrypoint.sh` is defined with many steps including importing conenctions and variables, authenticating with gcloud using ADC, and the last line is defined according to airflow official site to continue airflow default entrypoint.

#### **Note:**

- gcloud authentication line is very important to make the container be able to pull docker image from the private repository (GAR). After the entrypoint executed, the container will contains sensitive information from using ADC to `print-access-token` into `~/.docker/config.json`. This line of code is equal to `gcloud auth configure-docker <region>...`.
- However, this process do not make airflow image become sensitive, because we did not copy ADC file to docker image directly, instead we use mounting volume. This sensitive information will be contained only in container and after entrypoint is executed, not in image. Without proper setting up mounting part, the airflow container wouldn't operate properly.

#### Entrypoint Debugging:
```
Docker : exec /usr/bin/sh: exec format error
```
- Add shebang line (`#!/bin/bash`) as the first line in `airflow-entrypoint.sh`

```
denied: Permission "artifactregistry.repositories.uploadArtifacts" denied on resource "projects/xxx/locations/xxx/repositories/
```
- Use `gcloud auth configure-docker <gcp-region>-docker.pkg.dev` or specify `print-token-access` to `docker login -u oauth2accesstoken`, please check code example in `airflow-entrypoint.sh` file.

References:
- [Execute custom code before the Airflow entrypoint - Airflow Official](https://airflow.apache.org/docs/docker-stack/entrypoint.html#execute-custom-code-before-the-airflow-entrypoint)
- [shebang line in airflow-entrypoint.sh - Stack Overflow](https://stackoverflow.com/a/77155746)
- [gcloud authenticate in docker container using ADC - Stack Overflow](https://stackoverflow.com/a/77022687)

### 2.2 Authentication

### 2.2.1 Long-lived credentials method vs Others

- In fact, the most easiest way to interact with cloud services through code is to use long-lived tokens or credentials, but it's also the least safest way to authenticate to cloud provider considering existing of secret files.
- Mostly in the Airflow Operator code provided by official provider, there're always parameters that you can indicate where your credentials can be read from regarding to where the code is running on. So, you can pack your credentials together with the code to make your code be able to interact with cloud.
- Even though, there's a risk that anyone in the team can accidentially expose this secret file to public by pushing code to public GitHub repo or pushing image containing credentials footprint to public registry.
- So, there're gradually emerging of new ways to authenticate with cloud such as using ***Application Default Credentials (ADC)*** or **Workload Identity Federation (WIF)** provided by GCP dedicatedly intended to remove existing of secret files existence.
- We will use ADC for GCP, and long-lived credentials for AWS in this project for the code. But we will use WIF for CI/CD pipeline.

*ADC* will be elaborated on this Topic, but *WIF* will be elaborated on Topic 6: *ML Code Productionization and Deployment*

![authen-overview](./docs/authen-overview.png)

### 2.2.2 GCP ADC for Terraform

According to [2.1.2 Account Impersonation](#212-account-impersonation) and [2.2.1 Long-lived credentials method vs others](#221-long-lived-credentials-method-vs-others), we will use ADC with impersonation to manage cloud services through Terraform.
1. We manually configured IAM permissions.
2. We retrieved the SA email in the previous topics.
3. We executed `gcloud auth` with SA email and stored in in `ADC` file in
    - MacOS/Linux: `/.config/gcloud/application_defailt_credentials.json`
    - Windows: `${APPDATA}/gcloud/application_defailt_credentials.json`
4. After execution, GCP will send a refreshed token to relevant files in the same directory of ADC.
5. In the Terraform code, there's a part to be specified to use SA in `code/models/terraform.tfvars` *(hidden from repo)* to be used to authenticate with cloud platform.
6. Now, We can interact freely with GCP resources.

```bash
# workdir: code/models/terraform

terraform init
terraform plan
terraform apply
terraform destory

# workdir: code/models/terraform/auth
terraform init
terraform plan
terraform apply
# terraform destory
```

### 2.2.3 GCP ADC for Pipeline Code

After we did the steps above, we will have ADC file stored in local machine as `application_default_credentials.json` at `$GOOGLE_APPLICATION_CREDENTIALS`.
1. We will specify in `docker-compose.yml` in `airflow-common` section to mount this file into the containers and set `$GOOGLE_APPLICATION_CREDENTIALS` to that mount target path.
2. However, if we have docker-in-docker use cases such as orchestrating ML service task, we have to mount ADC to ML service container that's running in Airflow scheduler container to make ML Service also being able to communicate with cloud platform.
3. You can do by mounting Local ADC from host directly to ML Service container through DockerOperator `host/path/to/gcloud:/root/.config/gcloud`. Please, see mounting example in [src/dags/data_product/ml_dynamic_segment.py](./src/dags/data_product/ml_dynamic_segment.py)
4. Now, the container are available to interact with cloud resources after `airflow-entrypoint.sh` is executed. And by specifying `google_cloud_default` airflow connection, the ETL code will also be able to interact with GCP.

References:
- [Use Google Cloud user credentials when testing containers locally - Medium](https://medium.com/google-cloud/use-google-cloud-user-credentials-when-testing-containers-locally-acb57cd4e4da)
- [Authentication on GCP with Docker: Application Default Credentials - Medium](https://medium.com/datamindedbe/application-default-credentials-477879e31cb5)

### 2.3 Terraform

Instead of manual configuration through Web UI on cloud platform, We can manage cloud *infrastructure as code (IaC)* by **"Terraform"**, which is a better way to manage cloud resources by reducing error-prone from manual configuring. Terraform make it easier to create, track stage, and delete the resources.

The [code/models/terraform/main.tf](./code/models/terraform/main.tf) file, using some variables from [code/models/terraform/variables.tf](./code/models/terraform/variables.tf) file, will produce the following resources for ETL pipeline and ML Service:
- 3 data lake buckets *(landing/staging/curated)*
- 2 Bigquery datasets *(silver/gold)*
- 1 Artifact Registry Repository *(dev)*

The [code/models/terraform/auth/main.tf](./code/models/terraform/auth/main.tf) file, using some variables from [code/models/terraform/auth/variables.tf](./code/models/terraform/auth/variables.tf) file, will produce the following resources for CI/CD keyless authentication:
- 1 workload identity pool
- 1 workload identity pool provider

```bash
terraform init

terraform plan

terraform apply

terraform destroy
```

- `terraform init` initialize Terraform (where `main.tf` located) in your local machine (don't forget to `cd` into each terraform directory first).
- `terraform plan` to see what resources will be created, authentication validation, and syntax checking.
- `terraform apply` to create the resources.
- `terraform destroy` to delete the resources.

Eventually, you will see the results in your GCP services together with newly created files in local filesystem such as `*.tfstate*`, and `.terraform*`. Please also add these files to `.gitignore` to avoid credential exposing.

### 2.4 Extending Terraform to AWS

***Note: This section and `terraform/main.tf` were not revised properly considered as deprecated code, please, use with caution.***

Fow AWS, we will use **S3** as a data lake, and AWS **Redshift** as a data warehouse. Like GCP, we need to create IAM user (which equal to service account in GCP) and get the credentials file as `.csv` extension, then we use it to create S3 bucket and Redshift by **terraform**.

We will create an IAM user, and get the credentials file manually via Web IU. Access for AWS is quite more complex than GCP, composed of IAM user, IAM Role, and Policy which will not be described in detail in this project.

#### **IAM User and Policies**

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

In this part is quite complex due to **"static credentials"** behavior, since we don't need to hard-coded or type in the key derectly to the terraform file. So in general, we will use **"terraform.tfvars"** to pass the hard-coded credentials to terraform file and add `terraform.tfvars` to `.gitignore`.

The concept is simple: we create resources in `main.tf` where some part of it use `var.` to refer to the variable specified in `variables.tf` file. In `variables.tf` file, we specify the variable name, type, and default value, if the default value is not specified, we have to pass the value interactively after `terraform apply` as inputs **OR** pass it automatically by creating `terraform.tfvars` file and type in the variable name and value. This is where we will copy credentials from csv to put it in **(and again please don't forget to add both files to `.gitignore`)**.

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

Then, you good to go with `terraform apply`.

In Addition, configuring Redshift Serverless is quite complex, so I will not go into detail, but you can check the code in [main.tf](./terraform/main.tf). Basically we need to create the following:
- Data "aws_availability_zones" to get the availability zone.
- VPC
- Redshift subnet, and also config your available IP address in `terraform.tfvars`.
- IAM role for Redshift Serverless
- Grant some access and attach some policy for the role
- Workgroup
- Namespace

*Note: Redshift Serverless let us create a data warehouse without managing the cluster ourselves, it can **scale down to zero** or **pay as you use**, but it's still in preview.*

## 3. DAGs and ETL Pipelines

In this project, I used a dataset from kaggle which was:
- loaded to postgres database
- uploaded to this repo github as csv format
- and I wrote DAGs to use Kaggle API to obtain the dataset directly from the Kaggle website.

![etl-overview.png](./docs/etl-overview.png)

In this project, I wrote main script: [`ecomm_invoice_etl_dag.py`](src/dags/ecomm_invoice_etl_dag.py) to create 1 DAG of **(8+1) tasks**, which are:
1. Reading data from raw url from github that I uploaded myself. Then, upload it to GCP bucket as uncleaned data.
2. Fetching (Unloading) data from the postgres database that we simulate in docker containers as a data source. Then, upload it to GCP bucket as cleaned data.
3. Downloading from the Kaggle website using Kaggle API. Then, upload it to GCP bucket (GCS) as uncleaned data. 
4. Data Transformation: reformat to parquet file, and cleaning data to be ready for data analyst and data scientist to use, then load to staging area.
5. Loading data located in staging area to data warehouse (Bigquery) as cleaned data with native table approach.
6. Loading to data warehouse (Bigquery) as cleaned data with external table approach from staging area.
7. Loading to another Postgres database as cleaned data.
8. Clearing data in staging area (GCP bucket). **(this will not be used for external table approach that requires the source file to be exists)**

Additionally, I also wrote [**transform_load.py**](./src/dags/transform_load.py) and [**alternative_cloud_etl.py**](./src/dags/alternative_cloud_etl.py) as *utils* to enable different cloud services.

I intentionally separate the code for AWS and GCP, so we can easily find between them. The code for AWS is in [alternative_cloud_etl.py](./src/dags/alternative_cloud_etl.py) file. The code is quite similar to GCP, but there are some differences, such as:
- using `boto3` instead of `google.cloud.storage` to connect to S3.
- how we use credentials file to connect to the cloud.
- using `psycopg2` instead of `bigquery` to load to Redshift.
- uploading clean data to S3 bucket using `awswrangler`, which implemented on `pandas` library, instead of using `gcsfs`.
- how to fetch the data from postgres database to gcp and aws, which is different in the code, but the concept is the same.

**Note: Since we use the same file that is downloaded or fetched, so the deleting file from local airflow docker container will be deprecated avoiding conflict between clouds.** *Unless, it will cause a bug by `shutil` in the first run, because the file is not exists in the local container.*

*Note: Loading to Redshift part will be described more in the future*

**Note:** We have to change the parameters `conn_id` in [`ecomm_invoice_etl_dag.py`](src/dags/ecomm_invoice_etl_dag.py) to match airflow connections that we specified.

![airflow-dag-graph.jpg](./docs/airflow-dag-graph.jpg)

#### Development Note

- Referring to official documentation of how your code should be written is a good way to develop and allow other team members to maintain easily. It's always a good practice to stick with the best practice and avoid *working around*.
- When I first load the parquet file to pre-defined schema in Bigquery, I encountered the error that the schema is not matched. I found out that the schema of parquet file is not the same as the schema pre-defined in Bigquery by extra column: "index level 0". So, the solution is to drop the column before saving to parquet file in Google Cloud Storage by using `df.to_parquet(..., index=False)`
    - *(Even you download the `index=True` parquet file to check in pandas, it will not show the extra index column)*
- To hard refresh airflow session, we can remove local volumes by:
    ```bash
    docker compose down --volumes --rmi all

    docker system prune --all --volumes
    ```
    - Please, also remove local airflow backendDB `postgres-db-volume`, and `logs` files in `src` folder.

If you use different dataset, you might have to write your own DAGs what match your specific use cases.

#### Bigquery: Native tables vs External tables

<img src="./docs/native-vs-external.png" width="75%">

In the DAG script, we have 2 tasks to load data to Bigquery, one is using Native table, and another is using External table. the differences between them are their performance, how it works, and user interface for that table.

External tables are suitable when you want to query data without loading it into BigQuery, optimizing storage costs and leveraging existing data sources. Native tables, on the other hand, offer enhanced performance and advanced features within BigQuery, like partitioning and clustering and more user-friendly.

### 3.1 Airflow DAGs and Data warehouse in Production

- **Airflow**
    - In the best practice, we wrap ETL processes into Airflow Opeators separately, can be either importing from official sites or sometimes customizing it to meet team's agreement or according to team's data governance.
    - Separating processes by multiple operators and written as the *OOP* pattern can represent DAGs, airflow jobs, or workflows in comprehensive way and make a task to be atomic and isolated from each other.
    - Most of time, official providers have their own developed operators associating with their products such as `GCSOperator`, `BigQueryOperator`, etc. that prevent us from error-proning developing it ourselves.
- **Data Warehouse**
    - This aspect can be vary across companies depended on data architecture design and data stack. However, there's quite a new term for modern data engineering called ***ELT*** pattern, which introduces a huge different from ETL. Basically, ELT allow us to adopt *[medallion architecture](https://www.databricks.com/glossary/medallion-architecture)*, separating processes into multiple data stages, keeping raw extracted data for later data reconciliation, and enabling fault-tolerant architecture by convenient backfilling due to schema changes and unexpected failure.
    ![elt_example](./docs/elt_example.png)
    - External table makes ELT process easier to be implemented reducing the gap between data lake and data warehouse, but still can be benefited from partitioning by ***Hive-style Partitioning*** 
    - Actually, Parquet files embed data schema into the files. We can use some operators that support referring parquet internal schema to create external tables in data warehouse. So, we don't have to pre-define schema for every tables, except within an stardardized data processing framework.
    - Transformer from *Bronze* layer to *Silver* layer can be a custom framework that stardardizes data format and follows company's data governance. There's multiple relevant libraries can be mentioned here, such as ***pyspark***, ***dlt***, and etc. depended on data sources, acceptable cost, and team expertise in tools.
    - There're also multiple tools that support transforming data from *Silver* layer to *Gold* layer such as ***dbt***, ***SQL Mesh***, and etc. or even native airflow operator. Such tools give ability to visualize transformation logic and enable data lineage, native integrated data dict for a better maintenance.
        - Many companies discourage the use of these tools giving the reason that it's not appropriate for big scale projects and too messy to maintain. However, I disagree this because there're practices and principles that can solve this problem, like ***Data Mesh*** *(twisted from Domain-driven design: DDD)* and ***Modularity***. 
        - Anyway, it requires highly skilled data archietct and data engineer together to develop and maintain these tools along with the principles. I thought they were not being able to focus on this low-level foundation enough so they said it's a mess, because it can be very far from final products that seems to be more profitable to the company.

### 3.2 Data Modeling and Optimization in Production

This topic is quite important, since it can be highly involved in cost optimization.
- Although *ingestion* process is completed, we might need to transformed it, besides of raw cleaned and normalized data, with custom business logic, and tables aggregation to further analyze and exploit more from data.
- This process usually referred to *ELT* or *transformation* that is not the same *transformation* in ingestion process, which is just controling data quality by casting type, formatting date/timestamp or mapping data into specified schema by a framework.
- Transformed data models can be located in *curate* or *gold* layer in *medallion architecture*, but sometimes can be in *semantic layer* which is referred to another transformation sub-layer before being shown on dashboard. However, this also depends on companies' data archietcture and agreement between teams.
![semantic-concept](./docs/semantic-concept.jpg)
- Transformation process can be done with SQL by analytic engineer, data analyst, domain expert, etc. or data engineer if necessary to make data become more meaningful to business aspect. However, without proper expertise in SQL and data modeling, cost from dashboard usage and data preparation can be gone in a very wrong way.
- Tools that support transformation at aggregation level is emerging, since ELT pattern is still counted as modern at the moment for batch data engineering pipeline. Some of famous tools you might have heard could be *dbt*, and *SQLMesh* which enable ability to perform complex incremental data loading logic, SCD handling, data lineage, data dict, and offer built-in data quality controlling at high level. However, you could still implement traditional data aggregation with native airflow operator introduced by official provider by executing sql directly to data warehouse through Airflow.
- What should be considered in gold layer for technical aspects are:
    - SCD (Slow Changing Dimension) handling
    - snapshot and incremental transformation (which usually involved with how ingestion pipelines work and silver layer partitioning)
    - data transferring behavior or ingestion method (full dump, append, delta, or replace)
    - business logic (if semantic layer does not exist or it required pre-calculation in gold layer)

#### 3.3 How to Optimize Data Models

- Pre-calculated table (native table on gold layer) was proved to be more optimized when data is queried to dashboard compared with using view considered by both cost and time performance, because it don't have to scan component tables for aggregation multiple times to repeatedly create the same business-goal table, and the native table is claimed to be more optimized in time performance compared with view.
- Using view not only prevent downstream users from modifying table in ideal, but also manipulate the users to query from only necessary or specific partitions or columns resulting in lesser data scanning per read or per dashboard refresh which is important in cost optimization to reduce cost from unnecessary scanning when it come to larger scale in production database.
    - Using view can futher lead to *data governance* issues such as user permission to read from both table and view besides from dashboard permission.
- Modeling data warehouse to *star schema* or *snowflake schema* (considered to be fact/dimension pattern) is claimed multiple times to be more cost optimized, but it lacks of quantitative evidence.

## 4. EDA and Data Visualization

Once we have the data in the data warehouse, we can do the EDA and data visualization connecting data from the data warehouse. I will use **PowerBI** to create the dashboard, and Python for some ad-hoc analysis.

### 4.1 EDA

Actually, we might have to do the **EDA** (Exploratory Data Analysis) before loading data to data warehouse to see how we would clean the dataset, but in the analysis part, we can do in different objectives. In this part, I will do an EDA to see the characteristic of the data, and see if there is any insight that can be meaningful to the business.

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

<img src="./docs/boxplot_outlier.png" width="75%">

You can check the code in [**ecomm_eda.ipynb**](ecomm_eda.ipynb) file.

### 4.2 PowerBI Dashboard

Since, the current version of PowerBI has a bug of connecting to data warehouse, I will connect the data from local parquet file that loaded from data warehouse instead.

We can connect parquet file locally to Power BI:
- Go to `Get Data` > Type in `"Parquet"` > `Connect` > use Basic option, and type in URL box by this: `file:///C:/path/to/data.parquet` > Then, click OK

*Note: you have to change the path to your local path*

Now we can see the data in PowerBI, and do the data visualization.

![ecomm_pbi](./docs/ecomm_pbi.jpg)

I won't go into detail of how to create the dashboard or each component, but you can download and check it yourself in [**ecomm_bi.pbix**](ecomm_bi.pbix) file.

What worth to mention are:
- I used **DAX** to calculate Growth Rate as a measure that would be quite complex for some people who are not familiar with PowerBI or DAX, but it used just regular formula as: `(Sales in the Current Context month - Sales in the Previous month) / Sales in the Previous month`. I also used `CALCULATE` function to calculate the previous month value together with `ALLSELECTED`, and `DIVIDE` function to calculate the growth rate.
- I linked variables between reports to be able to **Drill Through** to other reports, making the dashboard more interactive, and more in-depth to analyze.

#### Note
- **Dashboard is about Storytelling**, so it's better to have a story in mind before creating the dashboard. It's not just about the data, but how to arrange the story from the data.
- **It's crucial to know who are the audiences of the dashboard, and what the objective of the dashboard is**. So, we can select the right metrics, right data, and right visualization.
- **Data model is very important**, it's the foundation of the dashboard. If the data model is incorrected, the dashboard will be wrong also. If the data model come in a good shape, the dashboard will be easier to create, and the data will be easier to analyze. (especially in aspect of **Time Intelligence**)

## 5. Machine Learning Model Development

The Model Development part is not fully finished yet, but some part of it are done and ready to be presented.

This is what I have planned so far:
1. [Customer Segmentation By RFM, KMeans, and Tree-based Model](#51-customer-segmentation-by-rfm-kmeans-and-tree-based-model) ***(Done)***
2. [Market Basket Analysis](#52-market-basket-analysis) ***(Done)***
3. [Demand Forecasting](#53-demand-forecasting) ***(Paused)***
4. Recommendation System *(Not started)*
5. Customer Churn Prediction *(Not started)*
6. Price Analysis and Optimization *(Not started)*

You can see the code in [**model_dev.ipynb**](model_dev.ipynb)

### 5.1 Customer Segmentation By RFM, KMeans, and Tree-based Model

**Introduction**

**RFM (Recency, Frequency, Monetary)** is a well known method to segment customers based on their behavior. **But recency, fequency, and monetary alone, are not effective enough to segment the diversity of customers**. We should use other available features or characteristics of customers to segment them more effectively that come to the harder to score with traditional RFM method.

As a result, **KMeans emerged as a crucial machine learning technique for reasonably effectively clustering clients** into more precise customer segments. **But, the Kmeans is not interpretable**, we can't explain the result or criteria to the business of how clusters are formed or how the customers are segmented, but only show what features we use in KMeans model (we don't even know which features are more influence).

**So, we can use Decision Tree to find the criteria of the clusters**, and explain the result to the business, leading to proper campaigns and strategies that suit to customer segments to be launched.

Moreover, we can use **XGBoost to find the most important features that influence the customer segments (or each segment)**, and further concern and develop strategies regarding that features.

#### **RFM and Features Details**

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

#### **KMeans**

Before we put the customer profile data into KMeans, we should scale the data first , in my case I used **Robust Scaler**, to make the data have the same scale, since KMeans algorithm use euclidean distance to calculate the clusters, so it could sensitive to the different scale of the data.

Then, we will use KMeans to segment customers into clusters. We will use **Elbow Method** and **Silhouette Plot** to find the optimal number of clusters which eventually is 10 clusters.

<img src="./docs/kmeans-elbow.png" alt="kmeans-elbow" width="75%">

![kmeans-silhouette](./docs/kmeans-silhouette.png)

Then, we label the clusters to customer profile table which not scaled yet, so we can see the customer profile of each cluster with the original scale to make it more interpretable. We will use this table to find the criteria of the clusters as an input and validation set for the Decision Tree.

References:
- [Selecting the number of clusters with silhouette analysis on KMeans clustering](https://scikit-learn.org/stable/auto_examples/cluster/plot_kmeans_silhouette_analysis.html)
- [Segmenting Customers using K-Means, RFM and Transaction Records](https://towardsdatascience.com/segmenting-customers-using-k-means-and-transaction-records-76f4055d856a)

#### **Decision Tree**

After that, we will use **Decision Tree** to find the criteria of the clusters to make the clustering result from KMeans become interpretable. I used **Random Search** with **Cross Validation** to find the best hyperparameters of the Decision Tree to make it the most accurate **resulting in accuracy 91.41% on test set, 94.11% on train set, macro averaged of F1 score 0.75, to clarify criteria of each cluster.**

![kmeans-tree](./docs/kmeans-tree.png)

***Note: The 91% accuracy doesn't mean we segmented the customers correctly by 91%**, because we can't be sure that Kmeans correctly 100% segmented the customers (and definitely, it didn't), but we can tell that the Decision Tree can explain the criteria of the clusters correctly by 91% in generalization, So we can take account of the criteria from the Decision Tree to cluster customer segmented to follow Kmeans result.*

#### **XGBoost**

Finally, we will use **XGBoost** to find the most important features that influence the customer segments or important factors to develop the strategies. I also used **Random Search** with **Cross Validation** to find the best hyperparameters of XGBoost resulting in accuracy 97.76% on test set, 100% on train set, macro averaged of F1 score 0.77, to find the most important features (seems overfitting, but it's not a main focused).

You may heard of feature importance from XGBoost that can describe which features are more influence to the model, but it's not effective enough due to its limitation, like how it treats the correlated features, how differently treats the categorical features and numerical features, and how it's not generalized to other models. So, I used **Permutation Feature Importance** instead, which is more interpretable and generalized for choosing the most influence features to each cluster. Moreover, it can be used with any model, not just XGBoost.

References:
- [Permutation feature importance - sklearn](https://scikit-learn.org/stable/modules/permutation_importance.html#permutation-importance)
- [Permutation Importance vs Random Forest Feature Importance (MDI)](https://scikit-learn.org/stable/auto_examples/inspection/plot_permutation_importance.html#sphx-glr-auto-examples-inspection-plot-permutation-importance-py)

The most important features for each cluster can be shown in the plot below:

<img src="./docs/permutation_feature_importance.png" alt="permutation_feature_importance" width="75%">

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

In general, we use current features or features that already happened fed into the model to predict the sales as a target. But, the problem is, if we want to predict the sales of the next month, we don't have the input of the next month yet. So, we have to use the records of the past to predict the sales of the next month.

Therefore, we have to perform feature engineering, transform data, create features to obtain the appropriate and effective features to predict the future sales. So, we will use **Lag Features** and **Rolling Window Statistics**.

But how can we know how much lag of the features and how many rolling window statistics should we use? first we can use **Auto-correlation** to find the optimal lag value or candidates to be used as lag features. Then, we can use **Cross-Validation** to find the optimal number of rolling window and the best candidate of lag features.

<img src="./docs/autocorrelation.png">

Additionally, I used **Fast Fourier Transform** to find seasonality of the data, which is the pattern that repeats itself at regular intervals of time. The seasonality can be used to create features to predict the future sales.

<img src="./docs/fourier.png" width="75%">

Personally, I thought using fast fourier transform to find seasonality is quite more quantitative than using autocorrelation. But, we can use both of them to find the seasonality of the data to ensure the result.

![predict-demand](./docs/predict-demand.png)

I think the most challenging part of timeseries forecasting is to find the appropriate features to predict the future sales. The features that we use to predict the future sales should be the features that already happened in the past, and we can't use the features that will happen in the future. So, checking how much lagged values of the features can be significant to predict the future sales.

Model | RMSE | MAPE
:---: | :---: | :---:
Baseline (Mean) | 3170.143 | 27.16%
LightGBM | 4884.230 | 32.29%

*(Current Result)*

Even we can see that the prediction result can a bit capture the trend of the data, but the result is not good enough compared with **mean** of the target.

I intended to **decompose** the data into trend, seasonality, and residual, then use them as features to predict the future sales to make it stationary **and also add moving average types** such as EMA (Exponential Moving Average) and LWMA (Linear Weighted Moving Average) to the model, to weight the recent data more than the old data. Moreover, I want to test traditional statistical model such as ARIMA, and SARIMA. But, I think it's enough for now, I will update the model later.

References:
- [How to Calculate Autocorrelation in Python?](https://www.geeksforgeeks.org/how-to-calculate-autocorrelation-in-python/)
- [How to detect seasonality, forecast and fill gaps in time series using Fast Fourier Transform](https://fischerbach.medium.com/introduction-to-fourier-analysis-of-time-series-42151703524a)
- [How To Apply Machine Learning To Demand Forecasting (Concept)](https://mobidev.biz/blog/machine-learning-methods-demand-forecasting-retail)
- [All Moving Averages (SMA, EMA, SMMA, and LWMA)](https://srading.com/all-moving-averages-sma-ema-smma-and-lwma/)
- [Finding Seasonal Trends in Time-Series Data with Python](https://towardsdatascience.com/finding-seasonal-trends-in-time-series-data-with-python-ce10c37aa861)
- [Various Techniques to Detect and Isolate Time Series Components Using Python (Technical)](https://www.analyticsvidhya.com/blog/2023/02/various-techniques-to-detect-and-isolate-time-series-components-using-python/)

## 6. ML Code Productionization and Deployment

After we developed and evaluated the model, we can deploy the model to production to leverage the business, bringing the model out from the Python notebook or your lab, and not only making it available to the data scientist.

We can deploy the model to production in many approaches, please refer to this repo: [MLOps - ML System](https://github.com/patcha-ranat/MLOps-ml-system) for further details of principles and research on *MLOps* methodology. However, most approaches required *docker container* for consistent runtime even in different environments, so I will elaborate more on containerization for this topic.

![deploy-overview](./docs/deploy-overview.png)

***Due to latest revise (Nov 2024), I decided to re-write this from [the previous version of this chapter](https://github.com/patcha-ranat/Ecommerce-Invoice-End-to-end/blob/7f57532552d5c948054753bbc0877d370cafd200/README.md#6-model-deployment-and-monitoring) into a new one, emphasizing less on typical concepts and focusing more on specific use case for this project. However, considering how huge scale of this project was, I also intended to separate this chapter into [MLOps - ML System](https://github.com/patcha-ranat/MLOps-ml-system?tab=readme-ov-file#mlops---ml-system) project to clarify more on my most interested topic in detail.***

*Anyway, documentation for this topic will mostly focus more on overview concepts of packaging ML code to be ML pipeline with airflow dag integration and together with deployment on cloud through CI/CD Workflow. Interface service of inferencing such as an API with backend database and Model Monitoring, will be further implemented on, again [MLOps - ML System](https://github.com/patcha-ranat/MLOps-ml-system).*

Although this project introduced multiple ML use cases, I choose [Customer Segmentation By RFM, KMeans, and Tree-based Model](#51-customer-segmentation-by-rfm-kmeans-and-tree-based-model) to be deployed with *precompute serving pattern*, so there's no model wrapped into a container or prediction API service, just ML Pipeline within docker container deployed with CI/CD Pipeline.

*MLOps Code Structure and Logic*
![kde-ecomm-mlops-code-structure](./docs/kde-ecomm-mlops-code-structure.png)

*MLOps Code Workflow*
![kde-ecomm-mlops-code-workflow](./docs/kde-ecomm-mlops-code-workflow.png)

### 6.1 Exporting from Notebook

First, we will focus on extracting the model development processes from notebook and transforming it into python module with *factory* pattern using `argparse` library and *OOP* concept. Some processes were improved to be more appropriate to be operated automatically through pipeline. Such as:
- How data read/written into ML code
    - Reader and Writer classes are implemented to support feeding input and retrieving output from ml service both local and cloud environments.
- Selecting K Value
    - Since now we can't consider optimal k value qualitatively by human through automated pipeline, clarifying logic/rules to find optimal K now become important.
    - Here is the behind logic represented in math:
    ![dynamic_k_equation](./docs/dynamic_k_equation.png)
    ![automated-kmeans.png](./docs/automated-kmeans.png)
- Model Development Changes
    - Changing feature engineering process imputing recency feature from `999` to the real recency
    - Replacing interpretation method from decision tree or xgboost feature importance with LightGBM and permutation feature importance for more interpretable and quantitative measurement.
    - *(please, check [ML-Sandbox/dynamic_segmentation/revise_ecomm_logic.ipynb](https://github.com/patcha-ranat/ML-Sandbox/blob/main/dynamic_segmentation/revise_ecomm_logic.ipynb) for final ML Logic)*
- Model Retaining Triggering is now implemented
- Final output will be 2 data models, 2 ML models and artifacts (metadata + control file) and scaler.
    - please check folder `code/models/output/` for output example
    - [df_cluster_importance.parquet](./code/models/output/data/2024-11-08/df_cluster_importance.parquet)
    - [df_cluster_importance.parquet](./code/models/output/data/2024-11-08/df_cluster_importance.parquet)

#### Related Files

- [code/models/main.py](./code/models/main.py) : entrypoint of the application using argparse
- [code/models/abstract.py](./code/models/abstract.py) : abstract classes defining overview of implemented classes
- [code/models/io_services.py](./code/models/io_services.py) : Input and Output related services (reader/wrtier)
    - `InputLocalReader`: Reader for input *(data and interpreter if exists)* on local
    - `InputGCPReader`: Reader for input *(data and interpreter if exists)* GCP cloud
    - `InputProcessor`: Reader type selector (factory) based on argparse argument
    - `OutputLocalWriter`: Writer for output on local
    - `OutputGCPWriter`: Writer for output on GCP Cloud
    - `OutputProcessor`: Writer type selector (factory) based on argparse argument
- [code/models/ml_services.py](./code/models/ml_services.py) : ML Code Logic as a pipeline including,
    - `CustomerProfilingService`: RFM - pre-processing, feature engineering to formulate RFM DataFrame
    - `CustomerSegmentationService`: Clustering service, related to using KMeans model, and cross-validation automated finding optimal K value.
    - `ClusterInterpretationService`: Interpretation service related to training/retraining LightGBM and utilizing permutation feature importance to find influenced features for each cluster
    - `MLProcessor`: Orchestrator of overall ML services
- [code/models/requirements.txt](./code/models/requirements.txt) : Python dependencies used in the app
- [code/models/Dockerfile](./code/models/Dockerfile) : Packaging the app and dependencies to a docker container, installing additional components as app required (e.g. lightgbm prerequisites)

**Please refer to [code/models/README.md](./code/models/README.md) for example of command usage.**

### 6.2 Docker Containerization

After python module works properly on local machine, we will package the code into docker container for the next step by [code/models/Dockerfile](./code/models/Dockerfile) file.

*Note: What worth to mention is that the path specifying in the Dockerfile is relative to the current working directory where we execute the `docker build` command. So, we have to be aware of the path we specify in the Dockerfile and how we execute the file via Command Line Interface (CLI).*

With [a few research](https://stackoverflow.com/questions/24537340/docker-adding-a-file-from-a-parent-directory), I found that it's not possible to specify the path to copy the files from the parent directory.

And when we execute the command, we have to be in the ***./code/models/*** realative to root of the project and use this command pattern:

```bash
# workdir: ./code/models/

docker build -t <image-name>:<tag> -f <path-to-dockerfile> <path-to-working-directory>
# -t: tag the image with the name
# -f: (Optional) specify the path to Dockerfile that we want to execute. Required when multiple Dockerfiles exist in the same working directory

# which can lead to this command
docker build -t ecomm/interpretable-dynamic-rfm-service:v2 .
```

1. Before packaging code into docker container, we have to make sure it works as expected within local environment.
    ```bash
    # test module local
    python main.py --env local --method filesystem --input_path '../../data/ecomm_invoice_transaction.parquet' --output_path output --exec_date 2024-11-03

    # test module local force_train
    python main.py --env local --method filesystem --input_path '../../data/ecomm_invoice_transaction.parquet' --output_path output --exec_date 2024-10-29 --force_train

    # test module against gcp
    # TODO: Replace with your bucket name
    python main.py --env gcp --project_id <project_id> --method filesystem --input_path 'gs://<landing_bucket_name>/input/data/2024-11-03/ecomm_invoice_transaction.parquet' --output_path 'gs://<staging_bucket_name>/output' --exec_date 2024-11-03
    ```
    *Note: Please refer to [initial_gcp.sh](./code/models/test/initial_gcp.sh) before testing against cloud environment*
2. After packaging code into docker container, we have to make sure that its functionality both local and cloud method work perfectly within the container
    ```bash
    # test module local via docker
    docker run \
        ecomm/interpretable-dynamic-rfm-service:v1 \
        --env local \
        --method filesystem \
        --input_path 'ecomm_invoice_transaction.parquet' \
        --output_path output \
        --exec_date 2024-10-29

    # test module against gcp via docker
    # TODO: Replace with your bucket name
    docker run \
        -v $HOME/.config/gcloud:/root/.config/gcloud \ # mount GCP default credential
        ecomm/interpretable-dynamic-rfm-service:v2 \
        --env gcp \
        --method filesystem \
        --input_path 'gs://<landing_bucket_name>/input/data/2024-11-03/ecomm_invoice_transaction.parquet' \
        --output_path 'gs://<staging_bucket_name>/output' \
        --exec_date 2024-11-03
    ```

If all steps work perfectly, we can now go to the next step: deploying to cloud

### 6.3 Deploying the Model to the Cloud Environment

After we have everything ready, we will deploy containerized ML Pipeline to cloud environment utilizing Google Artifact Registry (GAR), similar to *Docker Hub* but more secured within GCP.

So, what we need to do is:
1. **Prepare cloud environment and authentication**
2. **Build the Docker image locally and push it to the Artifact Registry**

#### 1. Prepare cloud environment and authentication

Since we have already set SA permission in Topic *2.1.1 Setting up IAM and Service Account*, so there will be just authentication confirmation to be explained

- *ADC* is a method to authenticate to GCP interactively through CLI via *gcloud*. It's a proper authentication method when there's human involved between the processes, but impossible with automated pipeline.
- ADC method let us login to GCP with email address through web browser interactively, and then it store a credential to gcloud default path to look for automatically when we try to authenticate with GCP with gcloud

Please, run these commands to authenticate with impersonated account
```bash
# For first time using gcloud
gcloud auth login

# Logic through impersonated account through ADC method
gcloud auth application-default login --impersonate-service-account <SERVICE_ACCT_EMAIL>

# Set project
gcloud config set project <project_id>
```

Then we execute these terraform configs
- [/code/models/terraform/main.tf](./code/models/terraform/main.tf) together with [/code/models/terraform/variables.tf](./code/models/terraform/variables.tf)
- [/code/models/terraform/auth/main.tf](./code/models/terraform/auth/main.tf) together with [/code/models/terraform/auth/variables.tf](./code/models/terraform/auth/variables.tf)
by
```bash
# workdir: code/models/terraform
terraform init
terraform plan
terraform apply
terraform destroy

# workdir: code/models/terraform/auth
terraform init
terraform plan
terraform apply
# terraform destroy
```
*Note: WIF shouldn't be created/deleted repeatedly, refer to [Stack Overflow - Terraform on GCP - Error 409: Requested entity already exists](https://stackoverflow.com/a/73944443)*
```bash
Error: Error creating WorkloadIdentityPool: googleapi: Error 409: Requested entity already exists
```

If below error occurred, please check if you have the '**Service Account Token Creator**' role in both owner account and service account. if yes, please try to reset `$GOOGLE_APPLICATION_DEFAULT` environment variable by `unset $GOOGLE_APPLICATION_DEFAULT`, restarting terminal and try to authenticate with `gcloud` with the above process again.
```bash
{
  "error": {
    "code": 403,
    "message": "Permission 'iam.serviceAccounts.getAccessToken' denied on resource (or it may not exist).",
    "status": "PERMISSION_DENIED",
    "details": [
        {
            "@type": "type.googleapis.com/google.rpc.ErrorInfo",
            "reason": "IAM_PERMISSION_DENIED",
            "domain": "iam.googleapis.com",
            "metadata": {
            "permission": "iam.serviceAccounts.getAccessToken"
        }
    ]
}
```

References:
- Terraform Code
    - [terraform-google-github-actions-runners - main.tf - Github](https://github.com/terraform-google-modules/terraform-google-github-actions-runners/blob/master/modules/gh-oidc/main.tf)
    - [terraform-google-github-actions-runners - variables.tf - Github](https://github.com/terraform-google-modules/terraform-google-github-actions-runners/blob/master/modules/gh-oidc/variables.tf)
    - [Example Usage - Iam Workload Identity Pool Provider Github Actions - Terraform](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/iam_workload_identity_pool_provider#example-usage---iam-workload-identity-pool-provider-github-actions)
- Debugging Authentication
    - [Terraform on GCP - Impersonation not working (missing permission in generating access token)](https://stackoverflow.com/a/76110825)
    - [Authentication for Terraform - Google Cloud Platform](https://cloud.google.com/docs/terraform/authentication)

#### 2. Build the Docker image locally and push it to the Artifact Registry

To puslish created docker image to GAR, please run the following commands:
```bash
# workdir: code/models/
# gcloud auth application-default login --impersonate-service-account <SERVICE_ACCT_EMAIL>

# gcloud configure docker
gcloud auth configure-docker "<$REGION>-docker.pkg.dev"

# Build image locally
docker build -t <any-image-name> .

# Change image name locally to acceptable format for GAR 
docker tag <previous-step-image-name> <$REGION>-docker.pkg.dev/<$PROJECT_ID>/<$REPOSITORY>/<$IMAGE_NAME>:<$TAG>

# Push image to GAR
docker push <$REGION>-docker.pkg.dev/<$PROJECT_ID>/<$REPOSITORY>/<$IMAGE_NAME>:<$TAG>
```
Check the result, pushed image, at artifact registry gcp service.

*Note: You can export the environment variables to avoid typing the same variables over and over again, reducing the chance of making a mistake.*

Reference:
- [Push and pull images - Google Cloud Platform](https://cloud.google.com/artifact-registry/docs/docker/pushing-and-pulling)

### 6.4 Unit Test, Pre-commit, and Linting

#### 6.4.1 Pre-commit, and Linting

Although Linting and Code Formatting is usually considered as supplement, adding these components make your code have more quality and is written in the best practice and convention. With pre-commit, you can prevent your development team to push low quality code to GitHub by regulating Linter which can be apply manually or through pre-commit hooks.

In this project I use Ruff for only python linter (no formatter) together with pre-commit ruff hook.

Related Files:
- [ruff.toml](./ruff.toml) : *To configure python linting options and styles.*
- [.pre-commit-config.yaml](./.pre-commit-config.yaml) : *To enable pre-commit and set configuration*

```bash
# pip install code/models/requirements.txt

pip install ruff==0.7.3 pre-commit==4.0.1
```
```bash
# ruff
ruff check

# ruff with glob pattern
ruff check code/models/*.py

# ruff format
ruff format
```
```bash
# pre-commit
pre-commit install

# try git commit, if your code is failed to commit, you will be unable to commit your code until it's fixed.

pre-commit uninstall
```

#### **Note:**

- Pre-commit will only apply on the files that are committed or staged changes, and if you specified additional conditions, like only specific path to be checked with through pre-commit, it will comply with it also.
- Please note that pre-commit in this project is configured to only check linting in `code/models/` path.
- Pre-commit also supported other languages with various types of hook.
- Ruff is currently the fastest linter for python, since it's written in rust. However, there's also other python linters and formatter in market such as pylint, flake8, black that are different by coding-style and how to configure it.

References:
- [Pre-commit Official Documentation](https://pre-commit.com/index.html)
- Python Linter: [The Ruff Linter - Ruff Official](https://docs.astral.sh/ruff/linter/)
- Ruff Configuration: [Configuring Ruff - Ruff Official](https://docs.astral.sh/ruff/configuration/)

#### 6.4.2 Unit Test

What more important is unit testing. Personally I don't really favor this topic, but it's the best practice to specify test cases and test it with your code (function, and class) to ensure that every changes in your code during development are still and will be working as expected. In this project I use pylint that's quite basic and popular among the communities for python.

Related File:
- [test_ml_services.py](./tests/unit/test_ml_services.py) : to demonstate applying unit testing for *"finding optimal K"* function in ML Service.

```bash
# make test

PYTHONPATH="code/models" pytest tests/unit/test_ml_services.py --disable-pytest-warnings
```

Note:
- Without specifying `PYTHONPATH`, we have to specify relative path to where the testing script is for `pytest` argument which could lead to more overhead of specifying path within your project.
- Use flag `--disable-pytest-warnings` to avoid minor warnings.

References:
- [Using of `PYTHONPATH` - Stack Overflow](https://stackoverflow.com/a/26544761)
- Multiple test cases with pytest - parametrize: [How to parametrize fixtures and test functions](https://docs.pytest.org/en/stable/how-to/parametrize.html)
- OOP pytest: [Object oriented testing in Python - geeksforgeeks](https://www.geeksforgeeks.org/object-oriented-testing-in-python/)
- Suppressing pytest warnings: [Stack Overflow](https://stackoverflow.com/questions/40710094/how-to-suppress-py-test-internal-deprecation-warnings)

### 6.5 CI/CD Workflow - Automated Deployment Process

When we make changes on the app service, we certainly need to re-deploy it to the cloud environment with a newer version for application improvement. But, we don't want to do it manually every time, especially on production environment, which can cause error-prone and time-consuming processes. So, this come to the important of automating the deployment process or CI/CD Workflow. CI/CD Pipeline make sure your developed application is well-maintained, qualified with tests, and deploy securely in predictable way without error-prone issue.

![cicd-workflow-trigger-example](./docs/cicd-workflow-trigger-example.png)

*Github Actions (GHA)* is one of many CI/CD tools that we can use to automate the deployment process. It can automate tasks when we manually trigger it or with specific events we specified as its condition within the workflow. For example, we can automate the deployment process when we push on specific branch or create pull request with a certain branch to the repository.

![cicd-workflow-example](./docs/cicd-workflow-example.png)

GHA workflow and code logic is not different from manual deployment process, including installing gcloud, authentication, building docker image from source code in GitHub, and pushing to specified image registry, except we can do this all processes with a few click. However, GHA workflow has its own syntax and some specific feature such as using pre-defined action, and accessing variables and GitHub default variables which make it a bit more complicated than regular bash command such as:

```yaml
# Using Environment Variable
echo $ENV_VAR

# This equal to above, but more useful in f-string scenario
echo ${ENV_VARIABLE}

# This call GitHub default variables
echo ${{ github.ref_name }}

echo ${{ inputs.xxx }}

echo ${{ env.xxx }}

# secret variable will be masked with '****' in GHA logs 
echo ${{ secrets.xxx }}

echo ${{ steps.STEPS_ID.outputs.* }}

# Example of Other GHA Syntax
...
runs-on: ubuntu-latest
    steps:
        - name: step name 1
          id: step_id_1
          uses: repo/action@v1
          with:
            xxx: xxx
        
        - name: step name 2
          id: step_id_2
          run: |
            echo xxx
...
```

Related Files:
- [.github/workflows/build-deploy-model.yml](./.github/workflows/build-deploy-model.yml) : CD Workflow
- [.github/workflows/pull-request-ci.yml](./.github/workflows/pull-request-ci.yml) : CI Workflow

Reference:
- CI Workflow - Pull Request Event Trigger: [Events that trigger workflows - GitHub Official](https://docs.github.com/en/actions/writing-workflows/choosing-when-your-workflow-runs/events-that-trigger-workflows#pull_request)

#### 6.5.1 Github Actions - Secrets Variable and Authentication

The most important aspect for *Continuous Deployment (CD)* Workflow is authentication, because we need to make some changes to cloud resources. Traditionally, using long-lived credentials just like password through ***GHA secrets variables*** is the most typical and straightforward solution.
- You have to get a credential file and copy its contents
- Go to **Repo** > **Settings** > **Secrets and variables** > **Actions**
- Create **New repository secret**, suppose secret name: `GCP_PASSWORD` and paste credential content as *Secret*
- Now, you can use the secret for authentication
    ```yaml
    - uses: 'google-github-actions/auth@v2'
      with:
        credentials_json: '${{ secrets.GCP_PASSWORD }}'
    ```

However, this long-lived credential is considered as security risk. So, there's a new service from GCP called *'Workload Identity Federation' (WIF)* which introduce authentication without credentials i.e. keyless authentication.

WIF is quite complex in architecture perspective, but for usage, we just need service account email (without credential), WIF Pools, WIF Provider, and some additional step for granting service account access.

- WIF in GHA
    ```yaml
    steps:
        - name:  Authenticate GCP - Workload Identity Federation
          uses: 'google-github-actions/auth@v2'
          with:
            project_id: ${{ secrets.PROJECT_ID }}
            workload_identity_provider: ${{ secrets.WIF_PROVIDER_URL }}
            service_account: ${{ secrets.SERVICE_ACCOUNT }}
    ```
    - we can see that it require no credential, but exposing security components or architecture may lead to security risk also.
- WIF in Terraform
    - [code/models/terraform/auth/main.tf](./code/models/terraform/auth/main.tf)
    - Keys to enable this services are: `attribute_mapping`, `attribute_condition`, and `issuer_url`
    ```
    resource "google_iam_workload_identity_pool_provider" "main_provider" {
        project                            = var.project_id
        workload_identity_pool_id          = google_iam_workload_identity_pool.main_pool.workload_identity_pool_id
        workload_identity_pool_provider_id = var.provider_id
        display_name                       = var.provider_display_name
        description                        = var.provider_description
        attribute_mapping                  = var.attribute_mapping
        attribute_condition                = var.attribute_condition
        oidc {
            issuer_uri        = var.issuer_uri
        }
    }
    ```
    - [code/models/terraform/auth/variable.tf](./code/models/terraform/auth/variable.tf)
    ```
    ...
    variable "attribute_mapping" {
        type = map(any)
        default = {
            "google.subject"                = "assertion.sub"
            "attribute.aud"                 = "assertion.aud"
            "attribute.repository_owner"    = "assertion.repository_owner"
        }
    }

    variable "attribute_condition" {
        type    = string
        default = <<EOT
            assertion.repository_owner == "patcha-ranat"
        EOT
    }

    variable "issuer_uri" {
        type    = string
        default = "https://token.actions.githubusercontent.com"
    }
    ...
    ```

In the last step, we have to grant additional access to the service account which is quite hard to execute via web-based UI.
```bash
gcloud iam service-accounts add-iam-policy-binding "${SERVICE_ACC_NAME}@${PROJECT}.iam.gserviceaccount.com" \
  --project=${PROJECT_ID} \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://${IAM_PRINCIPAL_ID}/attribute.repository_owner/patcha-ranat"
```
*Note:*
- *For UI approach, you have to grant access via WIF pool page, on the right panel with 'CONNECTED SERVICE ACCOUNTS' tabs*
- `IAM_PRICIPAL_ID` is retrieved from WIF pool page, also you have to replace *'pricipal'* to *'principalSet'*.

However, there's some constraints for WIF that's worth mentioning
- Assertion(s) in `attribute_condition` must be specified as subset of `attribute_mapping`. We don't have to specify condition for every mapped assertions, but recommendation from official GCP documentation is that we should specify 1 assertion atleast for security purpose.
- Although there're example to specify multiple conditions as a string of `attribute_condition` like the below code snippet, there's a problem when we're trying to grant service account an IAM access. `--member` argument don't accept multiple conditions pattern. Some of specific project example, instead use `assertion.full = "assertion.A+assertion.B+..."` for fine grained controls to allow access to specific service accounts from specific events on specific GH repo.
    ```
    variable "attribute_condition" {
        type    = string
        default = <<EOT
            assertion.sub == "xxx"
            assertion.aud == "xxx"
            assertion.repository_owner == "patcha-ranat"
            ...
        EOT
    }
    ```
    Instead for `assertion.full`
    ```
    attribute_mapping = {
        "google.subject" = "assertion.sub"
        "attribute.aud"  = "assertion.aud"
        "attibute.full"  = "assertion.repository+assertion.+repository_owner+..."
    }
    ```
    ```bash
    principalSet://${IAM_PRINCIPAL_ID}/attribute.full/${GH_REPO}${GH_REPO_OWENER}...
    ```
- `issuer_uri` is a static URL obtained from example and mentioned in documentation from GitHub

**References:**
- How to use `assertion.full`: [Stack Overflow](https://stackoverflow.com/a/76451745)
- Another example of `assertion.full`: [Stack Overflow](https://stackoverflow.com/questions/72752410/attribute-mappings-in-configuring-workload-identity-federation-between-gcp-and-g)

#### 6.5.2 Workload Identity Federation Debugging

```
# Error: 1

Error: google-github-actions/auth failed with: failed to generate Google Cloud federated token for //iam.googleapis.com/***: {"error":"invalid_request","error_description":"Invalid value for \"audience\". This value should be the full resource name of the Identity Provider
```
1. `GCP_WORKLOAD_IDENTITY_PROVIDER` in CD workflow must be in format: "project/<project_id>/..." with uri prefix removed.
    - [Solution Source - Stack Overflow](https://stackoverflow.com/questions/76146054/gitlab-ci-cd-fails-to-connect-with-gcp-using-workload-identity-federation-and-id)
```
# Error: 2

denied: Unauthenticated request. Unauthenticated requests do not have permission "artifactregistry.repositories.uploadArtifacts" on resource
```
2. `gcloud auth configure-docker` is required in CD workflow steps.
    - [Solution Source - Stack Overflow](https://stackoverflow.com/questions/75840164/permission-artifactregistry-repositories-uploadartifacts-denied-on-resource-usin)
```
# Error: 3

ERROR: (gcloud.auth.docker-helper) There was a problem refreshing your current auth tokens: ('Unable to acquire impersonated credentials'...
```
3. Granting additional iam access to service account specific for WIF is required
    ```bash
    gcloud iam service-accounts add-iam-policy-binding ...
    ```

```
# Error: 4

OIDC error: 403 'Unable to acquire impersonated credentials' [principalSet mismatch with the Subject claim]
```
- Attribute conditions specified when first creating WIF provider are not matched with conditions that we grant to Service Account in the last step.
    - [Solution Source- Stack Overflow](https://stackoverflow.com/a/76451745)


Finally, you can now manual trigger workflow for automated deployment (CD Workflow)

*References:*
1. **How OIDC work with cloud provider behind the scene, JWT attributes, and advance configuration**: [About security hardening with OpenID Connect](https://docs.github.com/en/actions/security-for-github-actions/security-hardening-your-deployments/about-security-hardening-with-openid-connect)
2. **Official GHA with GCP on Github Repo**: [Authenticate to Google Cloud from GitHub Actions - Direct Workload Identity Federation](https://github.com/google-github-actions/auth?tab=readme-ov-file#preferred-direct-workload-identity-federation)
3. **GHA steps for WIF, recommendation, and gcloud commands from official published article**: [Enabling keyless authentication from GitHub Actions - Google Cloud Platform](https://cloud.google.com/blog/products/identity-security/enabling-keyless-authentication-from-github-actions)
4. WIF & Terraform Medium Example: [Workload Identity Federation & GitHub Actions & Terraform - Medium](https://articles.arslanbekov.com/workload-identity-federation-github-actions-terraform-684813c201a9)
5. GitHub default variables: [Store information in variables](https://docs.github.com/en/actions/writing-workflows/choosing-what-your-workflow-does/store-information-in-variables#default-environment-variables)

For more resource for deployment overview, I recommended this [ML Deployment Chapter](https://github.com/alexeygrigorev/mlbookcamp-code/tree/master/course-zoomcamp/05-deployment) from [*Machine Learning Zoomcamp*](https://github.com/alexeygrigorev/mlbookcamp-code/tree/master).

### 6.6 ML Service Integration Test

![integration-test](./docs/integration-test.png)

To test the deployed image or ML service, we can use airfow to schedule it as a job. But to use it in airflow, we can utilize `GKEPodOperator` which is required K8s cluster or `DockerOperator` for more lightweight approach.

Everything is explained in the image above, but I think setting up this at first-run is quite harder than it's seemed, especially when there's an operator that is not popular among the communities and not well-documented from official provider.

Keys are about:
- auto-authentication using GCP ADC requiring airflow entrypoint that's explained in *2.2.3 GCP ADC for Pipeline Code* and *2.1.4 Airflow Entrypoint* 
- how to configure `DockerOperator` parameters, like mounting `docker.sock` or ADC and make the service to be able to communicate with cloud.

References:
- [`docker.sock` is required to be mounted to airflow scheduler to activate `DockerOperator` - docker-airflow GitHub](https://github.com/puckel/docker-airflow/issues/543)
- [Simple DockerOperator parameters configuration - Stack Overflow](https://stackoverflow.com/questions/61640090/apache-airflow-how-to-run-a-docker-operator-with-environment-variables-from-ano)
- [Example 1 of how to use `Mount` parameter in `DockerOperator` - Stack Overflow](https://stackoverflow.com/a/68457890)
- [Example 2 of how to use `Mount` parameter in `DockerOperator` - Stack Overflow](https://stackoverflow.com/a/73109627)

## 7. Conclusion

From this project, we learned how to:
- **Design Data Architecture**, and select tools for each process to be used in the project.
- **Set up Environment for Virtualization** to simulate the local environment such as database, and webserver.
- **Create ETL Pipelines** to extract data from various sources, transform the data, and load the data to the target database.
- **Utilize Cloud Services** to enable Data Lake, Data Warehouse, Image Repository, and etc on cloud.
- **Learn more on Advanced Cloud Authentication** with low security-risk.
- **Create a Dashboard** to visualize the data and analyze the business.
- **Develop Machine Learning Models** to predict the future sales, customer segments, and inferencing customer buying behavior.
- **Productionize Machine Learning Code/Pipeline** to cloud environment as an image through both manual process and CI/CD Pipeline with keyless authentication.

This is the biggest personal project I've ever done so far, and I learned a lot from it. I hope it can be useful for anyone who shares the same learning path.

***Thank you for your reading, happy learning.***

## 8. Appendix

There's relevant topic that I first intended to extend from this project, *Web Scraping*. However, the scope of this project is too huge to elaborate on the topic, but you can check the concept and the old written code in [web-scraping](https://github.com/patcha-ranat/Ecommerce-Invoice-End-to-end/tree/master/web-scraping) folder.

