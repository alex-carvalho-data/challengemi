# challenge mi

This project creates a pipeline that reads a MongoDB base, cleans and enrich 
its data and finally normalize it and save in a PostgreSQL db.

For tests purposes, a docker environment was defined and should be executed 
using docker-compose.

The code was written using spark.  
Currently, the code is set to be executed locally, in a single machine.  
However, the code is prepared to easily change the submission destination to a 
cluster and in this gain a considerable amount of performance in case to 
process larger datasets.

## Requirements

- docker (used version 20.10.2)
- docker-compose (used version 1.27.4)
- python (used version 3.7.9 )
- linux (used version ubuntu 20.04)

## Environment

To achieve this task was built an environment using docker-compose.  

The docker-compose file starts 3 containers:
- mongoDB 4.4
  listening on port: 27017
- mongo-express 0.54
    listening on port: 8081
- postgres 13.1
  listening on port: 5432

### User and passwords
- mongoDB 4.4
  - user: root
  - pass: alex213 
- mongo-express 0.54
  - user: root
  - pass: alex213 
- postgres 13.1
  - user: root
  - pass: alex213 

For environment startup go to this project sub-folder:
```shell
cd docker-compose_env
```

Start the docker environment:
```shell
docker-compose up -d
```

Mongo express is a web tool for mongoDB visualization.  
After environment startup it will be available at:  
http://localhost:8081
  - user: root
  - pass: alex213 

Stop docker environment and clean up all docker files:
```shell
docker-compose down -v
```

## Setup

### Linux dependencies

In order to connect to PostgreSQL some Linux packages are required.  
Install them with the following commands:
```shell
sudo apt update
sudo apt-get install libpq-dev python-dev python3.7-dev
```

### Python dependencies
From the root folder of this project, execute the following command to install
the python packages required for this project:
```shell
pip install -r requirements.txt
```

### Load MongoDB

Place the dataset file at:  
dataset/challenge_hearing_tests.json

From the root folder of this project, execute the following command to load the 
dataset into MongoDB:
```shell
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.0 setup/load_dataset_to_mongo.py
```

### Create PostgreSQL DB

From the root folder of this project, execute the following command to create
the PostgreSQL DB schema:
```shell
python3 setup/create_mipostgres_schema.py
```

If your Linux distribution isn't Ubuntu, the command may be:
```shell
python setup/create_mipostgres_schema.py
```

## Pipeline Execution

From the root folder of this project, execute the following command in order to
execute the pipeline:  
```shell
# spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.0 --jars postgresql-42.2.18.jar pipeline_mongo_postgres.py
```

## Questions and Code Considerations

The answers for this challenge can be found in this project at:  
answers/README.md