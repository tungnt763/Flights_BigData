# IT3943 - Big Data - SOICT - HUST

## Introduction
<ul>
  <li>Name of project: Build a Data Lake system to analyze flight data on Kaggle</li>
  <li>Project objective:
    <ul>
      <li>Process flight data on Kaggle with Data Lake system</li>
      <li>Use Spark to process data</li>
      <li>Use Trino to query data and Superset to visualize data</li>
    </ul>  
  </li>
</ul>

## Data flow

  <img src="pictures/flow.png">

## Deploy system
#### 1. Should pull and build images before
```sh
docker pull postgres bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8 bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8 bde2020/hadoop-resourcemanager:2.0.0-hadoop3.2.1-java8 bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8 bde2020/hadoop-historyserver:2.0.0-hadoop3.2.1-java8
```
```sh
docker build ./airflow -t airflow
```
```sh
docker build ./superset -t superset
```

#### 2. Start system
```sh
docker compose up -d
```

#### 3. Set Trino on Airflow cluster
```sh
docker exec -u root -it airflow-webserver chmod +x /opt/airflow/source/trino; docker exec -u root -it airflow-scheduler chmod +x /opt/airflow/source/trino
```

#### 4. Set Spark and Hadoop on Airflow cluster
```
Download Spark & Hadoop packages and Replace config in airflow/source
```
```
https://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz
```
```
https://spark.apache.org/downloads.html
```

#### 5. Download Data source to Airflow cluster
```
https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022/data?select=readme.md
```

#### 6. Build enviroment Superset
```sh
./superset/bootstrap-superset.sh
```
  
#### 7. Visualize data on Superset with SQLalchemy uri
```
trino://hive@trino:8080/hive
```

## Demo System
### YARN
  <img style="width:100%;" src="pictures/system/yarn.png">

### Trino
  <img style="width:100%;" src="pictures/system/trino.png">

### Superset
  <img style="width:100%;" src="pictures/system/superset.png">

## Demo Output
### Analysis of Day of Month
  <img style="width:100%;" src="pictures/charts/ngay-trong-thang-2024-12-01T02-17-37.154Z.jpg">

### Analysis of Month of Year
  <img style="width:100%;" src="pictures/charts/số-chuyến-bay-theo-thang-qua-cac-nam-2024-12-01T13-13-34.441Z.jpg">

## Report
[Group17 Report](https://drive.google.com/file/d/1pJD0A-e81DKolsvueNOyPMnbSy_xJbRq/view?usp=drive_link)
