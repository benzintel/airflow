# Apache Airflow Kick Starter
## Requirement (Install)
- Docker
## Requirement (Language)
- Python
- Docker

## Start Project
1. Start Docker Engine
2. Open terminal or CMD
3. Goto Directory Repo
4. run command bellow
```sh
$ cd ./{Apache Airflow Kick Starter}
$ docker build .
$ docker-compose up
```
If can't login airflow page on `localhost:8080`
```sh
$ docker exec -it airflow_webserver /bin/bash
$ airflow users create --role Admin --username airflow --password airflow --email airflow@airflow.com --firstname airflow --lastname airflow 
```