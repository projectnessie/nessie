# Demo Notebook

## Installation

_Supported python versions: 3.5 - 3.9_

* create virtualenv: `python -m venv venv`
* activate venv: `source venv/bin/activate`
* upgrade pip as otherwise installing some dependencies might fail: `pip install --upgrade pip`
* install dependencies: `pip install -r requirements.txt`
* ensure you have a spark env and set the environment variable `SPARK_HOME` appropriately (see the `Spark Setup Steps` for more info)
* download the spark jar (see below to choose Spark version and Iceberg/Delta) or if using Delta execute `mvn clean install` from the root directory - this builds the spark jars
* run the docker image `docker-compose up` or from `../../servers/quarkus-server` run `mvn quarkus:dev`
* run `jupyter-lab`


## Spark Jars

* [Spark 2 Iceberg](https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark/0.11.1/iceberg-spark-0.11.1.jar)
* [Spark 3 Iceberg](https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark3/0.11.1/iceberg-spark3-0.11.1.jar)
* [Spark 2 Delta](https://repo.maven.apache.org/maven2/org/projectnessie/nessie-deltalake-spark2/0.3.0/nessie-deltalake-spark2-0.3.0.jar)
* [Spark 3 Delta](https://repo.maven.apache.org/maven2/org/projectnessie/nessie-deltalake-spark3/0.3.0/nessie-deltalake-spark3-0.3.0.jar)


## Spark Setup Steps
The below steps show how to download Spark and set up `SPARK_HOME` in case you haven't Spark already installed.

#### Spark 3
```
wget https://downloads.apache.org/spark/spark-3.0.3/spark-3.0.3-bin-hadoop2.7.tgz
tar -xzvf spark-3.0.3-bin-hadoop2.7.tgz
export SPARK_HOME="`pwd`/spark-3.0.3-bin-hadoop2.7"
```

#### Spark 2
```
wget https://downloads.apache.org/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz
tar -xzvf spark-2.4.7-bin-hadoop2.7.tgz
export SPARK_HOME="`pwd`/spark-2.4.7-bin-hadoop2.7"
```
