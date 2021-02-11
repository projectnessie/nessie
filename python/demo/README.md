# Demo Notebook

## Installation

_Supported python versions: 3.5 - 3.9_

* create virtualenv `python -m venv venv`
* activate venv `source venv/bin/activate`
* install dependencies `pip install -r requirements.txt`
* ensure you have a spark env and set the environment variable `SPARK_HOME` appropirately
* download the spark jar (see below to choose Spark version and Iceberg/Delta) or if using Delta execute `mvn clean install` from the root directory - this builds the spark jars
* run the docker image `docker-compose up` or from `../../servers/quarkus-server` run `mvn quarkus:dev`
* run `jupyter-lab`


## Spark Jars*

* [Spark 2 Iceberg](https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark/0.11.0/iceberg-spark-0.11.0.jar)
* [Spark 3 Iceberg](https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark3/0.11.0/iceberg-spark3-0.11.0.jar)
* [Spark 2 Delta](https://repo.maven.apache.org/maven2/org/projectnessie/nessie-deltalake-spark2/0.3.0/nessie-deltalake-spark2-0.3.0.jar)
* [Spark 3 Delta](https://repo.maven.apache.org/maven2/org/projectnessie/nessie-deltalake-spark3/0.3.0/nessie-deltalake-spark3-0.3.0.jar)
