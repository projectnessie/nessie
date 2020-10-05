# Demo Notebook

## Installation

* create virtualenv `python -m venv venv`
* activate venv `source venv/bin/activate`
* install dependencies `pip install -r requirements.txt`
* ensure you have a spark env and set the environment variable `SPARK_HOME` appropirately
* download the spark jar (see below to choose Spark version and Iceberg/Delta) or execute `mvn clean install` from the root directory - this builds the spark jars
* run the docker image `docker-compose up` or from `../../servers/quarkus-server` run `mvn quarkus:dev`
* run `jupyter-lab`


## Spark Jars*

* [Spark 2 Iceberg](https://search.maven.org/remotecontent?filepath=org/projectnessie/nessie-iceberg-spark2/0.1.0/nessie-iceberg-spark2-0.1.0.jar)
* [Spark 3 Iceberg](https://search.maven.org/remotecontent?filepath=org/projectnessie/nessie-iceberg-spark3/0.1.0/nessie-iceberg-spark3-0.1.0.jar)
* [Spark 2 Delta](https://search.maven.org/remotecontent?filepath=org/projectnessie/nessie-deltalake-spark2/0.1.0/nessie-deltalake-spark2-0.1.0.jar)
* [Spark 3 Delta](https://search.maven.org/remotecontent?filepath=org/projectnessie/nessie-deltalake-spark3/0.1.0/nessie-deltalake-spark3-0.1.0.jar)
