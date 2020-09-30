# Demo Notebook

## Installation

* create virtualenv `python -m venv venv`
* activate venv `source venv/bin/activate`
* install dependencies `pip install -r requirements.txt`
* nessie_client may fail as its not yet on pip. Instead do `pip install ../`
* ensure you have a spark env and set the environment variable `SPARK_HOME` appropirately
* download the [spark jar]() or execute `mvn clean install` from the root directory - this builds the spark jar
* run the docker image `docker run -p 19120:19120 projectnessie/nessie` or from `../../servers/quarkus-server` run `mvn quarkus:dev`
* run `jupyter-lab`
