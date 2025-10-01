# sipin-pg-webhook-events-dispatcher

## Synopsis

Polls the Postgres webhook events and sends them to Svix

## Prerequisites

* Git
* Docker (optional)
* Python 3.12
* Access to the meemoo PyPi

## Usage

1. Clone this repository with:

    `$ git clone https://github.com/viaacode/sipin-pg-webhook-events-dispatcher.git`

2. Change into the new directory:

    `$ cd sipin-pg-webhook-events-dispatcher`

3. Set the needed config:

Included in this repository is a config.yml file detailing the required configuration. There is also an .env.example file containing all the needed env variables used in the config.yml file. All values in the config have to be set in order for the application to function correctly. You can use !ENV ${EXAMPLE} as a config value to make the application get the EXAMPLE environment variable.

### Running locally

1. Start by creating a virtual environment:

    `$ python -m venv venv`

2. Activate the virtual environment:

    `$ source venv/bin/activate`

3. Install the external modules:

    ```
    $ pip install . \
        --extra-index-url http://do-prd-mvn-01.do.viaa.be:8081/repository/pypi-all/simple \
        --trusted-host do-prd-mvn-01.do.viaa.be && \
      pip install .[dev]
    ```

4. Make sure to load in the ENV vars.

5. Run the tests with:

    `$ python -m pytest -v --cov=./app`

6. Run the application:

    `$ python -m main`

### Running using Docker

1. Build the container:

    `$ docker build -t sipin-pg-webhook-events-dispatcher .`

2. Run the tests in a container:

    `$ docker run --env-file .env.example --rm --entrypoint python sipin-pg-webhook-events-dispatcher:latest -m pytest -v --cov=./app`

3. Run the container (with specified `.env` file):

    `$ docker run --env-file .env --rm sipin-pg-webhook-events-dispatcher:latest`
