SHELL=/bin/bash

clean:
	make clean-python-caches

clean-python-caches:
	rm -rf *.egg-info

clear-poetry-cache:  # clear poetry/pypi cache. for user to do explicitly, never automatic
	poetry cache clear pypi --all

aws-ip-ranges:
	curl -o aws-ip-ranges.json https://ip-ranges.amazonaws.com/ip-ranges.json

macpoetry-install:
	scripts/macpoetry-install

configure:  # does any pre-requisite installs
	pip install --upgrade pip
	pip install wheel
	pip install poetry==1.4.2

build-poetry:
	make configure
	poetry install

macbuild-poetry:
	make configure
	make macpoetry-install

build:
	make build-poetry
	make build-after-poetry

macbuild:
	make macbuild-poetry
	make build-after-poetry

build-after-poetry:  # continuation of build after poetry install
	make aws-ip-ranges
	poetry run python setup_eb.py develop
	make fix-dist-info
	poetry run prepare-local-dev

fix-dist-info:
	@scripts/fix-dist-info

build-for-ga:
	make configure
	poetry config --local virtualenvs.create true
	poetry install

deploy1:  # starts postgres/ES locally and loads inserts, and also starts ingestion engine
	@DEBUGLOG=`pwd` SNOVAULT_DB_TEST_PORT=`grep 'sqlalchemy[.]url =' development.ini | sed -E 's|.*:([0-9]+)/.*|\1|'` dev-servers-snovault development.ini --app-name app --clear --init --load

psql-dev:  # starts psql with the url after 'sqlalchemy.url =' in development.ini
	@scripts/psql-start dev

psql-test:  # starts psql with a url constructed from data in 'ps aux'.
	@scripts/psql-start test

#kibana-start:  # starts a dev version of kibana (default port)
#	scripts/kibana-start
#
#kibana-start-test:  # starts a test version of kibana (port chosen for active tests)
#	scripts/kibana-start test
#
#kibana-stop:
#	scripts/kibana-stop

ES_URL = search-fourfront-testing-opensearch-kqm7pliix4wgiu4druk2indorq.us-east-1.es.amazonaws.com:443

LOCAL_INSTAFAIL_OPTIONS = --timeout=400 -xvv --instafail
LOCAL_MULTIFAIL_OPTIONS = --timeout=200 -vv
GA_CICD_TESTING_OPTIONS = --timeout=400 -xvvv --durations=100 --aws-auth --es ${ES_URL}
STATIC_ANALYSIS_OPTIONS =  -vv

test:
	@git log -1 --decorate | head -1
	@date
	make test-unit && make test-indexing && make test-static
	@git log -1 --decorate | head -1
	@date

ES_URL = search-fourfront-testing-opensearch-kqm7pliix4wgiu4druk2indorq.us-east-1.es.amazonaws.com:443

LOCAL_INSTAFAIL_OPTIONS = --timeout=400 -xvv --instafail
LOCAL_MULTIFAIL_OPTIONS = --timeout=200 -vv
GA_CICD_TESTING_OPTIONS = --timeout=400 -xvvv --durations=100 --aws-auth --es ${ES_URL}
STATIC_ANALYSIS_OPTIONS =  -vv

test-full:
	@git log -1 --decorate | head -1
	@date
	make test-unit-full
	make test-indexing-full
	make test-static || echo "Static tests failed."
	@git log -1 --decorate | head -1
	@date

test-unit:
	SQLALCHEMY_WARN_20=1 poetry run pytest ${LOCAL_INSTAFAIL_OPTIONS} -m "not indexing"

test-unit-full:
	SQLALCHEMY_WARN_20=1 poetry run pytest ${LOCAL_MULTIFAIL_OPTIONS} -m "not indexing"

test-indexing-full:
	make test-indexing-not-es-full
	make test-indexing-es-full

test-indexing-es-full:
	SQLALCHEMY_WARN_20=1 poetry run pytest ${LOCAL_MULTIFAIL_OPTIONS} -m "indexing and es"

test-indexing-not-es-full:
	SQLALCHEMY_WARN_20=1 poetry run pytest ${LOCAL_MULTIFAIL_OPTIONS} -m "indexing and not es"

test-indexing:
	make test-indexing-not-es && make test-indexing-es

test-indexing-es:
	SQLALCHEMY_WARN_20=1 poetry run pytest ${LOCAL_INSTAFAIL_OPTIONS} -m "indexing and es"

test-indexing-not-es:
	SQLALCHEMY_WARN_20=1 poetry run pytest ${LOCAL_INSTAFAIL_OPTIONS} -m "indexing and not es"

test-performance:
	@echo "snovault has no performance tests right now, but it could."

test-integrated:
	@echo "snovault has no integrated tests right now, but it could."

test-static:
	NO_SERVER_FIXTURES=TRUE USE_SAMPLE_ENVUTILS=TRUE poetry run python -m pytest -vv -m static
	make lint

TEST_NAME ?= missing_TEST_NAME_parameter

test-one:
	SQLALCHEMY_WARN_20=1 poetry run python -m pytest ${LOCAL_MULTIFAIL_OPTIONS} -k ${TEST_NAME}

remote-test:  # Actually, we don't normally use this. Instead the GA workflow sets up two parallel tests.
	make remote-test-indexing && make remote-test-unit

remote-test-unit:
	make remote-test-not-indexing

remote-test-not-indexing:
	SQLALCHEMY_WARN_20=1 poetry run pytest ${GA_CICD_TESTING_OPTIONS} -m "not indexing"

remote-test-indexing:
	 make remote-test-indexing-not-es && make remote-test-indexing-es

remote-test-indexing-es:
	SQLALCHEMY_WARN_20=1 poetry run pytest ${GA_CICD_TESTING_OPTIONS} -m "indexing and es"

remote-test-indexing-not-es:
	SQLALCHEMY_WARN_20=1 poetry run pytest ${GA_CICD_TESTING_OPTIONS} -m "indexing and not es"

update:
	poetry update

publish:
	poetry run publish-to-pypi --force-allow-username

publish-for-ga:
	poetry run publish-to-pypi --noconfirm

kill:  # kills back-end processes associated with the application. Use with care.
	pkill -f postgres &
	pkill -f opensearch &


lint-full:
	poetry run flake8 deploy/ || echo "flake8 failed for deploy/"
	poetry run flake8 snovault/ || echo "flake8 failed for snovault/"

lint:
	poetry run flake8 deploy/ && poetry run flake8 snovault/

help:
	@make info

info:
	@: $(info Here are some 'make' options:)
	   $(info - Use 'make aws-ip-ranges' to download latest ip range information. Invoked automatically when needed.)
	   $(info - Use 'make build' to build only application dependencies (or 'make macbuild' on OSX Catalina))
	   $(info - Use 'make clean' to clear out (non-python) dependencies)
	   $(info - Use 'make clear-poetry-cache' to clear the poetry pypi cache if in a bad state. (Safe, but later recaching can be slow.))
	   $(info - Use 'make configure' to install poetry, though 'make build' will do it automatically.)
	   $(info - Use 'make deploy1' to spin up postgres/elasticsearch and load inserts.)
	   $(info - Use 'make kill' to kill postgres and opensearch proccesses. Please use with care.)
	   $(info - Use 'make psql-dev' to start psql on data associated with an active 'make deploy1'.)
	   $(info - Use 'make psql-test' to start psql on data associated with an active test.)
	   $(info - Use 'make test' to run tests with the normal options we use on travis)
	   $(info - Use 'make update' to update dependencies (and the lock file))
