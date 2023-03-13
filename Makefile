clean:
	rm -rf *.egg-info

configure:  # does any pre-requisite installs
	@#pip install --upgrade pip==21.0.1
	pip install --upgrade pip
	@#pip install poetry==1.1.9  # this version is known to work. -kmp 5-Oct-2021
	pip install wheel
	pip install poetry

clear-poetry-cache:  # clear poetry/pypi cache. for user to do explicitly, never automatic
	poetry cache clear pypi --all

macpoetry-install:
	scripts/macpoetry-install

lint:
	poetry run flake8 snovault

macbuild:
	make configure
	make macpoetry-install
	make build-after-poetry

build:
	make configure
	make build-configured

build-configured:
	poetry install

build-for-ga:
	make configure
	poetry config --local virtualenvs.create true
	make build-configured

ES_URL = search-fourfront-testing-opensearch-kqm7pliix4wgiu4druk2indorq.us-east-1.es.amazonaws.com:443

LOCAL_INSTAFAIL_OPTIONS = --timeout=200 -xvv --instafail
LOCAL_MULTIFAIL_OPTIONS = --timeout=200 -vv
GA_CICD_TESTING_OPTIONS = --timeout=400 -xvvv --durations=100 --aws-auth --es ${ES_URL}
STATIC_ANALYSIS_OPTIONS =  -vv

TEST_NAME ?= missing_TEST_NAME

test-one:

	SQLALCHEMY_WARN_20=1 pytest ${LOCAL_MULTIFAIL_OPTIONS} -k ${TEST_NAME}

test:
	@git log -1 --decorate | head -1
	@date
	make test-static && make test-unit && make test-indexing
	@git log -1 --decorate | head -1
	@date

test-full:
	@git log -1 --decorate | head -1
	@date
	make test-unit-full
	make test-indexing-full
	make test-static || echo "Static tests failed."
	@git log -1 --decorate | head -1
	@date

test-indexing:
	SQLALCHEMY_WARN_20=1 poetry run pytest ${LOCAL_INSTAFAIL_OPTIONS} -m "indexing"

test-unit:
	SQLALCHEMY_WARN_20=1 poetry run pytest ${LOCAL_INSTAFAIL_OPTIONS} -m "not indexing"

test-indexing-full:
	SQLALCHEMY_WARN_20=1 poetry run pytest ${LOCAL_MULTIFAIL_OPTIONS} -m "indexing"

test-unit-full:
	SQLALCHEMY_WARN_20=1 poetry run pytest ${LOCAL_MULTIFAIL_OPTIONS} -m "not indexing"

test-static:
	NO_SERVER_FIXTURES=TRUE USE_SAMPLE_ENVUTILS=TRUE poetry run python -m pytest -vv -m static
	make lint

remote-test-indexing:
	SQLALCHEMY_WARN_20=1 poetry run pytest ${GA_CICD_TESTING_OPTIONS} -m "indexing"

remote-test-unit:
	SQLALCHEMY_WARN_20=1 poetry run pytest ${GA_CICD_TESTING_OPTIONS} -m "not indexing"

update:
	poetry update

publish:
	scripts/publish

publish-for-ga:
	scripts/publish --noconfirm

kill:
	pkill -f postgres &
	pkill -f elasticsearch &

help:
	@make info

info:
	@: $(info Here are some 'make' options:)
	   $(info - Use 'make clean' to clear out (non-python) dependencies)
	   $(info - Use 'make configure' to install poetry, though 'make build' will do it automatically.)
	   $(info - Use 'make build' to build only application dependencies (or 'make macbuild' on OSX Catalina))
	   $(info - Use 'make test' to run tests with the normal options we use on travis)
	   $(info - Use 'make update' to update dependencies (and the lock file))
