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
	flake8 snovault

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
	poetry configure --local virtualenvs.create true
	make build-configured

test:
	@git log -1 --decorate | head -1
	@date
	SQLALCHEMY_WARN_20=1 pytest -xvv --instafail --timeout=200
	@git log -1 --decorate | head -1
	@date

test-full:
	@git log -1 --decorate | head -1
	@date
	SQLALCHEMY_WARN_20=1 pytest -vv --timeout=200
	@git log -1 --decorate | head -1
	@date

test-static:
	NO_SERVER_FIXTURES=TRUE USE_SAMPLE_ENVUTILS=TRUE poetry run python -m pytest -vv -m static
	make lint

remote-test-npm:
	SQLALCHEMY_WARN_20=1 poetry run pytest -xvvv --timeout=400 --durations=100 --aws-auth --es search-fourfront-testing-opensearch-kqm7pliix4wgiu4druk2indorq.us-east-1.es.amazonaws.com:443 -m "indexing"

remote-test-unit:
	SQLALCHEMY_WARN_20=1 poetry run pytest -xvvv --timeout=400 --durations=100 --aws-auth --es search-fourfront-testing-opensearch-kqm7pliix4wgiu4druk2indorq.us-east-1.es.amazonaws.com:443 -m "not indexing"

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
