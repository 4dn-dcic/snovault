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
	poetry install
	make build-after-poetry

build-after-poetry:
	poetry run python setup_eb.py develop
	make fix-dist-info
	poetry run prepare-local-dev

fix-dist-info:
	@scripts/fix-dist-info

test:
	@git log -1 --decorate | head -1
	@date
	pytest -xvv --instafail --timeout=200
	@git log -1 --decorate | head -1
	@date

remote-test-npm:
	poetry run pytest -xvvv --timeout=400 --durations=100 --aws-auth --es search-fourfront-testing-opensearch-kqm7pliix4wgiu4druk2indorq.us-east-1.es.amazonaws.com:443 -m "indexing"

remote-test-unit:
	poetry run pytest -xvvv --timeout=400 --durations=100 --aws-auth --es search-fourfront-testing-opensearch-kqm7pliix4wgiu4druk2indorq.us-east-1.es.amazonaws.com:443 -m "not indexing"

deploy1:  # starts postgres/ES locally and loads inserts, and also starts ingestion engine
	@DEBUGLOG=`pwd` SNOVAULT_DB_TEST_PORT=`grep 'sqlalchemy[.]url =' development.ini | sed -E 's|.*:([0-9]+)/.*|\1|'` dev-servers development.ini --app-name app --clear --init --load

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
