clean:
	rm -rf *.egg-info

configure:  # does any pre-requisite installs
	pip install poetry

moto-setup:
	pip install "moto[server]==1.3.7"

macpoetry-install:
	bin/macpoetry-install

macbuild:
	make configure
	make macpoetry-install
	make moto-setup

build:
	make configure
	poetry install
	make moto-setup

test:
	pytest -vv --timeout=200

travis-test:
	pytest -vv --timeout=200 --aws-auth --cov --es search-fourfront-builds-uhevxdzfcv7mkm5pj5svcri3aq.us-east-1.es.amazonaws.com:80

update:
	poetry update

help:
	@make info

info:
	@: $(info Here are some 'make' options:)
	   $(info - Use 'make clean' to clear out (non-python) dependencies)
	   $(info - Use 'make configure' to install poetry, though 'make build' will do it automatically.)
	   $(info - Use 'make build' to build only application dependencies (or 'make macbuild' on OSX Catalina))
	   $(info - Use 'make moto-setup' if you did 'poetry install' but did not set up moto for testing.)
	   $(info - Use 'make test' to run tests with the normal options we use on travis)
	   $(info - Use 'make update' to update dependencies (and the lock file))
