clean:
	rm -rf *.egg-info

configure:  # does any pre-requisite installs
	pip install --upgrade pip
	pip install poetry==1.0.10  # pinned to avoid build problems we cannot fix in pyproject.toml

moto-setup:
	poetry run python -m pip install "moto[server]==1.3.7"

macpoetry-install:
	scripts/macpoetry-install

macbuild:
	make configure
	make macpoetry-install
	make build-after-poetry

build:
	make configure
	poetry install
	make build-after-poetry

build-after-poetry:  # continuation of build after poetry install
	make moto-setup

test:
	pytest -vv --timeout=200

travis-test:
	poetry run pytest -vvv --timeout=200 --aws-auth --es search-fourfront-testing-6-8-kncqa2za2r43563rkcmsvgn2fq.us-east-1.es.amazonaws.com:443

update:
	poetry update

publish:
	scripts/publish

publish-for-ga:
	scripts/publish --noconfirm

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
