clean:
	@echo "No cleaning action taken. Doing 'make clean' is no longer meaningful for this repository."

configure:  # does any pre-requisite installs
	pip install poetry

moto-setup:
	pip install "moto[server]"

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
	bin/test -vv --timeout=400

info:
	@: $(info Here are some 'make' options:)
	   $(info - Use 'make configure' to install poetry, though 'make build' will do it automatically.)
	   $(info - Use 'make build' to install dependencies using poetry.)
	   $(info - Use 'make macbuild' if 'make build' gets errors on MacOS Catalina.)
	   $(info - Use 'make moto-setup' if you did 'poetry install' but did not set up moto for testing.)
	   $(info - Use 'make test' to run tests with the normal options we use on travis)
