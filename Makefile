clean:
	@echo "No cleaning action taken. Doing 'make clean' is no longer meaningful for this repository."
moto-setup:
	pip install "moto[server]"
macpoetry-install:
	bin/macpoetry-install
macbuild:
	make macpoetry-install
	make moto-setup
test:
	bin/test --timeout=400
