clean:
	rm -rf node_modules eggs parts develop-eggs
	echo "No egg-info files should remain."
	rm -rf *.egg-info
moto-setup:
	pip install "moto[server]"
macpoetry-install:
	bin/macpoetry-install
macbuild:
	make clean
	make macpoetry-install
	make moto-setup
