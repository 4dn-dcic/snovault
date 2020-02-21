clean:
	rm -rf node_modules eggs parts develop-eggs
	echo "No egg-info files should remain."
	rm -rf *.egg-info
