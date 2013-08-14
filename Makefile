test:
	python setup.py test

publish:
	python setup.py sdist upload

clean:
	rm -rf *.egg-info
	rm -rf dist

.PHONY: test publish clean
