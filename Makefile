test:
	python setup.py test

publish:
	python setup.py sdist bdist_wheel upload

clean:
	rm -rf *.egg-info
	rm -rf dist

.PHONY: test publish clean
