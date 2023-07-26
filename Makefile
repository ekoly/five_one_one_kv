install:
	python setup.py install

test:
	python -m pytest

test-dispatch:
	python -m pytest tests/test_dispatch.py

clean:
	rm -rf server/server server/*.o build/ dist/ __pycache__/

uninstall:
	pip uninstall five-one-one-kv
