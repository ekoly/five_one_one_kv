server:
	$(MAKE) -C server

install:
	python setup.py install

test:
	python -m pytest

clean:
	rm -rf server/server server/*.o build/ dist/ __pycache__/
