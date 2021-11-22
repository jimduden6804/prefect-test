SHELL := /usr/bin/env bash

install:
	pipenv update --dev

run:
	pipenv run python ./src/main.py

test:
	pipenv run pytest

clean:
	rm -r ./tmp

lint:
	black .;
