#!/bin/bash
black .
isort .
flake8 .
mypy . 