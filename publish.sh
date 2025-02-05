#!/bin/bash

VERSION=$1
if [ -z "$VERSION" ]
then
    echo "Please provide a version number (e.g. ./publish.sh 0.1.1)"
    exit 1
fi

# Build images
docker build -t ewlarson/ogm-api:latest -t ewlarson/ogm-api:$VERSION .

# Push images
docker push ewlarson/ogm-api:latest
docker push ewlarson/ogm-api:$VERSION

echo "Published ewlarson/ogm-api:latest and ewlarson/ogm-api:$VERSION" 