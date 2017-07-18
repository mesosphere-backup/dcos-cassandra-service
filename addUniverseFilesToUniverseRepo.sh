#!/bin/bash

set -e 

UNIVERSE_BRANCH=$1
ENVRIONMENT=$2
UNIVERSE_FILES_PATH=$3
FRAMEWORK_VERSION=$4

rm -rf universeRepo
git clone git@github.com:adobe-mds/universe.git universeRepo
cd universeRepo
git checkout  $UNIVERSE_BRANCH

mkdir -p $ENVRIONMENT
cd $ENVRIONMENT


rm  -rf repo/packages/M/mds-cassandra/${FRAMEWORK_VERSION}
mkdir  -p repo/packages/M/mds-cassandra/${FRAMEWORK_VERSION}
cp  ../../${UNIVERSE_FILES_PATH}/*  repo/packages/M/mds-cassandra/${FRAMEWORK_VERSION}/

git add .
git diff-index --quiet HEAD || (git commit -m "framework file for cassandra added :${FRAMEWORK_VERSION}" && git push origin ${UNIVERSE_BRANCH})

cd ..

#updating main branch
git checkout version-3.x
git pull --rebase origin $UNIVERSE_BRANCH
git push origin version-3.x



