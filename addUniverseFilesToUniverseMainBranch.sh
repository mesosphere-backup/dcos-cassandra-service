#!/bin/bash

set -e 


ENVRIONMENT=$1
UNIVERSE_FILES_PATH=$2
FRAMEWORK_VERSION=$3
UNIVERSE_FOLDER_NUMBER=$4

MAIN_BRANCH=version-3.x

rm -rf universeRepo
git clone git@github.com:adobe-mds/universe.git universeRepo
cd universeRepo
git checkout $MAIN_BRANCH
mkdir -p $ENVRIONMENT
cd $ENVRIONMENT


rm  -rf repo/packages/M/mds-cassandra/${UNIVERSE_FOLDER_NUMBER}
mkdir  -p repo/packages/M/mds-cassandra/${UNIVERSE_FOLDER_NUMBER}
cp  ../../${UNIVERSE_FILES_PATH}/*  repo/packages/M/mds-cassandra/${UNIVERSE_FOLDER_NUMBER}/
cd ..
git add ${ENVRIONMENT}/repo/packages/M/mds-cassandra/${UNIVERSE_FOLDER_NUMBER}/*
git diff-index --quiet HEAD || (git commit -m "framework file for cassandra added :${FRAMEWORK_VERSION}  and folder number : ${UNIVERSE_FOLDER_NUMBER}" && git push origin $MAIN_BRANCH)


