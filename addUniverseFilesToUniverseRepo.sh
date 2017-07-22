#!/bin/bash

set -e 

UNIVERSE_BRANCH=$1
ENVRIONMENT=$2
UNIVERSE_FILES_PATH=$3
FRAMEWORK_VERSION=$4
UNIVERSE_FOLDER_NUMBER=$5

rm -rf universeRepo
git clone git@github.com:adobe-mds/universe.git universeRepo
cd universeRepo
git checkout  $UNIVERSE_BRANCH

mkdir -p $ENVRIONMENT
cd $ENVRIONMENT


rm  -rf repo/packages/M/mds-cassandra/${UNIVERSE_FOLDER_NUMBER}
mkdir  -p repo/packages/M/mds-cassandra/${UNIVERSE_FOLDER_NUMBER}
cp  ../../${UNIVERSE_FILES_PATH}/*  repo/packages/M/mds-cassandra/${UNIVERSE_FOLDER_NUMBER}/
cd ..
git add ${ENVRIONMENT}/repo/packages/M/mds-cassandra/${UNIVERSE_FOLDER_NUMBER}/*
git diff-index --quiet HEAD || (git commit -m "framework file for cassandra added :${FRAMEWORK_VERSION}  and folder number : ${UNIVERSE_FOLDER_NUMBER}" && git push origin ${UNIVERSE_BRANCH})



#updating main branch
git checkout version-3.x
git pull --rebase origin $UNIVERSE_BRANCH
git push origin version-3.x



