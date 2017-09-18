#!/bin/bash

set -e

######
# 1. Property whose value need to be changed
# 2. New value of property
# 3. File in which above property lies.
#####

if [ -z "$RELEASE_VERSION" ]; then
	echo "RELEASE_VERSION is not set, need to set release version so new branch can be created using release branch"
fi

setProperty(){
  awk -v pat="^$1=" -v value="$1=$2" '{ if ($0 ~ pat) print value; else print $0; }' $3 > $3.tmp
  mv $3.tmp $3
}

VERSION_FILE_NAME=version.txt
cat $VERSION_FILE_NAME | awk -f readProperties.awk > tempEnv.sh
source tempEnv.sh
rm tempEnv.sh


FRAMEWORK_VERSION=$RELEASE_VERSION
FOLDER_NUMBER=$universe_folder_number
NEW_FOLDER_NUMBER=$(expr $FOLDER_NUMBER + 1) 

setProperty "universe_folder_number" $NEW_FOLDER_NUMBER $VERSION_FILE_NAME # updating $VERSION_FILE_NAME i.e folder_number in version.txt


git add version.txt && git commit -m "dcos cassandra framework version updated to ${FRAMEWORK_VERSION} and folder number incremented to ${NEW_FOLDER_NUMBER}" && git push origin master
git checkout -b mds-${RELEASE_VERSION} && git push origin mds-${RELEASE_VERSION} 


