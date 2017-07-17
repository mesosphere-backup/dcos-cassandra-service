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


export FRAMEWORK_VERSION=$mds_version


NEW_FRAMEWORK_VERSION=$(expr $FRAMEWORK_VERSION + 1) # ex : version=21
export FRAMEWORK_VERSION=$NEW_FRAMEWORK_VERSION # ex : FRAMEWORK_VERSION now equal to 21

setProperty "mds_version" $FRAMEWORK_VERSION $VERSION_FILE_NAME # updating $VERSION_FILE_NAME i.e mds_version in version.txt


git add version.txt && git commit -m "dcos cassandra framework version updated to ${NEW_FRAMEWORK_VERSION}" && git push origin master
git checkout -b mds-${RELEASE_VERSION} && git push origin mds-${RELEASE_VERSION} 


