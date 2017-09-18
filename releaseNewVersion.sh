

RELEASE_VERSION=$1
CREATE_BRANCH=$2
S3_BUCKET_NAME=$3
AWS_SECRET_ACCESS_KEY=$4
AWS_ACCESS_KEY_ID=$5
MAVEN_ARTIFACTORY_URL=$6
MDSBOT_ARTIFACTORY_USERNAME=$7
MDSBOT_ARTIFACTORY_APIKEY=$8

export RELEASE_VERSION=${RELEASE_VERSION}  
if [[ "$CREATE_BRANCH" == 'true' ]]; then
	bash incrementFrameworkVersionAndCutBranch.sh
fi

if [ $? -ne 0 ]; then
	echo "Creating new branch and Incrementing folder number in version.txt fails" 
	exit 1
fi

bash devReleaseJob.sh \
	${RELEASE_VERSION} \
	${MDSBOT_ARTIFACTORY_APIKEY} \
	${MDSBOT_ARTIFACTORY_USERNAME} \
	${MAVEN_ARTIFACTORY_URL} \
	${AWS_SECRET_ACCESS_KEY} \
	${AWS_ACCESS_KEY_ID} \
	${S3_BUCKET_NAME}


if [ $? -ne 0 ]; then
	echo "Creating artifacts failed" 
	exit 1
fi

git clone git@github.com:adobe-mds/universe.git createUniverseJson
cd createUniverseJson
bash scripts/build.sh dev
./uploadUniverseToS3.sh dev $S3_BUCKET $AWS_SECRET_ACCESS_KEY $AWS_ACCESS_KEY_ID


              	            
                      