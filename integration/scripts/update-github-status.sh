#!/bin/bash

syntax () {
    echo "Syntax: $0 <state: pending|success|error|failure> <context_label> \"<a descriptive status message>\""
}

# check env settings
if [ -z "$GITHUB_TOKEN_REPO_STATUS" -o -z "$GITHUB_COMMIT_STATUS_URL" ]; then
    echo "Missing required GITHUB_TOKEN_REPO_STATUS or GITHUB_COMMIT_STATUS_URL in env, exiting:"
    env
    exit 1
fi

# check build-level args
if [ $# -lt 3 ]; then
    syntax
    exit 1
fi

STATE=$1
shift
CONTEXT_LABEL=$1
shift
MESSAGE=$@

case "$STATE" in
    pending)
        ;; # OK!
    success)
        ;; # OK!
    error)
        ;; # OK!
    failure)
        ;; # OK!
    *)
        echo "Unsupported state value: $STATE"
        syntax
        exit 1
        ;;
esac

set_repo_path() {
    if [ -z "$GIT_REPOSITORY_ROOT" ]; then
        # use pwd as the git root
        GIT_CMD_REPOSITORY_PATH=$(pwd)/.git
    else
        GIT_CMD_REPOSITORY_PATH=${GIT_REPOSITORY_ROOT}/.git
    fi
    if [ ! -d "$GIT_CMD_REPOSITORY_PATH" ]; then
        echo "Directory doesn't exist: ${GIT_CMD_REPOSITORY_PATH}"
        echo "Run this command from the root of the git repo, or provide GIT_REPOSITORY_ROOT"
    fi
}

# get commit id
if [ -n "$GIT_COMMIT_ENV_NAME" ]; then
    # grab commit from specified envvar
    GIT_COMMIT_ID=${!GIT_COMMIT_ENV_NAME}
    if [ -z "$GIT_COMMIT_ID" ]; then
        echo "Unable to retrieve git commit id from envvar named '$GIT_COMMIT_ENV_NAME'. Env is:"
        env
        exit 1
    fi
else
    # fall back to using .git/, which isn't present in teamcity
    set_repo_path
    GIT_COMMIT_ID=$(git --git-dir=$GIT_CMD_REPOSITORY_PATH rev-parse HEAD)
    if [ -z "$GIT_COMMIT_ID" ]; then
        echo "Unable to determine git commit id from $GIT_CMD_REPOSITORY_PATH. Run this script from within the git repo or point GIT_COMMIT_ENV_NAME to the envvar to be used."
        find .
        exit 1
    fi
fi

# get github repo path, if not already provided by the build env (teamcity should manually specify GITHUB_REPO_PATH)
if [ -z "$GITHUB_REPO_PATH" ]; then
    # extract commit id and project path on the fly from the repo itself
    # expected formats:
    # - 'https://github.com/mesosphere/kafka-private' => mesosphere/kafka-private
    # - 'git@github.com:mesosphere/kafka-private.git' => mesosphere/kafka-private
    # how it's implemented: this specifically avoids using busybox's sed, and does something kludgy in grep instead:
    # cut the string down to [:/]username/repo-name, then trim off the : or / at the start of the string
    set_repo_path
    GITHUB_REPO_PATH=$(git --git-dir=$GIT_CMD_REPOSITORY_PATH config remote.origin.url | grep -E -o '[/:][a-zA-Z0-9-]+/[a-zA-Z0-9-]+' | awk '{ print substr($1,2) }')
    if [ -z "$GITHUB_REPO_PATH" ]; then
        echo "Unable to determine repo path from $GIT_CMD_REPOSITORY_PATH => $(git --git-dir=$GIT_CMD_REPOSITORY_PATH config remote.origin.url). Run this script from within the git repo or specify GITHUB_REPO_PATH"
        find .
        exit 1
    fi
fi

URL="https://api.github.com/repos/${GITHUB_REPO_PATH}/commits/${GIT_COMMIT_ID}/statuses"
echo $URL

read -r -d '' JSON_PAYLOAD << EOF || true
{
  "state": "$STATE",
  "context": "$CONTEXT_LABEL",
  "description": "$MESSAGE",
  "target_url": "$GITHUB_COMMIT_STATUS_URL"
}
EOF
echo $JSON_PAYLOAD

echo $JSON_PAYLOAD | curl -u $GITHUB_TOKEN_REPO_STATUS -X POST --data @- $URL
