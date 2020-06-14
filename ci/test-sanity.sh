#!/usr/bin/env bash
set -e

cd "$(dirname "$0")/.."

source ci/_

# Look for failed mergify.io backports by searching leftover conflict markers
# Also check for any trailing whitespaces!
if [[ -n $BUILDKITE_PULL_REQUEST_BASE_BRANCH ]]; then
  base_branch=$BUILDKITE_PULL_REQUEST_BASE_BRANCH
else
  base_branch=$BUILDKITE_BRANCH
fi
_ git show $(git merge-base HEAD "origin/$base_branch")..HEAD --check --oneline

_ ci/nits.sh
_ ci/check-ssh-keys.sh

echo --- ok
