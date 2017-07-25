#!/bin/bash

set -e

LATEST_PUBLISHED_VERSION=$(mix hex.search db2kafka | tail -n 1 |  awk '{print $2}')
CURRENT_VERSION=$(mix version)

echo "latest published version: $LATEST_PUBLISHED_VERSION"
echo "current version: $CURRENT_VERSION"

if [ $LATEST_PUBLISHED_VERSION = $CURRENT_VERSION ]; then
  echo "ERROR: The current version set in mix.exs file is the same as the latest published version, please update before merging to master."
  exit 1
fi

if [ "$TRAVIS_PULL_REQUEST" = "false" -a "$TRAVIS_BRANCH" = "master" ]; then
  echo "Configuring hex credentials"
  mix hex.config username "$HEX_USERNAME" && (mix hex.config encrypted_key "$HEX_ENCRYPTED_KEY" > /dev/null 2>&1)

  echo "Pushing to hex..."
  echo -e "$HEX_PASSPHRASE\n" | mix hex.publish
fi
