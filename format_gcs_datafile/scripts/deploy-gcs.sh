#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SOURCE_DIR="${DIR}/../src"

source "${DIR}/.env.local"

gcloud functions \
  deploy ${FUNCTION_NAME} \
  --source=${SOURCE_DIR} \
  --runtime=python38 \
  --trigger-resource{${BUCKET_NAME}} \
  --trigger-event google.storage.object.finalize