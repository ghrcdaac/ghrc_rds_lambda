#!/bin/bash
#docker build . -t "${DOCKER_IMAGE_TAG}" &&
#  CID=$(docker create "${DOCKER_IMAGE_TAG}") &&
#  echo "$CID" &&
#  docker cp "${CID}":/var/task/package/lambda_package.zip ./lambda_package.zip &&
#  docker rm "${CID}" &&

python create_package.py

echo "Deploying lambda..."
if [[  -n $AWS_PROFILE ]]; then
  aws lambda update-function-code \
  --profile "${AWS_PROFILE}" --region=us-west-2 \
  --function-name "${FUNCTION_NAME}" \
  --zip-file fileb://rds_lambda_package.zip --publish
fi