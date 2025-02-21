#!/bin/bash
#docker build . -t "${DOCKER_IMAGE_TAG}" &&
#  CID=$(docker create "${DOCKER_IMAGE_TAG}") &&
#  echo "$CID" &&
#  docker cp "${CID}":/var/task/package/lambda_package.zip ./lambda_package.zip &&
#  docker rm "${CID}" &&

python create_package.py
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

echo "Deploying lambda..."
if [[  -n $AWS_PROFILE ]]; then
  aws lambda update-function-code \
  --profile "${AWS_PROFILE}" --region=us-west-2 \
  --function-name "arn:aws:lambda:us-west-2:${AWS_ACCOUNT_ID}:function:${STACK_PREFIX}-rds-lambda" \
  --zip-file fileb://ghrc_rds_lambda_package.zip --publish
fi