#!/usr/bash
if [ -f env.sh ]; then
  source env.sh
else
  echo "WARNING: No env.sh file configured. Will try to use existing environment variables."
  sleep 3
fi
FUNCTION_NAME=arn:aws:lambda:"${AWS_REGION}":"${AWS_ACCOUNT_ID}":function:"${STACK_PREFIX}"-rds-lambda && \
docker build . -t ghrc-rds && \
CID=$(docker create ghrc-rds) && \
package_name="ghrc_rds_lambda.zip" && \
docker cp "${CID}":/${package_name} ./${package_name} && \
docker rm "${CID}" && \
if [[ -n ${AWS_PROFILE} ]]; then
  aws lambda update-function-code --profile "${AWS_PROFILE}" --region=${AWS_REGION} --function-name ${FUNCTION_NAME} --zip-file fileb://${package_name} --publish
fi
