#!/usr/bin/env bash
# Copyright 2019 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -x

echo "please to use glocud make sure you completed authentication"
echo "gcloud config set project templates-user"
echo "gcloud auth application-default login"

PROJECT_ID=df-vision-api-test
JOB_NAME="vision-api-pipeline-`date +%Y%m%d-%H%M%S-%N`"
echo JOB_NAME=$JOB_NAME
GCS_IMAGE_FILE_PATH=gs://vision-api-data-test/*.jpeg
BIGQUERY_DATASET=VISION_APIS_DATASET
# Optional Parameter - By default LABEL_DETECTION is used
FEATURE_TYPE='{\"featureConfig\":[{\"type\":\"FACE_DETECTION\"},{\"type\":\"LANDMARK_DETECTION\"}]}'
# change only when you created your own image
DYNAMIC_TEMPLATE_BUCKET_SPEC=gs://vision-api-data-test/dynamic_template_spec/dynamic_template_vison_api.json
# change only when you created your own image
GCS_STAGING_LOCATION=gs://vision-api-data-test/dynamic_template_spec/log
PARAMETERS_CONFIG='{  
   "jobName":"'$JOB_NAME'",
   "parameters":{  
      "project":"'${PROJECT_ID}'",
      "inputFilePattern":"'${GCS_IMAGE_FILE_PATH}'",
      "datasetName":"'${BIGQUERY_DATASET}'",
      "visionApiProjectId":"'${PROJECT_ID}'",
	  "featureType":"'${FEATURE_TYPE}'",
      "region":"us-central-1",
      "experiments":"enable_streaming_engine",
      "workerMachineType":"n1-standard-4",
      "autoscalingAlgorithm":"THROUGHPUT_BASED",
      "maxNumWorkers":"15"
   }
}'

API_ROOT_URL="https://dataflow.googleapis.com"
TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT_ID}/templates:launch"
curl -X POST -H "Content-Type: application/json" \
 -H "Authorization: Bearer $(gcloud auth print-access-token)" \
 "${TEMPLATES_LAUNCH_API}"`
 `"?validateOnly=false"`
 `"&dynamicTemplate.gcsPath=${DYNAMIC_TEMPLATE_BUCKET_SPEC}"` \
 `"&dynamicTemplate.stagingLocation=${GCS_STAGING_LOCATION}" \
 -d "${PARAMETERS_CONFIG}"
 
