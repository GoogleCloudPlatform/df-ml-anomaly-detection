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
PROJECT_ID='REPLACE_WITH_YOUR_PROJECT_ID'
TEMPLATE_BUCKET='REPLACE_WITH_YOUR_BUCKET'
DF_TEMP_BUCKET='REPLACE_WITH_YOUR_BUCKET'

DYNAMIC_TEMPLATE_BUCKET_SPEC=gs://${TEMPLATE_BUCKET}/dynamic_template_secure_log_aggr_template.json
JOB_NAME="pipeline-`date +%Y%m%d-%H%M%S-%N`"
echo JOB_NAME=$JOB_NAME
# log location
GCS_STAGING_LOCATION=gs://${DF_TEMP_BUCKET}/log
PARAMETERS_CONFIG='{  
   "jobName":"'$JOB_NAME'",
   "parameters":{  
      "streaming":"true",
	  "autoscalingAlgorithm":"NONE",
      "workerMachineType": "n1-standard-8",
      "numWorkers":"50",
      "maxNumWorkers":"50",
      "subscriberId":"projects/${PROJECT_ID}/subscriptions/log-sub",
      "network":"custom-network-1",
      "tableSpec":"${PROJECT_ID}:network_logs.cluster_model_data",
      "subnetwork":"regions/us-central1/subnetworks/custom-network-1",
      "region":"us-central-1",
      "batchFrequency":"5",
      "customGcsTempLocation":"gs://${DF_TEMP_BUCKET}/file_load",
      "usePublicIps":"false",
      "clusterQuery":"gs://${TEMPLATE_BUCKET}/normalized_cluster_data.sql",
      "outlierTableSpec":"custom-network-test:network_logs.outlier_data",
      "windowInterval":"2",
      "tempLocation":"gs://${DF_TEMP_BUCKET}/temp",
      "writeMethod":"FILE_LOADS",
      "diskSizeGb":"500",
      "workerDiskType":"compute.googleapis.com/projects/${PROJECT_ID}/zones/us-central1-b/diskTypes/pd-ssd"
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