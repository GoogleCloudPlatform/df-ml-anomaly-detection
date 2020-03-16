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

PROJECT_ID=$1
SUBSCRIPTION_ID=$2
BQ_DATASET=$3
DATA_STORAGE_BUCKET=$4
API_KEY=$5
TABLE_SPEC="${PROJECT_ID}:${BQ_DATASET}.cluster_model_data"
CLUSTER_QUERY="gs://${DATA_STORAGE_BUCKET}/normalized_cluster_data.sql"
OUTLIER_TABLE_SPEC="${PROJECT_ID}:${BQ_DATASET}.outlier_data"
SUBSCRIPTION_ID="projects/${PROJECT_ID}/subscriptions/${SUBSCRIPTION_ID}"
BATCH_SIZE="100"
INPUT_FILE_PATTERN="gs://${DATA_STORAGE_BUCKET}/flow_log*.json"
# publicly hosted image
DYNAMIC_TEMPLATE_BUCKET_SPEC="gs://wesp-flow-logs/dynamic_template_secure_log_aggr_template.json"
JOB_NAME="netflow-anomaly-detection-`date +%Y%m%d-%H%M%S-%N`"
echo $JOB_NAME
GCS_STAGING_LOCATION="gs://${DATA_STORAGE_BUCKET}/log"
TEMP_LOCATION="gs://${DATA_STORAGE_BUCKET}/temp"
PARAMETERS_CONFIG='{  
   "jobName":"'$JOB_NAME'",
   "parameters":{  
     	 "streaming": "true",
	 "enableStreamingEngine": "false",
	 "autoscalingAlgorithm": "NONE",
    	 "workerMachineType": "n1-standard-4",
     	 "numWorkers": "5",
    	 "maxNumWorkers": "5",
	 "region": "us-central1",
      	 "tableSpec":"'$TABLE_SPEC'",
	  "batchFrequency":"2",
	  "clusterQuery":"'$CLUSTER_QUERY'",
	  "outlierTableSpec":"'$OUTLIER_TABLE_SPEC'",
	  "windowInterval":"60",
	  "writeMethod":"FILE_LOADS",
	  "subscriberId":"'$SUBSCRIPTION_ID'",
	  "deidTemplateName":"'$DEID_TEMPLATE_NAME'",
	  "batchSize":"'$BATCH_SIZE'",
	  "customGcsTempLocation":"'$TEMP_LOCATION'",
	  "inputFilePattern":"'$INPUT_FILE_PATTERN'" 
	}
}'
DF_API_ROOT_URL="https://dataflow.googleapis.com"
TEMPLATES_LAUNCH_API="${DF_API_ROOT_URL}/v1b3/projects/${PROJECT_ID}/templates:launch"
curl -X POST -H "Content-Type: application/json" \
 -H "Authorization: Bearer ${API_KEY}" \
 "${TEMPLATES_LAUNCH_API}"`
 `"?validateOnly=false"`
 `"&dynamicTemplate.gcsPath=${DYNAMIC_TEMPLATE_BUCKET_SPEC}"` \
 `"&dynamicTemplate.stagingLocation=${GCS_STAGING_LOCATION}" \
 -d "${PARAMETERS_CONFIG}"