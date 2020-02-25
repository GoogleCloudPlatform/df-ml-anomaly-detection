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
with train_info as (select iteration, centroid_id, cluster_radius
from ML.TRAINING_INFO (model network_logs.log_cluster_1) 
cross join unnest(cluster_info) as train_info ),
centroid_details as (
select 
centroid_id,
array_agg(STRUCT(feature AS name, round(numerical_value,1) AS value) order by centroid_id) AS cluster
from ML.CENTROIDS(model network_logs.log_cluster_1)
group by centroid_id
),
cluster as (select centroid_details.centroid_id as centroid_id,
(select value from unnest(cluster) WHERE name = 'number_of_unique_ips') AS number_of_unique_ips,
(select value from unnest(cluster) WHERE name = 'number_of_unique_ports') AS number_of_unique_ports,
(select value from unnest(cluster) WHERE name = 'number_of_records') AS number_of_records,
(select value from unnest(cluster) WHERE name = 'max_tx_bytes') AS max_tx_bytes,
(select value from unnest(cluster) WHERE name = 'min_tx_bytes') AS min_tx_bytes,
(select value from unnest(cluster) WHERE name = 'avg_tx_bytes') AS avg_tx_bytes,
(select value from unnest(cluster) WHERE name = 'max_rx_bytes') AS max_rx_bytes,
(select value from unnest(cluster) WHERE name = 'min_rx_bytes') AS min_rx_bytes,
(select value from unnest(cluster) WHERE name = 'avg_rx_bytes') AS avg_rx_bytes,
(select value from unnest(cluster) WHERE name = 'max_duration') AS max_duration,
(select value from unnest(cluster) WHERE name = 'min_duration') AS min_duration,
(select value from unnest(cluster) WHERE name = 'avg_duration') AS avg_duration
from centroid_details
order by centroid_id asc)
select cluster.centroid_id, train_info.cluster_radius, cluster.* except(centroid_id) from cluster
left join train_info ON train_info.centroid_id = cluster.centroid_id
where train_info.iteration = (select max(iteration) from train_info)
