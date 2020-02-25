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
--> train data select
CREATE OR replace TABLE network_logs.train_data AS
                        (
                               SELECT *
                               FROM   network_logs.cluster_model_data
                               WHERE  _partitiondate BETWEEN '2019-10-01' AND    '2019-11-18'
                               AND    NOT is_nan(avg_tx_bytes)
                               AND    NOT is_nan(avg_rx_bytes)
                               AND    NOT is_nan(avg_duration)
                        )
                        limit 99999;

--> creeate modelCREATE
OR
replace model network_logs.log_cluster_2 options(model_type='kmeans', num_clusters=4, standardize_features = true) ASSELECT *
EXCEPT
       (transaction_time,subscriber_id, number_of_unique_ips, number_of_unique_ports)
FROM   network_logs.train_data;

--> create normalize table for each centroidCREATE OR replace TABLE network_logs.normalized_centroid_data AS
                        (
                                                WITH centroid_details AS
                                                (
                                                         SELECT   centroid_id,
                                                                  array_agg(struct(feature AS NAME, round(numerical_value,1) AS value) ORDER BY centroid_id) AS cluster
                                                         FROM     ml.centroids(model network_logs.log_cluster_2)
                                                         GROUP BY centroid_id ),
                                                cluster AS
                                                (
                                                         SELECT   centroid_details.centroid_id AS centroid_id,
                                                                  (
                                                                         SELECT value
                                                                         FROM   unnest(cluster)
                                                                         WHERE  NAME = 'number_of_records') AS number_of_records,
                                                                  (
                                                                         SELECT value
                                                                         FROM   unnest(cluster)
                                                                         WHERE  NAME = 'max_tx_bytes') AS max_tx_bytes,
                                                                  (
                                                                         SELECT value
                                                                         FROM   unnest(cluster)
                                                                         WHERE  NAME = 'min_tx_bytes') AS min_tx_bytes,
                                                                  (
                                                                         SELECT value
                                                                         FROM   unnest(cluster)
                                                                         WHERE  NAME = 'avg_tx_bytes') AS avg_tx_bytes,
                                                                  (
                                                                         SELECT value
                                                                         FROM   unnest(cluster)
                                                                         WHERE  NAME = 'max_rx_bytes') AS max_rx_bytes,
                                                                  (
                                                                         SELECT value
                                                                         FROM   unnest(cluster)
                                                                         WHERE  NAME = 'min_rx_bytes') AS min_rx_bytes,
                                                                  (
                                                                         SELECT value
                                                                         FROM   unnest(cluster)
                                                                         WHERE  NAME = 'avg_rx_bytes') AS avg_rx_bytes,
                                                                  (
                                                                         SELECT value
                                                                         FROM   unnest(cluster)
                                                                         WHERE  NAME = 'max_duration') AS max_duration,
                                                                  (
                                                                         SELECT value
                                                                         FROM   unnest(cluster)
                                                                         WHERE  NAME = 'min_duration') AS min_duration,
                                                                  (
                                                                         SELECT value
                                                                         FROM   unnest(cluster)
                                                                         WHERE  NAME = 'avg_duration') AS avg_duration
                                                         FROM     centroid_details
                                                         ORDER BY centroid_id ASC),
                                                predict AS
                                                (
                                                       SELECT *
                                                       FROM   ml.predict(model network_logs.log_cluster_2,
                                                              (
                                                                     SELECT *
                                                                     FROM   network_logs.train_data)))SELECT     c.centroid_id                                                                                                                                                                                                        AS centroid_id,
                                                    (Stddev((p.number_of_records-c.number_of_records)+(p.max_tx_bytes-c.max_tx_bytes)+(p.min_tx_bytes-c.min_tx_bytes)+(p.avg_tx_bytes-c.min_tx_bytes)+(p.max_rx_bytes-c.max_rx_bytes)+(p.min_rx_bytes-c.min_rx_bytes)+(p.avg_rx_bytes-c.min_rx_bytes) +(p.max_duration-c.max_duration)+(p.min_duration-c.min_duration)+(p.avg_duration-c.avg_duration))) AS normalized_dest,
                                                    Any_value(c.number_of_records)                                                                                                                                                                                                        AS number_of_records,
                                                    Any_value(c.max_tx_bytes)                                                                                                                                                                                                        AS max_tx_bytes,
                                                    Any_value(c.min_tx_bytes)                                                                                                                                                                                                        AS min_tx_bytes ,
                                                    Any_value(c.avg_tx_bytes)                                                                                                                                                                                                        AS avg_tx_bytes,
                                                    Any_value(c.max_rx_bytes)                                                                                                                                                                                                        AS max_rx_bytes,
                                                    Any_value(c.min_tx_bytes)                                                                                                                                                                                                        AS min_rx_bytes ,
                                                    Any_value(c.avg_rx_bytes)                                                                                                                                                                                                        AS avg_rx_bytes,
                                                    Any_value(c.avg_duration)                                                                                                                                                                                                        AS avg_duration,
                                                    Any_value(c.max_duration)                                                                                                                                                                                                        AS max_duration ,
                                                    Any_value(c.min_duration)                                                                                                                                                                                                        AS min_duration
                                         FROM       predict                                                                                                                                                                                                        AS p
                                         INNER JOIN cluster                                                                                                                                                                                                        AS c
                                         ON         c.centroid_id = p.centroid_id
                                         GROUP BY   c.centroid_id
                        );