--> train data select
create or replace table network_logs.train_data as (select * from network_logs.cluster_model_data 
where _PARTITIONDATE between '2019-10-01' AND '2019-11-18' and not IS_NAN(avg_tx_bytes) and not IS_NAN(avg_rx_bytes) and not IS_NAN(avg_duration))  limit 99999;
--> creeate model
create OR replace model network_logs.log_cluster_2 options(model_type='kmeans', num_clusters=4, standardize_features = true) AS
select * except (transaction_time,subscriber_id, number_of_unique_ips, number_of_unique_ports) from network_logs.train_data;
--> create normalize table for each centroid
create or replace table network_logs.normalized_centroid_data as(
with centroid_details AS (
select 
centroid_id,
array_agg(struct(feature as name, round(numerical_value,1) as value) order by centroid_id) AS cluster
from ML.CENTROIDS(model network_logs.log_cluster_2)
group by centroid_id
),
cluster as (select centroid_details.centroid_id as centroid_id,
(select value from unnest(cluster) where name = 'number_of_records') AS number_of_records,
(select value from unnest(cluster) where name = 'max_tx_bytes') AS max_tx_bytes,
(select value from unnest(cluster) where name = 'min_tx_bytes') AS min_tx_bytes,
(select value from unnest(cluster) where name = 'avg_tx_bytes') AS avg_tx_bytes,
(select value from unnest(cluster) where name = 'max_rx_bytes') AS max_rx_bytes,
(select value from unnest(cluster) where name = 'min_rx_bytes') AS min_rx_bytes,
(select value from unnest(cluster) where name = 'avg_rx_bytes') AS avg_rx_bytes,
(select value from unnest(cluster) where name = 'max_duration') AS max_duration,
(select value from unnest(cluster) where name = 'min_duration') AS min_duration,
(select value from unnest(cluster) where name = 'avg_duration') AS avg_duration
from centroid_details order by centroid_id asc),
predict as (select * from ML.PREDICT(model network_logs.log_cluster_2, (select * from network_logs.train_data)))
select c.centroid_id as centroid_id, 
(stddev((p.number_of_records-c.number_of_records)+(p.max_tx_bytes-c.max_tx_bytes)+(p.min_tx_bytes-c.min_tx_bytes)+(p.avg_tx_bytes-c.min_tx_bytes)+(p.max_rx_bytes-c.max_rx_bytes)+(p.min_rx_bytes-c.min_rx_bytes)+(p.avg_rx_bytes-c.min_rx_bytes)
+(p.max_duration-c.max_duration)+(p.min_duration-c.min_duration)+(p.avg_duration-c.avg_duration)))
as normalized_dest, any_value(c.number_of_records) as number_of_records,any_value(c.max_tx_bytes) as max_tx_bytes,  any_value(c.min_tx_bytes) as min_tx_bytes , any_value(c.avg_tx_bytes) as avg_tx_bytes,any_value(c.max_rx_bytes) as max_rx_bytes,   any_value(c.min_tx_bytes) as min_rx_bytes ,  any_value(c.avg_rx_bytes) as avg_rx_bytes,  any_value(c.avg_duration) as avg_duration,any_value(c.max_duration) as max_duration ,  any_value(c.min_duration) as min_duration
from predict as p 
inner join cluster as c on c.centroid_id = p.centroid_id
group by c.centroid_id);