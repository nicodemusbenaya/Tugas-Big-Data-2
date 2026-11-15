-- SQL dari notebook 01_eksplorasi. Sesuaikan katalog dan bulan jika perlu.
CREATE OR REPLACE TEMP VIEW yellow_taxi_exploration AS
SELECT *
FROM workspace.nyc_yellow_taxi.yellow_taxi_bronze
WHERE batch_month = '2025-01';

SELECT CAST(tpep_pickup_datetime AS DATE) AS tanggal, COUNT(*) AS penjemputan
FROM yellow_taxi_exploration
GROUP BY 1
ORDER BY 1;

WITH durations AS (
  SELECT timestampdiff(MICROSECOND, tpep_pickup_datetime, tpep_dropoff_datetime)
         / 60000000.0 AS menit
  FROM yellow_taxi_exploration
)
SELECT COUNT(menit) AS durasi_tersedia, MIN(menit) AS min_menit,
       MAX(menit) AS max_menit, AVG(menit) AS avg_menit
FROM durations;
