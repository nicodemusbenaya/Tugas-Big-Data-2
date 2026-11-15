-- Kueri dataset dashboard. Sesuaikan nama katalog jika perlu.
SELECT pickup_date, pickup_zone, borough,
       hour(pickup_hour) AS jam,
       element_at(array('7 Min','1 Sen','2 Sel','3 Rab','4 Kam','5 Jum','6 Sab'),
                  dayofweek(pickup_date)) AS hari,
       jumlah_penjemputan, jumlah_durasi_valid, total_menit_durasi_valid
FROM workspace.nyc_yellow_taxi.yellow_taxi_gold_hourly_zone;
