# Tugas 2 Big Data: NYC Yellow Taxi

Proyek ini mengolah data perjalanan [NYC Taxi & Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) Januari–Februari 2025 di Databricks Free Edition. Job menjalankan Bronze → Silver → Gold secara berurutan. Tabel Gold menjadi sumber dashboard penjemputan per jam dan zona, sedangkan tabel Silver menjadi sumber pelatihan model prediksi durasi perjalanan.

## Menjalankan proyek di Databricks Free Edition

Impor `Pasang NYC Yellow Taxi.py` dari halaman **Workspace** ke folder Home Databricks. Buka notebook hasil impor, pilih **Serverless**, lalu klik **Run all**. Notebook menyiapkan folder proyek, schema `nyc_yellow_taxi`, volume `tlc_landing`, serta Job ETL manual. Nilai `run_demo=true` menjalankan uji Januari, Januari ulang, penambahan Februari, dan kasus parameter tidak valid. Setelah itu notebook menerbitkan dashboard Gold serta menjalankan pelatihan dan uji prediksi. Tautan Job, run, dashboard, dan hasil eksperimen muncul di output.

Jika ETL untuk kedua bulan sudah sukses, atur `run_demo=model` pada notebook instalasi dan jalankan kembali. Mode ini memperbarui notebook model dan menjalankan eksperimen MLflow dari tabel Silver yang sudah ada. Notebook menetapkan registry `databricks-uc` sebelum membuka eksperimen, memeriksa argumen yang didukung fungsi `log_model`, dan menyimpan versi pustaka bersama artefak model agar lingkungan Model Serving dapat dibangun kembali.

Jika akses HTTPS menuju TLC gagal, unggah kedua Parquet dan CSV resmi ke root volume `tlc_landing` yang tercetak pada notebook. Ubah widget `source_mode` menjadi `volume`, lalu jalankan ulang notebook. [Parquet Januari](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet) · [Parquet Februari](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-02.parquet) · [Lookup zona](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv)

Untuk menjalankan hanya setup, pilih `run_demo=false`. Untuk menjalankan eksplorasi, buka `01_eksplorasi` setelah Bronze memiliki data dan pilih `batch_month` yang ingin dilihat. Notebook `06_validasi_pipeline` menampilkan rekonsiliasi bulan dan memeriksa keunikan grain Gold.

- [notebooks/](notebooks/) berisi notebook Databricks untuk konfigurasi, eksplorasi, Bronze, Silver, Gold, pelatihan, dan validasi. Transformasi ETL memakai PySpark DataFrame API.
- [queries/](queries/) memuat SQL yang dipakai untuk eksplorasi Bronze dan dataset dashboard Gold. Jalankan SQL eksplorasi dalam satu sesi. Sesuaikan nama katalog dan bulan sesuai data yang tersedia.
- [setup_source.py](setup_source.py) adalah templat instalasi, [dashboard.py](dashboard.py) mendefinisikan dashboard, dan [Pasang NYC Yellow Taxi.py](Pasang%20NYC%20Yellow%20Taxi.py) adalah notebook instalasi lengkap yang siap diimpor.

Tautan Databricks di bawah merujuk pada workspace tempat demo dijalankan. Akses ke masing-masing aset mengikuti izin Databricks. [Notebook validasi di Databricks](https://dbc-ccd8bd5d-63c8.cloud.databricks.com/editor/notebooks/1286134211489946?o=7474653175709708).

## Bukti pengujian ETL

| Batch | Baris Bronze | Baris Silver | Baris agregat Gold | Penjemputan Gold | Pickup di luar bulan | Durasi dikecualikan |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Januari 2025 | 3.475.226 | 3.475.204 | 97.014 | 3.475.204 | 22 | 3.428 |
| Februari 2025 | 3.577.543 | 3.577.512 | 102.362 | 3.577.512 | 31 | 6.465 |
| Total | 7.052.769 | 7.052.716 | 199.376 | 7.052.716 | 53 | 9.893 |

- Januari pertama mengambil 3.475.226 baris melalui HTTPS. Silver gagal karena mode penulisan awal `errorifexists` ditolak Serverless dan Gold berhenti sesuai dependensi. Versi berikutnya memakai `append` untuk penulisan pertama. [Run awal Januari](https://dbc-ccd8bd5d-63c8.cloud.databricks.com/jobs/457408589023606/runs/184908582411181)
- Pada dua run Januari berikutnya, semua task sukses dan Bronze menulis 0 baris. Setelah penambahan 3.577.543 baris Februari melalui HTTPS, snapshot Januari tetap sama, termasuk 97.014 baris Gold dan 50.674.222,617 total menit dari 3.471.776 durasi valid. [Pemulihan Januari](https://dbc-ccd8bd5d-63c8.cloud.databricks.com/jobs/457408589023606/runs/614766274671176) · [Rerun Januari](https://dbc-ccd8bd5d-63c8.cloud.databricks.com/jobs/457408589023606/runs/940732363299144) · [Run penambahan Februari](https://dbc-ccd8bd5d-63c8.cloud.databricks.com/jobs/457408589023606/runs/967936634518087)
- Uji parameter `2025-13` sengaja menghasilkan `month must be in 1..12` pada Bronze. Silver dan Gold berstatus `UPSTREAM_FAILED`, sesuai dependensi Job. [Run uji parameter](https://dbc-ccd8bd5d-63c8.cloud.databricks.com/jobs/457408589023606/runs/1072916802118429)
- Databricks mengembalikan tautan dashboard terbit. [Dashboard Operasi NYC Yellow Taxi](https://dbc-ccd8bd5d-63c8.cloud.databricks.com/dashboardsv3/01f1e0cb281d14abb825d5f3963ba4ff/published?o=7474653175709708)

## Bukti evaluasi dan Model Serving

| Model | MAE validasi (menit) | MAE uji (menit) | RMSE uji (menit) | ID run MLflow |
| --- | ---: | ---: | ---: | --- |
| Median | 7,438 | 7,598 | 11,939 | `184c4603bf064b09822f6713d70f28d0` |
| Ridge | 6,652 | 6,828 | 9,907 | `f4900fa2f9c6481586fbaf81142f9c1c` |

Ridge terpilih dari MAE validasi dan mengurangi MAE uji 10,1% dibanding baseline median. Task model sukses dalam 40 detik. [Run pelatihan dan prediksi batch](https://dbc-ccd8bd5d-63c8.cloud.databricks.com/?o=7474653175709708#job/814360459594581/run/1721480532675)

Ridge terdaftar sebagai versi 1 di Unity Catalog. Model Serving mengembalikan `SUCCESS` dengan lima respons yang sama dengan prediksi batch. Contoh untuk demo memakai `pickup_zone_id=186`, `pickup_hour_of_day=17`, dan `pickup_day_of_week=3`. Prediksi 15,54 menit, durasi aktual 13,60 menit, dan galat absolut 1,94 menit. [Ridge versi 1](https://dbc-ccd8bd5d-63c8.cloud.databricks.com/explore/data/models/workspace/nyc_yellow_taxi/durasi_penjemputan/version/1?o=7474653175709708) · [Endpoint](https://dbc-ccd8bd5d-63c8.cloud.databricks.com/ml/endpoints/tugas2-nyc-yellow-taxi-duration)
