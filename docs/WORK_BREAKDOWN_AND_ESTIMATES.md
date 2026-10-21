# 📋 Bảng Phân Chia Công Việc & Ước Lượng Thời Gian (12 Tuần)

> **Quy ước ước lượng:** Mỗi thành viên dành trung bình **8–12 giờ/tuần** cho đồ án.  
> **Ký hiệu:** `TV1` (Lead - Infra/Storage/Experiments), `TV2` (Ingestion/ETL/Data Quality), `TV3` (Analytics/Trino/Dashboard).

---

## Giai đoạn 1: Khảo sát, Thiết kế & Hạ tầng (Tuần 1 – Tuần 4)

| Tuần | Mã Task | Công việc cụ thể | Phụ trách | Đầu ra bắt buộc (Deliverables) | Thời gian dự kiến |
| :---: | :---: | :--- | :---: | :--- | :---: |
| **W1** | `T1.1` | Tải mẫu dữ liệu NOAA Global Hourly + `isd-history.csv`, đọc tài liệu định dạng cột & cờ chất lượng của NOAA. | Cả nhóm | Ghi chú cấu trúc cột và thống nhất phạm vi năm/trạm (đạt ~30-60GB). | 4h / người |
| **W1** | `T1.2` | Khởi tạo GitHub Repo, thiết lập Kanban Board và phân quyền. | **TV1** | Repo hoàn chỉnh + Issues trên GitHub Projects. | 2h |
| **W2** | `T2.1` | Vẽ sơ đồ kiến trúc chi tiết trên K8s, ước lượng dung lượng lưu trữ (Raw vs Parquet vs Iceberg). | **TV1** | `docs/architecture_diagram.png` & bảng ước lượng tài nguyên. | 6h |
| **W2** | `T2.2` | Thiết kế Data Model (Schema tầng Raw, Silver/Curated, Gold) & danh sách cờ chất lượng cần lọc. | **TV2** | Bản nháp Schema & quy tắc chuẩn hóa đơn vị đo. | 6h |
| **W2** | `T2.3` | Đặc tả chi tiết 5 bài toán phân tích (công thức tính anomaly baseline, định nghĩa extreme events). | **TV3** | Tài liệu đặc tả công thức toán/thống kê cho 5 câu hỏi phân tích. | 6h |
| **W3-4** | `T3.1` | Dựng cụm Kubernetes, triển khai MinIO (tạo bucket `raw`, `warehouse`) và Spark Operator/Cluster + Iceberg Catalog. | **TV1** | Manifest trong `k8s/minio/`, `k8s/spark/`, `k8s/iceberg-catalog/` chạy thực tế. | 16h |
| **W3-4** | `T3.2` | Viết script tải tự động dữ liệu NOAA bulk files theo năm và đẩy thẳng vào MinIO (`s3a://raw/noaa/`). | **TV2** | Code trong `ingestion/download_noaa.py` + Dockerfile/Airflow DAG. | 14h |
| **W3-4** | `T3.3` | Triển khai thử nghiệm Trino và Apache Superset trên K8s (chạy với dữ liệu mẫu). | **TV3** | Manifest trong `k8s/trino/` và `k8s/superset/`. | 12h |

---

## Giai đoạn 2: Làm sạch Dữ liệu & Xử lý Phân tán với Spark (Tuần 5 – Tuần 7)

| Tuần | Mã Task | Công việc cụ thể | Phụ trách | Đầu ra bắt buộc (Deliverables) | Thời gian dự kiến |
| :---: | :---: | :--- | :---: | :--- | :---: |
| **W5** | `T5.1` | Viết tài liệu chính thức về Data Cleaning Policy (xử lý mã missing `9999`, cờ chất lượng NOAA, chuẩn hóa đơn vị °C, m/s, mm). | **TV2** | Hoàn thiện file `docs/data_cleaning_policy.md`. | 8h |
| **W5** | `T5.2` | Thiết kế DDL cho các bảng Apache Iceberg (bảng Curated chính và các bảng phục vụ so sánh Partitioning). | **TV1** | Script khởi tạo bảng Iceberg trong `spark-jobs/common/`. | 8h |
| **W5** | `T5.3` | Viết hàm tính chỉ số không gian (Geohash hoặc H3 index từ tọa độ `lat`/`lon` của trạm đo) để tích hợp vào ETL. | **TV3** | Module UDF/hàm xử lý không gian trong `spark-jobs/common/`. | 6h |
| **W6-7** | `T6.1` | Viết Spark ETL Job chính: Đọc Raw từ MinIO $\rightarrow$ Parse & Lọc cờ chất lượng $\rightarrow$ Broadcast Join với Station Metadata $\rightarrow$ Ghi ra Parquet & Iceberg. | **TV2** | `spark-jobs/etl/raw_to_parquet.py` và `raw_to_iceberg.py`. | 18h |
| **W6-7** | `T6.2` | Viết Spark Job tạo các bảng Iceberg với 3 kiểu Partitioning: (1) `year/month`, (2) `year/month/region` (hoặc `h3`), (3) Over-partitioned table. | **TV1** | `spark-jobs/experiments/partition_layouts.py`. | 14h |
| **W6-7** | `T6.3` | Viết Spark Batch Aggregation Job tính toán thống kê theo mùa, baseline lịch sử của từng trạm và phát hiện dị thường (Anomalies). | **TV3** | `spark-jobs/analytics/seasonal_anomalies.py`. | 16h |

---

## Giai đoạn 3: Tầng Serving (Trino) & Dashboard (Tuần 8 – Tuần 9)

| Tuần | Mã Task | Công việc cụ thể | Phụ trách | Đầu ra bắt buộc (Deliverables) | Thời gian dự kiến |
| :---: | :---: | :--- | :---: | :--- | :---: |
| **W8** | `T8.1` | Kết nối Trino với Iceberg Catalog trên MinIO, viết và tối ưu hóa 5 nhóm truy vấn SQL bắt buộc. | **TV3** | 5 file SQL trong `serving-analytics/trino_queries/`. | 12h |
| **W8** | `T8.2` | Hỗ trợ TV3 tinh chỉnh cấu hình Memory/Worker của Trino & Spark trên K8s để truy vấn quét dữ liệu lớn không bị OOM. | **TV1** | Cấu hình K8s tối ưu cho Trino & Spark. | 6h |
| **W8** | `T8.3` | Kiểm tra chất lượng dữ liệu đầu ra trên bảng Iceberg (đối chiếu số bản ghi bị lọc bỏ do cờ lỗi để đưa vào báo cáo). | **TV2** | Bảng thống kê Data Quality (tỷ lệ missing/bad quality flags). | 6h |
| **W9** | `T9.1` | Xây dựng Dashboard trên Superset/Grafana: biểu đồ độ phủ trạm, phân phối nhiệt/mưa, cực trị và bản đồ Spatio-Temporal. | **TV3** | Dashboard hoạt động + export file cấu hình vào `dashboard_exports/`. | 12h |
| **W9** | `T9.2` | Chạy kiểm thử luồng End-to-End từ lúc tải dữ liệu $\rightarrow$ Spark ETL $\rightarrow$ Iceberg $\rightarrow$ Trino $\rightarrow$ Dashboard. | **TV1 & TV2** | Pipeline chạy thông suốt không lỗi. | 8h |

---

## Giai đoạn 4: Thực nghiệm Hệ thống (Benchmarks) & Báo cáo (Tuần 10 – Tuần 12)

| Tuần | Mã Task | Công việc cụ thể | Phụ trách | Đầu ra bắt buộc (Deliverables) | Thời gian dự kiến |
| :---: | :---: | :--- | :---: | :--- | :---: |
| **W10** | `T10.1` | **Thí nghiệm 1 & 2 (Storage & Partitioning):** Đo dung lượng, thời gian ETL, tốc độ quét và Query Latency giữa Raw vs Parquet vs Iceberg và giữa các kiểu Partition (trích xuất `EXPLAIN ANALYZE`). | **TV1 & TV3** | Bảng số liệu + biểu đồ so sánh + log Execution Plan trong `docs/experiment_results/`. | 12h |
| **W10** | `T10.2` | **Thí nghiệm 3 & 4 (Scale-out & Data Scale):** Chạy job tổng hợp trên $1 \rightarrow 2 \rightarrow 4$ Spark workers và trên các mốc dung lượng $5\text{GB} \rightarrow 20\text{GB} \rightarrow 50\text{GB}$. Đo Runtime, Speedup, CPU/RAM. | **TV1 & TV2** | Script `benchmarks/run_scale_out_test.sh` + Biểu đồ Speedup/Throughput. | 12h |
| **W11** | `T11.1` | **Thí nghiệm 5 (Fault Tolerance):** Chạy job nặng trên 4 workers, dùng `kubectl delete pod` xóa 1 Spark executor giữa chừng. Thu thập Spark UI DAG, thời gian phục hồi và kiểm tra tính nhất quán dữ liệu. | **TV1** | Script `benchmarks/kill_worker_fault_test.sh` + Ảnh chụp Spark UI Stages retry + Phân tích. | 10h |
| **W11** | `T11.2` | Tổng hợp toàn bộ số liệu thực nghiệm, vẽ biểu đồ chuẩn bị cho báo cáo cuối kỳ. | **TV2 & TV3** | Bộ biểu đồ đánh giá hiệu năng hệ thống. | 8h |
| **W12** | `T12.1` | Viết Báo cáo kỹ thuật cuối kỳ, hoàn thiện hướng dẫn cài đặt (Reproducible Setup Guide), làm Slide và tập duyệt Demo (15-20 phút). | Cả nhóm | Báo cáo PDF, Slide thuyết trình, Kịch bản Demo trực tiếp. | 10h / người |