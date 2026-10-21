# 🌍 Global Weather and Extreme-Event Data Platform (Project 6)

> **Học phần:** Big Data Storage and Processing — SoICT, HUST  
> **Chủ đề:** Project 6 — Global Weather and Extreme-Event Data Platform  
> **Thời lượng:** 12 tuần | **Quy mô nhóm:** 3 sinh viên  

---

## 1. Giới thiệu & Bài toán (Business Question)
Các quan trắc khí tượng toàn cầu tạo ra tập dữ liệu không gian - thời gian (spatio-temporal) khổng lồ với nhiều giá trị khuyết (missing values), cờ kiểm soát chất lượng (quality flags), tần suất lấy mẫu không đồng đều và siêu dữ liệu trạm đo (station metadata).

**Câu hỏi cốt lõi:** *Làm thế nào để một nền tảng phân tán có khả năng mở rộng (scalable distributed platform) xử lý hiệu quả dữ liệu quan trắc thời tiết toàn cầu trong thời gian dài và phát hiện chính xác các dị thường (anomalies) cũng như hiện tượng thời tiết cực đoan theo không gian và thời gian?*

* **Nguồn dữ liệu chính:** [NOAA Integrated Surface Database (ISD) - Global Hourly](https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database) kết hợp với bảng `Station Metadata` (`isd-history.csv`).
* **Quy mô dữ liệu mục tiêu:** **30 GB – 80 GB** (Giai đoạn 2000–2025 hoặc tập con trạm đo toàn cầu đảm bảo bài toán xử lý phân tán thực thụ).

---

## 2. Kiến trúc Hệ thống (Reference Architecture)

Hệ thống được triển khai hoàn toàn trên **Kubernetes (K8s)** theo luồng End-to-End:

    [NOAA ISD Bulk Data + Station Metadata]
                      │
                      ▼
         [Airflow / K8s Ingestion Jobs]
                      │
                      ▼
       [MinIO Object Storage (Raw Layer)]   <── Lưu trữ bất biến (Immutable Raw Archives)
                      │
                      ▼
           [Apache Spark (Distributed)]     <── ETL, Xử lý cờ NOAA, Chuẩn hóa đơn vị, Join Metadata
                      │
                      ▼
       [Apache Iceberg (Curated/Silver)]    <── Lưu trữ dạng cột, Partitioning Không gian - Thời gian
                      │
                      ▼
           [Trino (Distributed SQL)]        <── Tầng Serving & Truy vấn OLAP tương tác
                      │
                      ▼
         [Apache Superset / Grafana]        <── Dashboard & Bản đồ Spatio-Temporal

---

## 3. Phân công Vai trò & Trách nhiệm (Team Roles)

| Thành viên | Họ và Tên | Email / GitHub | Vai trò chính (Role) | Trọng trách cốt lõi |
| :--- | :--- | :--- | :--- | :--- |
| **TV1 (Lead)** | *[Điền tên bạn]* | *[Điền email]* | **Platform, K8s & Storage Architect** | Cụm K8s, MinIO, Spark Cluster, Iceberg Catalog; Thiết kế Partitioning; Chủ trì thí nghiệm Storage, Scale-out ($1\rightarrow2\rightarrow4$ workers) & Fault-tolerance (Kill worker). |
| **TV2** | *[Điền tên TV2]* | *[Điền email]* | **Data Ingestion & Spark ETL Engineer** | Pipeline tải tự động NOAA; Tài liệu chính sách làm sạch & xử lý cờ chất lượng NOAA (Quality Flags); Spark ETL (Raw $\rightarrow$ Parquet/Iceberg) & Join Station Metadata. |
| **TV3** | *[Điền tên TV3]* | *[Điền email]* | **Analytics, Serving & BI Engineer** | Triển khai Trino & Superset trên K8s; Viết Spark Aggregation & 5 nhóm truy vấn Spatio-Temporal SQL; Tính toán Baseline dị thường lịch sử; Xây dựng Dashboard & đo Query Latency. |

---

## 4. Cấu trúc Thư mục (Repository Structure)

    ├── docs/                            # Tài liệu thiết kế, Cleaning Policy & kết quả Benchmark
    ├── k8s/                             # Manifests/Helm charts triển khai K8s (MinIO, Spark, Trino, Superset)
    ├── ingestion/                       # Script & DAGs tải dữ liệu tự động từ NOAA lên MinIO Raw
    ├── spark-jobs/                      # Code PySpark/Scala: ETL, Aggregation và Partitioning Experiments
    ├── serving-analytics/               # Các truy vấn SQL trên Trino & file cấu hình Dashboard Superset
    └── benchmarks/                      # Script tự động chạy thực nghiệm Scale-out, Storage & Fault-recovery

---

## 5. Lộ trình 12 Tuần (Milestones)

- [ ] **W1:** Chốt đề tài, câu hỏi nghiên cứu, khảo sát cấu trúc file NOAA ISD & Station Metadata.
- [ ] **W2:** Thiết kế kiến trúc K8s, mô hình dữ liệu Iceberg, ước lượng dung lượng & kế hoạch thực nghiệm.
- [ ] **W3–W4:** Dựng môi trường K8s phân tán, MinIO, pipeline Ingestion tự động & lưu trữ tầng Raw.
- [ ] **W5:** Chính sách làm sạch dữ liệu (NOAA quality flags), thiết kế schema & lưu trữ tầng Curated.
- [ ] **W6–W7:** Xử lý phân tán với Spark (ETL, chuẩn hóa đơn vị, Join trạm đo, tổng hợp theo thời gian/vùng).
- [ ] **W8:** Xây dựng tầng Serving (Trino) và 5 nhóm truy vấn phân tích bắt buộc.
- [ ] **W9:** Xây dựng Dashboard (Superset) tích hợp biểu đồ Không gian - Thời gian & ghép nối End-to-End.
- [ ] **W10:** Thực nghiệm khả năng mở rộng (Scale-out $1\rightarrow2\rightarrow4$ workers, Data scale, Storage/Partition).
- [ ] **W11:** Thực nghiệm chịu lỗi (Kill Spark executor đang chạy) và phân tích khả năng phục hồi.
- [ ] **W12:** Hoàn thiện báo cáo kỹ thuật, đóng gói hướng dẫn chạy lại (Reproducible Demo) & Thuyết trình.

---

## 6. Danh mục Phân tích & Thực nghiệm Bắt buộc

### 6.1. Mandatory Analytics (5 bài toán phân tích)
1. Độ phủ trạm quan trắc theo thời gian; phân phối nhiệt độ và lượng mưa.
2. Thống kê theo mùa (Seasonal statistics) và so sánh giữa các khu vực địa lý.
3. Thống kê tần suất các hiện tượng nhiệt độ cực đoan và thời tiết khắc nghiệt (dựa trên dữ liệu đạt chuẩn cờ chất lượng).
4. Phát hiện dị thường tại cấp độ trạm đo (Station-level anomalies) so với đường cơ sở lịch sử/theo mùa (Historical/Seasonal baseline).
5. Truy vấn và trực quan hóa kết hợp cả 2 chiều Không gian và Thời gian (Spatio-temporal visualization).

### 6.2. System Experiments (5 kịch bản thực nghiệm)
1. **Storage Format:** So sánh `Raw` vs `Parquet` vs `Iceberg` (Dung lượng, thời gian ETL, Scan throughput, Query latency).
2. **Partitioning:** So sánh `year/month` vs `year/month/region` (hoặc `spatial bucket` H3/Geohash); phân tích hiện tượng **Over-partitioning**.
3. **Scale-out:** Chạy tác vụ tổng hợp thời gian lớn trên $1 \rightarrow 2 \rightarrow 4$ Spark workers (Đo Runtime, Throughput, Speedup, CPU/RAM).
4. **Data Scale:** Lặp lại thực nghiệm với phạm vi số năm hoặc số lượng trạm tăng dần (ví dụ: $5\text{ GB} \rightarrow 20\text{ GB} \rightarrow 50\text{ GB}$).
5. **Fault Tolerance:** Chủ động chấm dứt (terminate) 1 Spark executor khi đang chạy job nặng; phân tích quá trình retry, recomputation và tính toàn vẹn dữ liệu.

---

## 7. Quy tắc Làm việc Nhóm trên Git (Git Workflow)
1. **Không push trực tiếp lên nhánh `main`**.
2. Mỗi thành viên khi làm task sẽ tạo nhánh riêng theo cú pháp:
   * `feat/tv1-<ten-task>` (Ví dụ: `feat/tv1-k8s-minio-setup`)
   * `feat/tv2-<ten-task>` (Ví dụ: `feat/tv2-noaa-ingestion`)
   * `feat/tv3-<ten-task>` (Ví dụ: `feat/tv3-trino-queries`)
3. Sau khi hoàn thành, tạo **Pull Request (PR)** vào `main` và tag ít nhất 1 thành viên khác vào Review trước khi Merge.