# 🧹 Chính sách Làm sạch Dữ liệu & Xử lý Cờ Chất lượng NOAA (Data Cleaning Policy)

## 1. Quy ước Giá trị Khuyết (Missing Values) của NOAA ISD
Trong bộ dữ liệu NOAA Global Hourly, các giá trị bị khuyết không để trống (`null`) mà được mã hóa bằng các số đặc biệt cần phải chuyển về `NULL` trong quá trình Spark ETL:
* **Nhiệt độ không khí (Air Temperature - `TMP`) & Điểm sương (`DEW`):** Giá trị `+9999` hoặc `9999` $\rightarrow$ Chuyển thành `NULL`.
* **Tốc độ gió (Wind Speed - `WND`):** Giá trị `9999` $\rightarrow$ Chuyển thành `NULL`.
* **Áp suất khí quyển (`SLP`):** Giá trị `99999` $\rightarrow$ Chuyển thành `NULL`.
* **Lượng mưa (`AA1` - Liquid Precipitation):** Giá trị `9999` $\rightarrow$ Chuyển thành `NULL`.

## 2. Chính sách Xử lý Cờ Chất lượng (NOAA Quality Control Flags)
Mỗi trường đo lường chính trong NOAA ISD đều đi kèm 1 ký tự cờ chất lượng (Quality Code):
* **Các mã cờ hợp lệ (Giữ lại):**
  * `0`: Passed gross limits check
  * `1`: Passed all quality control checks
  * `4`: Passed gross limits check, data originate from an NCEI data source
  * `5`: Passed all quality control checks, data originate from an NCEI data source
  * `9`: Passed gross limits check if element is present
* **Các mã cờ lỗi/nghi ngờ (Loại bỏ hoặc gán `NULL` cho giá trị đo đó):**
  * `2` (Suspect), `3` (Erroneous), `6` (Suspect - NCEI), `7` (Erroneous - NCEI).

## 3. Quy tắc Chuẩn hóa Đơn vị (Unit Normalization)
Dữ liệu gốc của NOAA lưu dưới dạng số nguyên có hệ số nhân (scaling factor = 10):
* `temperature_celsius = raw_temp / 10.0` (Đơn vị: °C)
* `dew_point_celsius = raw_dew / 10.0` (Đơn vị: °C)
* `wind_speed_ms = raw_wind_speed / 10.0` (Đơn vị: m/s)
* `sea_level_pressure_hpa = raw_slp / 10.0` (Đơn vị: hPa)
* `precipitation_mm = raw_precip_depth / 10.0` (Đơn vị: mm)

## 4. Tích hợp Station Metadata (`isd-history.csv`)
* Khóa kết nối (Join Key): Kết hợp `USAF` (6 ký tự) + `WBAN` (5 ký tự) tạo thành mã định danh trạm duy nhất `station_id`.
* Loại bỏ các trạm không có tọa độ địa lý hợp lệ (`LAT` hoặc `LON` bị `NULL` hoặc `= 0.0, 0.0` ngoài đại dương nếu không phải trạm phao).