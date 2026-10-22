# Hotel Booking Big Data Pipeline

Dự án xây dựng pipeline xử lý dữ liệu đặt phòng khách sạn bằng **Apache Kafka, Hadoop HDFS và Apache Spark** trên Windows, sử dụng Python và Jupyter Notebook.

## 1. Giới thiệu

Pipeline gồm các bước:

1. **Data Ingestion:** Python đọc dữ liệu CSV và gửi từng bản ghi vào Kafka.
2. **Data Storage:** Python Kafka Consumer đọc dữ liệu từ Kafka, chuyển thành JSONL và lưu lên HDFS.
3. **Data Processing:** Spark đọc JSONL từ HDFS, làm sạch dữ liệu, thống kê và ghi kết quả thành Parquet trên HDFS.

### Công nghệ sử dụng

| Công nghệ | Phiên bản tham khảo |
|---|---|
| Windows | Windows 10/11 |
| Java JDK | 17 |
| Python | 3.12 |
| Apache Kafka | 4.3.1 |
| Hadoop | 3.5.0 |
| Apache Spark | 4.2.0 |
| VS Code | Bản hiện hành |
| Jupyter Notebook | Chạy notebook phân tích dữ liệu |

Các phiên bản trên dựa theo môi trường phát triển ban đầu. Cần kiểm tra khả năng tương thích nếu sử dụng phiên bản khác.

## 2. Cấu trúc project

```text
Big-data-project-/
├── data/
│   └── hotel_bookings.csv
├── output/
│   ├── kafka_hotel_booking.jsonl
│   └── kafka_hotel_booking_full.jsonl
├── producer.py
├── kafka_to_hdfs.py
├── spark_process.py
├── hotel_booking_spark.ipynb
├── start_hadoop.ps1
├── run_jupyter.ps1
├── checkpoint.txt
├── .gitignore
└── README.md
```

**Lưu ý:** Thư mục `data/` và `output/` được Git bỏ qua để tránh tải dữ liệu lớn lên GitHub. Cần cung cấp file `data/hotel_bookings.csv` riêng trước khi chạy pipeline.

## 3. Yêu cầu trước khi chạy

Cài đặt Java JDK, Python, Apache Kafka, Hadoop, Apache Spark và VS Code.

Mở PowerShell và kiểm tra:

```powershell
java -version
python --version
git --version
```

Cài các thư viện Python cần thiết:

```powershell
python -m pip install kafka-python pyspark jupyter
```

Kiểm tra dữ liệu đầu vào:

```powershell
Test-Path ".\data\hotel_bookings.csv"
```

Kết quả cần là `True`.

### Cấu hình môi trường

Thiết lập các biến môi trường phù hợp với vị trí cài đặt trên máy:

- `JAVA_HOME`: thư mục JDK 17.
- `HADOOP_HOME`: thư mục Hadoop.
- `SPARK_HOME`: thư mục Spark.
- `PATH`: bổ sung các thư mục `bin` cần thiết.

Đồng thời kiểm tra cấu hình Hadoop:

- `fs.defaultFS`: `hdfs://localhost:9000`
- Replication: `1` đối với môi trường demo một DataNode.

Các đường dẫn trong những script PowerShell hiện tại được thiết lập theo máy phát triển ban đầu. Cần sửa đường dẫn nếu máy mới cài phần mềm ở vị trí khác.

## 4. Bước 1 — Khởi động Kafka

Mở một cửa sổ PowerShell riêng:

```powershell
cd C:\kafka_2.13-4.3.1
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

Giữ cửa sổ này mở trong quá trình chạy pipeline.

Mở một cửa sổ PowerShell khác để kiểm tra broker:

```powershell
cd C:\kafka_2.13-4.3.1
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

Topic chính của pipeline:

```text
hotel_booking_full
```

Nếu topic chưa tồn tại, tạo topic:

```powershell
.\bin\windows\kafka-topics.bat --create --topic hotel_booking_full --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Nếu topic đã tồn tại, không cần tạo lại.

**Lưu ý:** Khi chạy lại producer trên topic cũ, dữ liệu mới có thể được thêm vào dữ liệu cũ. Cần kiểm tra topic trước khi chạy lại để tránh xử lý trùng bản ghi.

## 5. Bước 2 — Khởi động Hadoop HDFS

Mở PowerShell mới và kiểm tra biến môi trường Hadoop, Java. Đường dẫn phải phù hợp với máy hiện tại.

Khởi động HDFS:

```powershell
& "C:\hadoop-3.5.0\sbin\start-dfs.cmd"
```

Kiểm tra các tiến trình Java:

```powershell
jps
```

Kiểm tra tình trạng HDFS:

```powershell
& "C:\hadoop-3.5.0\bin\hdfs.cmd" dfsadmin -report
```

Cần có ít nhất một DataNode ở trạng thái hoạt động.

Kiểm tra thư mục HDFS:

```powershell
& "C:\hadoop-3.5.0\bin\hdfs.cmd" dfs -ls /
```

Nếu NameNode hoặc DataNode chưa hoạt động, hãy xử lý lỗi Hadoop trước khi chuyển sang bước tiếp theo.

**Cảnh báo:** Chỉ format NameNode khi thiết lập môi trường HDFS mới và chắc chắn không có dữ liệu cần giữ. Không format mỗi lần khởi động.

## 6. Bước 3 — Gửi dữ liệu CSV vào Kafka

Mở PowerShell tại thư mục project:

```powershell
cd C:\Big-data-project-
```

Thay đường dẫn ví dụ trên bằng thư mục project thực tế.

Chạy producer:

```powershell
python producer.py
```

Script `producer.py` đọc file `data/hotel_bookings.csv` và gửi từng dòng vào topic `hotel_booking_full`.

Trong quá trình chạy, chương trình in tiến độ sau mỗi 10.000 bản ghi.

Kết quả mong đợi có dạng:

```text
Da gui: 10000 ban ghi
Da gui: 20000 ban ghi
...
Da gui xong ... ban ghi vao Kafka.
```

Số bản ghi phụ thuộc vào dữ liệu CSV đầu vào.

## 7. Bước 4 — Đọc Kafka và lưu dữ liệu lên HDFS

Sau khi producer gửi xong dữ liệu, chạy:

```powershell
python kafka_to_hdfs.py
```

Script sẽ:

1. Đọc dữ liệu từ topic `hotel_booking_full`.
2. Ghi các bản ghi thành file JSONL cục bộ.
3. Tạo thư mục HDFS nếu chưa tồn tại.
4. Đưa file JSONL lên HDFS.

Đường dẫn đầu ra:

**Local:**

```text
output/kafka_hotel_booking_full.jsonl
```

**HDFS:**

```text
/hotel_booking/full_from_kafka/kafka_hotel_booking_full.jsonl
```

Consumer hiện tại dừng khi không nhận thêm tin nhắn trong khoảng thời gian chờ. Vì vậy, hãy đảm bảo producer đã gửi xong dữ liệu và Kafka đang hoạt động trước khi chạy consumer.

Kiểm tra file trên HDFS:

```powershell
& "C:\hadoop-3.5.0\bin\hdfs.cmd" dfs -ls /hotel_booking/full_from_kafka
```

Đếm số dòng trong file JSONL trên máy:

```powershell
(Get-Content ".\output\kafka_hotel_booking_full.jsonl").Count
```

Với file lớn, thao tác đếm bằng PowerShell có thể mất một khoảng thời gian.

## 8. Bước 5 — Xử lý dữ liệu bằng Spark

Dự án có notebook:

```text
hotel_booking_spark.ipynb
```

Mở notebook trong VS Code và chọn Python environment đã cài `pyspark`.

Đường dẫn dữ liệu đầu vào cho **bộ dữ liệu đầy đủ**:

```python
hdfs_input = (
    "hdfs://localhost:9000/"
    "hotel_booking/full_from_kafka/kafka_hotel_booking_full.jsonl"
)

df = spark.read.json(hdfs_input)
```

Các bước xử lý chính:

- Đọc JSONL từ HDFS.
- Kiểm tra số dòng, số cột và schema.
- Chuyển kiểu dữ liệu cho `is_canceled`, `lead_time` và `adr`.
- Loại các bản ghi có `is_canceled` không hợp lệ.
- Tính tổng số lượt đặt phòng, thời gian đặt trước trung bình, giá phòng trung bình và tỷ lệ hủy.
- Thống kê theo loại khách sạn.
- Ghi kết quả sang định dạng Parquet trên HDFS.

Đường dẫn Parquet đầu ra:

```text
/hotel_booking/processed/bookings_parquet
```

Để xác nhận kết quả:

```python
print("Dữ liệu đầu vào:", df.count())
print("Dữ liệu sau clean:", df_clean.count())
print("Dữ liệu Parquet:", df_parquet.count())
```

Trong lần chạy đầy đủ trước đó của dự án, cả ba kết quả đều là `119390`. Kết quả thực tế trên máy mới phụ thuộc vào dữ liệu đầu vào và việc consumer có nhận đủ bản ghi hay không.

### Lưu ý về script Spark

File `spark_process.py` trong repository hiện tham chiếu đến đường dẫn dữ liệu demo cũ:

```text
/hotel_booking/from_kafka/kafka_hotel_booking.jsonl
```

Đường dẫn này khác đường dẫn dữ liệu đầy đủ trong notebook. Nếu chạy `spark_process.py` cho bộ dữ liệu đầy đủ, hãy sửa `HDFS_INPUT` trong script thành:

```python
HDFS_INPUT = (
    "hdfs://localhost:9000/"
    "hotel_booking/full_from_kafka/kafka_hotel_booking_full.jsonl"
)
```

Cần kiểm tra thêm đường dẫn Python, Java, Hadoop và Spark trong `run_jupyter.ps1` trước khi sử dụng script này trên máy khác.

## 9. Kiểm tra kết quả cuối cùng

Danh sách kiểm tra:

- [ ] Kafka broker hoạt động trên cổng `9092`.
- [ ] Topic `hotel_booking_full` tồn tại.
- [ ] Producer gửi dữ liệu thành công.
- [ ] Consumer tạo file JSONL cục bộ.
- [ ] File JSONL đã được tải lên HDFS.
- [ ] Spark đọc được dữ liệu đầy đủ từ HDFS.
- [ ] Dữ liệu sau làm sạch có số dòng hợp lý.
- [ ] Parquet được ghi và đọc lại thành công.

## 10. Các lỗi thường gặp

### Kafka không kết nối được

Kiểm tra broker có chạy không và kiểm tra cổng:

```powershell
Test-NetConnection localhost -Port 9092
```

### HDFS báo Connection refused

Kiểm tra NameNode và DataNode:

```powershell
jps
```

Sau đó kiểm tra:

```powershell
& "C:\hadoop-3.5.0\bin\hdfs.cmd" dfsadmin -report
```

### Không tìm thấy file CSV

Kiểm tra:

```powershell
Test-Path ".\data\hotel_bookings.csv"
```

Nếu trả về `False`, đặt file dữ liệu vào đúng thư mục.

### Spark không đọc được HDFS

Kiểm tra:

- HDFS đang hoạt động.
- File JSONL thực sự tồn tại trên HDFS.
- Đường dẫn trong notebook hoặc script đúng với vị trí file.
- Môi trường Java, Hadoop và Spark đã được thiết lập đúng.

### Số bản ghi không khớp

Đối chiếu số dòng CSV, số bản ghi producer gửi, số bản ghi consumer nhận và số dòng Spark đọc được. Không nên chỉ dựa vào việc script kết thúc mà kết luận dữ liệu đã đầy đủ.

## 11. Tắt các dịch vụ sau khi hoàn thành

Dừng Spark session và đóng Jupyter Notebook sau khi lưu kết quả.

Dừng Kafka bằng `Ctrl + C` trong cửa sổ chạy Kafka.

Dừng HDFS bằng PowerShell:

```powershell
& "C:\hadoop-3.5.0\sbin\stop-dfs.cmd"
```

Không xóa các thư mục dữ liệu Hadoop nếu muốn giữ dữ liệu cho lần chạy sau.

## 12. Tác giả và repository

GitHub repository:

https://github.com/ngthaibink/Big-data-project-

Dự án phục vụ mục đích học tập môn Dữ liệu lớn, minh họa luồng thu thập, lưu trữ và xử lý dữ liệu đặt phòng khách sạn bằng các công nghệ Big Data.
