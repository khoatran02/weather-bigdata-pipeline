# Kết quả thực nghiệm

Đo ngày 22/11/2026 trên một laptop AMD Ryzen 5 4600H (6 nhân, 12 luồng), RAM 30 GB, toàn bộ cụm chạy
bằng Docker Compose. Số liệu gốc nằm trong [evidence/](evidence/).

## 1. Dựng hệ thống từ số 0

`make demo` trên volume rỗng (đã có image cache và file archive): **10 phút 43 giây**, gồm backfill,
Gold, quality gate, huấn luyện và dự báo. Lần đầu cần thêm thời gian build image và tải archive
(khoảng 15 phút riêng cho archive, vì IEM giới hạn tốc độ).

## 2. Ingestion và tính đúng đắn

| Chỉ số | Giá trị |
|---|---:|
| Message lịch sử | 2.428.936 (822 MB JSON) |
| Producer, RF=3, `acks=all` | 34.440 msg/giây |
| Từ lúc gửi xong đến khi đủ ở Silver | khoảng 155 giây |
| Bronze | 2.428.936 |
| Silver | 2.426.741 |
| Quarantine | 2.186 |
| Trùng khóa được hợp nhất | 9 |
| **Mất mát** | **0** (Bronze = Silver + Quarantine + trùng) |
| Kích thước Bronze / Silver | 134 MB / 122 MB |

Kết quả còn được đối soát với một script Python thuần đọc thẳng CSV (không dùng Spark hay code dự án):
số khóa, số giá trị nhiệt độ hợp lệ và tổng nhiệt độ **khớp ở cả 7 trạm**.

Độ trễ live từ lúc collector lấy được bản ghi đến lúc sẵn sàng ghi InfluxDB là 11 và 19 giây trong hai
mẫu đo khi hệ thống ổn định. Đây là quan sát, chưa phải phân phối p95. Độ trễ của bản thân nguồn lớn
hơn nhiều (METAR khoảng 6 phút).

## 3. Chịu lỗi (`make e2e`)

Phát 333.000 message (300.000 sự kiện, 30.000 bản trùng, 3.000 bản hỏng). Trong lúc producer vẫn đang gửi:

| Giây | Sự kiện |
|---:|---|
| 15 | SIGKILL Kafka broker 2 |
| 30 | SIGKILL Spark driver của streaming |
| 40 | khởi động lại driver từ checkpoint |
| 60 | SIGKILL Spark worker 1 |
| 75 | khởi động lại broker 2 |
| 90 | khởi động lại worker 1 |

| Kiểm tra | Kỳ vọng | Thực tế |
|---|---:|---:|
| Bronze (history + live) | 333.000 | 333.000 |
| Offset phân biệt trong Bronze | 333.000 | 333.000 |
| Silver | 300.000 | 300.000 |
| event_id phân biệt | 300.000 | 300.000 |
| Quarantine | 3.000 | 3.000 |
| Checksum tổng nhiệt độ | 6.600.000 | 6.600.000 |

Phép thử mất từng process một, chưa thử mất đồng thời hai broker hay mất ổ đĩa.

## 4. Khả năng mở rộng (`make bench`)

Group-by theo trạm, bản sao và giờ trên **48.534.820 dòng** (dữ liệu thật nhân 20 lần), trung vị 3 lần
chạy sau một lần warm-up.

| Core executor | Thời gian (s) | Speedup | Hiệu suất |
|---:|---:|---:|---:|
| 1 | 150,54 | 1,00 | 100% |
| 2 | 80,56 | 1,87 | 94% |
| 4 | 51,45 | 2,93 | 73% |
| 8 | 39,30 | 3,83 | 48% |

Hiệu suất giảm vì máy chỉ có 6 nhân vật lý, phải chia cho executor, driver, Kafka và object storage
(8 core là đã dùng hyper-thread), vì turbo giảm khi nhiều nhân cùng tải và vì phần shuffle. Đây là
scale trên một máy, không phải bằng chứng scale ngang nhiều máy.

## 5. Định dạng lưu trữ (2.426.741 dòng, 7 cột, 4 core)

| Định dạng | Kích thước (MB) | Quét toàn bộ (s) | Một trạm, một năm (s) |
|---|---:|---:|---:|
| JSON | 399,8 | 1,73 | 1,16 |
| CSV | 165,4 | 1,16 | 1,20 |
| Parquet | 29,1 | 0,51 | 0,62 |
| **Delta** (phân vùng theo trạm, sắp theo thời gian) | **19,6** | 0,64 | **0,48** |

Delta nhỏ hơn JSON khoảng 20 lần. Nó chậm hơn Parquet một chút khi quét toàn bộ (phải đọc transaction
log) và nhanh hơn khi truy vấn chọn lọc (bỏ qua phân vùng và file). Lợi thế chọn lọc sẽ rõ hơn khi dữ
liệu lớn hơn.

## 6. Dự báo nhiệt độ

Train 2005–2023 (khoảng 605.000 mẫu), validation 2024 để chọn mô hình, test 01/2025–11/2026
(khoảng 96.000 mẫu). GBT thắng Linear Regression trên validation ở cả ba horizon.

| Horizon | Persistence | Baseline chu kỳ ngày | **GBT** | Cải thiện so với baseline tốt nhất |
|---:|---:|---:|---:|---:|
| +1 giờ | 0,613 | 0,590 | **0,547** | 7% |
| +3 giờ | 1,475 | 0,972 | **0,848** | 13% |
| +6 giờ | 2,512 | 1,345 | **1,099** | 18% |

Đơn vị là MAE (°C). Ở +1 giờ lợi ích nhỏ, một phần vì nhiệt độ METAR làm tròn 1 °C. Liên Khương không
có dữ liệu trước 2024 nên **không được dự báo**. Tập test được chấm lại mỗi lần sửa pipeline, nhưng
không dùng để chọn mô hình hay tham số.

## 7. ThingSpeak so với METAR gốc

552/552 dòng ghép được. Tỷ lệ khớp: nhiệt độ 98,6%, độ ẩm 97,3%, áp suất 98,9%, gió 96,2%.

## 8. Xu hướng nhiệt độ từ 1980 (thống kê mô tả)

| Trạm | °C/thập kỷ | R² | Số năm đủ dữ liệu |
|---|---:|---:|---:|
| Tân Sơn Nhất | 0,24 | 0,37 | 36 |
| Nội Bài | 0,29 | 0,35 | 31 |
| Đà Nẵng | 0,39 | 0,53 | 31 |
| Phú Bài | 0,53 | 0,46 | 30 |

**Không kết luận về biến đổi khí hậu từ bảng này**: chuỗi chưa đồng nhất hóa và có dấu hiệu đứt gãy
quanh 2010–2012 (xem [DATA.md](DATA.md), phát hiện 8).
