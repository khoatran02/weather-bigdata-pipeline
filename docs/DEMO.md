# Kịch bản demo và bảo vệ (khoảng 10 phút)

Chuẩn bị trước buổi báo cáo: chạy `make demo` (10 đến 15 phút khi đã có image và archive, lâu hơn ở
lần đầu), sau đó `make urls`.

| Phút | Mở | Thao tác và ý chính |
|---|---|---|
| 0–1 | sơ đồ trong README | bài toán, vì sao cần nguồn lịch sử bên cạnh ThingSpeak |
| 1–2 | Kafka UI `:18082` | 4 topic, RF=3, key là mã trạm, mở một message xem envelope |
| 2–4 | Spark `:18081`, streaming UI `:14040` | 2 worker, tab *Structured Streaming* có 5 query, watermark và state |
| 4–6 | Grafana, dashboard 1 | live, ThingSpeak trễ so với METAR, cửa sổ trượt, dự báo MLlib |
| 6–7 | Grafana, dashboard 2 | hơn 70 năm dữ liệu, giải thích bẫy lịch lấy mẫu |
| 7–8 | Airflow `:18083` | 3 DAG, quality gate chặn pipeline khi dữ liệu sai |
| 8–10 | Grafana, dashboard 3 và terminal | demo lỗi bên dưới |

## Demo chịu lỗi trực tiếp

```bash
docker compose kill kafka-2      # "Broker Kafka" về 2, có partition thiếu replica, pipeline vẫn chạy
docker compose start kafka-2     # ISR tự hồi phục
docker compose restart streaming # tiếp tục từ checkpoint
make check                       # không trùng, không sai khoảng
```

Bản tự động, có đối chiếu số lượng và checksum: `make e2e` (khoảng 5 phút).

## Câu hỏi thường gặp

**Dữ liệu này đã "lớn" chưa?**
2,4 triệu bản ghi (822 MB JSON) vẫn vừa một máy. Điểm cần chứng minh là kiến trúc mở rộng được: partition
Kafka, executor Spark, object storage. Benchmark nhân dữ liệu lên 20 lần để đo speedup.

**Vì sao cần cả Delta và InfluxDB?**
Delta là nguồn sự thật, phát lại và tính lại được. InfluxDB là bản phục vụ cho dashboard, mất thì
dựng lại từ lake.

**Vì sao không ghi thẳng Kafka → InfluxDB?**
Sẽ mất bản gốc, không sửa được luật làm sạch về sau, không có MERGE, không có tập huấn luyện.

**Vì sao dùng Spark mà không dùng Flink?**
Nguồn cập nhật 30 phút/lần nên micro-batch vài giây là đủ, và Spark lo luôn batch và ML.

**Exactly-once có thật không?**
Đúng cho Bronze và Quarantine, effectively-once cho Silver và InfluxDB. DLQ và alerts chỉ là
at-least-once, xem bảng ở [ARCHITECTURE.md](ARCHITECTURE.md#5-đảm-bảo-khi-có-lỗi).

**Làm sao biết mô hình không bị rò rỉ dữ liệu?**
Chia theo thời gian, có khoảng đệm bằng horizon, normals chỉ fit trên tập train, so với persistence và
baseline chu kỳ ngày.

**Dữ liệu đến muộn thì sao?**
Quá watermark thì không vào cửa sổ realtime, nhưng vẫn vào Silver và được tính trong Gold ở lần chạy sau.
