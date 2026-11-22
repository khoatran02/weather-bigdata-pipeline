# Dữ liệu và các vấn đề chất lượng

Số liệu khảo sát ngày 22/11/2026.

## 1. Nguồn

| Nguồn | Vai trò | Tần suất | Quy mô |
|---|---|---|---|
| [ThingSpeak 3574112](https://thingspeak.mathworks.com/channels/3574112) | IoT live, đề bài gốc | khoảng 30 phút | 545 bản ghi từ 11/11/2026 |
| [AviationWeather](https://aviationweather.gov/data/api/) | METAR live 7 sân bay | 30 phút | khoảng 330 bản ghi/ngày |
| [IEM ASOS/METAR archive](https://mesonet.agron.iastate.edu/request/download.phtml) | lịch sử | 30 phút đến 3 giờ | 2.428.936 bản ghi |

| Mã | Sân bay | Lịch sử từ | Số dòng |
|---|---|---|---:|
| VVTS | Tân Sơn Nhất | 1949 | 682.861 |
| VVDN | Đà Nẵng | 1956 | 590.496 |
| VVNB | Nội Bài (chuỗi Hà Nội) | 1958 | 521.824 |
| VVPB | Phú Bài | 1957 | 400.212 |
| VVCI | Cát Bi | 2017 | 164.759 |
| VVVD | Vân Đồn | 2023 | 65.966 |
| VVDL | Liên Khương | 09/2026 | 2.818 |

Riêng kênh ThingSpeak quá nhỏ cho bài toán dữ liệu lớn, nên được dùng làm nguồn live. Khối lượng đến
từ kho lịch sử cùng loại dữ liệu. Muốn lớn hơn: `make archive COUNTRIES=VN,TH,LA,KH`
(thêm 53 trạm, chưa chạy thử trong lần nghiệm thu này).

> **Thiếu ba sân bay Việt Nam:** IEM xếp Cần Thơ (VVCT, từ 1965), Cam Ranh (VVCR) và Phú Quốc (VVPQ)
> vào mạng **Lào**, nên `--countries VN` không lấy được. Dùng `make archive COUNTRIES=VN,LA` để có lịch
> sử của chúng, thêm mã vào `METAR_STATIONS` để có live và vào `STATIONS` trong
> `scripts/build_dashboards.py` để lên dashboard.

## 2. Các trường của ThingSpeak

| Trường | Tên trên kênh | Đơn vị thực tế | Căn cứ |
|---|---|---|---|
| field1 | Temperature (°C) | °C | trùng METAR VVDN |
| field2 | Humidity (%) | % | trùng giá trị Magnus tính từ T và Td |
| field3 | Rain | **không xác định** | xem phát hiện 2 |
| field4 | Pressure | hPa (QNH) | trùng nhóm `Qxxxx` |
| field5 | Wind speed | **km/h** | mọi giá trị là bội của 1 kt = 1,852 km/h |

## 3. Phát hiện và cách xử lý

**1. Kênh là bản sao trễ của METAR.**
`created_at` là giờ ghi lên ThingSpeak, không phải giờ quan trắc. Khớp tốt nhất với luật
`floor(created_at − 7 phút, 30 phút)`. Độ trễ 7 phút được chọn trên chính dữ liệu này nên tỷ lệ khớp
là mô tả, không phải kiểm định độc lập. Hệ thống giữ kênh như một trạm riêng (`TS3574112`), không trộn
vào Gold (tránh đếm đôi) và có bảng `reconciliation`.

**2. Cột Rain không rõ đơn vị và nguồn.**
METAR VVDN không báo lượng mưa, còn kênh có các giá trị 0,01 đến 0,91. Tương quan cao nhất với các mô
hình Open-Meteo đã thử chỉ là 0,67, tỷ lệ độ lớn cũng không ổn định. Hệ thống lưu thành `rain_raw` và
**không cộng thành mm**. Dấu hiệu mưa trong Gold lấy từ mã thời tiết METAR.

**3. Kho lịch sử có giá trị bất khả thi.** Ví dụ thật ở Đà Nẵng:

| Giá trị | Raw METAR | Nguyên nhân | Xử lý |
|---|---|---|---|
| gió 1360,69 kt | `251147Z 0700 020V110 …` | thiếu nhóm gió | null và `range:wind_kmh` |
| gió 111 kt | `070111T 7000 …` | lỗi gõ `07011KT` | null và cờ |
| 46 °C | `46/45 Q1004` | lỗi nhập | null cả T và RH |
| áp suất 2,98 inHg | `… Q101 TEMPO` | QNH bị cụt | null và cờ |

Chỉ giá trị sai bị xóa, các cột còn lại của dòng vẫn được giữ. Dòng không còn đại lượng nào dùng được
thì vào Quarantine.

**4. Lịch quan trắc đổi theo thời kỳ.**
Thập niên 1980 chủ yếu 3 giờ/lần và thiếu ca đêm, hiện nay 30 phút/lần. Lấy `avg()` thẳng sẽ tạo xu
hướng giả. Trung bình tháng chỉ dùng 8 giờ synop (00, 03, …, 21 UTC), trọng số bằng nhau, mỗi giờ cần
ít nhất 15 ngày. Trung bình năm cần đủ 12 tháng.

**5. Đơn vị gió đổi giữa MPS và KT** (hơn 60.000 bản tin MPS ở Đà Nẵng trong thập niên 2000). Cột
`sknt` của IEM đã quy đổi đúng ở 60.652 trên 60.656 dòng kiểm tra.

**6. Metadata IEM sai cho VVVD.** IEM ghi "Vo Dat" (11,13°N). AviationWeather ghi Van Don Intl
(21,118°N) và chu kỳ nhiệt năm (tháng 1 khoảng 17 °C) khẳng định đây là Vân Đồn.

**7. Độ ẩm của METAR là đại lượng suy ra** từ T và Td (làm tròn 1 °C), nên chỉ có một số mức rời rạc.

**8. Chuỗi năm có dấu hiệu đứt gãy.**
Phú Bài (VVPB) nhảy từ khoảng 24,3–25,6 °C (1993–2009) lên 25,5–27,7 °C (2012–2025). Đà Nẵng cũng tăng
mạnh sau 2010. Một phần của "xu hướng" vì vậy có thể đến từ trạm đo hoặc cách lưu trữ. Ngoài ra nhiều
năm bị thiếu (Đà Nẵng không có năm đủ dữ liệu nào từ 1980 đến 1991), nên biểu đồ năm để hở các khoảng này.

**9. Lỗi gõ gió nằm trong khoảng hợp lệ.**
`30053KT` giữa các bản tin 3 kt (VVPB 13/10/2012) vượt qua mọi luật khoảng giá trị và từng làm Phú Bài
có "bão" 150 km/h hầu như mỗi quý. Luật ở Gold: gió ≥ 25 kt, lớn hơn 3 lần trung vị của các điểm lân
cận trong 6 giờ, không có nhóm gust, không phải dông dưới 50 kt. Đã loại 383 giá trị. Bão thật được
giữ (Ketsana 2009, Yagi 2024).

**10. Một giai đoạn sai đơn vị hệ thống.**
Gió Tân Sơn Nhất từ 04/1988 đến hết 1990 có trung bình gấp 2 đến 3 lần các năm khác, và các mức phổ
biến 8, 14, 21, 27 kt đúng bằng 4, 7, 11, 14 × 1,944 (quy đổi m/s → kt hai lần). Loại 19.528 giá trị,
**không** tự chia lại vì không có nguồn đối chứng.

**11. Nhiệt độ sai theo ngữ cảnh.**
3 °C ở TP.HCM giữa tháng 6/1992 hay −4,8 °C ở Đà Nẵng đều nằm trong khoảng toàn quốc. Gold dùng biên
theo trạm và tháng (phân vị 0,1% và 99,9% ± 5 °C, fit trên dữ liệu trước 2024). Đã loại 36 giá trị,
cộng 31 spike. Đợt rét 5 °C ở Hà Nội năm 2016 được giữ.

## 4. Luật chất lượng

Khoảng hợp lệ nằm ở `LIMITS` trong
[transforms.py](../src/weather_pipeline/streaming/transforms.py). Ngoài ra:

- Td ≤ T, gió giật ≥ gió trung bình;
- thời điểm quan trắc không vượt quá giờ thu thập + 1 giờ;
- cổng chất lượng: không trạm nào có quá 6 ngày/năm gió từ 40 kt, để bắt lại lỗi kiểu 9 và 10;
- spike (Gold): lệch hơn 8 °C so với **cả hai** điểm lân cận trong 2 giờ, cùng chiều.

QC có ngữ cảnh (spike, gió, khí hậu) **chỉ chạy ở Gold**, vì cần điểm phía sau hoặc toàn bộ lịch sử.
Hệ quả: alert realtime vẫn có thể báo giả khi METAR gõ nhầm. Mô hình ML đọc từ Silver để không dùng
thông tin tương lai.

## 5. Lưu ý khi diễn giải

- Một vài cực trị gió cũ không có điểm lân cận trong 6 giờ nên không kiểm được, ví dụ VVTS 31/05/1977
  (84 kt) và VVDN 10/08/1981 (82 kt). Không nên trích dẫn chúng như kỷ lục.
- Chuỗi dài chưa được đồng nhất hóa (dời trạm, đổi thiết bị, đổi nguồn lưu trữ, đô thị hóa quanh sân
  bay). Cột xu hướng là thống kê mô tả của chuỗi sân bay, **không phải kết luận về biến đổi khí hậu**.
- Trung bình ngày của các thập niên cũ chịu ảnh hưởng của độ phủ, cần xem cùng cột `coverage`.
