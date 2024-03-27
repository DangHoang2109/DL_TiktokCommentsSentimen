mô tả:
dữ liệu vào sẽ gồm 2 file train_nor_811.csv và test_nor_811.csv
gồm 5 class Emotion

kịch bản của chương trình:
- union 2 file data
- chuyển thành vector
- tách train và test theo tỉ lệ 7:3
- train model
- chạy trên test
=> độ chính xác không cao vì khi conver từ xlxs -> csv bị lỗi font

PROBLEM CHƯA GIẢI QUYẾT ĐƯỢC
Nếu setup 2 file theo kịch bản trên thì chạy được test bình thường, nhưng khi thêm 1 file khác để test thì bị lỗi ( dù đã setup lại giống file trên)
=> hướng: khi có file mới đc craw về sẽ cộng dồn với file cũ ( nghe bị ngu nhưng mà chắc nó giải quyết được)

PROBLEM TRONG FILE DATA
File truyền vào phải có 2 cột là Sentence và Emotion mới có thể chạy. Đã thử để file data.csv vào nhưng ko được
Các dấu "," trong csv nó tự hiểu là qua cột. Ví dụ hôm nay, là thứ .... | Other -> thì nó sẽ hiểu là hôm nay | là thứ | Other 



