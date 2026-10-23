FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

# Giữ image dùng được cho cả spark-submit và các job PySpark.
# Các connector phụ thuộc phiên bản Spark/Hadoop được nạp khi submit job
# để không làm image chứa nhiều bản sao jar.
