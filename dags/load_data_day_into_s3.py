from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd
import os
from minio import Minio
from vnstock3 import Vnstock
from pyarrow import parquet as pq
import pyarrow as pa

# Thông tin MinIO
minio_endpoint = 'minio:9000'  
minio_access_key = 'minio_access_key'
minio_secret_key = 'minio_secret_key'
minio_bucket_name = 'stock-data-day'
parquet_file_name = 'all_stock_data_2021_to_present.parquet'

# Tạo kết nối đến MinIO
minio_client = Minio(
    minio_endpoint,
    access_key=minio_access_key,
    secret_key=minio_secret_key,
    secure=False  
)

# Định nghĩa DAG với Decorator
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 18),
}

with DAG(
    dag_id='update_stock_data_day_daily',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    @task
    def fetch_new_data():
        # Khởi tạo đối tượng Vnstock
        stock_api = Vnstock()

        # Lấy danh sách mã cổ phiếu niêm yết trên sàn HOSE
        hose_symbols = stock_api.stock(symbol='A32', source='VCI').listing.symbols_by_group('HOSE')
        symbols = hose_symbols.tolist()

        new_data = []

        for symbol in symbols:
            try:
                # Khởi tạo đối tượng Vnstock cho mã cổ phiếu hiện tại
                stock = Vnstock().stock(symbol=symbol, source='VCI')
                
                # Lấy dữ liệu giá cổ phiếu cho ngày hôm nay
                price_data = stock.quote.history(start=datetime.now().strftime('%Y-%m-%d'), end=datetime.now().strftime('%Y-%m-%d'), interval='1D', count_back=1)
                
                if not price_data.empty:
                    price_data['symbol'] = symbol
                    new_data.append(price_data)
            except Exception as e:
                print(f"Lỗi khi lấy dữ liệu cho mã cổ phiếu {symbol}: {e}")
        
        if new_data:
            new_data_df = pd.concat(new_data, ignore_index=True)
            return new_data_df
        return None

    @task
    def update_parquet_file(new_data_df: pd.DataFrame):
        if new_data_df is not None and not new_data_df.empty:
            # Tải file Parquet hiện có từ MinIO
            if minio_client.bucket_exists(minio_bucket_name):
                if minio_client.object_exists(minio_bucket_name, parquet_file_name):
                    # Tạo một bộ nhớ ảo để đọc dữ liệu từ MinIO
                    existing_data_buffer = io.BytesIO()
                    minio_client.get_object(minio_bucket_name, parquet_file_name).readinto(existing_data_buffer)
                    existing_data_buffer.seek(0)
                    
                    # Đọc dữ liệu Parquet hiện có từ buffer
                    existing_df = pd.read_parquet(existing_data_buffer, engine='pyarrow')
                    
                    # Kết hợp dữ liệu mới với dữ liệu hiện có
                    combined_df = pd.concat([existing_df, new_data_df], ignore_index=True)
                    
                    # Lưu dữ liệu kết hợp vào buffer
                    combined_data_buffer = io.BytesIO()
                    combined_df.to_parquet(combined_data_buffer, engine='pyarrow')
                    combined_data_buffer.seek(0)
                    
                    # Ghi dữ liệu đã kết hợp vào MinIO
                    minio_client.put_object(
                        minio_bucket_name,
                        parquet_file_name,
                        combined_data_buffer,
                        length=combined_data_buffer.getbuffer().nbytes,
                        content_type='application/octet-stream'
                    )
                    
                    print("Dữ liệu đã được cập nhật thành công.")
                else:
                    print(f"File Parquet hiện có không tồn tại trong MinIO.")
            else:
                print(f"Bucket {minio_bucket_name} không tồn tại trong MinIO.")
        else:
            print("Không có dữ liệu mới để cập nhật.")

    new_data_df = fetch_new_data()
    update_parquet_file(new_data_df)



# from airflow import DAG
# from airflow.decorators import task
# from datetime import datetime
# import pandas as pd
# from minio import Minio
# from vnstock3 import Vnstock
# from sqlalchemy import create_engine
# import pyarrow as pa
# import io

# # Thông tin MinIO
# minio_endpoint = 'minio:9000'  
# minio_access_key = 'minio_access_key'
# minio_secret_key = 'minio_secret_key'
# minio_bucket_name = 'stock-data-day'
# parquet_file_name = 'all_stock_data_2021_to_present.parquet'

# # Tạo kết nối đến MinIO
# minio_client = Minio(
#     minio_endpoint,
#     access_key=minio_access_key,
#     secret_key=minio_secret_key,
#     secure=False  
# )

# # Cấu hình Trino để kết nối với Iceberg
# trino_host = 'trino:8080'
# trino_catalog = 'iceberg'
# trino_schema = 'default'
# engine = create_engine(f'trino://{trino_host}/{trino_catalog}/{trino_schema}')

# # Định nghĩa DAG với Decorator
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2024, 8, 18),
# }

# with DAG(
#     dag_id='update_stock_data_day_daily',
#     default_args=default_args,
#     schedule_interval='@daily',
#     catchup=False,
# ) as dag:

#     @task
#     def fetch_new_data():
#         # Khởi tạo đối tượng Vnstock
#         stock_api = Vnstock()

#         # Lấy danh sách mã cổ phiếu niêm yết trên sàn HOSE
#         hose_symbols = stock_api.stock(symbol='A32', source='VCI').listing.symbols_by_group('HOSE')
#         symbols = hose_symbols.tolist()

#         new_data = []

#         for symbol in symbols:
#             try:
#                 # Khởi tạo đối tượng Vnstock cho mã cổ phiếu hiện tại
#                 stock = Vnstock().stock(symbol=symbol, source='VCI')
                
#                 # Lấy dữ liệu giá cổ phiếu cho ngày hôm nay
#                 price_data = stock.quote.history(start=datetime.now().strftime('%Y-%m-%d'), end=datetime.now().strftime('%Y-%m-%d'), interval='1D', count_back=1)
                
#                 if not price_data.empty:
#                     price_data['symbol'] = symbol
#                     new_data.append(price_data)
#             except Exception as e:
#                 print(f"Lỗi khi lấy dữ liệu cho mã cổ phiếu {symbol}: {e}")
        
#         if new_data:
#             new_data_df = pd.concat(new_data, ignore_index=True)
#             return new_data_df
#         return None

#     @task
#     def append_to_iceberg(new_data_df: pd.DataFrame):
#         if new_data_df is not None and not new_data_df.empty:
#             # Chuyển đổi DataFrame thành PyArrow Table
#             table = pa.Table.from_pandas(new_data_df)
            
#             # Tạo buffer để lưu trữ dữ liệu Parquet mới
#             new_data_buffer = io.BytesIO()
#             pq.write_table(table, new_data_buffer)
#             new_data_buffer.seek(0)
            
#             # Đọc dữ liệu từ MinIO và append vào bảng Iceberg
#             with engine.connect() as conn:
#                 # Đảm bảo bảng Iceberg đã được tạo và ánh xạ tới MinIO
#                 # Tạo hoặc cập nhật bảng Iceberg (thay đổi schema nếu cần)
#                 conn.execute(f"""
#                 CREATE TABLE IF NOT EXISTS iceberg_table (
#                     time TIMESTAMP,
#                     open DOUBLE,
#                     high DOUBLE,
#                     low DOUBLE,
#                     close DOUBLE,
#                     volume BIGINT,
#                     symbol VARCHAR
#                 )
#                 """)

#                 # Thực hiện append dữ liệu mới vào bảng Iceberg
#                 conn.execute(f"""
#                 INSERT INTO iceberg_table
#                 SELECT * FROM read_parquet('{new_data_buffer.getvalue()}')
#                 """)
                
#             print("Dữ liệu đã được cập nhật thành công vào Iceberg.")
#         else:
#             print("Không có dữ liệu mới để cập nhật.")

#     new_data_df = fetch_new_data()
#     append_to_iceberg(new_data_df)
