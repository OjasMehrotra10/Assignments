import psycopg2
import csv
import pandas as pd
import boto3
from io import BytesIO
import sys
from dotenv import load_dotenv
import os

# * Part 1 - modification

file_path = 'C:\\Users\\OjasMehrotra\\Downloads\\customer.xlsx'

df = pd.read_excel(file_path)

df['phone_no'] = df['phone_no'].str.replace('-', '').astype(int)

df['User_Id'] = df['email'].str.split('@').str[0]

csv_file_path = 'C:\\Users\\OjasMehrotra\\Downloads\\modified_customer.csv'
df.to_csv(csv_file_path, index=False)
print(f"File saved successfully as '{csv_file_path}'")

# * Part 2 - modification

file_path = 'C:\\Users\\OjasMehrotra\\Downloads\\realstate.csv'
df = pd.read_csv(file_path)
# ? to handle max case values so that commission price is always in limit upto 2 decimal places
MAX_FLOAT = sys.float_info.max
df['commission_price'] = df.apply(
    lambda row: 0 if row['profit'] < 0 else round(min((row['profit'] * row['commission_rate']) / 100, MAX_FLOAT), 2), 
    axis=1
)
output_file_path = 'C:\\Users\\OjasMehrotra\\Downloads\\modified_realstate.csv'
df.to_csv(output_file_path, index=False)

print(f"File saved successfully as '{output_file_path}'")

# * get db connection details from .env
load_dotenv()
conn = psycopg2.connect(
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT')
)

# * This inserts the data rows from the files into the tables - customer and real_estate 
cursor = conn.cursor()

with open(csv_file_path, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        cursor.execute("""
            INSERT INTO customer_data.customer (id, first_name, last_name, email, gender, ip_address, phone_no, city, state, country, user_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]))
with open(output_file_path, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        cursor.execute("""
            INSERT INTO customer_data.real_estate (property_id, property_type, address, city, state, zipcode, price, bedrooms, bathrooms, square_feet, year_built, lot_size, garage_spaces, pool, hoa_fee, listing_date, sale_date, agent_id, agent_name, agent_email, buyer_id, buyer_name, buyer_email, mortgage_amount, mortgage_rate, mortgage_term, closing_date, sold_price, profit, commission_rate, commission_price)
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (property_id) DO NOTHING;
        """, (
            row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20], row[21], row[22], row[23], row[24], row[25], row[26], row[27], row[28], row[29], row[30]
        ))
conn.commit()
cursor.close()

print(f"Data inserted successfully into the 'customer' and 'real_estate' tables.")


# Initialize S3 client with proper arguments
s3_client = boto3.client('s3',aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))

bucket_name = 'lirik_traning'

# Upload the modified customer data file
customer_object_name = 'modified_customer.csv'
s3_client.upload_file(csv_file_path, bucket_name, customer_object_name)
print(f"File '{customer_object_name}' uploaded successfully to S3 bucket '{bucket_name}'.")

# Upload the modified real estate data file
realstate_object_name = 'modified_realstate.csv'
s3_client.upload_file(output_file_path, bucket_name, realstate_object_name)
print(f"File '{realstate_object_name}' uploaded successfully to S3 bucket '{bucket_name}'.")
