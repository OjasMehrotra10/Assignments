-- Create a new schema (if you don't have one)
CREATE SCHEMA IF NOT EXISTS customer_data;

-- Create the customer table within the 'customer_data' schema
CREATE TABLE customer_data.customer (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    gender VARCHAR(100),
    ip_address VARCHAR(100),
    phone_no VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    user_id VARCHAR(100) UNIQUE
);

-- Create the customer table within the 'customer_data' schema
CREATE TABLE customer_data.real_estate (
    property_id SERIAL PRIMARY KEY,
    property_type TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zipcode TEXT,
    price DECIMAL(20,2),
    bedrooms INT,
    bathrooms INT,
    square_feet BIGINT,
    year_built INT,
    lot_size DECIMAL(15,2),
    garage_spaces INT,
    pool BOOLEAN,
    hoa_fee DECIMAL(15,2),
    listing_date DATE,
    sale_date DATE,
    agent_id BIGINT,
    agent_name TEXT,
    agent_email TEXT,
    buyer_id BIGINT,
    buyer_name TEXT,
    buyer_email TEXT,
    mortgage_amount DECIMAL(20,2),
    mortgage_rate DECIMAL(10,5),
    mortgage_term INT,
    closing_date DATE,
    sold_price DECIMAL(20,2),
    profit DECIMAL(20,2),
    commission_rate DECIMAL(10,5),
    commission_price DECIMAL(20,2)
);

