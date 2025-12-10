-- Create the database
CREATE DATABASE recon_bd;

-- Connect to the database
\c recon_bd;

-- ===========================
-- Drop tables if they exist
-- ===========================
DROP TABLE IF EXISTS atm_transactions;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS uploaded_files;
DROP TABLE IF EXISTS switch_transactions;

-- ===========================
-- Create atm_transactions table
-- ===========================
CREATE TABLE atm_transactions (
    id BIGSERIAL PRIMARY KEY,
    datetime TIMESTAMP NULL,
    terminalid VARCHAR(50) NULL,
    location VARCHAR(255) NULL,
    atmindex VARCHAR(50) NULL,
    pan_masked VARCHAR(30) NULL,
    account_masked VARCHAR(30) NULL,
    transactiontype VARCHAR(50) NULL,
    amount DECIMAL(15,2) NULL,
    currency VARCHAR(10) NULL,
    stan VARCHAR(20) NULL,
    rrn VARCHAR(50) NOT NULL,
    auth VARCHAR(20) NULL,
    responsecode VARCHAR(10) NULL,
    responsedesc VARCHAR(255) NULL,
    uploaded_by BIGINT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- ===========================
-- Create transactions table
-- ===========================
CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    amount FLOAT NOT NULL,
    transaction_datetime TIMESTAMP NOT NULL,
    transaction_type VARCHAR(50) NOT NULL,
    atm_terminal VARCHAR(100) NULL,
    card VARCHAR(50) NULL,
    status_response VARCHAR(50) NULL,
    transaction_id VARCHAR(100) NOT NULL UNIQUE,
    channel_type VARCHAR(50) NULL,
    response_code VARCHAR(50) NULL,
    description VARCHAR(255) NULL,
    created_at TIMESTAMP DEFAULT NOW() NOT NULL,
    updated_at TIMESTAMP DEFAULT NOW() NOT NULL
);

-- ===========================
-- Create uploaded_files table
-- ===========================
CREATE TABLE uploaded_files (
    id BIGSERIAL PRIMARY KEY,
    file_description TEXT NULL,
    uploaded_by BIGINT NULL,
    status VARCHAR(50) NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);


CREATE TABLE switch_transactions (
    id BIGSERIAL PRIMARY KEY,
    datetime TIMESTAMP NOT NULL,
    direction VARCHAR(50),
    mti INT,
    pan_masked VARCHAR(50),
    processingcode INT,
    amountminor NUMERIC,
    currency VARCHAR(10),
    stan BIGINT,
    rrn BIGINT,
    terminalid VARCHAR(50),
    source VARCHAR(50),
    destination VARCHAR(50),
    responsecode NUMERIC,
    authid VARCHAR(50),
    uploaded_by BIGINT,       
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

