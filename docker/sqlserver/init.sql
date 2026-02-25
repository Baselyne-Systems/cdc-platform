-- SQL Server CDC initialisation for integration tests.
-- Creates the cdc_demo database, enables CDC, creates tables, and seeds data.
-- Run once after SQL Server Agent is healthy.

USE master;
GO

IF DB_ID('cdc_demo') IS NULL
BEGIN
    CREATE DATABASE cdc_demo;
END
GO

USE cdc_demo;
GO

-- Enable CDC on the database (requires SQL Server Agent).
IF NOT EXISTS (
    SELECT 1 FROM sys.databases WHERE name = 'cdc_demo' AND is_cdc_enabled = 1
)
BEGIN
    EXEC sys.sp_cdc_enable_db;
END
GO

-- Customers table ---------------------------------------------------------
IF OBJECT_ID('dbo.customers', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.customers (
        id          INT          IDENTITY(1,1) PRIMARY KEY,
        email       VARCHAR(255) NOT NULL,
        full_name   VARCHAR(255) NOT NULL,
        created_at  DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM cdc.change_tables
    WHERE source_object_id = OBJECT_ID('dbo.customers')
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name   = N'customers',
        @role_name     = NULL;
END
GO

-- Orders table ------------------------------------------------------------
IF OBJECT_ID('dbo.orders', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.orders (
        id          INT          IDENTITY(1,1) PRIMARY KEY,
        customer_id INT          NOT NULL,
        amount_cents BIGINT      NOT NULL,
        status      VARCHAR(50)  NOT NULL DEFAULT 'pending',
        created_at  DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM cdc.change_tables
    WHERE source_object_id = OBJECT_ID('dbo.orders')
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name   = N'orders',
        @role_name     = NULL;
END
GO

-- Seed data ---------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM dbo.customers WHERE email = 'alice@example.com')
BEGIN
    INSERT INTO dbo.customers (email, full_name) VALUES
        ('alice@example.com', 'Alice Johnson'),
        ('bob@example.com',   'Bob Smith');
END
GO
