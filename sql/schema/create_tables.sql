-- 负数发票匹配系统 - 数据表结构定义
-- 创建日期: 2025-09-27

-- 蓝票行表
DROP TABLE IF EXISTS blue_lines CASCADE;
CREATE TABLE blue_lines (
    line_id BIGSERIAL PRIMARY KEY,
    ticket_id BIGINT NOT NULL,
    tax_rate SMALLINT NOT NULL,
    buyer_id INTEGER NOT NULL,
    seller_id INTEGER NOT NULL,
    product_name VARCHAR(200),
    original_amount DECIMAL(15,2),
    remaining DECIMAL(15,2) NOT NULL,
    batch_id VARCHAR(50),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 匹配记录表
DROP TABLE IF EXISTS match_records CASCADE;
CREATE TABLE match_records (
    match_id BIGSERIAL PRIMARY KEY,
    batch_id VARCHAR(50),
    negative_invoice_id BIGINT NOT NULL,
    blue_line_id BIGINT NOT NULL,
    amount_used DECIMAL(15,2) NOT NULL,
    match_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'active'
);

-- 测试结果表
DROP TABLE IF EXISTS test_results CASCADE;
CREATE TABLE test_results (
    test_id BIGSERIAL PRIMARY KEY,
    batch_id VARCHAR(50),
    test_name VARCHAR(100),
    total_negatives INTEGER,
    success_count INTEGER,
    failed_count INTEGER,
    total_amount DECIMAL(15,2),
    matched_amount DECIMAL(15,2),
    execution_time_ms INTEGER,
    fragment_created INTEGER,
    test_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 批次元数据表
DROP TABLE IF EXISTS batch_metadata CASCADE;
CREATE TABLE batch_metadata (
    batch_id VARCHAR(50) PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL DEFAULT 'blue_lines',
    total_lines INTEGER NOT NULL,
    inserted_lines INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'running' CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    resumed_at TIMESTAMP,
    resumed_from INTEGER,
    error_message TEXT,
    created_by VARCHAR(100) DEFAULT USER
);