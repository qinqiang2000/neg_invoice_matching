-- 负数发票匹配系统 - 完整数据库结构定义（表+索引）
-- 创建日期: 2025-09-27
-- 说明: 包含所有表定义和性能优化索引，避免遗漏索引导致性能问题

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

-- ======================
-- 性能优化索引
-- ======================

-- 核心性能索引：用于快速查找有余额的蓝票行（最重要）
-- 支持匹配算法的核心查询: WHERE tax_rate=? AND buyer_id=? AND seller_id=? AND remaining>0
CREATE INDEX idx_blue_lines_matching
ON blue_lines (tax_rate, buyer_id, seller_id)
WHERE remaining > 0;

-- 包含排序的复合索引：避免额外排序操作
-- 支持 ORDER BY remaining ASC 的查询
CREATE INDEX idx_blue_lines_matching_sorted
ON blue_lines (tax_rate, buyer_id, seller_id, remaining)
WHERE remaining > 0;

-- 票据索引：用于按票据ID查询
CREATE INDEX idx_ticket ON blue_lines (ticket_id);

-- 余额索引：用于统计分析和范围查询
CREATE INDEX idx_remaining ON blue_lines (remaining);

-- 批次索引：用于批次管理和追踪
CREATE INDEX idx_batch ON blue_lines (batch_id);

-- 批次状态复合索引：用于查询特定批次的数据状态
CREATE INDEX idx_batch_status ON blue_lines (batch_id, remaining)
WHERE batch_id IS NOT NULL;

-- 匹配记录索引：优化匹配记录查询
CREATE INDEX idx_match_batch ON match_records (batch_id);
CREATE INDEX idx_match_negative ON match_records (negative_invoice_id);
CREATE INDEX idx_match_blue_line ON match_records (blue_line_id);

-- 测试结果索引：优化测试结果查询
CREATE INDEX idx_test_batch ON test_results (batch_id);
CREATE INDEX idx_test_time ON test_results (test_time);

-- 更新统计信息以优化查询计划
ANALYZE blue_lines;
ANALYZE match_records;
ANALYZE test_results;
ANALYZE batch_metadata;