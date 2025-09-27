-- 负数发票匹配系统 - 索引定义
-- 创建日期: 2025-09-27

-- 核心部分索引：用于快速查找有余额的蓝票行
-- 这是最重要的索引，支持匹配算法的核心查询
CREATE INDEX idx_active ON blue_lines (tax_rate, buyer_id, seller_id) WHERE remaining > 0;

-- 票据索引：用于按票据ID查询
CREATE INDEX idx_ticket ON blue_lines (ticket_id);

-- 余额索引：用于统计分析
CREATE INDEX idx_remaining ON blue_lines (remaining);

-- 批次索引：用于批次管理和追踪
CREATE INDEX idx_batch ON blue_lines (batch_id);

-- 批次状态复合索引：用于查询特定批次的数据
CREATE INDEX idx_batch_status ON blue_lines (batch_id, remaining) WHERE batch_id IS NOT NULL;

-- 更新统计信息以优化查询计划
ANALYZE blue_lines;