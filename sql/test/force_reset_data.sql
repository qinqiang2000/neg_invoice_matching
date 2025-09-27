-- 强制重置所有测试数据到完全未使用状态
-- 确保所有数据都可用于性能测试

-- 清空匹配记录
TRUNCATE TABLE match_records CASCADE;

-- 强制恢复所有蓝票行余额到原始金额（不管当前状态）
UPDATE blue_lines
SET remaining = original_amount,
    last_update = CURRENT_TIMESTAMP;

-- 验证数据恢复状态
SELECT
    COUNT(*) as total_lines,
    COUNT(CASE WHEN remaining = original_amount THEN 1 END) as fully_available_lines,
    COUNT(CASE WHEN remaining = 0 THEN 1 END) as exhausted_lines,
    ROUND(AVG(remaining), 2) as avg_remaining,
    ROUND(AVG(original_amount), 2) as avg_original,
    ROUND(SUM(remaining) / SUM(original_amount) * 100, 2) as availability_percent
FROM blue_lines;