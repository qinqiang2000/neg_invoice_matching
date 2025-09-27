-- 负数发票匹配系统 - 重置测试数据
-- 用于清理测试过程中的数据，恢复到初始状态

-- 清空匹配记录（级联删除相关数据）
TRUNCATE TABLE match_records CASCADE;

-- 完全恢复蓝票行余额（从original_amount）
-- 确保测试数据的一致性和可重现性
UPDATE blue_lines
SET remaining = original_amount,
    last_update = CURRENT_TIMESTAMP
WHERE remaining != original_amount;

-- 验证数据恢复状态
-- 检查是否有异常数据
SELECT
    COUNT(*) as total_lines,
    COUNT(CASE WHEN remaining = original_amount THEN 1 END) as restored_lines,
    COUNT(CASE WHEN remaining != original_amount THEN 1 END) as inconsistent_lines,
    ROUND(AVG(remaining), 2) as avg_remaining,
    ROUND(AVG(original_amount), 2) as avg_original
FROM blue_lines;