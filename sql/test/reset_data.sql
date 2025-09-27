-- 负数发票匹配系统 - 重置测试数据
-- 用于清理测试过程中的数据，恢复到初始状态

-- 清空匹配记录
TRUNCATE TABLE match_records;

-- 恢复蓝票行余额（从original_amount）
-- 假设平均使用了20%，恢复为原始金额的80%
UPDATE blue_lines
SET remaining = original_amount * 0.8
WHERE remaining != original_amount;