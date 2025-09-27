-- 负数发票匹配系统 - 统计查询
-- 用于分析数据分布和测试验证

-- 余额分布统计
-- @description: 按余额范围统计数据分布，用于了解数据特征
SELECT
    CASE
        WHEN remaining = 0 THEN '0（已用尽）'
        WHEN remaining < 50 THEN '1-50元（碎片）'
        WHEN remaining < 100 THEN '50-100元'
        WHEN remaining < 500 THEN '100-500元'
        WHEN remaining < 1000 THEN '500-1000元'
        ELSE '1000元以上'
    END as range,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 /
        (SELECT COUNT(*) FROM blue_lines), 2) as percentage
FROM blue_lines
GROUP BY range
ORDER BY
    CASE range
        WHEN '0（已用尽）' THEN 0
        WHEN '1-50元（碎片）' THEN 1
        WHEN '50-100元' THEN 2
        WHEN '100-500元' THEN 3
        WHEN '500-1000元' THEN 4
        ELSE 5
    END;

-- 税率分布统计
-- @description: 统计各税率的数据量分布
SELECT tax_rate, COUNT(*),
       ROUND(COUNT(*) * 100.0 /
            (SELECT COUNT(*) FROM blue_lines), 2) as percentage
FROM blue_lines
GROUP BY tax_rate
ORDER BY COUNT(*) DESC;

-- 活跃数据统计
-- @description: 统计有余额（可用于匹配）的数据比例
SELECT
    COUNT(*) FILTER (WHERE remaining > 0) as active_count,
    COUNT(*) as total_count,
    ROUND(COUNT(*) FILTER (WHERE remaining > 0) * 100.0 /
          COUNT(*), 2) as active_rate
FROM blue_lines;