-- 测试优化后的查询策略
-- 验证是否可以移除ORDER BY子句

-- 1. 测试当前查询（带ORDER BY）
\echo '=== 当前查询（带ORDER BY）==='
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT line_id, remaining, tax_rate, buyer_id, seller_id
FROM blue_lines
WHERE tax_rate = 13 AND buyer_id = 1 AND seller_id = 1 AND remaining > 0
ORDER BY remaining ASC
LIMIT 100;

-- 2. 测试优化后查询（不带ORDER BY，依赖索引顺序）
\echo '\n=== 优化查询（无ORDER BY，依赖索引）==='
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT line_id, remaining, tax_rate, buyer_id, seller_id
FROM blue_lines
WHERE tax_rate = 13 AND buyer_id = 1 AND seller_id = 1 AND remaining > 0
LIMIT 100;

-- 3. 验证两个查询结果的一致性
\echo '\n=== 验证结果一致性 ==='

-- 执行两个查询并比较结果
WITH
ordered_query AS (
    SELECT line_id, remaining, tax_rate, buyer_id, seller_id, 1 as query_type
    FROM blue_lines
    WHERE tax_rate = 13 AND buyer_id = 1 AND seller_id = 1 AND remaining > 0
    ORDER BY remaining ASC
    LIMIT 100
),
optimized_query AS (
    SELECT line_id, remaining, tax_rate, buyer_id, seller_id, 2 as query_type
    FROM blue_lines
    WHERE tax_rate = 13 AND buyer_id = 1 AND seller_id = 1 AND remaining > 0
    LIMIT 100
)
SELECT
    '结果一致性检查' as test_name,
    COUNT(*) FILTER (WHERE query_type = 1) as ordered_count,
    COUNT(*) FILTER (WHERE query_type = 2) as optimized_count,
    CASE
        WHEN COUNT(*) FILTER (WHERE query_type = 1) = COUNT(*) FILTER (WHERE query_type = 2)
        THEN '✅ 结果数量一致'
        ELSE '❌ 结果数量不一致'
    END as count_check
FROM (
    SELECT * FROM ordered_query
    UNION ALL
    SELECT * FROM optimized_query
) combined;

-- 4. 测试不同组合的索引使用情况
\echo '\n=== 测试不同组合的查询性能 ==='

-- 测试组合1
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT line_id, remaining
FROM blue_lines
WHERE tax_rate = 17 AND buyer_id = 50 AND seller_id = 75 AND remaining > 0
LIMIT 50;

-- 测试组合2
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT line_id, remaining
FROM blue_lines
WHERE tax_rate = 6 AND buyer_id = 25 AND seller_id = 30 AND remaining > 0
LIMIT 50;

-- 5. 分析索引选择性
\echo '\n=== 索引选择性分析 ==='
SELECT
    'tax_rate' as column_name,
    COUNT(DISTINCT tax_rate) as unique_values,
    COUNT(*) as total_rows,
    ROUND(COUNT(DISTINCT tax_rate)::numeric / COUNT(*), 4) as selectivity
FROM blue_lines
WHERE remaining > 0
UNION ALL
SELECT
    'buyer_id' as column_name,
    COUNT(DISTINCT buyer_id) as unique_values,
    COUNT(*) as total_rows,
    ROUND(COUNT(DISTINCT buyer_id)::numeric / COUNT(*), 4) as selectivity
FROM blue_lines
WHERE remaining > 0
UNION ALL
SELECT
    'seller_id' as column_name,
    COUNT(DISTINCT seller_id) as unique_values,
    COUNT(*) as total_rows,
    ROUND(COUNT(DISTINCT seller_id)::numeric / COUNT(*), 4) as selectivity
FROM blue_lines
WHERE remaining > 0
ORDER BY selectivity DESC;

\echo '\n=== 查询优化测试完成 ==='
\echo '如果覆盖索引已经提供了所需的排序，可以安全移除ORDER BY子句'