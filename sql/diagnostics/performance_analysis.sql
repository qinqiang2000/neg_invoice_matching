-- 负数发票匹配系统性能诊断SQL
-- 目标：分析查询性能、索引使用情况和优化建议

-- 1. 检查当前表的数据量和分布
\echo '=== 数据量统计 ==='
SELECT
    'blue_lines' as table_name,
    COUNT(*) as total_count,
    COUNT(*) FILTER (WHERE remaining > 0) as available_count,
    COUNT(*) FILTER (WHERE remaining = 0) as used_count,
    ROUND(AVG(remaining::numeric), 2) as avg_remaining,
    MIN(remaining) as min_remaining,
    MAX(remaining) as max_remaining
FROM blue_lines
UNION ALL
SELECT
    'match_records' as table_name,
    COUNT(*) as total_count,
    NULL, NULL, NULL, NULL, NULL
FROM match_records;

-- 2. 检查数据分布（买方、卖方、税率）
\echo '\n=== 数据分布分析 ==='
SELECT
    'buyer_distribution' as metric,
    COUNT(DISTINCT buyer_id) as unique_count,
    COUNT(*) / COUNT(DISTINCT buyer_id) as avg_per_group
FROM blue_lines
UNION ALL
SELECT
    'seller_distribution' as metric,
    COUNT(DISTINCT seller_id) as unique_count,
    COUNT(*) / COUNT(DISTINCT seller_id) as avg_per_group
FROM blue_lines
UNION ALL
SELECT
    'tax_rate_distribution' as metric,
    COUNT(DISTINCT tax_rate) as unique_count,
    COUNT(*) / COUNT(DISTINCT tax_rate) as avg_per_group
FROM blue_lines;

-- 3. 检查索引使用情况
\echo '\n=== 索引分析 ==='
SELECT
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
WHERE tablename IN ('blue_lines', 'match_records')
ORDER BY tablename, idx_scan DESC;

-- 4. 分析典型查询的执行计划
\echo '\n=== 查询执行计划分析 ==='

-- 模拟典型的候选查询
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT line_id, remaining, tax_rate, buyer_id, seller_id
FROM blue_lines
WHERE tax_rate = 0.13 AND buyer_id = 'BUYER_001' AND seller_id = 'SELLER_001' AND remaining > 0
ORDER BY remaining ASC
LIMIT 100;

-- 5. 检查表的统计信息
\echo '\n=== 表统计信息 ==='
SELECT
    schemaname,
    tablename,
    n_tup_ins,
    n_tup_upd,
    n_tup_del,
    n_live_tup,
    n_dead_tup,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables
WHERE tablename IN ('blue_lines', 'match_records');

-- 6. 检查表的大小和存储情况
\echo '\n=== 存储使用分析 ==='
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) as index_size
FROM pg_tables
WHERE tablename IN ('blue_lines', 'match_records')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- 7. 检查连接和锁情况
\echo '\n=== 连接和锁分析 ==='
SELECT
    state,
    COUNT(*) as connection_count,
    AVG(EXTRACT(epoch FROM (now() - state_change))) as avg_duration_seconds
FROM pg_stat_activity
WHERE datname = current_database()
GROUP BY state;

-- 8. 慢查询分析（如果启用了pg_stat_statements）
\echo '\n=== 慢查询分析 ==='
-- 注意：需要先启用pg_stat_statements扩展
SELECT
    query,
    calls,
    total_time,
    mean_time,
    rows,
    100.0 * shared_blks_hit / nullif(shared_blks_hit + shared_blks_read, 0) AS hit_percent
FROM pg_stat_statements
WHERE query LIKE '%blue_lines%'
ORDER BY mean_time DESC
LIMIT 10;

-- 9. 当前候选查询性能基准测试
\echo '\n=== 候选查询性能基准 ==='

-- 测试不同数据分布下的查询性能
DO $$
DECLARE
    start_time timestamp;
    end_time timestamp;
    duration_ms numeric;
    test_tax_rate numeric := 0.13;
    test_buyer_id text := 'BUYER_001';
    test_seller_id text := 'SELLER_001';
    result_count integer;
BEGIN
    -- 测试基本查询
    start_time := clock_timestamp();

    SELECT COUNT(*) INTO result_count
    FROM blue_lines
    WHERE tax_rate = test_tax_rate
      AND buyer_id = test_buyer_id
      AND seller_id = test_seller_id
      AND remaining > 0;

    end_time := clock_timestamp();
    duration_ms := EXTRACT(epoch FROM (end_time - start_time)) * 1000;

    RAISE NOTICE '基本过滤查询: %ms, 结果数: %', ROUND(duration_ms, 2), result_count;

    -- 测试排序查询
    start_time := clock_timestamp();

    PERFORM line_id, remaining
    FROM blue_lines
    WHERE tax_rate = test_tax_rate
      AND buyer_id = test_buyer_id
      AND seller_id = test_seller_id
      AND remaining > 0
    ORDER BY remaining ASC
    LIMIT 100;

    end_time := clock_timestamp();
    duration_ms := EXTRACT(epoch FROM (end_time - start_time)) * 1000;

    RAISE NOTICE '排序限制查询: %ms', ROUND(duration_ms, 2);
END $$;

-- 10. 建议的优化索引
\echo '\n=== 索引优化建议 ==='
\echo '-- 当前索引情况:'
SELECT
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename = 'blue_lines'
ORDER BY indexname;

\echo '\n-- 建议创建的覆盖索引:'
\echo 'CREATE INDEX CONCURRENTLY idx_blue_lines_covering'
\echo '    ON blue_lines (tax_rate, buyer_id, seller_id, remaining, line_id)'
\echo '    WHERE remaining > 0;'

\echo '\n-- 建议创建的部分索引:'
\echo 'CREATE INDEX CONCURRENTLY idx_blue_lines_available_sorted'
\echo '    ON blue_lines (tax_rate, buyer_id, seller_id, remaining)'
\echo '    WHERE remaining > 0'
\echo '    INCLUDE (line_id);'