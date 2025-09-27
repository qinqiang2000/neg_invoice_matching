-- 负数发票匹配系统 - 性能优化索引
-- 创建覆盖索引以减少回表查询和提升性能

-- 检查现有索引状态
\echo '=== 当前索引状态 ==='
SELECT
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
WHERE relname = 'blue_lines'
ORDER BY idx_scan DESC;

-- 1. 创建最重要的覆盖索引
-- 这个索引将包含查询所需的所有字段，避免回表查询
\echo '\n=== 创建覆盖索引 ==='
CREATE INDEX CONCURRENTLY idx_blue_lines_covering
ON blue_lines (tax_rate, buyer_id, seller_id, remaining, line_id)
WHERE remaining > 0;

-- 验证索引创建
\echo '验证覆盖索引创建状态...'
SELECT
    indexname,
    indexdef,
    pg_size_pretty(pg_relation_size(indexname::regclass)) as index_size
FROM pg_indexes
WHERE tablename = 'blue_lines'
  AND indexname = 'idx_blue_lines_covering';

-- 2. 创建部分索引优化（包含INCLUDE列的PostgreSQL 11+特性）
-- 注意：INCLUDE语法需要PostgreSQL 11+
\echo '\n=== 创建INCLUDE索引（如果支持）==='
DO $$
BEGIN
    -- 检查PostgreSQL版本是否支持INCLUDE
    IF (SELECT setting::int FROM pg_settings WHERE name = 'server_version_num') >= 110000 THEN
        EXECUTE 'CREATE INDEX CONCURRENTLY idx_blue_lines_include
                 ON blue_lines (tax_rate, buyer_id, seller_id, remaining)
                 INCLUDE (line_id)
                 WHERE remaining > 0';
        RAISE NOTICE '成功创建INCLUDE索引';
    ELSE
        RAISE NOTICE 'PostgreSQL版本不支持INCLUDE语法，跳过';
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '创建INCLUDE索引失败: %', SQLERRM;
END $$;

-- 3. 删除可能冗余的旧索引（谨慎操作）
-- 注意：只有在新索引验证有效后才删除
\echo '\n=== 分析索引冗余性 ==='
SELECT
    'idx_blue_lines_matching' as old_index,
    'idx_blue_lines_covering' as new_index,
    '新覆盖索引完全包含旧索引功能' as analysis;

-- 检查索引大小对比
\echo '\n=== 索引大小对比 ==='
SELECT
    indexname,
    pg_size_pretty(pg_relation_size(indexname::regclass)) as size_pretty,
    pg_relation_size(indexname::regclass) as size_bytes
FROM pg_indexes
WHERE tablename = 'blue_lines'
  AND indexname IN ('idx_blue_lines_matching', 'idx_blue_lines_matching_sorted', 'idx_blue_lines_covering')
ORDER BY pg_relation_size(indexname::regclass) DESC;

-- 4. 测试新索引性能
\echo '\n=== 测试新索引性能 ==='

-- 强制使用新索引进行查询测试
SET enable_seqscan = OFF;
SET enable_bitmapscan = OFF;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT line_id, remaining, tax_rate, buyer_id, seller_id
FROM blue_lines
WHERE tax_rate = 13 AND buyer_id = 1 AND seller_id = 1 AND remaining > 0
ORDER BY remaining ASC
LIMIT 100;

-- 恢复优化器设置
RESET enable_seqscan;
RESET enable_bitmapscan;

-- 5. 更新表统计信息
\echo '\n=== 更新统计信息 ==='
ANALYZE blue_lines;

-- 检查新索引的使用情况
\echo '\n=== 验证索引使用状况 ==='
SELECT
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch,
    CASE
        WHEN idx_scan > 0 THEN ROUND(idx_tup_read::numeric / idx_scan, 2)
        ELSE 0
    END as avg_tuples_per_scan
FROM pg_stat_user_indexes
WHERE relname = 'blue_lines'
  AND indexname LIKE 'idx_blue_lines_%'
ORDER BY idx_scan DESC;

\echo '\n=== 索引创建完成 ==='
\echo '建议：'
\echo '1. 观察新索引使用情况1-2天'
\echo '2. 确认性能提升后，可考虑删除冗余的idx_blue_lines_matching索引'
\echo '3. 监控索引维护开销'
\echo '4. 运行性能测试验证改进效果'