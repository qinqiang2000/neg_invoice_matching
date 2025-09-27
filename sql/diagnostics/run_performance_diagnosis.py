#!/usr/bin/env python3
"""
数据库性能诊断脚本

执行数据库性能分析，收集关键指标，生成诊断报告
"""

import sys
import os
import psutil
import time
from datetime import datetime
from typing import Dict, List, Any
import json

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.db_manager import DatabaseManager
from config.config import get_db_config


class PerformanceDiagnostics:
    """数据库性能诊断工具"""

    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.db_manager = DatabaseManager(db_config)
        self.diagnosis_results = {}

    def run_full_diagnosis(self) -> Dict[str, Any]:
        """运行完整的性能诊断"""
        print("=== 开始数据库性能诊断 ===\n")

        # 1. 基本数据统计
        print("1. 收集基本数据统计...")
        self.diagnosis_results['data_stats'] = self._collect_data_stats()

        # 2. 索引使用分析
        print("2. 分析索引使用情况...")
        self.diagnosis_results['index_analysis'] = self._analyze_indexes()

        # 3. 查询性能基准测试
        print("3. 执行查询性能基准测试...")
        self.diagnosis_results['query_benchmarks'] = self._run_query_benchmarks()

        # 4. 存储分析
        print("4. 分析存储使用情况...")
        self.diagnosis_results['storage_analysis'] = self._analyze_storage()

        # 5. 连接和资源分析
        print("5. 分析连接和系统资源...")
        self.diagnosis_results['resource_analysis'] = self._analyze_resources()

        # 6. 生成诊断报告
        print("6. 生成诊断报告...")
        report = self._generate_diagnosis_report()

        print("\n=== 性能诊断完成 ===")
        return {
            'raw_data': self.diagnosis_results,
            'report': report,
            'timestamp': datetime.now().isoformat()
        }

    def _collect_data_stats(self) -> Dict[str, Any]:
        """收集基本数据统计"""
        conn = self.db_manager.pool.getconn()
        try:
            with conn.cursor() as cur:
                stats = {}

                # 表数据量统计
                cur.execute("""
                    SELECT
                        COUNT(*) as total_count,
                        COUNT(*) FILTER (WHERE remaining > 0) as available_count,
                        COUNT(*) FILTER (WHERE remaining = 0) as used_count,
                        ROUND(AVG(remaining::numeric), 2) as avg_remaining,
                        MIN(remaining) as min_remaining,
                        MAX(remaining) as max_remaining
                    FROM blue_lines
                """)
                blue_lines_stats = cur.fetchone()
                stats['blue_lines'] = {
                    'total_count': blue_lines_stats[0],
                    'available_count': blue_lines_stats[1],
                    'used_count': blue_lines_stats[2],
                    'avg_remaining': float(blue_lines_stats[3]) if blue_lines_stats[3] else 0,
                    'min_remaining': float(blue_lines_stats[4]) if blue_lines_stats[4] else 0,
                    'max_remaining': float(blue_lines_stats[5]) if blue_lines_stats[5] else 0
                }

                # 数据分布分析
                cur.execute("""
                    SELECT
                        COUNT(DISTINCT buyer_id) as unique_buyers,
                        COUNT(DISTINCT seller_id) as unique_sellers,
                        COUNT(DISTINCT tax_rate) as unique_tax_rates,
                        COUNT(DISTINCT (buyer_id, seller_id, tax_rate)) as unique_combinations
                    FROM blue_lines
                """)
                distribution = cur.fetchone()
                stats['distribution'] = {
                    'unique_buyers': distribution[0],
                    'unique_sellers': distribution[1],
                    'unique_tax_rates': distribution[2],
                    'unique_combinations': distribution[3],
                    'avg_records_per_combination': blue_lines_stats[0] / distribution[3] if distribution[3] > 0 else 0
                }

                return stats
        finally:
            self.db_manager.pool.putconn(conn)

    def _analyze_indexes(self) -> Dict[str, Any]:
        """分析索引使用情况"""
        conn = self.db_manager.pool.getconn()
        try:
            with conn.cursor() as cur:
                # 索引使用统计
                cur.execute("""
                    SELECT
                        indexrelname as indexname,
                        idx_scan,
                        idx_tup_read,
                        idx_tup_fetch
                    FROM pg_stat_user_indexes
                    WHERE relname = 'blue_lines'
                    ORDER BY idx_scan DESC
                """)
                index_stats = []
                for row in cur.fetchall():
                    index_stats.append({
                        'indexname': row[0],
                        'idx_scan': row[1],
                        'idx_tup_read': row[2],
                        'idx_tup_fetch': row[3]
                    })

                # 索引定义
                cur.execute("""
                    SELECT indexname, indexdef
                    FROM pg_indexes
                    WHERE tablename = 'blue_lines'
                    ORDER BY indexname
                """)
                index_definitions = []
                for row in cur.fetchall():
                    index_definitions.append({
                        'indexname': row[0],
                        'definition': row[1]
                    })

                return {
                    'usage_stats': index_stats,
                    'definitions': index_definitions
                }
        finally:
            self.db_manager.pool.putconn(conn)

    def _run_query_benchmarks(self) -> Dict[str, Any]:
        """运行查询性能基准测试"""
        conn = self.db_manager.pool.getconn()
        try:
            with conn.cursor() as cur:
                benchmarks = {}

                # 测试基本过滤查询
                test_params = {
                    'tax_rate': 13,  # tax_rate 是 SMALLINT，使用整数
                    'buyer_id': 1,
                    'seller_id': 1
                }

                # 1. 基本计数查询
                start_time = time.perf_counter()
                cur.execute("""
                    SELECT COUNT(*)
                    FROM blue_lines
                    WHERE tax_rate = %s AND buyer_id = %s AND seller_id = %s AND remaining > 0
                """, (test_params['tax_rate'], test_params['buyer_id'], test_params['seller_id']))
                count_result = cur.fetchone()[0]
                count_time = (time.perf_counter() - start_time) * 1000

                benchmarks['basic_count'] = {
                    'duration_ms': round(count_time, 2),
                    'result_count': count_result
                }

                # 2. 排序查询（模拟实际业务查询）
                start_time = time.perf_counter()
                cur.execute("""
                    SELECT line_id, remaining, tax_rate, buyer_id, seller_id
                    FROM blue_lines
                    WHERE tax_rate = %s AND buyer_id = %s AND seller_id = %s AND remaining > 0
                    ORDER BY remaining ASC
                    LIMIT 100
                """, (test_params['tax_rate'], test_params['buyer_id'], test_params['seller_id']))
                sorted_results = cur.fetchall()
                sort_time = (time.perf_counter() - start_time) * 1000

                benchmarks['sorted_query'] = {
                    'duration_ms': round(sort_time, 2),
                    'result_count': len(sorted_results)
                }

                # 3. 执行计划分析
                cur.execute("""
                    EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
                    SELECT line_id, remaining, tax_rate, buyer_id, seller_id
                    FROM blue_lines
                    WHERE tax_rate = %s AND buyer_id = %s AND seller_id = %s AND remaining > 0
                    ORDER BY remaining ASC
                    LIMIT 100
                """, (test_params['tax_rate'], test_params['buyer_id'], test_params['seller_id']))
                explain_result = cur.fetchone()[0]

                benchmarks['explain_analysis'] = explain_result[0]
                benchmarks['test_parameters'] = test_params

                return benchmarks
        finally:
            self.db_manager.pool.putconn(conn)

    def _analyze_storage(self) -> Dict[str, Any]:
        """分析存储使用情况"""
        conn = self.db_manager.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        tablename,
                        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
                        pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
                        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) as index_size,
                        pg_total_relation_size(schemaname||'.'||tablename) as total_bytes
                    FROM pg_tables
                    WHERE tablename IN ('blue_lines', 'match_records')
                    ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
                """)

                storage_info = []
                for row in cur.fetchall():
                    storage_info.append({
                        'tablename': row[0],
                        'total_size': row[1],
                        'table_size': row[2],
                        'index_size': row[3],
                        'total_bytes': row[4]
                    })

                return {'table_sizes': storage_info}
        finally:
            self.db_manager.pool.putconn(conn)

    def _analyze_resources(self) -> Dict[str, Any]:
        """分析系统资源和连接情况"""
        conn = self.db_manager.pool.getconn()
        try:
            with conn.cursor() as cur:
                # 数据库连接状态
                cur.execute("""
                    SELECT
                        state,
                        COUNT(*) as connection_count
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                    GROUP BY state
                """)
                connection_stats = []
                for row in cur.fetchall():
                    connection_stats.append({
                        'state': row[0],
                        'count': row[1]
                    })

                # 系统资源
                system_resources = {
                    'cpu_percent': psutil.cpu_percent(interval=1),
                    'memory_percent': psutil.virtual_memory().percent,
                    'memory_available_mb': psutil.virtual_memory().available / 1024 / 1024
                }

                return {
                    'database_connections': connection_stats,
                    'system_resources': system_resources
                }
        finally:
            self.db_manager.pool.putconn(conn)

    def _generate_diagnosis_report(self) -> str:
        """生成诊断报告"""
        results = self.diagnosis_results
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        report = f"""
# 数据库性能诊断报告

**诊断时间**: {timestamp}

## 1. 数据概况

### 表数据统计
- **总记录数**: {results['data_stats']['blue_lines']['total_count']:,}
- **可用记录数**: {results['data_stats']['blue_lines']['available_count']:,}
- **已使用记录数**: {results['data_stats']['blue_lines']['used_count']:,}
- **可用率**: {results['data_stats']['blue_lines']['available_count'] / results['data_stats']['blue_lines']['total_count'] * 100:.1f}%

### 数据分布
- **买方数量**: {results['data_stats']['distribution']['unique_buyers']}
- **卖方数量**: {results['data_stats']['distribution']['unique_sellers']}
- **税率种类**: {results['data_stats']['distribution']['unique_tax_rates']}
- **组合总数**: {results['data_stats']['distribution']['unique_combinations']}
- **平均每组合记录数**: {results['data_stats']['distribution']['avg_records_per_combination']:.1f}

## 2. 查询性能分析

### 性能基准测试结果
"""

        # 查询性能分析
        benchmarks = results['query_benchmarks']
        basic_count = benchmarks['basic_count']
        sorted_query = benchmarks['sorted_query']

        report += f"""
- **基本计数查询**: {basic_count['duration_ms']}ms (结果: {basic_count['result_count']} 条)
- **排序限制查询**: {sorted_query['duration_ms']}ms (结果: {sorted_query['result_count']} 条)

### 性能问题诊断
"""

        # 性能问题分析
        if sorted_query['duration_ms'] > 100:
            report += f"""
⚠️ **严重性能问题发现**:
- 排序查询耗时 {sorted_query['duration_ms']}ms，远超预期(<50ms)
- 问题可能原因：
  1. 缺少有效的覆盖索引
  2. ORDER BY操作成本过高
  3. 数据分布导致查询集过大

"""

        if basic_count['duration_ms'] > 10:
            report += f"""
⚠️ **基础查询性能问题**:
- 基本过滤查询耗时 {basic_count['duration_ms']}ms，超过预期(<10ms)
- 建议检查索引使用情况

"""

        # 执行计划分析
        explain = benchmarks['explain_analysis']
        if 'Plan' in explain:
            plan = explain['Plan']
            report += f"""
### 执行计划分析
- **执行时间**: {explain.get('Execution Time', 0):.2f}ms
- **规划时间**: {explain.get('Planning Time', 0):.2f}ms
- **主要操作**: {plan.get('Node Type', 'Unknown')}
- **扫描方式**: {'索引扫描' if 'Index' in plan.get('Node Type', '') else '顺序扫描'}

"""

        # 索引分析
        report += f"""
## 3. 索引使用分析

### 当前索引状态
"""
        for idx in results['index_analysis']['definitions']:
            usage_info = next((u for u in results['index_analysis']['usage_stats'] if u['indexname'] == idx['indexname']), {})
            scans = usage_info.get('idx_scan', 0)
            report += f"""
- **{idx['indexname']}**: 扫描次数 {scans}
  ```sql
  {idx['definition']}
  ```

"""

        # 存储分析
        report += f"""
## 4. 存储使用分析

"""
        for table in results['storage_analysis']['table_sizes']:
            report += f"""
### {table['tablename']} 表
- **总大小**: {table['total_size']}
- **表大小**: {table['table_size']}
- **索引大小**: {table['index_size']}

"""

        # 优化建议
        report += f"""
## 5. 优化建议

### 立即执行（高优先级）

1. **创建覆盖索引**：
   ```sql
   CREATE INDEX CONCURRENTLY idx_blue_lines_covering
   ON blue_lines (tax_rate, buyer_id, seller_id, remaining, line_id)
   WHERE remaining > 0;
   ```

2. **优化查询策略**：
   - 考虑移除不必要的ORDER BY操作
   - 实现批量查询减少数据库往返

3. **连接池优化**：
   - 检查连接释放逻辑
   - 调整连接池配置参数

### 中期改进（中优先级）

1. **部分索引优化**：
   ```sql
   CREATE INDEX CONCURRENTLY idx_blue_lines_available_sorted
   ON blue_lines (tax_rate, buyer_id, seller_id, remaining)
   WHERE remaining > 0
   INCLUDE (line_id);
   ```

2. **查询策略改进**：
   - 实现预筛选机制
   - 优化候选集大小控制

### 长期规划（低优先级）

1. **数据分区**：考虑按tax_rate或buyer_id进行分区
2. **缓存层**：实现候选集缓存
3. **监控体系**：部署实时性能监控

## 6. 预期改进效果

实施上述优化后，预期性能改善：
- **查询时间**：从{sorted_query['duration_ms']}ms降至10-50ms
- **系统吞吐量**：提升10-50倍
- **资源使用**：减少50-80%

---
*报告生成时间: {timestamp}*
"""

        return report

    def save_results(self, results: Dict[str, Any], filename: str = None):
        """保存诊断结果到文件"""
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"docs/performance_diagnosis_{timestamp}.md"

        # 保存报告
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(results['report'])

        # 保存原始数据
        json_filename = filename.replace('.md', '_raw_data.json')
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(results['raw_data'], f, indent=2, ensure_ascii=False, default=str)

        print(f"✓ 诊断报告已保存: {filename}")
        print(f"✓ 原始数据已保存: {json_filename}")

        return filename

    def close(self):
        """关闭资源"""
        # DatabaseManager 会自动管理连接池


def main():
    """主函数"""
    print("负数发票匹配系统 - 数据库性能诊断工具")
    print("=" * 50)

    # 获取数据库配置
    db_config = get_db_config('test')

    # 创建诊断工具
    diagnostics = PerformanceDiagnostics(db_config)

    try:
        # 运行诊断
        results = diagnostics.run_full_diagnosis()

        # 保存结果
        report_file = diagnostics.save_results(results)

        print(f"\n🎉 性能诊断完成！")
        print(f"📄 详细报告: {report_file}")

        # 输出关键发现
        data_stats = results['raw_data']['data_stats']
        query_benchmarks = results['raw_data']['query_benchmarks']

        print(f"\n📊 关键发现:")
        print(f"  数据量: {data_stats['blue_lines']['total_count']:,} 条")
        print(f"  可用数据: {data_stats['blue_lines']['available_count']:,} 条 ({data_stats['blue_lines']['available_count'] / data_stats['blue_lines']['total_count'] * 100:.1f}%)")
        print(f"  排序查询耗时: {query_benchmarks['sorted_query']['duration_ms']}ms")

        if query_benchmarks['sorted_query']['duration_ms'] > 100:
            print(f"  ⚠️  查询性能需要优化")
        else:
            print(f"  ✅ 查询性能良好")

    except Exception as e:
        print(f"❌ 诊断过程中发生错误: {e}")
        raise
    finally:
        diagnostics.close()


if __name__ == "__main__":
    main()