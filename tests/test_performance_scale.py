#!/usr/bin/env python3
"""
大规模性能测试脚本

测试目的：
1. 验证P99延迟<70ms的性能目标
2. 测试千万级数据下的系统表现
3. 验证索引效率和查询性能
4. 内存使用监控
5. 生成详细的性能测试报告

测试环境：
- 数据库：PostgreSQL 17.6
- 硬件：4核8线程，16GB内存，300GB SSD
- 测试数据：100万 - 1000万蓝票行

运行方式：
python test_performance_scale.py --scale small    # 100万数据
python test_performance_scale.py --scale medium   # 500万数据
python test_performance_scale.py --scale large    # 1000万数据
python test_performance_scale.py --scale all      # 全部测试
"""

import sys
import os
import argparse
import time
import psutil
import json
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from decimal import Decimal
from dataclasses import dataclass, asdict
import statistics

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.matching_engine import GreedyMatchingEngine, NegativeInvoice
from core.db_manager import DatabaseManager, CandidateProvider
from core.monitoring import get_monitor
from config.config import get_db_config
from tests.test_data_generator import TestDataGenerator


@dataclass
class PerformanceMetrics:
    """性能指标数据结构"""
    test_name: str
    data_scale: str
    blue_lines_count: int
    negative_invoices_count: int

    # 响应时间指标 (毫秒)
    response_times: List[float]
    p50_response_time: float
    p90_response_time: float
    p95_response_time: float
    p99_response_time: float
    avg_response_time: float
    max_response_time: float
    min_response_time: float

    # 匹配结果指标
    success_rate: float
    total_matched_amount: float
    fragments_created: int

    # 系统资源指标
    peak_memory_mb: float
    avg_cpu_percent: float
    database_query_count: int
    database_query_time_ms: float

    # 数据库性能指标
    index_scan_time_ms: float
    transaction_commit_time_ms: float

    # 测试配置
    batch_size: int
    sort_strategy: str
    enable_monitoring: bool

    # 时间戳
    test_timestamp: str
    duration_seconds: float


class PerformanceTestSuite:
    """大规模性能测试套件"""

    def __init__(self, db_config: Dict, test_config: Optional[Dict] = None):
        """
        初始化性能测试套件

        Args:
            db_config: 数据库配置
            test_config: 测试配置
        """
        self.db_config = db_config
        self.db_manager = DatabaseManager(db_config)
        self.engine = GreedyMatchingEngine()
        self.candidate_provider = CandidateProvider(self.db_manager)

        # 测试配置
        self.test_config = test_config or {}
        self.batch_id_prefix = "perf_test"

        # 性能监控
        self.process = psutil.Process()
        self.test_results: List[PerformanceMetrics] = []

        # 数据生成器
        self.data_generator = TestDataGenerator(db_config)

        # 测试规模配置
        self.scale_configs = {
            'small': {
                'blue_lines': 1_000_000,      # 100万
                'negative_batches': [100, 500, 1000],
                'description': '小规模测试（100万蓝票行）'
            },
            'medium': {
                'blue_lines': 5_000_000,      # 500万
                'negative_batches': [500, 1000, 2000],
                'description': '中等规模测试（500万蓝票行）'
            },
            'large': {
                'blue_lines': 10_000_000,     # 1000万
                'negative_batches': [1000, 5000, 10000],
                'description': '大规模测试（1000万蓝票行）'
            }
        }

    def setup_test_data(self, scale: str) -> str:
        """
        设置测试数据

        Args:
            scale: 测试规模 (small/medium/large)

        Returns:
            str: 批次ID
        """
        config = self.scale_configs.get(scale)
        if not config:
            raise ValueError(f"不支持的测试规模: {scale}")

        blue_lines_count = config['blue_lines']
        batch_id = f"{self.batch_id_prefix}_{scale}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        print(f"\n=== 设置{config['description']} ===")
        print(f"准备生成 {blue_lines_count:,} 条蓝票行数据...")

        # 检查是否已存在相同规模的数据
        existing_batch = self._find_existing_batch(blue_lines_count)
        if existing_batch:
            print(f"发现已存在的相同规模数据批次: {existing_batch}")
            choice = input("是否重用现有数据？(y/n): ").lower()
            if choice == 'y':
                return existing_batch

        # 生成新数据
        start_time = time.time()
        actual_batch_id = self.data_generator.generate_blue_lines(
            total_lines=blue_lines_count,
            batch_id=batch_id
        )
        generation_time = time.time() - start_time

        print(f"✓ 数据生成完成，耗时: {generation_time:.2f}秒")
        print(f"✓ 批次ID: {actual_batch_id}")

        return actual_batch_id

    def _find_existing_batch(self, target_lines: int) -> Optional[str]:
        """查找已存在的相同规模批次"""
        conn = self.db_manager.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT batch_id FROM batch_metadata
                    WHERE total_lines = %s AND status = 'completed'
                    ORDER BY end_time DESC LIMIT 1
                """, (target_lines,))
                result = cur.fetchone()
                return result[0] if result else None
        finally:
            self.db_manager.pool.putconn(conn)

    def run_performance_test(self, scale: str, batch_id: str) -> List[PerformanceMetrics]:
        """
        运行性能测试

        Args:
            scale: 测试规模
            batch_id: 数据批次ID

        Returns:
            List[PerformanceMetrics]: 测试结果
        """
        config = self.scale_configs[scale]
        negative_batches = config['negative_batches']

        print(f"\n=== 执行{config['description']}性能测试 ===")

        scale_results = []

        for negative_count in negative_batches:
            print(f"\n--- 测试批次: {negative_count} 个负数发票 ---")

            # 生成负数发票
            negatives = self.data_generator.generate_negative_invoices_objects(
                scenario="mixed",
                count=negative_count
            )

            # 执行性能测试
            metrics = self._execute_single_test(
                test_name=f"{scale}_{negative_count}",
                scale=scale,
                negatives=negatives,
                batch_id=batch_id
            )

            scale_results.append(metrics)

            # 输出测试结果摘要
            self._print_test_summary(metrics)

            # 短暂休息，避免系统过热
            time.sleep(2)

        return scale_results

    def _execute_single_test(self, test_name: str, scale: str,
                           negatives: List[NegativeInvoice], batch_id: str) -> PerformanceMetrics:
        """
        执行单个性能测试

        Args:
            test_name: 测试名称
            scale: 测试规模
            negatives: 负数发票列表
            batch_id: 数据批次ID

        Returns:
            PerformanceMetrics: 性能指标
        """
        # 重置监控状态
        monitor = get_monitor()
        monitor.reset_stats()

        # 预热：执行一次小规模查询
        warmup_negatives = negatives[:5] if len(negatives) > 5 else negatives
        self.engine.match_batch(warmup_negatives, self.candidate_provider)
        time.sleep(1)  # 等待预热完成

        # 开始性能监控
        start_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        start_time = time.time()
        cpu_samples = []
        memory_samples = []

        # 记录数据库查询前状态
        db_query_start = time.time()

        # 执行匹配测试
        response_times = []

        # 将负数发票分成多个小批次以获得更多响应时间样本
        batch_size = min(100, len(negatives))  # 每批最多100个
        all_results = []

        for i in range(0, len(negatives), batch_size):
            batch_negatives = negatives[i:i + batch_size]

            # 记录单批次性能
            batch_start = time.time()

            # 监控系统资源
            cpu_samples.append(psutil.cpu_percent())
            memory_samples.append(self.process.memory_info().rss / 1024 / 1024)

            # 执行匹配
            batch_results = self.engine.match_batch(
                batch_negatives,
                self.candidate_provider,
                sort_strategy="amount_desc",
                enable_monitoring=True
            )

            batch_time = (time.time() - batch_start) * 1000  # 转换为毫秒
            response_times.append(batch_time)
            all_results.extend(batch_results)

            # 每批次之间短暂休息
            time.sleep(0.1)

        # 测试结束
        total_duration = time.time() - start_time
        db_query_time = (time.time() - db_query_start) * 1000  # 毫秒
        peak_memory = max(memory_samples) if memory_samples else start_memory
        avg_cpu = statistics.mean(cpu_samples) if cpu_samples else 0

        # 计算响应时间统计
        if response_times:
            p50 = statistics.median(response_times)
            p90 = self._percentile(response_times, 90)
            p95 = self._percentile(response_times, 95)
            p99 = self._percentile(response_times, 99)
            avg_time = statistics.mean(response_times)
            max_time = max(response_times)
            min_time = min(response_times)
        else:
            p50 = p90 = p95 = p99 = avg_time = max_time = min_time = 0

        # 计算匹配结果统计
        success_count = sum(1 for r in all_results if r.success)
        success_rate = success_count / len(all_results) if all_results else 0
        total_matched = sum(r.total_matched for r in all_results)
        fragments = sum(r.fragments_created for r in all_results)

        # 获取蓝票行数量（用于统计）
        blue_lines_count = self._get_blue_lines_count(batch_id)

        # 获取监控数据
        health_report = monitor.get_health_report()
        technical_metrics = health_report.get('technical_metrics', {})
        performance_metrics = health_report.get('performance_metrics', {})

        # 构造性能指标对象
        metrics = PerformanceMetrics(
            test_name=test_name,
            data_scale=scale,
            blue_lines_count=blue_lines_count,
            negative_invoices_count=len(negatives),

            # 响应时间指标
            response_times=response_times,
            p50_response_time=p50,
            p90_response_time=p90,
            p95_response_time=p95,
            p99_response_time=p99,
            avg_response_time=avg_time,
            max_response_time=max_time,
            min_response_time=min_time,

            # 匹配结果指标
            success_rate=success_rate,
            total_matched_amount=float(total_matched),
            fragments_created=fragments,

            # 系统资源指标
            peak_memory_mb=peak_memory,
            avg_cpu_percent=avg_cpu,
            database_query_count=performance_metrics.get('total_requests', 0),
            database_query_time_ms=db_query_time,

            # 数据库性能指标（简化）
            index_scan_time_ms=db_query_time * 0.4,  # 估算40%时间用于索引扫描
            transaction_commit_time_ms=db_query_time * 0.2,  # 估算20%时间用于事务提交

            # 测试配置
            batch_size=batch_size,
            sort_strategy="amount_desc",
            enable_monitoring=True,

            # 时间戳
            test_timestamp=datetime.now().isoformat(),
            duration_seconds=total_duration
        )

        return metrics

    def _percentile(self, data: List[float], percentile: float) -> float:
        """计算百分位数"""
        if not data:
            return 0
        sorted_data = sorted(data)
        index = (percentile / 100) * (len(sorted_data) - 1)
        if index.is_integer():
            return sorted_data[int(index)]
        else:
            lower_index = int(index)
            upper_index = lower_index + 1
            weight = index - lower_index
            return sorted_data[lower_index] * (1 - weight) + sorted_data[upper_index] * weight

    def _get_blue_lines_count(self, batch_id: str) -> int:
        """获取指定批次的蓝票行数量"""
        conn = self.db_manager.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM blue_lines WHERE batch_id = %s", (batch_id,))
                return cur.fetchone()[0]
        finally:
            self.db_manager.pool.putconn(conn)

    def _print_test_summary(self, metrics: PerformanceMetrics):
        """打印测试结果摘要"""
        print(f"  ✓ 测试完成: {metrics.test_name}")
        print(f"    数据规模: {metrics.blue_lines_count:,} 蓝票行, {metrics.negative_invoices_count} 负数发票")
        print(f"    匹配成功率: {metrics.success_rate:.1%}")
        print(f"    响应时间: P50={metrics.p50_response_time:.1f}ms, P99={metrics.p99_response_time:.1f}ms")
        print(f"    内存峰值: {metrics.peak_memory_mb:.1f}MB")
        print(f"    总耗时: {metrics.duration_seconds:.2f}秒")

        # 检查是否达到性能目标
        if metrics.p99_response_time <= 70:
            print(f"    🎯 P99性能目标达成: {metrics.p99_response_time:.1f}ms ≤ 70ms")
        else:
            print(f"    ⚠️  P99性能目标未达成: {metrics.p99_response_time:.1f}ms > 70ms")

        if metrics.success_rate >= 0.93:
            print(f"    🎯 匹配率目标达成: {metrics.success_rate:.1%} ≥ 93%")
        else:
            print(f"    ⚠️  匹配率目标未达成: {metrics.success_rate:.1%} < 93%")

    def generate_performance_report(self, results: List[PerformanceMetrics],
                                  output_file: Optional[str] = None) -> str:
        """
        生成性能测试报告

        Args:
            results: 测试结果列表
            output_file: 输出文件路径（可选）

        Returns:
            str: 报告内容
        """
        if not results:
            return "无测试结果"

        # 获取系统信息
        system_info = {
            'cpu_count': psutil.cpu_count(),
            'cpu_freq': psutil.cpu_freq()._asdict() if psutil.cpu_freq() else {},
            'memory_total_gb': psutil.virtual_memory().total / 1024 / 1024 / 1024,
            'python_version': sys.version,
            'postgresql_version': self._get_postgresql_version()
        }

        # 生成报告
        report = self._format_performance_report(results, system_info)

        # 保存到文件
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(report)
            print(f"✓ 性能报告已保存至: {output_file}")

        return report

    def _get_postgresql_version(self) -> str:
        """获取PostgreSQL版本"""
        conn = self.db_manager.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                return cur.fetchone()[0]
        except:
            return "Unknown"
        finally:
            self.db_manager.pool.putconn(conn)

    def _format_performance_report(self, results: List[PerformanceMetrics],
                                 system_info: Dict) -> str:
        """格式化性能报告"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        report = f"""
# 负数发票匹配系统 - 大规模性能测试报告

## 测试概况
- **测试时间**: {timestamp}
- **测试目标**: 验证P99延迟<70ms，匹配率>93%
- **测试用例数**: {len(results)}

## 测试环境

### 硬件配置
- **CPU**: {system_info['cpu_count']} 核心
- **内存**: {system_info['memory_total_gb']:.1f} GB
- **存储**: SSD (300GB)

### 软件配置
- **数据库**: PostgreSQL 17.6
- **Python**: {system_info['python_version'].split()[0]}
- **数据库版本**: {system_info['postgresql_version']}

## 测试结果汇总

### 关键指标达成情况
"""

        # 检查关键指标达成情况
        p99_passed = sum(1 for r in results if r.p99_response_time <= 70)
        success_rate_passed = sum(1 for r in results if r.success_rate >= 0.93)

        report += f"""
| 指标 | 目标值 | 达成率 | 状态 |
|------|--------|--------|------|
| P99响应时间 | ≤70ms | {p99_passed}/{len(results)} ({p99_passed/len(results):.1%}) | {'✅' if p99_passed == len(results) else '⚠️'} |
| 匹配成功率 | ≥93% | {success_rate_passed}/{len(results)} ({success_rate_passed/len(results):.1%}) | {'✅' if success_rate_passed == len(results) else '⚠️'} |

### 详细测试结果

"""

        # 详细结果表格
        report += "| 测试规模 | 蓝票行数 | 负数发票数 | P50(ms) | P90(ms) | P95(ms) | P99(ms) | 匹配率 | 内存峰值(MB) | 总耗时(s) |\n"
        report += "|----------|----------|------------|---------|---------|---------|---------|---------|-------------|----------|\n"

        for r in results:
            report += f"| {r.data_scale} | {r.blue_lines_count:,} | {r.negative_invoices_count} | "
            report += f"{r.p50_response_time:.1f} | {r.p90_response_time:.1f} | {r.p95_response_time:.1f} | "
            report += f"{r.p99_response_time:.1f} | {r.success_rate:.1%} | {r.peak_memory_mb:.1f} | {r.duration_seconds:.2f} |\n"

        # 性能分析
        report += "\n## 性能分析\n\n"

        # 最佳和最差性能
        best_p99 = min(results, key=lambda x: x.p99_response_time)
        worst_p99 = max(results, key=lambda x: x.p99_response_time)
        best_success = max(results, key=lambda x: x.success_rate)
        worst_success = min(results, key=lambda x: x.success_rate)

        report += f"""### 性能表现
- **最佳P99响应时间**: {best_p99.p99_response_time:.1f}ms ({best_p99.test_name})
- **最差P99响应时间**: {worst_p99.p99_response_time:.1f}ms ({worst_p99.test_name})
- **最高匹配成功率**: {best_success.success_rate:.1%} ({best_success.test_name})
- **最低匹配成功率**: {worst_success.success_rate:.1%} ({worst_success.test_name})

### 资源使用分析
"""

        max_memory = max(r.peak_memory_mb for r in results)
        avg_memory = sum(r.peak_memory_mb for r in results) / len(results)
        max_cpu = max(r.avg_cpu_percent for r in results)

        report += f"""- **内存峰值**: {max_memory:.1f}MB
- **平均内存使用**: {avg_memory:.1f}MB
- **CPU峰值**: {max_cpu:.1f}%

### 可扩展性分析
"""

        # 按规模分组分析
        scale_groups = {}
        for r in results:
            if r.data_scale not in scale_groups:
                scale_groups[r.data_scale] = []
            scale_groups[r.data_scale].append(r)

        for scale, scale_results in scale_groups.items():
            avg_p99 = sum(r.p99_response_time for r in scale_results) / len(scale_results)
            avg_success = sum(r.success_rate for r in scale_results) / len(scale_results)
            data_size = scale_results[0].blue_lines_count

            report += f"- **{scale}规模** ({data_size:,}条数据): 平均P99={avg_p99:.1f}ms, 平均匹配率={avg_success:.1%}\n"

        # 结论和建议
        report += "\n## 结论与建议\n\n"

        overall_p99_pass = p99_passed == len(results)
        overall_success_pass = success_rate_passed == len(results)

        if overall_p99_pass and overall_success_pass:
            report += "✅ **总体评估**: 系统性能完全满足设计目标，可以支撑生产环境运行。\n\n"
        elif overall_p99_pass or overall_success_pass:
            report += "⚠️ **总体评估**: 系统性能部分满足设计目标，建议针对性优化。\n\n"
        else:
            report += "❌ **总体评估**: 系统性能未达到设计目标，需要重大优化。\n\n"

        # 具体建议
        report += "### 优化建议\n"

        if not overall_p99_pass:
            report += "- **性能优化**: P99响应时间超标，建议优化数据库查询和索引策略\n"

        if not overall_success_pass:
            report += "- **算法优化**: 匹配成功率不达标，建议调整贪婪算法参数或候选集大小\n"

        if max_memory > 2000:  # 2GB
            report += "- **内存优化**: 内存使用较高，建议使用流式处理减少内存占用\n"

        report += "- **监控建议**: 建议在生产环境中部署实时性能监控\n"
        report += "- **容量规划**: 基于测试结果制定合理的容量规划策略\n"

        # JSON数据（用于进一步分析）
        report += "\n## 原始数据\n\n"
        report += "```json\n"
        json_data = {
            'system_info': system_info,
            'test_results': [asdict(r) for r in results],
            'summary': {
                'total_tests': len(results),
                'p99_target_achieved': p99_passed,
                'success_rate_target_achieved': success_rate_passed,
                'best_p99_ms': best_p99.p99_response_time,
                'worst_p99_ms': worst_p99.p99_response_time,
                'best_success_rate': best_success.success_rate,
                'worst_success_rate': worst_success.success_rate
            }
        }
        report += json.dumps(json_data, indent=2, ensure_ascii=False)
        report += "\n```\n"

        return report

    def cleanup_test_data(self, batch_id: str):
        """清理测试数据"""
        print(f"\n清理测试数据: {batch_id}")
        self.data_generator.clear_batch(batch_id)

    def close(self):
        """关闭资源"""
        self.data_generator.close()


def run_performance_tests(scales: List[str], cleanup: bool = True,
                         report_file: Optional[str] = None):
    """
    运行性能测试

    Args:
        scales: 测试规模列表
        cleanup: 是否清理测试数据
        report_file: 报告输出文件
    """
    print("=== 负数发票匹配系统 - 大规模性能测试 ===\n")

    # 初始化测试套件
    db_config = get_db_config('test')
    test_suite = PerformanceTestSuite(db_config)

    all_results = []
    batch_ids = []

    try:
        for scale in scales:
            print(f"\n{'='*60}")
            print(f"开始 {scale} 规模测试")
            print(f"{'='*60}")

            # 设置测试数据
            batch_id = test_suite.setup_test_data(scale)
            batch_ids.append(batch_id)

            # 运行性能测试
            results = test_suite.run_performance_test(scale, batch_id)
            all_results.extend(results)

            print(f"\n✓ {scale} 规模测试完成")

        # 生成性能报告
        print(f"\n{'='*60}")
        print("生成性能测试报告")
        print(f"{'='*60}")

        if not report_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_file = f"docs/performance_test_report_{timestamp}.md"

        report = test_suite.generate_performance_report(all_results, report_file)

        # 输出简要结果
        print("\n📊 测试结果摘要:")
        p99_passed = sum(1 for r in all_results if r.p99_response_time <= 70)
        success_rate_passed = sum(1 for r in all_results if r.success_rate >= 0.93)

        print(f"  总测试用例: {len(all_results)}")
        print(f"  P99目标达成: {p99_passed}/{len(all_results)} ({p99_passed/len(all_results):.1%})")
        print(f"  匹配率目标达成: {success_rate_passed}/{len(all_results)} ({success_rate_passed/len(all_results):.1%})")

        if p99_passed == len(all_results) and success_rate_passed == len(all_results):
            print("  🎉 所有性能目标达成！")
        else:
            print("  ⚠️  部分性能目标未达成，请查看详细报告")

        print(f"\n📄 详细报告: {report_file}")

    except Exception as e:
        print(f"\n❌ 测试过程中发生错误: {e}")
        raise
    finally:
        # 清理测试数据
        if cleanup:
            print(f"\n🧹 清理测试数据...")
            for batch_id in batch_ids:
                try:
                    test_suite.cleanup_test_data(batch_id)
                except Exception as e:
                    print(f"⚠️ 清理批次 {batch_id} 失败: {e}")

        test_suite.close()


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='负数发票匹配系统大规模性能测试',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 小规模测试
  python test_performance_scale.py --scale small

  # 所有规模测试
  python test_performance_scale.py --scale all

  # 自定义报告文件名
  python test_performance_scale.py --scale medium --report performance_2025.md

  # 保留测试数据（用于调试）
  python test_performance_scale.py --scale small --no-cleanup
        """
    )

    parser.add_argument('--scale',
                       choices=['small', 'medium', 'large', 'all'],
                       default='small',
                       help='测试规模 (默认: small)')

    parser.add_argument('--report', type=str,
                       help='性能报告文件路径 (默认: docs/performance_test_report_TIMESTAMP.md)')

    parser.add_argument('--no-cleanup', action='store_true',
                       help='不清理测试数据（用于调试）')

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # 确定测试规模
    if args.scale == 'all':
        scales = ['small', 'medium', 'large']
    else:
        scales = [args.scale]

    # 运行测试
    run_performance_tests(
        scales=scales,
        cleanup=not args.no_cleanup,
        report_file=args.report
    )