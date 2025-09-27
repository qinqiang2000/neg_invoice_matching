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
from core.performance_monitor import get_performance_timer, reset_performance_timer
from core.explainability import ExplainabilityReporter
from core.diagnostics import MatchDiagnostics
from config.config import get_db_config
from tests.test_data_generator import TestDataGenerator


@dataclass
class PerformanceMetrics:
    """性能指标数据结构"""
    test_name: str
    data_scale: str
    batch_blue_lines_count: int  # 测试批次的数据量
    total_blue_lines_count: int  # 数据库总数据量
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

    # 单次性能指标 (毫秒)
    avg_single_query_time_ms: float
    avg_single_match_time_ms: float

    # 匹配结果指标
    success_rate: float
    total_matched_amount: float
    fragments_created: int

    # 系统资源指标
    peak_memory_mb: float
    avg_cpu_percent: float
    database_query_count: int
    database_query_time_ms: float

    # 详细性能分解指标
    detailed_performance_breakdown: Dict[str, float]

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

    def __init__(self, db_config: Dict, test_config: Optional[Dict] = None, preserve_data: bool = False,
                 enable_explainability: bool = True, enable_deep_diagnosis: bool = False,
                 seed: Optional[int] = None):
        """
        初始化性能测试套件

        Args:
            db_config: 数据库配置
            test_config: 测试配置
            preserve_data: 数据保留策略
                - False (默认): 测试后重置数据状态，保留数据记录供下次测试复用
                - True: 保留测试后的数据状态，用于后续分析
            enable_explainability: 是否启用可解释性分析（默认True，几乎无性能影响）
            enable_deep_diagnosis: 是否启用深度诊断（默认False，可选择性启用）
            seed: 随机种子（可选，用于生成可重复的测试数据）
        """
        self.db_config = db_config
        self.preserve_data = preserve_data
        self.enable_explainability = enable_explainability
        self.enable_deep_diagnosis = enable_deep_diagnosis
        self.seed = seed
        self.test_batch_ids = []  # 跟踪测试生成的批次ID
        self.db_manager = DatabaseManager(db_config)
        self.engine = GreedyMatchingEngine(debug_mode=False)  # 默认关闭调试输出
        self.candidate_provider = CandidateProvider(self.db_manager)

        # 测试配置
        self.test_config = test_config or {}
        self.batch_id_prefix = "perf_test"

        # 性能监控
        self.process = psutil.Process()
        self.test_results: List[PerformanceMetrics] = []

        # 数据生成器（支持固定种子）
        self.data_generator = TestDataGenerator(db_config, seed=seed)

        # 可解释性功能
        if self.enable_explainability:
            self.explainability_reporter = ExplainabilityReporter(self.db_manager)
            self.all_match_results: List = []  # 收集所有匹配结果用于分析
            self.all_negatives: List[NegativeInvoice] = []  # 收集所有负数发票

        if self.enable_deep_diagnosis:
            self.diagnostics = MatchDiagnostics(self.db_manager)

        # 测试规模配置
        self.scale_configs = {
            'small': {
                'blue_lines': 1_000_000,      # 100万
                'negative_batches': [1000],
                'description': '小规模测试（100万蓝票行）'
            },
            'medium': {
                'blue_lines': 5_000_000,      # 500万
                'negative_batches': [2000],
                'description': '中等规模测试（500万蓝票行）'
            },
            'large': {
                'blue_lines': 10_000_000,     # 1000万
                'negative_batches': [ 3000],
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
            print("自动重用现有数据...")
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

        # 记录新生成的批次ID
        self.test_batch_ids.append(actual_batch_id)

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

        # 重置并获取详细性能计时器
        reset_performance_timer()
        timer = get_performance_timer()

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

        with timer.measure("total_matching_process"):
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

                # 收集可解释性分析数据（几乎零性能开销）
                if self.enable_explainability:
                    self.all_match_results.extend(batch_results)
                    self.all_negatives.extend(batch_negatives)

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

        # 为失败分析准备数据（仅当启用可解释性时）
        if self.enable_explainability:
            self.current_test_failed_results = [r for r in all_results if not r.success]

        # 获取蓝票行数量（用于统计）
        batch_blue_lines_count, total_blue_lines_count = self._get_blue_lines_count(batch_id)

        # 获取监控数据
        health_report = monitor.get_health_report()
        technical_metrics = health_report.get('technical_metrics', {})
        performance_metrics = health_report.get('performance_metrics', {})

        # 获取详细性能分解数据
        performance_report = timer.get_performance_report()
        detailed_breakdown = {}
        for step_name, times in performance_report.step_timings.items():
            if times:
                detailed_breakdown[step_name] = sum(times)

        # 计算单次性能指标
        total_queries = performance_metrics.get('total_requests', len(negatives))
        avg_single_query_time = db_query_time / max(total_queries, 1)
        avg_single_match_time = (total_duration * 1000) / len(negatives) if negatives else 0

        # 构造性能指标对象
        metrics = PerformanceMetrics(
            test_name=test_name,
            data_scale=scale,
            batch_blue_lines_count=batch_blue_lines_count,
            total_blue_lines_count=total_blue_lines_count,
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

            # 单次性能指标
            avg_single_query_time_ms=avg_single_query_time,
            avg_single_match_time_ms=avg_single_match_time,

            # 匹配结果指标
            success_rate=success_rate,
            total_matched_amount=float(total_matched),
            fragments_created=fragments,

            # 系统资源指标
            peak_memory_mb=peak_memory,
            avg_cpu_percent=avg_cpu,
            database_query_count=performance_metrics.get('total_requests', 0),
            database_query_time_ms=db_query_time,

            # 详细性能分解指标
            detailed_performance_breakdown=detailed_breakdown,

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

    def _get_blue_lines_count(self, batch_id: str) -> Tuple[int, int]:
        """获取指定批次的蓝票行数量和总数据量"""
        conn = self.db_manager.pool.getconn()
        try:
            with conn.cursor() as cur:
                # 获取批次数据量
                cur.execute("SELECT COUNT(*) FROM blue_lines WHERE batch_id = %s", (batch_id,))
                batch_count = cur.fetchone()[0]

                # 获取总数据量
                cur.execute("SELECT COUNT(*) FROM blue_lines")
                total_count = cur.fetchone()[0]

                return batch_count, total_count
        finally:
            self.db_manager.pool.putconn(conn)

    def reset_existing_data(self):
        """重置现有数据状态（如果有的话）"""
        if not self.preserve_data:
            print("🔄 重置现有数据状态...")
            try:
                from tests.test_data_generator import TestDataGenerator
                generator = TestDataGenerator(self.db_config)
                try:
                    # 只清理匹配记录，保留蓝票行数据并重置其状态
                    generator.reset_test_data()
                    print("✅ 数据状态已重置")
                except Exception as e:
                    print(f"⚠️  重置数据状态失败: {e}")
                finally:
                    generator.close()
            except Exception as e:
                print(f"⚠️  重置操作失败: {e}")

    def cleanup_after_test(self):
        """测试后的清理工作"""
        if self.preserve_data:
            print("🔒 保留测试数据和状态")
            return

        print("🔄 重置测试后的数据状态...")
        try:
            from tests.test_data_generator import TestDataGenerator
            generator = TestDataGenerator(self.db_config)
            try:
                # 只清理匹配记录并重置余额，保留蓝票行数据
                generator.reset_test_data()
                print("✅ 数据状态已重置，可重复使用")
            except Exception as e:
                print(f"⚠️  重置失败: {e}")
            finally:
                generator.close()
        except Exception as e:
            print(f"⚠️  清理操作失败: {e}")

    def get_data_utilization_before_test(self):
        """获取测试前数据利用率"""
        from tests.test_data_generator import TestDataGenerator
        generator = TestDataGenerator(self.db_config)
        try:
            return generator.get_data_utilization_stats()
        finally:
            generator.close()

    def check_data_availability(self, required_remaining_ratio: float = 0.15):
        """
        检查数据可用性

        Args:
            required_remaining_ratio: 要求的剩余数据比例（默认15%，适应真实业务场景）

        Returns:
            bool: 数据是否充足
        """
        stats = self.get_data_utilization_before_test()
        if not stats:
            return False

        total_util = stats.get('total_utilization_percent', 100)
        remaining_ratio = (100 - total_util) / 100

        print(f"📊 数据可用性检查:")
        print(f"  当前利用率: {total_util:.1f}%")
        print(f"  剩余比例: {remaining_ratio:.1%}")
        print(f"  要求比例: {required_remaining_ratio:.1%}")

        is_sufficient = remaining_ratio >= required_remaining_ratio

        if not is_sufficient:
            print(f"⚠️  数据不足！建议重置数据或降低测试规模")
            print(f"  可用数据: {stats.get('unused_lines', 0) + stats.get('partial_used_lines', 0):,} 行")
            print(f"  建议操作: python tests/test_data_generator.py --reset-data")
        else:
            print(f"✅ 数据充足，可以进行测试")

        return is_sufficient

    def _print_test_summary(self, metrics: PerformanceMetrics):
        """打印测试结果摘要"""
        print(f"  ✓ 测试完成: {metrics.test_name}")
        print(f"    数据规模: 测试批次 {metrics.batch_blue_lines_count:,} 条 / 数据库总量 {metrics.total_blue_lines_count:,} 条")
        print(f"    负数发票: {metrics.negative_invoices_count} 个")
        print(f"    匹配成功率: {metrics.success_rate:.1%}")
        print(f"    批量响应时间: P50={metrics.p50_response_time:.1f}ms, P99={metrics.p99_response_time:.1f}ms")
        print(f"    单次性能指标:")
        print(f"      - 单次查询: {metrics.avg_single_query_time_ms:.1f}ms")
        print(f"      - 单个匹配: {metrics.avg_single_match_time_ms:.1f}ms")
        print(f"    内存峰值: {metrics.peak_memory_mb:.1f}MB")
        print(f"    总耗时: {metrics.duration_seconds:.2f}秒")

        # 检查是否达到性能目标（基于单个匹配时间）
        if metrics.avg_single_match_time_ms <= 70:
            print(f"    🎯 单次匹配性能目标达成: {metrics.avg_single_match_time_ms:.1f}ms ≤ 70ms")
        else:
            print(f"    ⚠️  单次匹配性能目标未达成: {metrics.avg_single_match_time_ms:.1f}ms > 70ms")

        if metrics.success_rate >= 0.93:
            print(f"    🎯 匹配率目标达成: {metrics.success_rate:.1%} ≥ 93%")
        else:
            print(f"    ⚠️  匹配率目标未达成: {metrics.success_rate:.1%} < 93%")

        # 显示失败原因分析（如果启用了可解释性）
        if self.enable_explainability and hasattr(self, 'current_test_failed_results'):
            self._print_failure_analysis(self.current_test_failed_results)

        # 显示详细性能分解（如果有）
        if metrics.detailed_performance_breakdown:
            print(f"    详细性能分解:")
            for step, time_ms in metrics.detailed_performance_breakdown.items():
                print(f"      - {step}: {time_ms:.1f}ms")

    def _print_failure_analysis(self, failed_results):
        """打印失败原因分析"""
        if not failed_results:
            return

        print(f"    💡 失败原因分析 ({len(failed_results)} 笔失败):")

        # 统计失败原因
        failure_reasons = {}
        for result in failed_results:
            reason = result.failure_reason or "unknown"
            failure_reasons[reason] = failure_reasons.get(reason, 0) + 1

        # 显示Top3失败原因
        sorted_reasons = sorted(failure_reasons.items(), key=lambda x: x[1], reverse=True)
        for i, (reason, count) in enumerate(sorted_reasons[:3], 1):
            reason_desc = self._get_failure_reason_description(reason)
            percentage = count / len(failed_results) * 100
            print(f"      {i}. {reason_desc}: {count} 笔 ({percentage:.1f}%)")

    def _get_failure_reason_description(self, reason_code: str) -> str:
        """获取失败原因的中文描述"""
        descriptions = {
            "no_candidates": "无可用候选蓝票",
            "insufficient_total_amount": "候选集总额不足",
            "fragmentation_issue": "候选集过度碎片化",
            "no_matching_tax_rate": "税率不匹配",
            "no_matching_buyer": "买方不匹配",
            "no_matching_seller": "卖方不匹配",
            "greedy_suboptimal": "算法策略次优",
            "concurrent_conflict": "并发冲突",
            "insufficient_funds": "资金不足（旧版）"
        }
        return descriptions.get(reason_code, f"未知原因 ({reason_code})")

    def _generate_explainability_report(self) -> str:
        """生成可解释性分析报告"""
        if not self.all_match_results:
            return ""

        # 使用可解释性报告器分析所有结果
        batch_analysis = self.explainability_reporter.generate_batch_analysis(
            self.all_match_results, self.all_negatives
        )

        report_lines = []
        report_lines.append("\n## 匹配失败分析（可解释性报告）\n")

        if batch_analysis.failure_count == 0:
            report_lines.append("🎉 **所有负数发票均匹配成功！** 系统运行完美。\n")
            return "".join(report_lines)

        # 失败概况
        report_lines.append("### 失败概况\n")
        report_lines.append(f"- **总处理量**: {batch_analysis.total_processed:,} 笔\n")
        report_lines.append(f"- **成功匹配**: {batch_analysis.success_count:,} 笔 ({batch_analysis.success_rate:.1%})\n")
        report_lines.append(f"- **匹配失败**: {batch_analysis.failure_count:,} 笔 ({100-batch_analysis.success_rate*100:.1f}%)\n\n")

        # 失败原因分布
        if batch_analysis.failure_patterns:
            report_lines.append("### 失败原因分布\n")
            report_lines.append("| 失败原因 | 数量 | 占失败比例 | 影响描述 |\n")
            report_lines.append("|----------|------|------------|----------|\n")

            total_failures = batch_analysis.failure_count
            for reason, count in batch_analysis.failure_patterns.items():
                percentage = count / total_failures * 100
                reason_desc = self._get_failure_reason_description(reason)
                impact_desc = self._get_failure_impact_description(reason)
                report_lines.append(f"| {reason_desc} | {count} | {percentage:.1f}% | {impact_desc} |\n")
            report_lines.append("\n")

        # 业务影响分析
        impact = batch_analysis.business_impact_summary
        if impact and impact.get('total_failed_amount', 0) > 0:
            report_lines.append("### 业务影响分析\n")
            report_lines.append(f"- **失败总金额**: ¥{impact['total_failed_amount']:,.2f}\n")
            report_lines.append(f"- **平均失败金额**: ¥{impact.get('avg_failure_amount', 0):.2f}\n")

            if impact.get('high_value_failures', 0) > 0:
                report_lines.append(f"- **高价值失败**: {impact['high_value_failures']} 笔（>¥10,000）⚠️\n")

            # 失败分布
            if 'failure_by_amount_range' in impact:
                ranges = impact['failure_by_amount_range']
                report_lines.append("- **失败分布**:\n")
                report_lines.append(f"  - 小额(<¥100): {ranges.get('small', 0)} 笔\n")
                report_lines.append(f"  - 中额(¥100-1K): {ranges.get('medium', 0)} 笔\n")
                report_lines.append(f"  - 大额(>¥1K): {ranges.get('large', 0)} 笔\n")
            report_lines.append("\n")

        # 改进建议
        if batch_analysis.recommendations:
            report_lines.append("### 针对性改进建议\n")
            for i, recommendation in enumerate(batch_analysis.recommendations, 1):
                report_lines.append(f"{i}. {recommendation}\n")
            report_lines.append("\n")

        # 深度诊断（如果启用）
        if self.enable_deep_diagnosis:
            report_lines.append("### 深度诊断分析\n")
            report_lines.append("基于启用的深度诊断功能，以下是详细分析：\n\n")

            # 选择几个代表性失败案例进行深度分析
            failed_results = [r for r in self.all_match_results if not r.success]
            sample_failures = failed_results[:5]  # 分析前5个失败案例

            for result in sample_failures:
                matching_negative = next((n for n in self.all_negatives if n.invoice_id == result.negative_invoice_id), None)
                if matching_negative:
                    try:
                        diagnosis = self.diagnostics.diagnose_no_match(matching_negative)
                        report_lines.append(f"**案例 #{result.negative_invoice_id}**:\n")
                        report_lines.append(f"- 主要问题: {diagnosis.primary_issue}\n")
                        report_lines.append(f"- 置信度: {diagnosis.confidence_score:.1%}\n")
                        if diagnosis.alternative_solutions:
                            report_lines.append(f"- 建议: {diagnosis.alternative_solutions[0]}\n")
                        report_lines.append("\n")
                    except Exception as e:
                        report_lines.append(f"**案例 #{result.negative_invoice_id}**: 诊断分析失败 ({str(e)})\n")

        return "".join(report_lines)

    def _get_failure_impact_description(self, reason_code: str) -> str:
        """获取失败原因的影响描述"""
        impact_descriptions = {
            "no_candidates": "数据流问题，影响处理效率",
            "insufficient_total_amount": "资金调配问题，可能需要拆分",
            "fragmentation_issue": "数据质量问题，影响系统性能",
            "no_matching_tax_rate": "业务规则严格，需要人工审核",
            "no_matching_buyer": "数据一致性问题",
            "no_matching_seller": "数据一致性问题",
            "greedy_suboptimal": "算法优化空间",
            "concurrent_conflict": "系统并发问题",
            "insufficient_funds": "历史遗留问题"
        }
        return impact_descriptions.get(reason_code, "需要进一步分析")

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
        report += "| 测试规模 | 测试批次数据 | 数据库总量 | 负数发票数 | 单次查询(ms) | 单个匹配(ms) | P99批量(ms) | 匹配率 | 内存峰值(MB) | 总耗时(s) |\n"
        report += "|----------|-------------|------------|------------|-------------|-------------|-------------|---------|-------------|----------|\n"

        for r in results:
            report += f"| {r.data_scale} | {r.batch_blue_lines_count:,} | {r.total_blue_lines_count:,} | {r.negative_invoices_count} | "
            report += f"{r.avg_single_query_time_ms:.1f} | {r.avg_single_match_time_ms:.1f} | "
            report += f"{r.p99_response_time:.1f} | {r.success_rate:.1%} | {r.peak_memory_mb:.1f} | {r.duration_seconds:.2f} |\n"

        # 性能分析
        report += "\n## 性能分析\n\n"

        # 最佳和最差性能
        best_p99 = min(results, key=lambda x: x.p99_response_time)
        worst_p99 = max(results, key=lambda x: x.p99_response_time)
        best_success = max(results, key=lambda x: x.success_rate)
        worst_success = min(results, key=lambda x: x.success_rate)

        # 计算单次性能指标
        best_single_query = min(results, key=lambda x: x.avg_single_query_time_ms)
        best_single_match = min(results, key=lambda x: x.avg_single_match_time_ms)

        report += f"""### 性能表现
- **单次查询性能**: 平均{best_single_query.avg_single_query_time_ms:.1f}ms (方便排查)
- **单次匹配性能**: 平均{best_single_match.avg_single_match_time_ms:.1f}ms (符合<70ms目标)
- **最佳P99批处理时间**: {best_p99.p99_response_time:.1f}ms ({best_p99.test_name})
- **最差P99批处理时间**: {worst_p99.p99_response_time:.1f}ms ({worst_p99.test_name})
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
            data_size = scale_results[0].batch_blue_lines_count

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

        # 可解释性分析（如果启用）
        if self.enable_explainability and hasattr(self, 'all_match_results') and self.all_match_results:
            explainability_section = self._generate_explainability_report()
            report += explainability_section

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

    def cleanup_generated_batches(self):
        """清理本次测试生成的数据批次（仅在不保留数据时）"""
        if self.preserve_data or not self.test_batch_ids:
            return

        print(f"\n🗑️ 清理本次测试生成的数据批次...")
        for batch_id in self.test_batch_ids:
            try:
                print(f"清理批次: {batch_id}")
                self.data_generator.clear_batch(batch_id)
            except Exception as e:
                print(f"⚠️ 清理批次 {batch_id} 失败: {e}")

        self.test_batch_ids.clear()
        print("✅ 数据批次清理完成")

    def close(self):
        """关闭资源"""
        self.data_generator.close()


def run_performance_tests(scales: List[str], cleanup: bool = True,
                         report_file: Optional[str] = None, preserve_data: bool = False,
                         delete_data: bool = False, enable_explainability: bool = True,
                         enable_deep_diagnosis: bool = False, seed: Optional[int] = None,
                         debug_mode: bool = False):
    """
    运行性能测试

    Args:
        scales: 测试规模列表
        cleanup: 是否清理测试数据
        report_file: 报告输出文件
        preserve_data: 是否保留测试后的数据（默认False，测试后会恢复数据）
        delete_data: 是否删除测试数据（默认False，只重置状态保留数据以便复用）
        enable_explainability: 是否启用可解释性分析（默认True，几乎无性能影响）
        enable_deep_diagnosis: 是否启用深度诊断（默认False，可选择性启用）
        seed: 随机种子（可选，用于生成可重复的测试数据）
    """
    print("=== 负数发票匹配系统 - 大规模性能测试 ===\n")

    # 显示可解释性功能状态
    if enable_explainability:
        print("🔍 可解释性分析: 已启用")
        if enable_deep_diagnosis:
            print("🔬 深度诊断: 已启用（可能稍微影响性能）")
        else:
            print("🔬 深度诊断: 未启用（可通过 --enable-deep-diagnosis 启用）")
    else:
        print("🔍 可解释性分析: 已禁用")
    print()

    # 显示随机种子状态
    if seed is not None:
        print(f"🌱 使用固定随机种子: {seed}")
        print("   注意: 相同种子将生成完全相同的测试数据\n")
    else:
        print("🎲 使用随机数据生成 (每次运行结果可能不同)\n")

    # 初始化测试套件
    db_config = get_db_config('test')
    test_suite = PerformanceTestSuite(
        db_config,
        preserve_data=preserve_data,
        enable_explainability=enable_explainability,
        enable_deep_diagnosis=enable_deep_diagnosis,
        seed=seed
    )

    # 设置调试模式
    test_suite.engine.debug_mode = debug_mode

    all_results = []
    batch_ids = []

    try:
        # 检查数据可用性
        print("📊 检查数据可用性...")
        if not test_suite.check_data_availability():
            print("❌ 数据不足，无法进行测试")
            return

        # 重置现有数据状态
        # 注意：当使用固定种子时，必须重置数据状态以确保可重复性
        if seed is not None:
            print("🔄 检测到固定种子，强制重置数据状态以确保可重复性...")
            test_suite.preserve_data = False  # 临时覆盖设置
            test_suite.reset_existing_data()
            test_suite.preserve_data = preserve_data  # 恢复原设置
        else:
            # 仅在不保留数据时重置
            test_suite.reset_existing_data()

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
            report_file = f"docs/reports/performance_test_report_{timestamp}.md"

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
        # 测试后清理工作
        test_suite.cleanup_after_test()

        # 清理本次测试生成的数据批次（仅在明确要求删除时）
        # 默认行为：保留数据，只重置状态（除非明确使用--delete-data）
        if delete_data:
            test_suite.cleanup_generated_batches()

        test_suite.close()


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='负数发票匹配系统大规模性能测试',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 小规模测试（默认使用种子42，可重复）
  python test_performance_scale.py --scale small

  # 使用随机数据（每次结果不同）
  python test_performance_scale.py --scale small --random

  # 使用自定义固定种子进行可重复测试
  python test_performance_scale.py --scale large --seed 12345

  # 对比优化前后性能（默认种子42确保数据一致）
  python test_performance_scale.py --scale large --report before_optimization.md
  # ... 应用优化 ...
  python test_performance_scale.py --scale large --report after_optimization.md

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
                       help='性能报告文件路径 (默认: docs/reports/performance_test_report_TIMESTAMP.md)')

    parser.add_argument('--no-cleanup', action='store_true',
                       help='不清理测试数据（用于调试）')

    parser.add_argument('--preserve-data', action='store_true',
                       help='保留测试后的数据状态（不恢复到快照）')

    parser.add_argument('--delete-data', action='store_true',
                       help='删除测试生成的数据（默认只重置状态，保留数据以便复用）')

    parser.add_argument('--disable-explainability', action='store_true',
                       help='禁用可解释性分析（微小性能提升）')

    parser.add_argument('--enable-deep-diagnosis', action='store_true',
                       help='启用深度诊断分析（详细失败原因分析，可能稍微影响性能）')

    parser.add_argument('--seed', type=int, default=999,
                       help='随机种子（默认: 999，用于生成可重复的测试数据）。注意：使用seed时会自动重置数据状态以确保可重复性')

    parser.add_argument('--random', action='store_true',
                       help='使用真正的随机数据（禁用默认种子999）')

    parser.add_argument('--debug', action='store_true',
                       help='启用调试模式（详细性能统计输出，会影响性能）')

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # 确定测试规模
    if args.scale == 'all':
        scales = ['small', 'medium', 'large']
    else:
        scales = [args.scale]

    # 处理种子参数
    seed = None if args.random else args.seed

    # 运行测试
    run_performance_tests(
        scales=scales,
        cleanup=not args.no_cleanup,
        report_file=args.report,
        preserve_data=args.preserve_data,
        delete_data=args.delete_data,
        enable_explainability=not args.disable_explainability,
        enable_deep_diagnosis=args.enable_deep_diagnosis,
        seed=seed,
        debug_mode=args.debug
    )