"""
负数发票匹配系统监控模块

实现技术文档第8.1节定义的关键监控指标：
- 业务健康度指标
- 技术健康度指标
- 性能监控
"""

import time
import logging
from typing import Dict, List, Optional
from decimal import Decimal
from dataclasses import dataclass, field
from threading import Lock
from .matching_engine import MatchResult

logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetrics:
    """性能监控指标"""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_execution_time: float = 0.0
    min_execution_time: float = float('inf')
    max_execution_time: float = 0.0

    # 分组优化相关
    total_groups: int = 0
    queries_saved: int = 0


@dataclass
class BusinessMetrics:
    """业务健康度指标"""
    total_negative_invoices: int = 0
    successful_matches: int = 0
    failed_matches: int = 0
    total_requested_amount: Decimal = Decimal('0')
    total_matched_amount: Decimal = Decimal('0')
    fragments_created: int = 0

    # 碎片相关统计
    fragment_amounts: List[Decimal] = field(default_factory=list)
    large_fragment_count: int = 0  # > 100元的碎片

    # 响应时间相关
    response_times: List[float] = field(default_factory=list)


class SystemMonitor:
    """系统监控器"""

    def __init__(self):
        self.performance = PerformanceMetrics()
        self.business = BusinessMetrics()
        self.lock = Lock()
        self.start_time = time.time()

        # 健康度阈值（来自技术文档表格）
        self.thresholds = {
            'success_rate_healthy': 0.93,
            'success_rate_warning': 0.90,
            'fragment_rate_healthy': 0.15,
            'fragment_rate_warning': 0.25,
            'avg_match_time_healthy': 50,  # ms
            'avg_match_time_warning': 100,  # ms
            'fragment_lifetime_healthy': 7 * 24 * 3600,  # 7天（秒）
            'fragment_lifetime_warning': 30 * 24 * 3600,  # 30天（秒）
        }

    def record_batch_execution(self,
                             execution_time: float,
                             results: List[MatchResult],
                             negatives_count: int,
                             groups_count: int = 1):
        """记录批次执行结果"""
        with self.lock:
            # 更新性能指标
            self.performance.total_requests += 1
            self.performance.total_execution_time += execution_time
            self.performance.min_execution_time = min(
                self.performance.min_execution_time, execution_time
            )
            self.performance.max_execution_time = max(
                self.performance.max_execution_time, execution_time
            )
            self.performance.total_groups += groups_count

            # 计算查询节省数（假设无分组时每个负数发票1次查询）
            queries_without_grouping = negatives_count
            queries_with_grouping = groups_count
            self.performance.queries_saved += (queries_without_grouping - queries_with_grouping)

            # 更新业务指标
            self.business.total_negative_invoices += negatives_count
            self.business.response_times.append(execution_time * 1000)  # 转换为毫秒

            success_count = sum(1 for r in results if r.success)
            self.business.successful_matches += success_count
            self.business.failed_matches += (negatives_count - success_count)

            if results:
                # 统计金额
                matched_amount = sum(r.total_matched for r in results if r.success)
                self.business.total_matched_amount += matched_amount

                # 统计碎片
                total_fragments = sum(r.fragments_created for r in results if r.success)
                self.business.fragments_created += total_fragments

            # 更新请求成功失败统计
            if success_count == negatives_count:
                self.performance.successful_requests += 1
            else:
                self.performance.failed_requests += 1

            logger.debug(f"监控记录: {negatives_count}个负数发票, {success_count}个成功, "
                        f"耗时{execution_time:.3f}s, 分组数{groups_count}")


    def get_health_report(self) -> Dict:
        """获取健康度报告"""
        with self.lock:
            current_time = time.time()
            uptime = current_time - self.start_time

            # 计算业务健康度指标
            success_rate = (self.business.successful_matches /
                          max(self.business.total_negative_invoices, 1))

            # 碎片率（这里简化计算，实际需要从数据库获取剩余金额分布）
            fragment_rate = 0.0  # 需要从数据库计算

            # 平均匹配时间
            avg_match_time = (self.performance.total_execution_time * 1000 /
                            max(self.performance.total_requests, 1))

            # 查询优化效果
            query_optimization_rate = (self.performance.queries_saved /
                                     max(self.business.total_negative_invoices, 1))

            # 健康状态评估
            health_status = self._assess_health(success_rate, fragment_rate, avg_match_time)

            return {
                'timestamp': current_time,
                'uptime_seconds': uptime,
                'health_status': health_status,

                # 业务健康度指标
                'business_metrics': {
                    'success_rate': success_rate,
                    'fragment_rate': fragment_rate,
                    'avg_match_time_ms': avg_match_time,
                    'total_invoices': self.business.total_negative_invoices,
                    'successful_matches': self.business.successful_matches,
                    'failed_matches': self.business.failed_matches,
                    'fragments_created': self.business.fragments_created,
                    'total_matched_amount': float(self.business.total_matched_amount),
                },

                # 技术健康度指标
                'technical_metrics': {
                    'query_optimization_rate': query_optimization_rate,
                    'avg_groups_per_batch': (self.performance.total_groups /
                                           max(self.performance.total_requests, 1)),
                    'queries_saved_total': self.performance.queries_saved,
                },

                # 性能指标
                'performance_metrics': {
                    'total_requests': self.performance.total_requests,
                    'successful_requests': self.performance.successful_requests,
                    'failed_requests': self.performance.failed_requests,
                    'min_execution_time_ms': self.performance.min_execution_time * 1000,
                    'max_execution_time_ms': self.performance.max_execution_time * 1000,
                    'avg_execution_time_ms': avg_match_time,
                }
            }

    def _assess_health(self, success_rate: float, fragment_rate: float, avg_time_ms: float) -> str:
        """评估系统健康状态"""
        issues = []

        if success_rate < self.thresholds['success_rate_warning']:
            issues.append('low_success_rate')

        if fragment_rate > self.thresholds['fragment_rate_warning']:
            issues.append('high_fragment_rate')

        if avg_time_ms > self.thresholds['avg_match_time_warning']:
            issues.append('slow_response')

        if not issues:
            if (success_rate >= self.thresholds['success_rate_healthy'] and
                fragment_rate <= self.thresholds['fragment_rate_healthy'] and
                avg_time_ms <= self.thresholds['avg_match_time_healthy']):
                return 'healthy'
            else:
                return 'warning'
        else:
            return 'critical'

    def get_fragment_analysis(self, db_manager) -> Dict:
        """获取碎片分析（需要访问数据库）"""
        try:
            stats = db_manager.get_statistics()
            distribution = stats.get('distribution', {})

            total_lines = sum(dist.get('count', 0) for dist in distribution.values())
            total_amount = sum(dist.get('amount', 0) for dist in distribution.values())

            fragment_count = distribution.get('1_fragment', {}).get('count', 0)
            fragment_amount = distribution.get('1_fragment', {}).get('amount', 0)

            fragment_rate = fragment_count / max(total_lines, 1)
            fragment_amount_rate = fragment_amount / max(total_amount, 1)

            return {
                'fragment_rate': fragment_rate,
                'fragment_amount_rate': fragment_amount_rate,
                'fragment_count': fragment_count,
                'fragment_amount': fragment_amount,
                'total_lines': total_lines,
                'total_amount': total_amount,
                'distribution': distribution
            }
        except Exception as e:
            logger.error(f"获取碎片分析失败: {e}")
            return {}

    def reset_stats(self):
        """重置统计信息"""
        with self.lock:
            self.performance = PerformanceMetrics()
            self.business = BusinessMetrics()
            self.start_time = time.time()
            logger.info("监控统计已重置")


# 全局监控实例
system_monitor = SystemMonitor()


def get_monitor() -> SystemMonitor:
    """获取全局监控实例"""
    return system_monitor