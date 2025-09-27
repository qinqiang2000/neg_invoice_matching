"""
详细性能监控器

提供精细化的性能监控和分析功能，支持：
1. 各个步骤的精确计时
2. 数据库查询性能分析
3. 资源使用监控
4. 性能报告生成
"""

import time
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from threading import Lock
from contextlib import contextmanager
import psutil
import json
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class TimingRecord:
    """单次计时记录"""
    name: str
    start_time: float
    end_time: float
    duration: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PerformanceReport:
    """性能报告"""
    total_duration: float
    step_timings: Dict[str, List[float]]
    step_statistics: Dict[str, Dict[str, float]]
    resource_usage: Dict[str, Any]
    database_statistics: Dict[str, Any]
    timestamp: str


class PerformanceTimer:
    """详细性能计时器"""

    def __init__(self):
        self.records: List[TimingRecord] = []
        self.lock = Lock()
        self.current_sessions = {}
        self.resource_snapshots = []

    def reset(self):
        """重置所有计时记录"""
        with self.lock:
            self.records.clear()
            self.current_sessions.clear()
            self.resource_snapshots.clear()

    @contextmanager
    def measure(self, name: str, metadata: Optional[Dict] = None):
        """
        上下文管理器，自动计时

        Args:
            name: 步骤名称
            metadata: 附加元数据

        Usage:
            with timer.measure("database_query", {"sql": "SELECT ..."}):
                # 执行代码
                pass
        """
        start_time = time.time()

        # 记录资源使用快照
        try:
            process = psutil.Process()
            resource_snapshot = {
                'timestamp': start_time,
                'memory_mb': process.memory_info().rss / 1024 / 1024,
                'cpu_percent': process.cpu_percent()
            }
            self.resource_snapshots.append(resource_snapshot)
        except:
            pass

        try:
            yield
        finally:
            end_time = time.time()
            duration = end_time - start_time

            record = TimingRecord(
                name=name,
                start_time=start_time,
                end_time=end_time,
                duration=duration,
                metadata=metadata or {}
            )

            with self.lock:
                self.records.append(record)

            logger.debug(f"⏱️  {name}: {duration*1000:.2f}ms")

    def start_timing(self, name: str, metadata: Optional[Dict] = None) -> str:
        """
        开始计时（手动方式）

        Returns:
            str: 会话ID
        """
        session_id = f"{name}_{time.time()}"
        self.current_sessions[session_id] = {
            'name': name,
            'start_time': time.time(),
            'metadata': metadata or {}
        }
        return session_id

    def end_timing(self, session_id: str):
        """结束计时"""
        if session_id not in self.current_sessions:
            logger.warning(f"计时会话不存在: {session_id}")
            return

        session = self.current_sessions.pop(session_id)
        end_time = time.time()
        duration = end_time - session['start_time']

        record = TimingRecord(
            name=session['name'],
            start_time=session['start_time'],
            end_time=end_time,
            duration=duration,
            metadata=session['metadata']
        )

        with self.lock:
            self.records.append(record)

    def get_step_statistics(self) -> Dict[str, Dict[str, float]]:
        """获取各步骤的统计信息"""
        step_data = {}

        for record in self.records:
            if record.name not in step_data:
                step_data[record.name] = []
            step_data[record.name].append(record.duration * 1000)  # 转换为毫秒

        statistics = {}
        for step_name, durations in step_data.items():
            if durations:
                statistics[step_name] = {
                    'count': len(durations),
                    'total_ms': sum(durations),
                    'avg_ms': sum(durations) / len(durations),
                    'min_ms': min(durations),
                    'max_ms': max(durations),
                    'median_ms': sorted(durations)[len(durations)//2],
                    'percentage': sum(durations) / sum([sum(d) for d in step_data.values()]) * 100
                }

        return statistics

    def get_performance_report(self, db_manager=None) -> PerformanceReport:
        """生成完整的性能报告"""
        if not self.records:
            return PerformanceReport(
                total_duration=0,
                step_timings={},
                step_statistics={},
                resource_usage={},
                database_statistics={},
                timestamp=datetime.now().isoformat()
            )

        total_duration = max(r.end_time for r in self.records) - min(r.start_time for r in self.records)

        # 按步骤分组时间
        step_timings = {}
        for record in self.records:
            if record.name not in step_timings:
                step_timings[record.name] = []
            step_timings[record.name].append(record.duration * 1000)

        # 计算统计信息
        step_statistics = self.get_step_statistics()

        # 资源使用统计
        resource_usage = {}
        if self.resource_snapshots:
            memory_values = [s['memory_mb'] for s in self.resource_snapshots]
            cpu_values = [s['cpu_percent'] for s in self.resource_snapshots if s['cpu_percent'] > 0]

            resource_usage = {
                'peak_memory_mb': max(memory_values) if memory_values else 0,
                'avg_memory_mb': sum(memory_values) / len(memory_values) if memory_values else 0,
                'avg_cpu_percent': sum(cpu_values) / len(cpu_values) if cpu_values else 0,
                'samples_count': len(self.resource_snapshots)
            }

        # 数据库统计信息
        database_statistics = {}
        if db_manager:
            try:
                database_statistics = self._get_database_statistics(db_manager)
            except Exception as e:
                logger.warning(f"获取数据库统计失败: {e}")

        return PerformanceReport(
            total_duration=total_duration,
            step_timings=step_timings,
            step_statistics=step_statistics,
            resource_usage=resource_usage,
            database_statistics=database_statistics,
            timestamp=datetime.now().isoformat()
        )

    def _get_database_statistics(self, db_manager) -> Dict[str, Any]:
        """获取数据库统计信息"""
        conn = db_manager.pool.getconn()
        try:
            with conn.cursor() as cur:
                # 查询统计信息
                cur.execute("""
                    SELECT
                        seq_scan,
                        seq_tup_read,
                        idx_scan,
                        idx_tup_fetch,
                        n_tup_ins,
                        n_tup_upd,
                        n_tup_del
                    FROM pg_stat_user_tables
                    WHERE relname = 'blue_lines'
                """)

                result = cur.fetchone()
                if result:
                    return {
                        'sequential_scans': result[0] or 0,
                        'seq_tuples_read': result[1] or 0,
                        'index_scans': result[2] or 0,
                        'index_tuples_fetched': result[3] or 0,
                        'tuples_inserted': result[4] or 0,
                        'tuples_updated': result[5] or 0,
                        'tuples_deleted': result[6] or 0
                    }
                return {}
        finally:
            db_manager.pool.putconn(conn)

    def print_summary(self):
        """打印性能摘要"""
        if not self.records:
            print("📊 无性能数据")
            return

        statistics = self.get_step_statistics()

        print("\n📊 性能监控摘要")
        print("=" * 60)

        # 按耗时排序
        sorted_steps = sorted(statistics.items(),
                            key=lambda x: x[1]['total_ms'],
                            reverse=True)

        print(f"{'步骤':<25} {'次数':<6} {'总耗时':<10} {'平均':<10} {'占比':<8}")
        print("-" * 60)

        for step_name, stats in sorted_steps:
            print(f"{step_name:<25} {stats['count']:<6} "
                  f"{stats['total_ms']:<10.1f} {stats['avg_ms']:<10.1f} "
                  f"{stats['percentage']:<8.1f}%")

        total_time = sum(stats['total_ms'] for stats in statistics.values())
        print("-" * 60)
        print(f"{'总计':<25} {'':<6} {total_time:<10.1f} {'':<10} {'100.0':<8}%")

        # 资源使用
        if self.resource_snapshots:
            memory_values = [s['memory_mb'] for s in self.resource_snapshots]
            print(f"\n💾 内存峰值: {max(memory_values):.1f}MB")

    def export_json(self, filepath: str):
        """导出详细数据为JSON"""
        export_data = {
            'records': [
                {
                    'name': r.name,
                    'duration_ms': r.duration * 1000,
                    'start_time': r.start_time,
                    'end_time': r.end_time,
                    'metadata': r.metadata
                }
                for r in self.records
            ],
            'statistics': self.get_step_statistics(),
            'resource_snapshots': self.resource_snapshots,
            'export_time': datetime.now().isoformat()
        }

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)

        print(f"📄 性能数据已导出: {filepath}")


# 全局性能计时器实例
_global_timer = PerformanceTimer()


def get_performance_timer() -> PerformanceTimer:
    """获取全局性能计时器"""
    return _global_timer


def reset_performance_timer():
    """重置全局性能计时器"""
    _global_timer.reset()


@contextmanager
def measure_performance(name: str, metadata: Optional[Dict] = None):
    """全局性能测量装饰器"""
    with _global_timer.measure(name, metadata):
        yield


# 便捷的装饰器
def performance_measure(name: str = None, metadata: Optional[Dict] = None):
    """性能测量装饰器"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            measure_name = name or f"{func.__module__}.{func.__name__}"
            with _global_timer.measure(measure_name, metadata):
                return func(*args, **kwargs)
        return wrapper
    return decorator