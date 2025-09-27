"""
性能基准测试模块
专注于测试系统在不同负载下的性能表现
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import uuid
import statistics
import concurrent.futures
from typing import List, Dict
import matplotlib.pyplot as plt
import pandas as pd
from decimal import Decimal

from core.matching_engine import GreedyMatchingEngine, NegativeInvoice
from core.db_manager import DatabaseManager, CandidateProvider
from test_data_generator import TestDataGenerator
from config.config import get_db_config

class PerformanceBenchmark:
    """
    性能基准测试类
    专注于：
    1. 不同数据规模的性能
    2. 并发性能
    3. 延迟分布
    4. 吞吐量测试
    """
    
    def __init__(self, db_config: Dict):
        self.db_manager = DatabaseManager(db_config)
        self.engine = GreedyMatchingEngine()
        self.results = []
        
    def benchmark_scalability(self):
        """
        测试不同数据规模的性能
        评估算法的可扩展性
        """
        print("=== 可扩展性基准测试 ===\n")
        
        # 不同规模的测试
        test_sizes = [10, 50, 100, 200, 500, 1000]
        results = []
        
        for size in test_sizes:
            print(f"测试规模: {size} 条负数发票")
            
            # 准备测试数据
            invoices = self._generate_test_invoices(size)
            candidate_provider = CandidateProvider(self.db_manager)
            
            # 预热
            self._warmup(invoices[:min(10, size)], candidate_provider)
            
            # 执行测试（多次取平均）
            latencies = []
            for i in range(5):  # 每个规模测试5次
                start_time = time.perf_counter()
                
                batch_results = self.engine.match_batch(
                    invoices,
                    candidate_provider,
                    sort_strategy="amount_desc"
                )
                
                elapsed = (time.perf_counter() - start_time) * 1000  # ms
                latencies.append(elapsed)
                
                # 清理缓存，确保每次测试独立
                candidate_provider.cache.clear()
            
            # 计算统计指标
            avg_latency = statistics.mean(latencies)
            p50_latency = statistics.median(latencies)
            p99_latency = sorted(latencies)[int(len(latencies) * 0.99)]
            throughput = size / (avg_latency / 1000)  # TPS
            
            results.append({
                'size': size,
                'avg_latency_ms': avg_latency,
                'p50_latency_ms': p50_latency,
                'p99_latency_ms': p99_latency,
                'throughput_tps': throughput,
                'latency_per_item_ms': avg_latency / size
            })
            
            print(f"  平均延迟: {avg_latency:.2f}ms")
            print(f"  P50延迟: {p50_latency:.2f}ms")
            print(f"  P99延迟: {p99_latency:.2f}ms")
            print(f"  吞吐量: {throughput:.1f} TPS")
            print(f"  单条延迟: {avg_latency/size:.2f}ms\n")
        
        # 保存结果
        self.results.append(('scalability', results))
        return results
    
    def benchmark_concurrency(self):
        """
        并发性能测试
        测试多线程同时匹配的性能
        """
        print("=== 并发性能基准测试 ===\n")
        
        concurrent_levels = [1, 2, 5, 10, 20]
        batch_size = 50  # 每个线程处理50条
        results = []
        
        for num_threads in concurrent_levels:
            print(f"并发数: {num_threads}")
            
            # 准备测试数据（每个线程独立的数据）
            all_invoices = [
                self._generate_test_invoices(batch_size) 
                for _ in range(num_threads)
            ]
            
            # 执行并发测试
            start_time = time.perf_counter()
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = []
                for invoices in all_invoices:
                    future = executor.submit(self._process_batch, invoices)
                    futures.append(future)
                
                # 等待所有任务完成
                concurrent.futures.wait(futures)
            
            total_elapsed = (time.perf_counter() - start_time) * 1000
            total_processed = num_threads * batch_size
            
            # 计算指标
            throughput = total_processed / (total_elapsed / 1000)
            avg_latency = total_elapsed / num_threads
            
            results.append({
                'threads': num_threads,
                'total_items': total_processed,
                'total_time_ms': total_elapsed,
                'throughput_tps': throughput,
                'avg_latency_ms': avg_latency
            })
            
            print(f"  总处理数: {total_processed}")
            print(f"  总耗时: {total_elapsed:.2f}ms")
            print(f"  吞吐量: {throughput:.1f} TPS")
            print(f"  平均延迟: {avg_latency:.2f}ms\n")
        
        self.results.append(('concurrency', results))
        return results
    
    def benchmark_latency_distribution(self):
        """
        延迟分布测试
        分析不同场景下的延迟分布
        """
        print("=== 延迟分布基准测试 ===\n")
        
        # 测试不同金额范围的延迟
        scenarios = {
            'small': (10, 100, 100),      # 金额范围10-100，100条
            'medium': (100, 1000, 100),   # 金额范围100-1000，100条
            'large': (1000, 5000, 100),   # 金额范围1000-5000，100条
            'mixed': (10, 5000, 100)      # 混合范围，100条
        }
        
        results = {}
        
        for scenario_name, (min_amt, max_amt, count) in scenarios.items():
            print(f"场景: {scenario_name} (金额 {min_amt}-{max_amt})")
            
            # 生成测试数据
            invoices = self._generate_test_invoices_with_range(count, min_amt, max_amt)
            candidate_provider = CandidateProvider(self.db_manager)
            
            # 逐个测试，收集延迟数据
            latencies = []
            for invoice in invoices:
                start_time = time.perf_counter()
                
                result = self.engine.match_single(
                    invoice,
                    candidate_provider.get_candidates(
                        invoice.tax_rate,
                        invoice.buyer_id,
                        invoice.seller_id
                    )
                )
                
                elapsed = (time.perf_counter() - start_time) * 1000
                latencies.append(elapsed)
            
            # 计算百分位数
            latencies_sorted = sorted(latencies)
            percentiles = {
                'p50': latencies_sorted[int(len(latencies) * 0.50)],
                'p75': latencies_sorted[int(len(latencies) * 0.75)],
                'p90': latencies_sorted[int(len(latencies) * 0.90)],
                'p95': latencies_sorted[int(len(latencies) * 0.95)],
                'p99': latencies_sorted[int(len(latencies) * 0.99)],
                'max': max(latencies),
                'min': min(latencies),
                'avg': statistics.mean(latencies),
                'stdev': statistics.stdev(latencies)
            }
            
            results[scenario_name] = {
                'latencies': latencies,
                'percentiles': percentiles
            }
            
            print(f"  P50: {percentiles['p50']:.2f}ms")
            print(f"  P90: {percentiles['p90']:.2f}ms")
            print(f"  P99: {percentiles['p99']:.2f}ms")
            print(f"  Max: {percentiles['max']:.2f}ms")
            print(f"  Avg: {percentiles['avg']:.2f}ms ± {percentiles['stdev']:.2f}ms\n")
        
        self.results.append(('latency_distribution', results))
        return results
    
    def benchmark_fragment_impact(self):
        """
        测试碎片对性能的影响
        比较不同碎片率下的匹配性能
        """
        print("=== 碎片影响基准测试 ===\n")
        
        # 这需要特殊准备的数据集，模拟不同碎片率
        # 暂时简化实现
        pass
    
    def _process_batch(self, invoices: List[NegativeInvoice]):
        """处理一批发票（用于并发测试）"""
        candidate_provider = CandidateProvider(self.db_manager)
        return self.engine.match_batch(
            invoices,
            candidate_provider,
            sort_strategy="amount_desc"
        )
    
    def _warmup(self, invoices: List[NegativeInvoice], candidate_provider):
        """预热，避免冷启动影响"""
        self.engine.match_batch(invoices, candidate_provider, "amount_desc")
    
    def _generate_test_invoices(self, count: int) -> List[NegativeInvoice]:
        """生成测试发票"""
        invoices = []
        for i in range(count):
            invoices.append(NegativeInvoice(
                invoice_id=i + 1,
                amount=Decimal(str(random.uniform(10, 5000))),
                tax_rate=random.choice([13, 6, 3, 0]),
                buyer_id=random.randint(1, 100),
                seller_id=random.randint(1, 100)
            ))
        return invoices
    
    def _generate_test_invoices_with_range(self, count: int, min_amt: float, max_amt: float):
        """生成指定金额范围的测试发票"""
        invoices = []
        for i in range(count):
            invoices.append(NegativeInvoice(
                invoice_id=i + 1,
                amount=Decimal(str(random.uniform(min_amt, max_amt))),
                tax_rate=random.choice([13, 6, 3, 0]),
                buyer_id=random.randint(1, 100),
                seller_id=random.randint(1, 100)
            ))
        return invoices
    
    def generate_report(self):
        """
        生成性能测试报告
        包括图表和分析
        """
        print("\n=== 生成性能报告 ===\n")
        
        # 创建报告目录
        os.makedirs('benchmark_reports', exist_ok=True)
        
        # 生成图表
        for test_name, data in self.results:
            if test_name == 'scalability':
                self._plot_scalability(data)
            elif test_name == 'concurrency':
                self._plot_concurrency(data)
            elif test_name == 'latency_distribution':
                self._plot_latency_distribution(data)
        
        # 生成文本报告
        with open('benchmark_reports/report.txt', 'w') as f:
            f.write("负数发票匹配系统性能基准测试报告\n")
            f.write("=" * 50 + "\n\n")
            
            for test_name, data in self.results:
                f.write(f"{test_name.upper()} 测试结果\n")
                f.write("-" * 30 + "\n")
                f.write(str(data) + "\n\n")
        
        print("报告已生成到 benchmark_reports/ 目录")
    
    def _plot_scalability(self, data):
        """绘制可扩展性图表"""
        df = pd.DataFrame(data)
        
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        
        # 延迟 vs 规模
        axes[0, 0].plot(df['size'], df['avg_latency_ms'], 'b-o')
        axes[0, 0].set_xlabel('负数发票数量')
        axes[0, 0].set_ylabel('平均延迟 (ms)')
        axes[0, 0].set_title('延迟 vs 规模')
        axes[0, 0].grid(True)
        
        # 吞吐量 vs 规模
        axes[0, 1].plot(df['size'], df['throughput_tps'], 'g-o')
        axes[0, 1].set_xlabel('负数发票数量')
        axes[0, 1].set_ylabel('吞吐量 (TPS)')
        axes[0, 1].set_title('吞吐量 vs 规模')
        axes[0, 1].grid(True)
        
        # 单条延迟 vs 规模
        axes[1, 0].plot(df['size'], df['latency_per_item_ms'], 'r-o')
        axes[1, 0].set_xlabel('负数发票数量')
        axes[1, 0].set_ylabel('单条延迟 (ms)')
        axes[1, 0].set_title('单条处理延迟 vs 规模')
        axes[1, 0].grid(True)
        
        # P50 vs P99
        axes[1, 1].plot(df['size'], df['p50_latency_ms'], 'b-o', label='P50')
        axes[1, 1].plot(df['size'], df['p99_latency_ms'], 'r-o', label='P99')
        axes[1, 1].set_xlabel('负数发票数量')
        axes[1, 1].set_ylabel('延迟 (ms)')
        axes[1, 1].set_title('P50 vs P99 延迟')
        axes[1, 1].legend()
        axes[1, 1].grid(True)
        
        plt.tight_layout()
        plt.savefig('benchmark_reports/scalability.png')
        plt.close()


import random

# 运行基准测试
if __name__ == "__main__":
    # 配置
    db_config = get_db_config('test')
    
    # 初始化基准测试
    benchmark = PerformanceBenchmark(db_config)
    
    # 运行各项测试
    print("开始性能基准测试...\n")
    
    # 1. 可扩展性测试
    benchmark.benchmark_scalability()
    
    # 2. 并发测试
    benchmark.benchmark_concurrency()
    
    # 3. 延迟分布测试
    benchmark.benchmark_latency_distribution()
    
    # 4. 生成报告
    benchmark.generate_report()
    
    print("\n性能基准测试完成！")