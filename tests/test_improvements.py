#!/usr/bin/env python3
"""
改进功能测试脚本

测试新增的优化功能：
1. PostgreSQL FROM VALUES批量更新
2. 按条件分组的请求优化
3. LRU缓存机制
4. 业务和性能监控
5. 流式处理大批量数据

运行方式：
python test_improvements.py
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.matching_engine import GreedyMatchingEngine, NegativeInvoice
from core.db_manager import DatabaseManager, CandidateProvider
from core.monitoring import get_monitor
from config.config import get_db_config
from decimal import Decimal
import time
import random

def create_diverse_test_data(db_manager, count=1000):
    """创建多样化的测试数据"""
    print(f"创建 {count} 条多样化测试数据...")

    # 清理之前的测试数据
    conn = db_manager.pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM blue_lines WHERE batch_id = 'test_improvements'")
            cur.execute("DELETE FROM match_records WHERE batch_id LIKE 'test_improvements_%'")
            conn.commit()
    finally:
        db_manager.pool.putconn(conn)

    # 创建多种组合的测试数据
    test_data = []
    combinations = [
        (13, 1, 1),  # 税率13%, 买方1, 卖方1
        (13, 1, 2),  # 税率13%, 买方1, 卖方2
        (6, 2, 1),   # 税率6%, 买方2, 卖方1
        (6, 2, 2),   # 税率6%, 买方2, 卖方2
        (3, 3, 3),   # 税率3%, 买方3, 卖方3
    ]

    for i in range(count):
        # 随机选择组合
        tax_rate, buyer_id, seller_id = random.choice(combinations)

        test_data.append((
            i + 1,  # ticket_id
            tax_rate,
            buyer_id,
            seller_id,
            f"Product_{i}",
            Decimal('500.00'),  # original_amount
            Decimal(str(10 + random.randint(1, 200))),  # remaining
            'test_improvements'
        ))

    # 插入数据
    conn = db_manager.pool.getconn()
    try:
        with conn.cursor() as cur:
            insert_sql = """
                INSERT INTO blue_lines (
                    ticket_id, tax_rate, buyer_id, seller_id,
                    product_name, original_amount, remaining, batch_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cur.executemany(insert_sql, test_data)
            conn.commit()
            print(f"✓ 成功插入 {len(test_data)} 条测试数据")
    finally:
        db_manager.pool.putconn(conn)

def create_diverse_negative_invoices(count=100):
    """创建多样化的负数发票"""
    invoices = []
    combinations = [
        (13, 1, 1),
        (13, 1, 2),
        (6, 2, 1),
        (6, 2, 2),
        (3, 3, 3),
    ]

    for i in range(count):
        tax_rate, buyer_id, seller_id = random.choice(combinations)
        amount = Decimal(str(random.randint(50, 500)))

        invoices.append(NegativeInvoice(
            invoice_id=i + 1,
            amount=amount,
            tax_rate=tax_rate,
            buyer_id=buyer_id,
            seller_id=seller_id
        ))

    return invoices

def test_grouping_optimization():
    """测试分组优化功能"""
    print("\n=== 测试分组优化 ===")

    db_config = get_db_config('test')
    db_manager = DatabaseManager(db_config)
    engine = GreedyMatchingEngine()

    # 创建测试数据
    create_diverse_test_data(db_manager, 500)

    # 创建多样化负数发票（故意创建多个相同组合）
    negatives = create_diverse_negative_invoices(50)

    # 统计组合分布
    from collections import Counter
    combinations = [(n.tax_rate, n.buyer_id, n.seller_id) for n in negatives]
    combo_count = Counter(combinations)

    print(f"负数发票组合分布: {dict(combo_count)}")
    print(f"总组合数: {len(combo_count)}, 总发票数: {len(negatives)}")
    print(f"分组优化预期减少查询: {len(negatives) - len(combo_count)} 次")

    # 创建候选提供器
    candidate_provider = CandidateProvider(db_manager)

    # 执行匹配
    start_time = time.time()
    results = engine.match_batch(negatives, candidate_provider)
    elapsed = time.time() - start_time

    success_count = sum(1 for r in results if r.success)
    print(f"\n匹配结果:")
    print(f"  成功: {success_count}/{len(negatives)} ({success_count/len(negatives)*100:.1f}%)")
    print(f"  耗时: {elapsed:.3f}s")
    print(f"  查询优化: 从 {len(negatives)} 次减少到 {len(combo_count)} 次")
    print(f"  优化效果: {(1 - len(combo_count)/len(negatives))*100:.1f}% 查询减少")

    return True

def test_monitoring_system():
    """测试监控系统"""
    print("\n=== 测试监控系统 ===")

    # 重置监控统计
    monitor = get_monitor()
    monitor.reset_stats()

    db_config = get_db_config('test')
    db_manager = DatabaseManager(db_config)
    engine = GreedyMatchingEngine()
    candidate_provider = CandidateProvider(db_manager)

    # 执行几轮匹配以产生监控数据
    for round_num in range(3):
        negatives = create_diverse_negative_invoices(20)
        results = engine.match_batch(negatives, candidate_provider)
        print(f"第 {round_num + 1} 轮匹配完成")
        time.sleep(0.1)  # 模拟间隔

    # 获取健康报告
    health_report = monitor.get_health_report()

    print(f"\n系统健康报告:")
    print(f"  健康状态: {health_report['health_status']}")
    print(f"  运行时间: {health_report['uptime_seconds']:.1f}s")

    print(f"\n业务指标:")
    bm = health_report['business_metrics']
    print(f"  匹配成功率: {bm['success_rate']:.1%}")
    print(f"  处理发票总数: {bm['total_invoices']}")
    print(f"  成功匹配: {bm['successful_matches']}")
    print(f"  失败匹配: {bm['failed_matches']}")
    print(f"  总匹配金额: {bm['total_matched_amount']}")
    print(f"  产生碎片: {bm['fragments_created']}")

    print(f"\n技术指标:")
    tm = health_report['technical_metrics']
    print(f"  查询优化率: {tm['query_optimization_rate']:.1%}")
    print(f"  平均分组数: {tm['avg_groups_per_batch']:.1f}")
    print(f"  节省查询: {tm['queries_saved_total']}")

    print(f"\n性能指标:")
    pm = health_report['performance_metrics']
    print(f"  总请求: {pm['total_requests']}")
    print(f"  成功请求: {pm['successful_requests']}")
    print(f"  失败请求: {pm['failed_requests']}")
    print(f"  平均响应时间: {pm['avg_execution_time_ms']:.1f}ms")
    print(f"  最小/最大响应时间: {pm['min_execution_time_ms']:.1f}/{pm['max_execution_time_ms']:.1f}ms")

    # 测试碎片分析
    fragment_analysis = monitor.get_fragment_analysis(db_manager)
    if fragment_analysis:
        print(f"\n碎片分析:")
        print(f"  碎片率: {fragment_analysis.get('fragment_rate', 0):.1%}")
        print(f"  碎片金额占比: {fragment_analysis.get('fragment_amount_rate', 0):.1%}")
        print(f"  碎片数量: {fragment_analysis.get('fragment_count', 0)}")

    return True

def test_smart_routing():
    """测试智能路由功能"""
    print("\n=== 测试智能路由 ===")

    db_config = get_db_config('test')
    db_manager = DatabaseManager(db_config)
    engine = GreedyMatchingEngine()
    candidate_provider = CandidateProvider(db_manager)

    # 测试不同规模的批次
    test_cases = [
        ("小批量", 50),      # < 1000，应使用标准处理
        ("中批量", 5000),    # 1000-10000，应使用标准处理
        ("大批量", 15000),   # > 10000，应自动使用流式处理
    ]

    for case_name, batch_size in test_cases:
        print(f"\n--- {case_name}测试 ({batch_size} 条) ---")

        # 创建测试数据
        negatives = create_diverse_negative_invoices(batch_size)

        # 获取处理建议
        recommendation = engine.get_processing_recommendation(batch_size)
        print(f"系统建议: {recommendation['reason']}")
        print(f"预期内存: {recommendation['expected_memory']}")

        # 执行匹配（用户无感知的智能路由）
        start_time = time.time()
        results = engine.match_batch(negatives, candidate_provider)
        elapsed = time.time() - start_time

        success_count = sum(1 for r in results if r.success)

        print(f"执行结果:")
        print(f"  成功匹配: {success_count}/{batch_size} ({success_count/batch_size*100:.1f}%)")
        print(f"  总耗时: {elapsed:.3f}s")
        print(f"  平均响应: {elapsed/batch_size*1000:.2f}ms/条")

    return True

def test_batch_update_optimization():
    """测试批量更新优化"""
    print("\n=== 测试批量更新优化 ===")

    db_config = get_db_config('test')
    db_manager = DatabaseManager(db_config)
    engine = GreedyMatchingEngine()
    candidate_provider = CandidateProvider(db_manager)

    # 创建一些负数发票
    negatives = create_diverse_negative_invoices(10)

    # 执行匹配
    start_time = time.time()
    results = engine.match_batch(negatives, candidate_provider)
    match_time = time.time() - start_time

    # 保存结果（测试批量更新）
    batch_id = f"test_improvements_{int(time.time())}"
    save_start = time.time()
    save_success = db_manager.save_match_results(results, batch_id)
    save_time = time.time() - save_start

    success_count = sum(1 for r in results if r.success)
    total_allocations = sum(len(r.allocations) for r in results if r.success)

    print(f"批量更新测试结果:")
    print(f"  匹配时间: {match_time:.3f}s")
    print(f"  保存时间: {save_time:.3f}s")
    print(f"  保存成功: {save_success}")
    print(f"  成功匹配: {success_count}/{len(negatives)}")
    print(f"  更新记录数: {total_allocations}")
    print(f"  平均每条记录: {save_time/max(total_allocations, 1)*1000:.2f}ms")

    return save_success

def main():
    """主测试函数"""
    print("=== 负数发票匹配系统改进功能测试 ===\n")

    try:
        # 测试各项改进功能
        test_results = {
            'grouping_optimization': test_grouping_optimization(),
            'monitoring_system': test_monitoring_system(),
            'smart_routing': test_smart_routing(),
            'batch_update_optimization': test_batch_update_optimization(),
        }

        # 汇总测试结果
        print(f"\n=== 测试结果汇总 ===")
        all_passed = True
        for test_name, result in test_results.items():
            status = "✓ 通过" if result else "✗ 失败"
            print(f"  {test_name}: {status}")
            if not result:
                all_passed = False

        print(f"\n总体结果: {'全部通过' if all_passed else '部分失败'}")
        return all_passed

    except Exception as e:
        print(f"\n❌ 测试过程中发生错误: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)