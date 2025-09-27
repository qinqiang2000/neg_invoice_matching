#!/usr/bin/env python3
"""
手工测试用例2：边界情况和特殊场景测试

测试目的：
1. 验证各种边界情况的处理
2. 测试系统在特殊场景下的鲁棒性
3. 验证错误处理和异常情况

测试场景：
- 完全匹配：负数发票金额正好等于蓝票行余额
- 部分匹配：需要多个蓝票行组合匹配
- 无法匹配：余额不足的情况
- 碎片产生：匹配后产生小额碎片
- 并发冲突：模拟并发访问情况

运行方式：
python test_edge_cases.py
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.matching_engine import GreedyMatchingEngine, NegativeInvoice
from core.db_manager import DatabaseManager, CandidateProvider
from config.config import get_db_config
from decimal import Decimal
import time
import uuid

def setup_edge_case_data(db_manager):
    """设置边界情况测试数据"""
    print("准备边界情况测试数据...")

    # 清理之前的测试数据
    conn = db_manager.pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM blue_lines WHERE batch_id = 'test_edge'")
            cur.execute("DELETE FROM match_records WHERE batch_id = 'test_edge'")
            conn.commit()
            print("✓ 清理旧数据完成")
    finally:
        db_manager.pool.putconn(conn)

    # 创建特殊测试数据
    test_data = []

    # 场景1：完全匹配数据 - 买方1、卖方1、税率13%
    exact_amounts = [Decimal('100.00'), Decimal('200.00'), Decimal('500.00')]
    for i, amount in enumerate(exact_amounts):
        test_data.append((
            1,  # ticket_id
            13, # tax_rate
            1,  # buyer_id
            1,  # seller_id
            f"Exact_Match_{i}",  # product_name
            amount,  # original_amount
            amount,  # remaining (完全相等)
            'test_edge'  # batch_id
        ))

    # 场景2：小额碎片数据 - 买方1、卖方1、税率13%
    small_amounts = [Decimal('1.00'), Decimal('2.50'), Decimal('4.99'), Decimal('5.01'), Decimal('3.33')]
    for i, amount in enumerate(small_amounts):
        test_data.append((
            2,  # ticket_id
            13, # tax_rate
            1,  # buyer_id
            1,  # seller_id
            f"Small_Fragment_{i}",  # product_name
            amount * 2,  # original_amount
            amount,  # remaining
            'test_edge'  # batch_id
        ))

    # 场景3：大额单一数据 - 买方2、卖方2、税率6%
    test_data.append((
        3,  # ticket_id
        6,  # tax_rate
        2,  # buyer_id
        2,  # seller_id
        "Large_Single",  # product_name
        Decimal('10000.00'),  # original_amount
        Decimal('8888.88'),  # remaining
        'test_edge'  # batch_id
    ))

    # 场景4：零余额数据（已用完）- 买方1、卖方1、税率13%
    for i in range(3):
        test_data.append((
            4,  # ticket_id
            13, # tax_rate
            1,  # buyer_id
            1,  # seller_id
            f"Zero_Remaining_{i}",  # product_name
            Decimal('100.00'),  # original_amount
            Decimal('0.00'),  # remaining (已用完)
            'test_edge'  # batch_id
        ))

    # 场景5：不同买卖方组合 - 验证筛选逻辑
    buyers_sellers = [(3, 3), (4, 4), (5, 5)]
    for i, (buyer, seller) in enumerate(buyers_sellers):
        test_data.append((
            5 + i,  # ticket_id
            13, # tax_rate
            buyer,  # buyer_id
            seller, # seller_id
            f"Different_Pair_{i}",  # product_name
            Decimal('150.00'),  # original_amount
            Decimal('75.00'),  # remaining
            'test_edge'  # batch_id
        ))

    # 场景6：不同税率数据
    tax_rates = [3, 0]  # 3%和0%税率
    for i, tax_rate in enumerate(tax_rates):
        test_data.append((
            8 + i,  # ticket_id
            tax_rate, # tax_rate
            1,  # buyer_id
            1,  # seller_id
            f"Tax_Rate_{tax_rate}_{i}",  # product_name
            Decimal('300.00'),  # original_amount
            Decimal('200.00'),  # remaining
            'test_edge'  # batch_id
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
            print(f"✓ 插入 {len(test_data)} 条边界情况测试数据")
    finally:
        db_manager.pool.putconn(conn)

def create_edge_case_negative_invoices():
    """创建边界情况测试负数发票"""
    return [
        # 测试1：完全匹配
        NegativeInvoice(101, Decimal('100.00'), 13, 1, 1, priority=1),

        # 测试2：需要多个小额组合
        NegativeInvoice(102, Decimal('15.83'), 13, 1, 1, priority=2),

        # 测试3：超大金额（应该部分匹配）
        NegativeInvoice(103, Decimal('999999.00'), 13, 1, 1, priority=3),

        # 测试4：无法匹配的组合（不存在的买卖方）
        NegativeInvoice(104, Decimal('100.00'), 13, 999, 999, priority=4),

        # 测试5：零金额（边界值）
        NegativeInvoice(105, Decimal('0.01'), 13, 1, 1, priority=5),

        # 测试6：错误税率（不存在的税率）
        NegativeInvoice(106, Decimal('50.00'), 99, 1, 1, priority=6),

        # 测试7：大额单一匹配
        NegativeInvoice(107, Decimal('8000.00'), 6, 2, 2, priority=7),

        # 测试8：碎片阈值测试
        NegativeInvoice(108, Decimal('4.99'), 13, 1, 1, priority=8),  # 低于阈值
        NegativeInvoice(109, Decimal('5.01'), 13, 1, 1, priority=9),  # 高于阈值

        # 测试9：不同税率测试
        NegativeInvoice(110, Decimal('150.00'), 3, 1, 1, priority=10),
    ]

def run_edge_cases_test():
    """运行边界情况测试"""
    print("=== 边界情况和特殊场景测试 ===\n")

    # 初始化组件
    db_config = get_db_config('test')
    db_manager = DatabaseManager(db_config)
    engine = GreedyMatchingEngine(fragment_threshold=Decimal('5.0'))
    candidate_provider = CandidateProvider(db_manager)

    try:
        # 设置测试数据
        setup_edge_case_data(db_manager)

        # 创建测试负数发票
        test_invoices = create_edge_case_negative_invoices()
        print(f"创建 {len(test_invoices)} 个边界情况测试负数发票\n")

        # 执行单个测试用例
        run_individual_tests(engine, candidate_provider, test_invoices)

        # 执行批量测试
        run_batch_test(engine, candidate_provider, test_invoices, db_manager)

        print(f"\n✓ 边界情况测试完成")
        return True

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # 清理数据
        cleanup_edge_test_data(db_manager)

def run_individual_tests(engine, candidate_provider, test_invoices):
    """运行单个测试用例"""
    print("=== 单个测试用例 ===")

    for i, invoice in enumerate(test_invoices):
        print(f"\n测试 {i+1}: 负数发票 {invoice.invoice_id}")
        print(f"  金额: {invoice.amount}, 税率: {invoice.tax_rate}%, "
              f"买方: {invoice.buyer_id}, 卖方: {invoice.seller_id}")

        # 获取候选项
        candidates = candidate_provider.get_candidates(
            invoice.tax_rate,
            invoice.buyer_id,
            invoice.seller_id
        )
        print(f"  找到候选项: {len(candidates)} 个")

        if candidates:
            total_available = sum(c.remaining for c in candidates)
            print(f"  总可用余额: {total_available}")

        # 执行匹配
        start_time = time.time()
        result = engine.match_single(invoice, candidates)
        elapsed = time.time() - start_time

        # 输出结果
        if result.success:
            print(f"  ✓ 匹配成功 - 匹配金额: {result.total_matched}")
            print(f"    使用 {len(result.allocations)} 个蓝票行")
            if result.fragments_created > 0:
                print(f"    ⚠️ 产生碎片: {result.fragments_created} 个")
        else:
            print(f"  ✗ 匹配失败 - 原因: {result.failure_reason}")
            if result.total_matched > 0:
                print(f"    部分匹配: {result.total_matched}")

        print(f"  耗时: {elapsed*1000:.2f}ms")

        # 特殊情况验证
        validate_special_cases(invoice, result, candidates)

def validate_special_cases(invoice, result, candidates):
    """验证特殊情况"""
    # 验证1：完全匹配的情况
    if invoice.invoice_id == 101:  # 完全匹配测试
        if result.success and result.total_matched == invoice.amount:
            print("    ✓ 完全匹配验证通过")
        else:
            print("    ⚠️ 完全匹配验证失败")

    # 验证2：无候选项的情况
    if invoice.buyer_id == 999 or invoice.seller_id == 999:
        if not result.success and result.failure_reason == "no_candidates":
            print("    ✓ 无候选项验证通过")
        else:
            print("    ⚠️ 无候选项验证失败")

    # 验证3：碎片阈值验证
    if invoice.invoice_id in [108, 109]:  # 碎片阈值测试
        if invoice.amount < Decimal('5.0'):
            print(f"    碎片阈值测试: 金额 {invoice.amount} < 5.0")
        else:
            print(f"    碎片阈值测试: 金额 {invoice.amount} >= 5.0")

def run_batch_test(engine, candidate_provider, test_invoices, db_manager):
    """运行批量测试"""
    print("\n=== 批量测试 ===")

    batch_id = f"test_edge_{int(time.time())}"
    start_time = time.time()

    # 执行批量匹配
    results = engine.match_batch(
        test_invoices,
        candidate_provider,
        sort_strategy="priority_desc"  # 按优先级排序
    )

    elapsed = time.time() - start_time

    # 保存结果
    try:
        save_success = db_manager.save_match_results(results, batch_id)
        print(f"✓ 匹配结果已保存到数据库 (批次: {batch_id})")
    except Exception as e:
        print(f"⚠️ 保存结果失败: {e}")

    # 统计分析
    analyze_batch_results(test_invoices, results, elapsed)

def analyze_batch_results(invoices, results, elapsed):
    """分析批量结果"""
    print("\n=== 批量结果分析 ===")

    success_count = sum(1 for r in results if r.success)
    total_requested = sum(inv.amount for inv in invoices)
    total_matched = sum(r.total_matched for r in results)
    fragment_count = sum(r.fragments_created for r in results)

    print(f"总负数发票: {len(invoices)}")
    print(f"匹配成功: {success_count} ({success_count/len(invoices)*100:.1f}%)")
    print(f"匹配失败: {len(invoices) - success_count}")
    print(f"总请求金额: {total_requested}")
    print(f"总匹配金额: {total_matched}")
    print(f"匹配覆盖率: {total_matched/total_requested*100:.1f}%")
    print(f"产生碎片: {fragment_count} 个")
    print(f"执行时间: {elapsed:.3f} 秒")
    print(f"平均每单耗时: {elapsed/len(invoices)*1000:.2f} 毫秒")

    # 失败原因统计
    failure_reasons = {}
    for result in results:
        if not result.success:
            reason = result.failure_reason or "unknown"
            failure_reasons[reason] = failure_reasons.get(reason, 0) + 1

    if failure_reasons:
        print("\n失败原因统计:")
        for reason, count in failure_reasons.items():
            print(f"  {reason}: {count} 次")

    # 边界情况特殊验证
    print("\n=== 边界情况验证 ===")

    # 验证完全匹配
    exact_match_results = [r for i, r in enumerate(results) if invoices[i].invoice_id == 101]
    if exact_match_results and exact_match_results[0].success:
        print("✓ 完全匹配场景验证通过")
    else:
        print("⚠️ 完全匹配场景验证失败")

    # 验证无候选项场景
    no_candidate_results = [r for i, r in enumerate(results)
                           if invoices[i].buyer_id == 999 or invoices[i].seller_id == 999]
    if no_candidate_results and not no_candidate_results[0].success:
        print("✓ 无候选项场景验证通过")
    else:
        print("⚠️ 无候选项场景验证失败")

    # 验证碎片控制
    fragment_results = [r for r in results if r.fragments_created > 0]
    print(f"✓ 碎片控制: {len(fragment_results)} 个匹配产生了碎片")

def cleanup_edge_test_data(db_manager):
    """清理边界测试数据"""
    print("\n清理边界测试数据...")

    conn = db_manager.pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM match_records WHERE batch_id LIKE 'test_edge_%'")
            cur.execute("DELETE FROM blue_lines WHERE batch_id = 'test_edge'")
            conn.commit()
            print("✓ 边界测试数据清理完成")
    finally:
        db_manager.pool.putconn(conn)

if __name__ == "__main__":
    success = run_edge_cases_test()
    sys.exit(0 if success else 1)