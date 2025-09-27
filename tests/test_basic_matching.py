#!/usr/bin/env python3
"""
手工测试用例1：基本匹配功能测试

测试目的：
1. 验证负数发票匹配系统的基本功能是否正常
2. 测试小规模数据的匹配准确性
3. 验证数据库操作的正确性

测试场景：
- 创建100条蓝票行测试数据
- 生成5个负数发票（不同金额范围）
- 执行匹配并验证结果

运行方式：
python test_basic_matching.py
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

def setup_test_data(db_manager):
    """设置测试数据"""
    print("准备测试数据...")

    # 清理之前的测试数据
    conn = db_manager.pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM blue_lines WHERE batch_id = 'test_basic'")
            cur.execute("DELETE FROM match_records WHERE batch_id = 'test_basic'")
            conn.commit()
            print("✓ 清理旧数据完成")
    finally:
        db_manager.pool.putconn(conn)

    # 创建蓝票行测试数据
    test_data = []

    # 买方1、卖方1、税率13% - 50条数据
    for i in range(50):
        test_data.append((
            1,  # ticket_id
            13, # tax_rate
            1,  # buyer_id
            1,  # seller_id
            f"Product_A_{i}",  # product_name
            Decimal('100.00'),  # original_amount
            Decimal(str(10 + i * 2)),  # remaining (10, 12, 14, ..., 108)
            'test_basic'  # batch_id
        ))

    # 买方2、卖方2、税率6% - 30条数据
    for i in range(30):
        test_data.append((
            2,  # ticket_id
            6,  # tax_rate
            2,  # buyer_id
            2,  # seller_id
            f"Product_B_{i}",  # product_name
            Decimal('200.00'),  # original_amount
            Decimal(str(20 + i * 5)),  # remaining (20, 25, 30, ..., 165)
            'test_basic'  # batch_id
        ))

    # 买方1、卖方1、税率13% - 额外20条大额数据
    for i in range(20):
        test_data.append((
            3,  # ticket_id
            13, # tax_rate
            1,  # buyer_id
            1,  # seller_id
            f"Product_C_{i}",  # product_name
            Decimal('500.00'),  # original_amount
            Decimal(str(200 + i * 50)),  # remaining (200, 250, 300, ..., 1150)
            'test_basic'  # batch_id
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
            print(f"✓ 插入 {len(test_data)} 条蓝票行数据")
    finally:
        db_manager.pool.putconn(conn)

def create_test_negative_invoices():
    """创建测试负数发票"""
    return [
        # 小额负数发票 - 应该能轻松匹配
        NegativeInvoice(1, Decimal('50.00'), 13, 1, 1),

        # 中等金额 - 需要多个蓝票行组合
        NegativeInvoice(2, Decimal('300.00'), 13, 1, 1),

        # 不同税率和买卖方
        NegativeInvoice(3, Decimal('150.00'), 6, 2, 2),

        # 大额负数发票 - 测试大额匹配
        NegativeInvoice(4, Decimal('1000.00'), 13, 1, 1),

        # 边界测试：正好等于某个蓝票行余额
        NegativeInvoice(5, Decimal('10.00'), 13, 1, 1),
    ]

def run_basic_matching_test():
    """运行基本匹配测试"""
    print("=== 基本匹配功能测试 ===\n")

    # 初始化组件
    db_config = get_db_config('test')
    db_manager = DatabaseManager(db_config)
    engine = GreedyMatchingEngine(fragment_threshold=Decimal('5'))
    candidate_provider = CandidateProvider(db_manager)

    try:
        # 设置测试数据
        setup_test_data(db_manager)

        # 创建测试负数发票
        test_invoices = create_test_negative_invoices()
        print(f"创建 {len(test_invoices)} 个测试负数发票\n")

        # 执行匹配
        batch_id = f"test_basic_{int(time.time())}"
        start_time = time.time()

        results = engine.match_batch(
            test_invoices,
            candidate_provider,
            sort_strategy="amount_desc"
        )

        elapsed = time.time() - start_time

        # 保存结果
        save_success = db_manager.save_match_results(results, batch_id)

        # 输出详细结果
        print_detailed_results(test_invoices, results, elapsed)

        # 验证结果
        verify_results(results, test_invoices)

        # 输出数据库状态
        print_database_status(db_manager)

        print(f"\n✓ 基本匹配测试完成 (批次: {batch_id})")
        return True

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        return False
    finally:
        # 清理数据（可选，用于重复测试）
        cleanup_test_data(db_manager)

def print_detailed_results(invoices, results, elapsed):
    """输出详细匹配结果"""
    print("=== 详细匹配结果 ===")

    for i, (invoice, result) in enumerate(zip(invoices, results)):
        print(f"\n{i+1}. 负数发票 {invoice.invoice_id}:")
        print(f"   金额: {invoice.amount}, 税率: {invoice.tax_rate}%, "
              f"买方: {invoice.buyer_id}, 卖方: {invoice.seller_id}")

        if result.success:
            print(f"   ✓ 匹配成功 - 总匹配金额: {result.total_matched}")
            print(f"   使用了 {len(result.allocations)} 个蓝票行:")
            for j, alloc in enumerate(result.allocations, 1):
                print(f"     {j}) 蓝票行 {alloc.blue_line_id}: "
                      f"使用 {alloc.amount_used}, 剩余 {alloc.remaining_after}")
            if result.fragments_created > 0:
                print(f"   ⚠️  产生碎片: {result.fragments_created} 个")
        else:
            print(f"   ✗ 匹配失败 - 原因: {result.failure_reason}")
            print(f"   已匹配: {result.total_matched}, "
                  f"未匹配: {invoice.amount - result.total_matched}")

    print(f"\n执行时间: {elapsed:.3f} 秒")

def verify_results(results, invoices):
    """验证匹配结果的正确性"""
    print("\n=== 结果验证 ===")

    success_count = sum(1 for r in results if r.success)
    total_matched = sum(r.total_matched for r in results)
    total_requested = sum(inv.amount for inv in invoices)
    fragment_count = sum(r.fragments_created for r in results)

    print(f"匹配成功率: {success_count}/{len(results)} ({success_count/len(results)*100:.1f}%)")
    print(f"总请求金额: {total_requested}")
    print(f"总匹配金额: {total_matched}")
    print(f"匹配覆盖率: {total_matched/total_requested*100:.1f}%")
    print(f"产生碎片: {fragment_count} 个")

    # 基本验证
    assert success_count >= 4, f"期望至少4个成功匹配，实际: {success_count}"
    assert total_matched >= total_requested * Decimal('0.8'), f"期望至少80%匹配率"

    print("✓ 所有验证通过")

def print_database_status(db_manager):
    """输出数据库状态"""
    print("\n=== 数据库状态 ===")

    conn = db_manager.pool.getconn()
    try:
        with conn.cursor() as cur:
            # 统计蓝票行状态
            cur.execute("""
                SELECT
                    COUNT(*) as total_lines,
                    SUM(CASE WHEN remaining > 0 THEN 1 ELSE 0 END) as active_lines,
                    SUM(remaining) as total_remaining
                FROM blue_lines
                WHERE batch_id = 'test_basic'
            """)
            row = cur.fetchone()
            print(f"蓝票行总数: {row[0]}, 活跃行数: {row[1]}, 总余额: {row[2]}")

            # 统计匹配记录
            cur.execute("""
                SELECT COUNT(*), SUM(amount_used)
                FROM match_records
                WHERE batch_id LIKE 'test_basic_%'
            """)
            row = cur.fetchone()
            print(f"匹配记录数: {row[0]}, 总使用金额: {row[1] or 0}")

    finally:
        db_manager.pool.putconn(conn)

def cleanup_test_data(db_manager):
    """清理测试数据"""
    print("\n清理测试数据...")

    conn = db_manager.pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM match_records WHERE batch_id LIKE 'test_basic_%'")
            cur.execute("DELETE FROM blue_lines WHERE batch_id = 'test_basic'")
            conn.commit()
            print("✓ 测试数据清理完成")
    finally:
        db_manager.pool.putconn(conn)

if __name__ == "__main__":
    success = run_basic_matching_test()
    sys.exit(0 if success else 1)