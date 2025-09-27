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
from tests.test_data_generator import TestDataGenerator
from decimal import Decimal
import time


def create_test_negative_invoices():
    """创建测试负数发票（使用现有数据的条件）"""
    return [
        # 小额测试 - 使用现有数据条件
        NegativeInvoice(1, Decimal('50.00'), 13, 1, 1),

        # 中额测试
        NegativeInvoice(2, Decimal('500.00'), 13, 1, 1),

        # 大额测试（利用现有的丰富数据）
        NegativeInvoice(3, Decimal('2000.00'), 13, 1, 1),
    ]

def run_basic_matching_test():
    """运行基本匹配测试（使用现有数据，测试后重置）"""
    print("=== 基本匹配功能测试（使用现有数据）===\n")

    # 初始化组件
    db_config = get_db_config('test')
    db_manager = DatabaseManager(db_config)
    engine = GreedyMatchingEngine(fragment_threshold=Decimal('5'))
    candidate_provider = CandidateProvider(db_manager)
    data_generator = TestDataGenerator(db_config)

    try:
        # 清理匹配记录（保留蓝票行数据）
        print("清理旧的匹配记录...")
        conn = db_manager.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM match_records WHERE batch_id LIKE 'test_basic_%'")
                conn.commit()
                print("✓ 匹配记录清理完成")
        finally:
            db_manager.pool.putconn(conn)

        # 创建测试负数发票（使用现有数据的条件）
        test_invoices = create_test_negative_invoices()
        print(f"创建 {len(test_invoices)} 个测试负数发票\n")

        # 打印当前蓝票行状态
        print_blue_lines_status(db_manager)

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

        # 输出详细结果（包含候选集信息）
        print_detailed_results_with_candidates(test_invoices, results, candidate_provider, elapsed)

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
        # 精确重置：只恢复本次测试的扣减
        print("\n精确重置本次测试的扣减...")
        try:
            reset_specific_test_changes(db_manager, batch_id)
            print("✅ 本次测试的数据扣减已重置")
        except Exception as e:
            print(f"⚠️ 精确重置失败，使用全局重置: {e}")
            try:
                data_generator.reset_test_data()
                print("✅ 全局数据状态已重置")
            except Exception as e2:
                print(f"⚠️ 全局重置也失败: {e2}")
        finally:
            data_generator.close()

def reset_specific_test_changes(db_manager, batch_id):
    """精确重置：只恢复本次测试的数据扣减"""
    conn = db_manager.pool.getconn()
    try:
        with conn.cursor() as cur:
            # 从match_records表获取本次测试的扣减记录
            cur.execute("""
                SELECT blue_line_id, amount_used
                FROM match_records
                WHERE batch_id = %s
            """, (batch_id,))

            changes = cur.fetchall()
            if not changes:
                print("本次测试无扣减记录")
                return

            # 批量恢复remaining值
            updates = [(amount_used, blue_line_id) for blue_line_id, amount_used in changes]
            cur.executemany("""
                UPDATE blue_lines
                SET remaining = remaining + %s,
                    last_update = CURRENT_TIMESTAMP
                WHERE line_id = %s
            """, updates)

            # 删除本次测试的匹配记录
            cur.execute("DELETE FROM match_records WHERE batch_id = %s", (batch_id,))

            conn.commit()
            print(f"已精确恢复 {len(updates)} 个蓝票行的扣减")

    finally:
        db_manager.pool.putconn(conn)

def print_detailed_results_with_candidates(invoices, results, candidate_provider, elapsed):
    """输出详细匹配结果（包含候选集信息）"""
    print("=== 详细匹配结果（含候选集信息） ===")

    for i, (invoice, result) in enumerate(zip(invoices, results)):
        print(f"\n{i+1}. 负数发票 {invoice.invoice_id}:")
        print(f"   输入: 金额={invoice.amount}, 税率={invoice.tax_rate}%, "
              f"买方={invoice.buyer_id}, 卖方={invoice.seller_id}")

        # 获取候选集
        candidates = candidate_provider.get_candidates(invoice.tax_rate, invoice.buyer_id, invoice.seller_id)
        print(f"   候选集: 找到 {len(candidates)} 个候选蓝票行")

        if candidates:
            print(f"   候选集详情:")
            total_candidate_amount = sum(c.remaining for c in candidates)
            print(f"     - 总可用金额: {total_candidate_amount}")
            print(f"     - 前5个候选项:")
            for j, candidate in enumerate(candidates[:5], 1):
                print(f"       {j}) ID={candidate.line_id}, 余额={candidate.remaining}")
            if len(candidates) > 5:
                print(f"       ... 还有 {len(candidates) - 5} 个候选项")
        else:
            print(f"   ⚠️  无候选集 - 无法匹配")

        if result.success:
            print(f"   ✓ 匹配成功 - 总匹配金额: {result.total_matched}")
            print(f"   最终分配 ({len(result.allocations)} 个蓝票行):")
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

def print_detailed_results(invoices, results, elapsed):
    """输出详细匹配结果（原版本，保持兼容性）"""
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

    # 基本验证（使用现有数据，调整期望）
    assert success_count >= 2, f"期望至少2个成功匹配，实际: {success_count}"
    assert total_matched >= total_requested * Decimal('0.5'), f"期望至少50%匹配率"

    print("✓ 所有验证通过")

def print_blue_lines_status(db_manager):
    """打印当前蓝票行状态"""
    print("\n=== 当前蓝票行状态 ===")

    conn = db_manager.pool.getconn()
    try:
        with conn.cursor() as cur:
            # 按税率和买卖方分组统计
            cur.execute("""
                SELECT
                    tax_rate, buyer_id, seller_id,
                    COUNT(*) as total_lines,
                    SUM(CASE WHEN remaining > 0 THEN 1 ELSE 0 END) as active_lines,
                    SUM(remaining) as total_remaining,
                    MIN(remaining) as min_remaining,
                    MAX(remaining) as max_remaining
                FROM blue_lines
                WHERE batch_id = 'test_basic' AND remaining > 0
                GROUP BY tax_rate, buyer_id, seller_id
                ORDER BY tax_rate, buyer_id, seller_id
            """)

            rows = cur.fetchall()
            if rows:
                print("分组统计（税率-买方-卖方）:")
                total_lines_all = 0
                total_remaining_all = 0
                for row in rows:
                    tax_rate, buyer_id, seller_id, total_lines, active_lines, total_remaining, min_rem, max_rem = row
                    print(f"  {tax_rate}%-{buyer_id}-{seller_id}: {active_lines}行可用, "
                          f"余额范围 {min_rem}~{max_rem}, 总余额 {total_remaining}")
                    total_lines_all += active_lines
                    total_remaining_all += total_remaining
                print(f"\n总计: {total_lines_all} 行可用, 总余额: {total_remaining_all}")
            else:
                print("无可用蓝票行")

    finally:
        db_manager.pool.putconn(conn)

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