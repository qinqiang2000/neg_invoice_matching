import sys
sys.path.append('..')

from core.matching_engine import GreedyMatchingEngine, NegativeInvoice
from core.db_manager import DatabaseManager, CandidateProvider
from decimal import Decimal
import time
import uuid

def test_matching():
    """测试匹配功能"""
    
    # 初始化组件
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'invoice_test',
        'user': 'postgres',
        'password': 'postgres'
    }
    
    db_manager = DatabaseManager(db_config)
    engine = GreedyMatchingEngine(fragment_threshold=Decimal('5'))
    candidate_provider = CandidateProvider(db_manager)
    
    # 准备测试数据
    test_invoices = [
        NegativeInvoice(1, Decimal('500'), 13, 1, 1),
        NegativeInvoice(2, Decimal('1000'), 13, 1, 1),
        NegativeInvoice(3, Decimal('100'), 6, 2, 2),
    ]
    
    # 测试匹配
    batch_id = str(uuid.uuid4())[:8]
    start_time = time.time()
    
    results = engine.match_batch(
        test_invoices,
        candidate_provider,
        sort_strategy="amount_desc"
    )
    
    elapsed = time.time() - start_time
    
    # 保存结果
    save_success = db_manager.save_match_results(results, batch_id)
    
    # 输出详细匹配结果
    print("\n=== 详细匹配结果 ===")
    for i, (invoice, result) in enumerate(zip(test_invoices, results)):
        print(f"\n负数发票 {invoice.invoice_id} (金额: {invoice.amount}, 税率: {invoice.tax_rate}%, 买方: {invoice.buyer_id}, 卖方: {invoice.seller_id}):")

        if result.success:
            print(f"  ✓ 匹配成功 - 总匹配金额: {result.total_matched}")
            print(f"  分配详情:")
            for j, alloc in enumerate(result.allocations, 1):
                print(f"    {j}. 蓝票行 {alloc.blue_line_id}: 使用金额 {alloc.amount_used}, 剩余 {alloc.remaining_after}")
            if result.fragments_created > 0:
                print(f"  ⚠️  产生碎片: {result.fragments_created} 个")
        else:
            print(f"  ✗ 匹配失败 - 原因: {result.failure_reason}")
            print(f"  已匹配金额: {result.total_matched}, 未匹配金额: {invoice.amount - result.total_matched}")

    # 输出总体统计
    print(f"\n=== 总体统计 ===")
    metrics = engine.calculate_metrics(results)
    print(f"匹配成功率: {metrics['success_rate']:.2%}")
    print(f"执行时间: {elapsed:.3f}秒")
    print(f"产生碎片: {metrics['total_fragments']}")
    print(f"总匹配金额: {metrics['total_matched_amount']}")

    # 验证结果
    assert metrics['success_rate'] >= 0.9, "成功率低于90%"
    assert elapsed < 1.0, "执行时间超过1秒"

    print("\n✓ 测试通过")

if __name__ == "__main__":
    test_matching()