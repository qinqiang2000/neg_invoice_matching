from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)

@dataclass
class BlueLineItem:
    """蓝票行数据模型"""
    line_id: int
    remaining: Decimal
    tax_rate: int
    buyer_id: int
    seller_id: int

@dataclass
class NegativeInvoice:
    """负数发票数据模型"""
    invoice_id: int
    amount: Decimal
    tax_rate: int
    buyer_id: int
    seller_id: int
    priority: int = 0  # 优先级，用于排序

@dataclass
class MatchAllocation:
    """匹配分配结果"""
    blue_line_id: int
    amount_used: Decimal
    remaining_after: Decimal

@dataclass
class MatchResult:
    """匹配结果"""
    negative_invoice_id: int
    success: bool
    allocations: List[MatchAllocation]
    total_matched: Decimal
    fragments_created: int
    failure_reason: Optional[str] = None

class GreedyMatchingEngine:
    """
    贪婪匹配算法引擎
    核心业务逻辑，不依赖具体的数据库实现
    """
    
    def __init__(self, fragment_threshold: Decimal = Decimal('5.0')):
        """
        初始化匹配引擎
        
        Args:
            fragment_threshold: 碎片阈值，低于此值视为碎片
        """
        self.fragment_threshold = fragment_threshold
        
    def match_single(self, 
                    negative: NegativeInvoice,
                    candidates: List[BlueLineItem]) -> MatchResult:
        """
        匹配单个负数发票
        
        Args:
            negative: 负数发票
            candidates: 候选蓝票行列表（应已排序）
            
        Returns:
            MatchResult: 匹配结果
        """
        if not candidates:
            return MatchResult(
                negative_invoice_id=negative.invoice_id,
                success=False,
                allocations=[],
                total_matched=Decimal('0'),
                fragments_created=0,
                failure_reason="no_candidates"
            )
        
        need = negative.amount
        allocations = []
        fragments_created = 0
        
        # 贪婪分配：从小到大使用
        for blue_line in candidates:
            if need <= Decimal('0.01'):  # 允许1分钱误差
                break

            # 计算使用量
            use_amount = min(need, blue_line.remaining)
            remaining_after = blue_line.remaining - use_amount

            allocations.append(MatchAllocation(
                blue_line_id=blue_line.line_id,
                amount_used=use_amount,
                remaining_after=remaining_after
            ))

            # 统计碎片
            if Decimal('0') < remaining_after < self.fragment_threshold:
                fragments_created += 1

            need -= use_amount

            # 调试输出
            logger.debug(f"使用蓝票行 {blue_line.line_id}: 使用 {use_amount}, 剩余需求 {need}")
        
        # 判断是否成功
        total_matched = negative.amount - need
        success = need <= Decimal('0.01')
        
        return MatchResult(
            negative_invoice_id=negative.invoice_id,
            success=success,
            allocations=allocations if success else [],
            total_matched=total_matched if success else Decimal('0'),
            fragments_created=fragments_created if success else 0,
            failure_reason=None if success else "insufficient_funds"
        )
    
    def match_batch(self,
                   negatives: List[NegativeInvoice],
                   candidate_provider,
                   sort_strategy: str = "amount_desc") -> List[MatchResult]:
        """
        批量匹配负数发票
        
        Args:
            negatives: 负数发票列表
            candidate_provider: 提供候选蓝票行的函数/对象
            sort_strategy: 排序策略
                - amount_desc: 金额降序（大额优先）
                - amount_asc: 金额升序（小额优先）
                - priority: 按优先级
                
        Returns:
            List[MatchResult]: 匹配结果列表
        """
        # 排序负数发票，但保持原始索引用于结果排序
        indexed_negatives = [(i, neg) for i, neg in enumerate(negatives)]
        sorted_indexed = sorted(indexed_negatives,
                               key=lambda x: self._get_sort_key(x[1], sort_strategy))

        # 初始化结果列表，保持原始顺序
        results = [None] * len(negatives)
        used_blue_lines = set()  # 记录已使用的蓝票行

        for original_index, negative in sorted_indexed:
            # 获取候选（过滤已使用的）
            all_candidates = candidate_provider.get_candidates(
                negative.tax_rate,
                negative.buyer_id, 
                negative.seller_id
            )
            
            # 过滤已完全使用的蓝票行
            available_candidates = [
                c for c in all_candidates 
                if c.line_id not in used_blue_lines
            ]
            
            # 执行匹配
            result = self.match_single(negative, available_candidates)
            results[original_index] = result
            
            # 记录已使用的蓝票行
            if result.success:
                for alloc in result.allocations:
                    if alloc.remaining_after <= Decimal('0.01'):
                        used_blue_lines.add(alloc.blue_line_id)
            
            logger.info(f"匹配负数发票 {negative.invoice_id}: "
                       f"{'成功' if result.success else '失败'}, "
                       f"金额: {negative.amount}")
        
        return results
    
    def _get_sort_key(self, negative: NegativeInvoice, strategy: str):
        """获取排序键值"""
        if strategy == "amount_desc":
            return -negative.amount  # 负号实现降序
        elif strategy == "amount_asc":
            return negative.amount
        elif strategy == "priority_desc":
            return (-negative.priority, -negative.amount)
        else:
            return 0  # 不排序

    def _sort_negatives(self,
                       negatives: List[NegativeInvoice],
                       strategy: str) -> List[NegativeInvoice]:
        """负数发票排序（保留兼容性）"""
        if strategy == "amount_desc":
            return sorted(negatives, key=lambda x: x.amount, reverse=True)
        elif strategy == "amount_asc":
            return sorted(negatives, key=lambda x: x.amount)
        elif strategy == "priority_desc":
            return sorted(negatives, key=lambda x: (-x.priority, -x.amount))
        else:
            return negatives
    
    def calculate_metrics(self, results: List[MatchResult]) -> Dict:
        """计算匹配指标"""
        total = len(results)
        success = sum(1 for r in results if r.success)
        
        return {
            'total': total,
            'success': success,
            'failed': total - success,
            'success_rate': success / total if total > 0 else 0,
            'total_fragments': sum(r.fragments_created for r in results),
            'total_matched_amount': sum(r.total_matched for r in results)
        }