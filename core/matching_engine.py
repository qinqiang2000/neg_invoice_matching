from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from decimal import Decimal
import logging
import time
import copy

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
    
    def _match_batch_standard(self,
                             negatives: List[NegativeInvoice],
                             candidate_provider,
                             sort_strategy: str = "amount_desc",
                             enable_monitoring: bool = True) -> List[MatchResult]:
        """
        批量匹配负数发票
        采用分组策略减少数据库查询次数

        Args:
            negatives: 负数发票列表
            candidate_provider: 提供候选蓝票行的函数/对象
            sort_strategy: 排序策略
                - amount_desc: 金额降序（大额优先）
                - amount_asc: 金额升序（小额优先）
                - priority: 按优先级
            enable_monitoring: 是否启用监控

        Returns:
            List[MatchResult]: 匹配结果列表
        """
        start_time = time.time()

        # 第一步：按(tax_rate, buyer_id, seller_id)分组负数发票
        groups = self._group_negatives_by_conditions(negatives)
        logger.info(f"将 {len(negatives)} 个负数发票分为 {len(groups)} 组")

        # 初始化结果列表，保持原始顺序
        results = [None] * len(negatives)

        # 第二步：预取所有组的候选集（批量查询优化）
        group_candidates = self._prefetch_candidates_for_groups(groups, candidate_provider)

        # 第三步：按组处理负数发票
        for group_key, group_negatives in groups.items():
            logger.debug(f"处理组 {group_key}: {len(group_negatives)} 个负数发票")

            # 获取该组的候选集
            candidates = group_candidates[group_key]
            if not candidates:
                logger.warning(f"组 {group_key} 没有可用候选")
                # 标记该组所有发票为失败
                for original_index, negative in group_negatives:
                    results[original_index] = MatchResult(
                        negative_invoice_id=negative.invoice_id,
                        success=False,
                        allocations=[],
                        total_matched=Decimal('0'),
                        fragments_created=0,
                        failure_reason="no_candidates"
                    )
                continue

            # 组内排序并匹配
            group_results = self._match_group(group_negatives, candidates, sort_strategy)

            # 将结果放回原始位置
            for (original_index, _), result in zip(group_negatives, group_results):
                results[original_index] = result

        # 计算总执行时间
        execution_time = time.time() - start_time

        # 记录监控数据
        if enable_monitoring:
            try:
                from .monitoring import get_monitor
                monitor = get_monitor()
                monitor.record_batch_execution(
                    execution_time=execution_time,
                    results=results,
                    negatives_count=len(negatives),
                    groups_count=len(groups)
                )


            except ImportError:
                logger.debug("监控模块未导入，跳过监控记录")
            except Exception as e:
                logger.warning(f"记录监控数据失败: {e}")

        return results

    def _group_negatives_by_conditions(self,
                                     negatives: List[NegativeInvoice]) -> Dict[tuple, List[tuple]]:
        """
        按(tax_rate, buyer_id, seller_id)分组负数发票

        Returns:
            Dict[tuple, List[tuple]]: 分组结果，key为条件元组，value为(原始索引, 负数发票)列表
        """
        groups = {}
        for i, negative in enumerate(negatives):
            group_key = (negative.tax_rate, negative.buyer_id, negative.seller_id)
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append((i, negative))

        return groups

    def _prefetch_candidates_for_groups(self,
                                      groups: Dict[tuple, List[tuple]],
                                      candidate_provider) -> Dict[tuple, List[BlueLineItem]]:
        """
        为所有组预取候选集
        """
        group_candidates = {}

        for group_key in groups.keys():
            tax_rate, buyer_id, seller_id = group_key
            candidates = candidate_provider.get_candidates(tax_rate, buyer_id, seller_id)
            group_candidates[group_key] = candidates
            logger.debug(f"组 {group_key} 获取到 {len(candidates)} 个候选")

        return group_candidates

    def _match_group(self,
                    group_negatives: List[tuple],
                    candidates: List[BlueLineItem],
                    sort_strategy: str) -> List[MatchResult]:
        """
        匹配单个组内的负数发票
        需要实时更新候选集的remaining值，避免重复分配
        """
        # 组内排序
        sorted_group = sorted(group_negatives,
                            key=lambda x: self._get_sort_key(x[1], sort_strategy))

        results = []
        # 创建候选集的深拷贝以实时更新remaining
        local_candidates = {c.line_id: copy.deepcopy(c) for c in candidates}

        for original_index, negative in sorted_group:
            # 过滤remaining为0的蓝票行，并转换为列表
            available_candidates = [
                c for c in local_candidates.values()
                if c.remaining > Decimal('0.01')
            ]

            # 按remaining升序排序（贪婪算法要求）
            available_candidates.sort(key=lambda x: x.remaining)

            # 执行匹配
            result = self.match_single(negative, available_candidates)
            results.append(result)

            # 实时更新本地候选集的remaining值
            if result.success:
                for alloc in result.allocations:
                    if alloc.blue_line_id in local_candidates:
                        local_candidates[alloc.blue_line_id].remaining = alloc.remaining_after

            logger.debug(f"匹配负数发票 {negative.invoice_id}: "
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
    
    def match_batch_streaming(self,
                             negatives: List[NegativeInvoice],
                             candidate_provider,
                             batch_size: int = 1000,
                             sort_strategy: str = "amount_desc",
                             enable_monitoring: bool = True) -> List[MatchResult]:
        """
        流式批量匹配负数发票
        适用于大批量数据，减少内存使用

        Args:
            negatives: 负数发票列表
            candidate_provider: 提供候选蓝票行的函数/对象
            batch_size: 每批处理的负数发票数量
            sort_strategy: 排序策略
            enable_monitoring: 是否启用监控

        Returns:
            List[MatchResult]: 匹配结果列表
        """
        total_count = len(negatives)
        logger.info(f"流式处理 {total_count} 个负数发票，批次大小: {batch_size}")

        all_results = []
        start_time = time.time()

        # 分批处理
        for i in range(0, total_count, batch_size):
            batch_end = min(i + batch_size, total_count)
            batch_negatives = negatives[i:batch_end]

            logger.debug(f"处理批次 {i//batch_size + 1}/{(total_count-1)//batch_size + 1}: "
                        f"发票 {i+1}-{batch_end}")

            # 处理当前批次（禁用子批次监控，最后统一记录）
            batch_results = self._match_batch_standard(
                batch_negatives,
                candidate_provider,
                sort_strategy,
                enable_monitoring=False
            )

            all_results.extend(batch_results)


            logger.debug(f"批次完成，当前总进度: {len(all_results)}/{total_count}")

        # 计算总执行时间
        total_execution_time = time.time() - start_time

        # 记录监控数据（整体统计）
        if enable_monitoring:
            try:
                from .monitoring import get_monitor
                monitor = get_monitor()

                # 计算分组数量（估算）
                groups = self._group_negatives_by_conditions(negatives)
                groups_count = len(groups)

                monitor.record_batch_execution(
                    execution_time=total_execution_time,
                    results=all_results,
                    negatives_count=total_count,
                    groups_count=groups_count
                )


            except ImportError:
                logger.debug("监控模块未导入，跳过监控记录")
            except Exception as e:
                logger.warning(f"记录监控数据失败: {e}")

        logger.info(f"流式处理完成: {total_count} 个负数发票，总耗时 {total_execution_time:.3f}s")
        return all_results

    def match_batch(self,
                   negatives: List[NegativeInvoice],
                   candidate_provider,
                   sort_strategy: str = "amount_desc",
                   enable_monitoring: bool = True) -> List[MatchResult]:
        """
        批量匹配负数发票
        自动根据数据量选择最优处理方式

        Args:
            negatives: 负数发票列表
            candidate_provider: 提供候选蓝票行的函数/对象
            sort_strategy: 排序策略
            enable_monitoring: 是否启用监控

        Returns:
            List[MatchResult]: 匹配结果列表
        """
        batch_count = len(negatives)

        # 智能路由：自动选择最优处理方式
        if batch_count >= 10000:
            # 大批量：使用流式处理
            logger.debug(f"大批量数据 ({batch_count} 条)，自动启用流式处理")
            return self.match_batch_streaming(
                negatives=negatives,
                candidate_provider=candidate_provider,
                batch_size=1000,
                sort_strategy=sort_strategy,
                enable_monitoring=enable_monitoring
            )
        else:
            # 小中批量：使用标准处理
            logger.debug(f"标准批量数据 ({batch_count} 条)，使用常规处理")
            return self._match_batch_standard(
                negatives=negatives,
                candidate_provider=candidate_provider,
                sort_strategy=sort_strategy,
                enable_monitoring=enable_monitoring
            )

    def get_processing_recommendation(self, batch_size: int) -> Dict:
        """
        获取处理方式建议

        Args:
            batch_size: 批次大小

        Returns:
            Dict: 包含建议信息的字典
        """
        if batch_size < 1000:
            return {
                'recommended_method': 'match_batch',
                'reason': '小批量数据，标准处理即可',
                'expected_memory': f'~{batch_size * 0.1:.1f}MB',
                'processing_time': '快速',
                'stability': '优秀'
            }
        elif batch_size < 10000:
            return {
                'recommended_method': 'match_batch',
                'reason': '中等批量数据，标准处理最优',
                'expected_memory': f'~{batch_size * 0.1:.1f}MB',
                'processing_time': '中等',
                'stability': '良好'
            }
        else:
            return {
                'recommended_method': 'match_batch_streaming',
                'reason': '大批量数据，建议流式处理',
                'expected_memory': f'~{min(1000, batch_size) * 0.1:.1f}MB (恒定)',
                'processing_time': '较慢但稳定',
                'stability': '优秀',
                'recommended_batch_size': min(1000, batch_size // 10)
            }

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