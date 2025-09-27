"""
匹配诊断工具 - 深度分析匹配失败原因

提供详细的诊断功能，帮助理解为什么特定的负数发票无法匹配，
以及如何解决这些问题。
"""

from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from decimal import Decimal
import logging
from datetime import datetime

from .matching_engine import (
    NegativeInvoice, BlueLineItem, MatchResult, MatchFailureDetail,
    FailureReasons, GreedyMatchingEngine
)

logger = logging.getLogger(__name__)


@dataclass
class DiagnosisResult:
    """诊断结果"""
    negative_invoice_id: int
    primary_issue: str                          # 主要问题
    secondary_issues: List[str]                 # 次要问题
    root_cause_analysis: Dict                   # 根因分析
    alternative_solutions: List[str]            # 替代解决方案
    manual_actions: List[str]                   # 建议人工操作
    confidence_score: float                     # 诊断置信度 (0-1)


@dataclass
class AlternativeMatch:
    """替代匹配方案"""
    description: str                            # 方案描述
    relaxed_conditions: Dict                    # 放宽的条件
    potential_candidates: List[BlueLineItem]    # 潜在候选
    success_probability: float                  # 成功概率
    trade_offs: List[str]                      # 权衡说明


@dataclass
class ManualAction:
    """人工干预建议"""
    action_type: str                           # 操作类型
    description: str                           # 操作描述
    priority: str                              # 优先级 (high/medium/low)
    estimated_time: str                        # 预估时间
    required_permissions: List[str]            # 需要的权限


class MatchDiagnostics:
    """匹配诊断工具"""

    def __init__(self, db_manager, fragment_threshold: Decimal = Decimal('5.0')):
        """
        初始化诊断工具

        Args:
            db_manager: 数据库管理器
            fragment_threshold: 碎片阈值
        """
        self.db_manager = db_manager
        self.fragment_threshold = fragment_threshold
        self.engine = GreedyMatchingEngine(fragment_threshold)

    def diagnose_no_match(self, negative: NegativeInvoice) -> DiagnosisResult:
        """
        深度诊断为什么匹配失败

        Args:
            negative: 负数发票

        Returns:
            DiagnosisResult: 诊断结果
        """
        logger.info(f"开始诊断负数发票 {negative.invoice_id} 的匹配失败原因")

        # 1. 基础条件检查
        basic_issues = self._check_basic_conditions(negative)

        # 2. 查找候选集
        candidates = self._get_candidates_for_diagnosis(negative)

        # 3. 分析资金可用性
        fund_analysis = self._analyze_fund_availability(negative, candidates)

        # 4. 检查碎片化程度
        fragmentation_analysis = self._analyze_fragmentation(candidates)

        # 5. 模拟不同策略
        strategy_analysis = self._test_alternative_strategies(negative, candidates)

        # 6. 综合分析确定主要问题
        primary_issue = self._determine_primary_issue(
            basic_issues, fund_analysis, fragmentation_analysis, strategy_analysis
        )

        # 7. 生成根因分析
        root_cause = self._generate_root_cause_analysis(
            negative, candidates, fund_analysis, fragmentation_analysis, strategy_analysis
        )

        # 8. 生成解决方案
        alternative_solutions = self._generate_alternative_solutions(negative, candidates, primary_issue)
        manual_actions = self._generate_manual_actions(negative, primary_issue, root_cause)

        # 9. 计算置信度
        confidence = self._calculate_confidence(basic_issues, fund_analysis, len(candidates))

        return DiagnosisResult(
            negative_invoice_id=negative.invoice_id,
            primary_issue=primary_issue,
            secondary_issues=basic_issues,
            root_cause_analysis=root_cause,
            alternative_solutions=alternative_solutions,
            manual_actions=manual_actions,
            confidence_score=confidence
        )

    def _check_basic_conditions(self, negative: NegativeInvoice) -> List[str]:
        """检查基础匹配条件"""
        issues = []

        # 检查金额是否过小
        if negative.amount < Decimal('0.01'):
            issues.append("负数发票金额过小（小于0.01元）")

        # 检查金额是否过大
        if negative.amount > Decimal('1000000'):
            issues.append("负数发票金额过大（超过100万元），可能难以找到足够候选")

        # 检查税率是否常见
        common_tax_rates = [0, 3, 6, 9, 13, 17]  # 常见税率
        if negative.tax_rate not in common_tax_rates:
            issues.append(f"非常见税率 {negative.tax_rate}%，可能限制候选集")

        return issues

    def _get_candidates_for_diagnosis(self, negative: NegativeInvoice) -> List[BlueLineItem]:
        """获取用于诊断的候选集（包含更详细信息）"""
        conn = self.db_manager.pool.getconn()
        try:
            with conn.cursor() as cur:
                # 查找符合条件的候选
                cur.execute("""
                    SELECT line_id, remaining, tax_rate, buyer_id, seller_id
                    FROM blue_lines
                    WHERE tax_rate = %s AND buyer_id = %s AND seller_id = %s
                      AND remaining > 0
                    ORDER BY remaining ASC
                    LIMIT 1000
                """, (negative.tax_rate, negative.buyer_id, negative.seller_id))

                rows = cur.fetchall()
                return [
                    BlueLineItem(
                        line_id=row[0],
                        remaining=Decimal(str(row[1])),
                        tax_rate=row[2],
                        buyer_id=row[3],
                        seller_id=row[4]
                    )
                    for row in rows
                ]
        finally:
            self.db_manager.pool.putconn(conn)

    def _analyze_fund_availability(self, negative: NegativeInvoice,
                                 candidates: List[BlueLineItem]) -> Dict:
        """分析资金可用性"""
        if not candidates:
            return {
                "total_available": 0,
                "shortage": float(negative.amount),
                "shortage_percentage": 100.0,
                "fund_adequacy": "no_funds"
            }

        total_available = sum(c.remaining for c in candidates)
        shortage = float(negative.amount - total_available)
        shortage_pct = shortage / float(negative.amount) * 100 if shortage > 0 else 0

        # 评估资金充足性
        if shortage <= 0:
            adequacy = "sufficient"
        elif shortage_pct < 10:
            adequacy = "barely_sufficient"
        elif shortage_pct < 50:
            adequacy = "moderate_shortage"
        else:
            adequacy = "severe_shortage"

        return {
            "total_available": float(total_available),
            "needed": float(negative.amount),
            "shortage": shortage,
            "shortage_percentage": shortage_pct,
            "fund_adequacy": adequacy,
            "candidate_count": len(candidates),
            "largest_single": float(max(c.remaining for c in candidates)),
            "smallest_single": float(min(c.remaining for c in candidates)),
            "average_size": float(total_available / len(candidates))
        }

    def _analyze_fragmentation(self, candidates: List[BlueLineItem]) -> Dict:
        """分析候选集碎片化程度"""
        if not candidates:
            return {"fragmentation_score": 0, "analysis": "no_candidates"}

        total_candidates = len(candidates)
        fragment_count = len([c for c in candidates if c.remaining < self.fragment_threshold])
        fragmentation_score = fragment_count / total_candidates

        # 计算金额分布
        amounts = [float(c.remaining) for c in candidates]
        amounts.sort()

        # 统计不同金额范围的分布
        ranges = {
            "micro": len([a for a in amounts if a < 1]),           # <1元
            "small": len([a for a in amounts if 1 <= a < 10]),    # 1-10元
            "medium": len([a for a in amounts if 10 <= a < 100]), # 10-100元
            "large": len([a for a in amounts if a >= 100])        # >=100元
        }

        # 评估碎片化严重程度
        if fragmentation_score < 0.2:
            severity = "low"
        elif fragmentation_score < 0.5:
            severity = "moderate"
        elif fragmentation_score < 0.8:
            severity = "high"
        else:
            severity = "severe"

        return {
            "fragmentation_score": fragmentation_score,
            "fragment_count": fragment_count,
            "total_candidates": total_candidates,
            "severity": severity,
            "amount_distribution": ranges,
            "median_amount": amounts[len(amounts)//2] if amounts else 0,
            "analysis": f"{severity}_fragmentation"
        }

    def _test_alternative_strategies(self, negative: NegativeInvoice,
                                   candidates: List[BlueLineItem]) -> Dict:
        """测试不同匹配策略的结果"""
        if not candidates:
            return {"no_candidates": True}

        strategies = {}

        # 测试金额升序（当前策略）
        asc_candidates = sorted(candidates, key=lambda x: x.remaining)
        asc_result = self.engine.match_single(negative, asc_candidates)
        strategies["amount_asc"] = {
            "success": asc_result.success,
            "matched": float(asc_result.total_matched),
            "fragments": asc_result.fragments_created
        }

        # 测试金额降序
        desc_candidates = sorted(candidates, key=lambda x: x.remaining, reverse=True)
        desc_result = self.engine.match_single(negative, desc_candidates)
        strategies["amount_desc"] = {
            "success": desc_result.success,
            "matched": float(desc_result.total_matched),
            "fragments": desc_result.fragments_created
        }

        # 分析最优策略
        best_strategy = "amount_asc"
        best_score = asc_result.total_matched

        if desc_result.total_matched > best_score:
            best_strategy = "amount_desc"
            best_score = desc_result.total_matched

        return {
            "strategies": strategies,
            "best_strategy": best_strategy,
            "best_matched": float(best_score),
            "strategy_helps": desc_result.success != asc_result.success
        }

    def _determine_primary_issue(self, basic_issues: List[str], fund_analysis: Dict,
                               fragmentation_analysis: Dict, strategy_analysis: Dict) -> str:
        """确定主要问题"""

        # 如果没有候选集，这是最严重的问题
        if fund_analysis.get("candidate_count", 0) == 0:
            return FailureReasons.NO_CANDIDATES

        # 如果资金严重不足
        if fund_analysis.get("fund_adequacy") in ["severe_shortage", "moderate_shortage"]:
            return FailureReasons.INSUFFICIENT_TOTAL_AMOUNT

        # 如果碎片化严重且策略无法解决
        if (fragmentation_analysis.get("severity") in ["high", "severe"] and
            not strategy_analysis.get("strategy_helps", False)):
            return FailureReasons.FRAGMENTATION_ISSUE

        # 如果不同策略有显著差异，可能是算法次优
        if strategy_analysis.get("strategy_helps", False):
            return FailureReasons.GREEDY_SUBOPTIMAL

        # 默认为资金不足
        return FailureReasons.INSUFFICIENT_TOTAL_AMOUNT

    def _generate_root_cause_analysis(self, negative: NegativeInvoice, candidates: List[BlueLineItem],
                                     fund_analysis: Dict, fragmentation_analysis: Dict,
                                     strategy_analysis: Dict) -> Dict:
        """生成根因分析"""
        return {
            "timestamp": datetime.now().isoformat(),
            "negative_invoice": {
                "id": negative.invoice_id,
                "amount": float(negative.amount),
                "tax_rate": negative.tax_rate,
                "buyer_id": negative.buyer_id,
                "seller_id": negative.seller_id
            },
            "market_conditions": {
                "candidate_availability": fund_analysis.get("candidate_count", 0),
                "total_market_capacity": fund_analysis.get("total_available", 0),
                "market_fragmentation": fragmentation_analysis.get("fragmentation_score", 0)
            },
            "algorithm_performance": {
                "current_strategy_success": strategy_analysis.get("strategies", {}).get("amount_asc", {}).get("success", False),
                "alternative_strategy_success": strategy_analysis.get("strategies", {}).get("amount_desc", {}).get("success", False),
                "strategy_impact": strategy_analysis.get("strategy_helps", False)
            },
            "business_impact": self._assess_business_impact(negative, fund_analysis)
        }

    def _assess_business_impact(self, negative: NegativeInvoice, fund_analysis: Dict) -> Dict:
        """评估业务影响"""
        amount = float(negative.amount)

        # 影响级别评估
        if amount < 100:
            impact_level = "low"
        elif amount < 10000:
            impact_level = "medium"
        else:
            impact_level = "high"

        # 紧急程度评估
        shortage_pct = fund_analysis.get("shortage_percentage", 0)
        if shortage_pct < 10:
            urgency = "low"
        elif shortage_pct < 50:
            urgency = "medium"
        else:
            urgency = "high"

        return {
            "impact_level": impact_level,
            "urgency": urgency,
            "amount_category": "large" if amount > 1000 else "small",
            "processing_priority": "high" if impact_level == "high" and urgency == "high" else "normal"
        }

    def _generate_alternative_solutions(self, negative: NegativeInvoice,
                                      candidates: List[BlueLineItem], primary_issue: str) -> List[str]:
        """生成替代解决方案"""
        solutions = []

        if primary_issue == FailureReasons.NO_CANDIDATES:
            solutions.extend([
                "等待相同条件的新蓝票入库",
                "检查是否可以放宽买方或卖方条件",
                "考虑使用相近税率的蓝票（需业务确认）"
            ])

        elif primary_issue == FailureReasons.INSUFFICIENT_TOTAL_AMOUNT:
            amount = float(negative.amount)
            if amount > 1000:
                solutions.append("拆分为多张小额负数发票分批处理")
            solutions.extend([
                "等待更多蓝票入库后重试",
                "检查是否有临时的金额调整方案"
            ])

        elif primary_issue == FailureReasons.FRAGMENTATION_ISSUE:
            solutions.extend([
                "执行数据碎片整理，合并小额蓝票",
                "调整匹配策略，优先使用大额蓝票",
                "定期清理过小的碎片蓝票"
            ])

        elif primary_issue == FailureReasons.GREEDY_SUBOPTIMAL:
            solutions.extend([
                "尝试使用金额降序匹配策略",
                "考虑实现更智能的组合优化算法",
                "人工辅助选择最优蓝票组合"
            ])

        return solutions

    def _generate_manual_actions(self, negative: NegativeInvoice, primary_issue: str,
                               root_cause: Dict) -> List[str]:
        """生成人工干预建议"""
        actions = []

        business_impact = root_cause.get("business_impact", {})
        priority = business_impact.get("processing_priority", "normal")

        if priority == "high":
            actions.append("高优先级：立即人工审核和处理")

        if primary_issue == FailureReasons.NO_CANDIDATES:
            actions.extend([
                "确认负数发票的税率、买卖方信息是否正确",
                "检查相关蓝票是否已正确入库",
                "联系业务人员确认是否可以调整匹配条件"
            ])

        elif primary_issue == FailureReasons.INSUFFICIENT_TOTAL_AMOUNT:
            actions.extend([
                "评估是否可以拆分负数发票",
                "确认是否有预期的蓝票即将入库",
                "考虑临时的资金调拨方案"
            ])

        actions.append("记录处理结果，用于优化未来的匹配策略")

        return actions

    def _calculate_confidence(self, basic_issues: List[str], fund_analysis: Dict,
                            candidate_count: int) -> float:
        """计算诊断置信度"""
        base_confidence = 0.8

        # 如果有足够的候选数据，置信度更高
        if candidate_count > 10:
            base_confidence += 0.1
        elif candidate_count == 0:
            base_confidence += 0.1  # 无候选的情况很明确

        # 如果有明显的基础问题，置信度降低
        if len(basic_issues) > 2:
            base_confidence -= 0.2

        # 资金分析的明确性
        adequacy = fund_analysis.get("fund_adequacy", "unknown")
        if adequacy in ["sufficient", "severe_shortage"]:
            base_confidence += 0.1

        return min(1.0, max(0.3, base_confidence))

    def find_alternative_matches(self, negative: NegativeInvoice) -> List[AlternativeMatch]:
        """寻找替代匹配方案（放宽条件）"""
        alternatives = []

        # 方案1：放宽税率条件（±1%）
        for tax_delta in [-1, 1]:
            relaxed_tax_rate = negative.tax_rate + tax_delta
            if 0 <= relaxed_tax_rate <= 20:  # 合理的税率范围
                candidates = self._get_candidates_with_conditions(
                    tax_rate=relaxed_tax_rate,
                    buyer_id=negative.buyer_id,
                    seller_id=negative.seller_id
                )
                if candidates:
                    total_available = sum(c.remaining for c in candidates)
                    success_prob = min(1.0, float(total_available / negative.amount))

                    alternatives.append(AlternativeMatch(
                        description=f"使用税率{relaxed_tax_rate}%的蓝票（当前要求{negative.tax_rate}%）",
                        relaxed_conditions={"tax_rate": relaxed_tax_rate},
                        potential_candidates=candidates[:10],  # 只返回前10个作为示例
                        success_probability=success_prob,
                        trade_offs=["需要财务确认税率差异的合规性"]
                    ))

        # 方案2：查找相同买方但不同卖方的蓝票
        other_seller_candidates = self._get_candidates_with_conditions(
            tax_rate=negative.tax_rate,
            buyer_id=negative.buyer_id,
            seller_id=None  # 不限制卖方
        )

        # 过滤掉原卖方的蓝票
        other_seller_candidates = [c for c in other_seller_candidates if c.seller_id != negative.seller_id]

        if other_seller_candidates:
            total_available = sum(c.remaining for c in other_seller_candidates)
            success_prob = min(1.0, float(total_available / negative.amount))

            alternatives.append(AlternativeMatch(
                description="使用相同买方但不同卖方的蓝票",
                relaxed_conditions={"seller_flexibility": True},
                potential_candidates=other_seller_candidates[:10],
                success_probability=success_prob,
                trade_offs=["需要确认业务上是否允许跨卖方匹配"]
            ))

        return alternatives

    def _get_candidates_with_conditions(self, tax_rate: Optional[int] = None,
                                      buyer_id: Optional[int] = None,
                                      seller_id: Optional[int] = None) -> List[BlueLineItem]:
        """根据指定条件获取候选集"""
        conn = self.db_manager.pool.getconn()
        try:
            with conn.cursor() as cur:
                where_conditions = ["remaining > 0"]
                params = []

                if tax_rate is not None:
                    where_conditions.append("tax_rate = %s")
                    params.append(tax_rate)

                if buyer_id is not None:
                    where_conditions.append("buyer_id = %s")
                    params.append(buyer_id)

                if seller_id is not None:
                    where_conditions.append("seller_id = %s")
                    params.append(seller_id)

                query = f"""
                    SELECT line_id, remaining, tax_rate, buyer_id, seller_id
                    FROM blue_lines
                    WHERE {' AND '.join(where_conditions)}
                    ORDER BY remaining ASC
                    LIMIT 100
                """

                cur.execute(query, params)
                rows = cur.fetchall()

                return [
                    BlueLineItem(
                        line_id=row[0],
                        remaining=Decimal(str(row[1])),
                        tax_rate=row[2],
                        buyer_id=row[3],
                        seller_id=row[4]
                    )
                    for row in rows
                ]
        finally:
            self.db_manager.pool.putconn(conn)