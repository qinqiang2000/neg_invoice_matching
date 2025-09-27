"""
可解释性报告生成器

生成人类可读的匹配失败报告，帮助财务人员理解匹配结果
和采取相应的行动。
"""

from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from decimal import Decimal
import json
from datetime import datetime
from collections import defaultdict, Counter
import logging

from .matching_engine import MatchResult, NegativeInvoice, MatchFailureDetail, FailureReasons
from .diagnostics import DiagnosisResult, MatchDiagnostics

logger = logging.getLogger(__name__)


@dataclass
class BatchAnalysisReport:
    """批量分析报告"""
    total_processed: int                        # 总处理数量
    success_count: int                          # 成功数量
    failure_count: int                          # 失败数量
    success_rate: float                         # 成功率
    failure_patterns: Dict[str, int]           # 失败模式统计
    top_failure_reasons: List[Tuple[str, int]] # 主要失败原因
    business_impact_summary: Dict              # 业务影响汇总
    recommendations: List[str]                 # 改进建议
    generated_at: str                          # 生成时间


class ExplainabilityReporter:
    """可解释性报告生成器"""

    def __init__(self, db_manager=None):
        """
        初始化报告生成器

        Args:
            db_manager: 数据库管理器（可选，用于深度分析）
        """
        self.db_manager = db_manager
        self.diagnostics = MatchDiagnostics(db_manager) if db_manager else None

    def generate_failure_report(self, match_result: MatchResult,
                               negative: Optional[NegativeInvoice] = None) -> str:
        """
        生成单个失败的详细报告

        Args:
            match_result: 匹配结果
            negative: 负数发票信息（可选，用于更详细的分析）

        Returns:
            str: 人类可读的失败报告
        """
        if match_result.success:
            return self._generate_success_report(match_result)

        report_lines = []
        report_lines.append("=" * 50)
        report_lines.append(f"负数发票 #{match_result.negative_invoice_id} 匹配失败分析")
        report_lines.append("=" * 50)

        # 基础失败信息
        failure_detail = match_result.failure_detail
        if failure_detail:
            report_lines.append(f"失败原因: {failure_detail.reason_description}")
            report_lines.append(f"错误代码: {failure_detail.reason_code}")
            report_lines.append("")

            # 详细诊断数据
            if failure_detail.diagnostic_data:
                report_lines.append("详细分析:")
                report_lines.extend(self._format_diagnostic_data(failure_detail.diagnostic_data))
                report_lines.append("")

            # 建议操作
            if failure_detail.suggested_actions:
                report_lines.append("建议操作:")
                for i, action in enumerate(failure_detail.suggested_actions, 1):
                    report_lines.append(f"  {i}. {action}")
                report_lines.append("")

        # 匹配尝试过程
        if match_result.match_attempts:
            report_lines.append("匹配过程:")
            for attempt in match_result.match_attempts:
                status = "✓" if attempt.success else "✗"
                report_lines.append(f"  {status} {attempt.step}: {attempt.reason}")
            report_lines.append("")

        # 深度诊断（如果可用）
        if negative and self.diagnostics:
            try:
                diagnosis = self.diagnostics.diagnose_no_match(negative)
                report_lines.extend(self._format_diagnosis_result(diagnosis))
            except Exception as e:
                logger.warning(f"深度诊断失败: {e}")

        # 总结
        report_lines.append("处理建议:")
        if failure_detail and failure_detail.reason_code == FailureReasons.NO_CANDIDATES:
            report_lines.append("  • 这是一个数据可用性问题，建议检查基础数据")
        elif failure_detail and failure_detail.reason_code == FailureReasons.INSUFFICIENT_TOTAL_AMOUNT:
            report_lines.append("  • 这是一个资金不足问题，建议等待或拆分")
        else:
            report_lines.append("  • 建议联系技术支持进行详细分析")

        report_lines.append("")
        report_lines.append(f"报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("=" * 50)

        return "\n".join(report_lines)

    def _generate_success_report(self, match_result: MatchResult) -> str:
        """生成成功匹配的报告"""
        report_lines = []
        report_lines.append(f"✅ 负数发票 #{match_result.negative_invoice_id} 匹配成功")
        report_lines.append(f"匹配金额: ¥{match_result.total_matched}")
        report_lines.append(f"使用蓝票: {len(match_result.allocations)} 张")
        report_lines.append(f"产生碎片: {match_result.fragments_created} 个")

        if match_result.allocations:
            report_lines.append("\n分配详情:")
            for alloc in match_result.allocations:
                report_lines.append(f"  蓝票 #{alloc.blue_line_id}: 使用 ¥{alloc.amount_used}")

        return "\n".join(report_lines)

    def _format_diagnostic_data(self, diagnostic_data: Dict) -> List[str]:
        """格式化诊断数据"""
        lines = []

        # 需求信息
        if "needed_amount" in diagnostic_data:
            lines.append(f"• 需求金额: ¥{diagnostic_data['needed_amount']:.2f}")

        # 候选集信息
        if "total_available" in diagnostic_data:
            lines.append(f"• 候选集总额: ¥{diagnostic_data['total_available']:.2f}")

        if "shortage" in diagnostic_data and diagnostic_data["shortage"] > 0:
            shortage = diagnostic_data["shortage"]
            shortage_pct = diagnostic_data.get("shortage_percentage", 0)
            lines.append(f"• 缺口: ¥{shortage:.2f} ({shortage_pct:.1f}%)")

        # 候选集规模
        if "candidate_count" in diagnostic_data:
            count = diagnostic_data["candidate_count"]
            lines.append(f"• 候选蓝票: {count} 张")

            if count > 0:
                if "largest_single_amount" in diagnostic_data:
                    lines.append(f"• 最大单笔: ¥{diagnostic_data['largest_single_amount']:.2f}")

                if "fragmentation_score" in diagnostic_data:
                    frag_score = diagnostic_data["fragmentation_score"]
                    lines.append(f"• 碎片化程度: {frag_score:.1%}")

        # 搜索条件
        if "search_conditions" in diagnostic_data:
            conditions = diagnostic_data["search_conditions"]
            lines.append("• 匹配条件:")
            lines.append(f"  - 税率: {conditions.get('tax_rate', 'N/A')}%")
            lines.append(f"  - 买方ID: {conditions.get('buyer_id', 'N/A')}")
            lines.append(f"  - 卖方ID: {conditions.get('seller_id', 'N/A')}")

        return lines

    def _format_diagnosis_result(self, diagnosis: DiagnosisResult) -> List[str]:
        """格式化诊断结果"""
        lines = []
        lines.append("深度诊断结果:")
        lines.append(f"• 主要问题: {diagnosis.primary_issue}")

        if diagnosis.secondary_issues:
            lines.append("• 次要问题:")
            for issue in diagnosis.secondary_issues:
                lines.append(f"  - {issue}")

        if diagnosis.alternative_solutions:
            lines.append("• 替代解决方案:")
            for solution in diagnosis.alternative_solutions:
                lines.append(f"  - {solution}")

        lines.append(f"• 诊断置信度: {diagnosis.confidence_score:.1%}")
        lines.append("")

        return lines

    def generate_batch_analysis(self, results: List[MatchResult],
                              negatives: Optional[List[NegativeInvoice]] = None) -> BatchAnalysisReport:
        """
        批量分析失败模式

        Args:
            results: 匹配结果列表
            negatives: 负数发票列表（可选）

        Returns:
            BatchAnalysisReport: 批量分析报告
        """
        total_processed = len(results)
        success_count = sum(1 for r in results if r.success)
        failure_count = total_processed - success_count
        success_rate = success_count / total_processed if total_processed > 0 else 0

        # 统计失败模式
        failure_patterns = defaultdict(int)
        failure_reasons = []

        for result in results:
            if not result.success:
                reason = result.failure_reason or "unknown"
                failure_patterns[reason] += 1
                failure_reasons.append(reason)

        # 获取主要失败原因
        reason_counter = Counter(failure_reasons)
        top_failure_reasons = reason_counter.most_common(5)

        # 业务影响分析
        business_impact = self._analyze_business_impact(results, negatives)

        # 生成改进建议
        recommendations = self._generate_improvement_suggestions(failure_patterns, business_impact)

        return BatchAnalysisReport(
            total_processed=total_processed,
            success_count=success_count,
            failure_count=failure_count,
            success_rate=success_rate,
            failure_patterns=dict(failure_patterns),
            top_failure_reasons=top_failure_reasons,
            business_impact_summary=business_impact,
            recommendations=recommendations,
            generated_at=datetime.now().isoformat()
        )

    def _analyze_business_impact(self, results: List[MatchResult],
                               negatives: Optional[List[NegativeInvoice]]) -> Dict:
        """分析业务影响"""
        impact = {
            "total_failed_amount": 0.0,
            "high_value_failures": 0,
            "failure_by_amount_range": defaultdict(int),
            "avg_failure_amount": 0.0
        }

        if not negatives:
            return impact

        # 创建ID到负数发票的映射
        negative_map = {n.invoice_id: n for n in negatives}

        failed_amounts = []
        for result in results:
            if not result.success and result.negative_invoice_id in negative_map:
                negative = negative_map[result.negative_invoice_id]
                amount = float(negative.amount)
                failed_amounts.append(amount)

                impact["total_failed_amount"] += amount

                # 高价值失败统计（>10000元）
                if amount > 10000:
                    impact["high_value_failures"] += 1

                # 按金额范围统计
                if amount < 100:
                    impact["failure_by_amount_range"]["small"] += 1
                elif amount < 1000:
                    impact["failure_by_amount_range"]["medium"] += 1
                else:
                    impact["failure_by_amount_range"]["large"] += 1

        if failed_amounts:
            impact["avg_failure_amount"] = sum(failed_amounts) / len(failed_amounts)

        return impact

    def _generate_improvement_suggestions(self, failure_patterns: Dict[str, int],
                                        business_impact: Dict) -> List[str]:
        """基于失败统计生成改进建议"""
        suggestions = []

        total_failures = sum(failure_patterns.values())
        if total_failures == 0:
            return ["系统运行良好，无需特别优化"]

        # 基于失败原因的建议
        for reason, count in failure_patterns.items():
            percentage = count / total_failures * 100

            if reason == FailureReasons.NO_CANDIDATES and percentage > 30:
                suggestions.append(f"无候选问题占{percentage:.1f}%，建议检查数据入库流程和匹配条件设置")

            elif reason == FailureReasons.INSUFFICIENT_TOTAL_AMOUNT and percentage > 40:
                suggestions.append(f"资金不足问题占{percentage:.1f}%，建议优化资金调配策略或实施拆分机制")

            elif reason == FailureReasons.FRAGMENTATION_ISSUE and percentage > 20:
                suggestions.append(f"碎片化问题占{percentage:.1f}%，建议定期执行数据整理和碎片清理")

        # 基于业务影响的建议
        high_value_failures = business_impact.get("high_value_failures", 0)
        if high_value_failures > 0:
            suggestions.append(f"发现{high_value_failures}笔高价值失败，建议建立高价值订单的专项处理流程")

        total_failed_amount = business_impact.get("total_failed_amount", 0)
        if total_failed_amount > 100000:
            suggestions.append(f"失败总金额达¥{total_failed_amount:,.2f}，建议提高匹配算法优先级")

        # 通用建议
        if not suggestions:
            suggestions.append("建议持续监控匹配模式，定期优化算法参数")

        return suggestions

    def generate_detailed_batch_report(self, batch_analysis: BatchAnalysisReport) -> str:
        """生成详细的批量分析报告"""
        lines = []
        lines.append("=" * 60)
        lines.append("负数发票匹配系统 - 批量处理分析报告")
        lines.append("=" * 60)
        lines.append("")

        # 概览
        lines.append("📊 处理概览")
        lines.append("-" * 30)
        lines.append(f"总处理量: {batch_analysis.total_processed:,} 笔")
        lines.append(f"成功匹配: {batch_analysis.success_count:,} 笔")
        lines.append(f"匹配失败: {batch_analysis.failure_count:,} 笔")
        lines.append(f"成功率: {batch_analysis.success_rate:.1%}")
        lines.append("")

        # 失败分析
        if batch_analysis.failure_count > 0:
            lines.append("❌ 失败原因分析")
            lines.append("-" * 30)

            total_failures = batch_analysis.failure_count
            for reason, count in batch_analysis.failure_patterns.items():
                percentage = count / total_failures * 100
                reason_desc = self._get_reason_description(reason)
                lines.append(f"• {reason_desc}: {count} 笔 ({percentage:.1f}%)")
            lines.append("")

            # 主要失败原因
            lines.append("🎯 主要失败原因")
            lines.append("-" * 30)
            for reason, count in batch_analysis.top_failure_reasons:
                reason_desc = self._get_reason_description(reason)
                lines.append(f"1. {reason_desc}: {count} 笔")
            lines.append("")

        # 业务影响
        impact = batch_analysis.business_impact_summary
        if impact:
            lines.append("💰 业务影响分析")
            lines.append("-" * 30)

            if "total_failed_amount" in impact:
                lines.append(f"失败总金额: ¥{impact['total_failed_amount']:,.2f}")

            if "high_value_failures" in impact and impact["high_value_failures"] > 0:
                lines.append(f"高价值失败: {impact['high_value_failures']} 笔（>¥10,000）")

            if "avg_failure_amount" in impact:
                lines.append(f"平均失败金额: ¥{impact['avg_failure_amount']:.2f}")

            # 失败分布
            if "failure_by_amount_range" in impact:
                ranges = impact["failure_by_amount_range"]
                lines.append("失败分布:")
                lines.append(f"  小额(<¥100): {ranges.get('small', 0)} 笔")
                lines.append(f"  中额(¥100-1K): {ranges.get('medium', 0)} 笔")
                lines.append(f"  大额(>¥1K): {ranges.get('large', 0)} 笔")

            lines.append("")

        # 改进建议
        if batch_analysis.recommendations:
            lines.append("💡 改进建议")
            lines.append("-" * 30)
            for i, recommendation in enumerate(batch_analysis.recommendations, 1):
                lines.append(f"{i}. {recommendation}")
            lines.append("")

        # 报告信息
        lines.append("📋 报告信息")
        lines.append("-" * 30)
        lines.append(f"生成时间: {batch_analysis.generated_at}")
        lines.append("报告版本: v1.0")
        lines.append("")
        lines.append("=" * 60)

        return "\n".join(lines)

    def _get_reason_description(self, reason_code: str) -> str:
        """获取失败原因的中文描述"""
        descriptions = {
            FailureReasons.NO_CANDIDATES: "无可用候选蓝票",
            FailureReasons.INSUFFICIENT_TOTAL_AMOUNT: "候选集总额不足",
            FailureReasons.FRAGMENTATION_ISSUE: "候选集过度碎片化",
            FailureReasons.NO_MATCHING_TAX_RATE: "税率不匹配",
            FailureReasons.NO_MATCHING_BUYER: "买方不匹配",
            FailureReasons.NO_MATCHING_SELLER: "卖方不匹配",
            FailureReasons.GREEDY_SUBOPTIMAL: "算法策略次优",
            FailureReasons.CONCURRENT_CONFLICT: "并发冲突",
            "insufficient_funds": "资金不足（旧版）",
            "no_candidates": "无候选（旧版）"
        }
        return descriptions.get(reason_code, f"未知原因 ({reason_code})")

    def export_analysis_to_json(self, batch_analysis: BatchAnalysisReport) -> str:
        """将分析结果导出为JSON格式"""
        return json.dumps({
            "summary": {
                "total_processed": batch_analysis.total_processed,
                "success_count": batch_analysis.success_count,
                "failure_count": batch_analysis.failure_count,
                "success_rate": batch_analysis.success_rate
            },
            "failure_analysis": {
                "patterns": batch_analysis.failure_patterns,
                "top_reasons": [{"reason": r[0], "count": r[1]} for r in batch_analysis.top_failure_reasons]
            },
            "business_impact": batch_analysis.business_impact_summary,
            "recommendations": batch_analysis.recommendations,
            "metadata": {
                "generated_at": batch_analysis.generated_at,
                "report_version": "1.0"
            }
        }, indent=2, ensure_ascii=False)

    def generate_failure_summary_for_user(self, results: List[MatchResult]) -> str:
        """为用户生成简洁的失败摘要"""
        failure_results = [r for r in results if not r.success]
        if not failure_results:
            return "✅ 所有负数发票均匹配成功！"

        lines = []
        lines.append(f"⚠️ {len(failure_results)} 笔负数发票匹配失败：")
        lines.append("")

        # 按失败原因分组
        reason_groups = defaultdict(list)
        for result in failure_results:
            reason = result.failure_reason or "unknown"
            reason_groups[reason].append(result.negative_invoice_id)

        for reason, invoice_ids in reason_groups.items():
            reason_desc = self._get_reason_description(reason)
            lines.append(f"• {reason_desc} ({len(invoice_ids)} 笔):")

            # 显示前5个ID，如果更多则显示省略号
            if len(invoice_ids) <= 5:
                id_list = ", ".join(f"#{id}" for id in invoice_ids)
            else:
                id_list = ", ".join(f"#{id}" for id in invoice_ids[:5]) + f", ...等{len(invoice_ids)}笔"

            lines.append(f"  {id_list}")
            lines.append("")

        lines.append("💡 建议查看详细报告了解具体原因和解决方案")

        return "\n".join(lines)