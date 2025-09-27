import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool
from typing import List, Optional, Dict
from decimal import Decimal
from .matching_engine import BlueLineItem, NegativeInvoice, MatchResult
from .performance_monitor import get_performance_timer
from config.config import DYNAMIC_LIMIT_BASE, DYNAMIC_LIMIT_MAX
import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    """数据库管理器，负责所有数据库操作"""
    
    def __init__(self, db_config: dict, pool_size: int = 20):
        """初始化数据库连接池"""
        self.pool = SimpleConnectionPool(
            2, pool_size,  # 最小连接数从1增加到2
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password'],
            # 连接池优化配置
            connect_timeout=5,  # 连接超时5秒（降低延迟）
            keepalives_idle=300,  # 空闲300秒后发送keepalive（减少）
            keepalives_interval=10,  # keepalive间隔10秒（减少）
            keepalives_count=3,  # 最多3次keepalive失败后断开
            # 减少网络往返开销
            application_name='neg_invoice_matching'
        )
    
    def get_candidates(self,
                      tax_rate: int,
                      buyer_id: int,
                      seller_id: int,
                      limit: int = 10000) -> List[BlueLineItem]:
        """
        获取候选蓝票行

        Returns:
            List[BlueLineItem]: 按remaining升序排列的候选列表
        """
        timer = get_performance_timer()

        with timer.measure("database_connection_acquire"):
            conn = self.pool.getconn()

        try:
            with timer.measure("database_query_execution", {
                'query_type': 'get_candidates',
                'tax_rate': tax_rate,
                'buyer_id': buyer_id,
                'seller_id': seller_id,
                'limit': limit
            }):
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT line_id, remaining, tax_rate, buyer_id, seller_id
                        FROM blue_lines
                        WHERE tax_rate = %s
                          AND buyer_id = %s
                          AND seller_id = %s
                          AND remaining > 0
                        ORDER BY remaining ASC
                        LIMIT %s
                    """, (tax_rate, buyer_id, seller_id, limit))

                    rows = cur.fetchall()

            with timer.measure("data_conversion", {
                'rows_count': len(rows)
            }):
                result = [
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
            with timer.measure("database_connection_release"):
                try:
                    # 对于只读查询，不需要rollback，直接释放连接
                    if conn and not conn.closed:
                        self.pool.putconn(conn)
                except Exception as e:
                    logger.error(f"连接释放错误: {e}")
                    # 如果连接有问题，强制关闭而不是放回池中
                    try:
                        if conn and not conn.closed:
                            conn.close()
                    except:
                        pass

        return result

    def get_candidates_batch(self,
                           conditions: List[tuple],
                           limit: int = 10000,
                           group_counts: Dict[tuple, int] = None) -> Dict[tuple, List[BlueLineItem]]:
        """
        批量获取候选蓝票行，减少数据库往返次数

        Args:
            conditions: [(tax_rate, buyer_id, seller_id), ...] 条件列表
            limit: 默认每个条件的限制数量
            group_counts: {条件: 该组负数发票数量} 用于动态计算limit

        Returns:
            Dict[tuple, List[BlueLineItem]]: 条件到候选列表的映射
        """
        if not conditions:
            return {}

        timer = get_performance_timer()

        with timer.measure("database_connection_acquire"):
            conn = self.pool.getconn()

        try:
            with timer.measure("database_query_execution", {
                'query_type': 'get_candidates_batch',
                'conditions_count': len(conditions),
                'limit': limit
            }):
                with conn.cursor() as cur:
                    # 使用UNION ALL + 动态LIMIT优化查询性能
                    union_queries = []

                    for condition in conditions:
                        tax_rate, buyer_id, seller_id = condition

                        # 动态计算该条件的limit
                        if group_counts and condition in group_counts:
                            # 使用配置的动态limit参数
                            actual_limit = min(DYNAMIC_LIMIT_BASE * group_counts[condition], DYNAMIC_LIMIT_MAX)
                        else:
                            # 兼容模式：使用默认limit
                            actual_limit = limit

                        union_queries.append(f"""
                            (SELECT line_id, remaining, tax_rate, buyer_id, seller_id
                             FROM blue_lines
                             WHERE tax_rate = {tax_rate}
                               AND buyer_id = {buyer_id}
                               AND seller_id = {seller_id}
                               AND remaining > 0
                             ORDER BY remaining ASC
                             LIMIT {actual_limit})
                        """)

                    # 合并所有子查询
                    query = " UNION ALL ".join(union_queries)
                    cur.execute(query)
                    all_rows = cur.fetchall()

            with timer.measure("data_conversion", {
                'rows_count': len(all_rows)
            }):
                # 按条件分组结果
                result = {condition: [] for condition in conditions}

                for row in all_rows:
                    condition = (row[2], row[3], row[4])  # tax_rate, buyer_id, seller_id
                    if condition in result and len(result[condition]) < limit:
                        result[condition].append(BlueLineItem(
                            line_id=row[0],
                            remaining=Decimal(str(row[1])),
                            tax_rate=row[2],
                            buyer_id=row[3],
                            seller_id=row[4]
                        ))

        finally:
            with timer.measure("database_connection_release"):
                try:
                    # 对于只读查询，不需要rollback，直接释放连接
                    if conn and not conn.closed:
                        self.pool.putconn(conn)
                except Exception as e:
                    logger.error(f"连接释放错误: {e}")
                    try:
                        if conn and not conn.closed:
                            conn.close()
                    except:
                        pass

        return result

    def save_match_results(self,
                          results: List[MatchResult],
                          batch_id: str) -> bool:
        """
        保存匹配结果到数据库
        使用事务确保原子性，采用PostgreSQL FROM VALUES优化批量更新
        """
        timer = get_performance_timer()

        with timer.measure("save_results_preparation"):
            # 收集所有成功的分配记录
            all_allocations = []
            match_records = []

            for result in results:
                if not result.success:
                    continue

                for alloc in result.allocations:
                    all_allocations.append((alloc.blue_line_id, alloc.amount_used))
                    match_records.append((
                        batch_id,
                        result.negative_invoice_id,
                        alloc.blue_line_id,
                        alloc.amount_used
                    ))

        if not all_allocations:
            logger.info("没有成功的匹配结果需要保存")
            return True

        with timer.measure("database_connection_acquire"):
            conn = self.pool.getconn()

        try:
            conn.autocommit = False

            with timer.measure("database_transaction_update", {
                'allocations_count': len(all_allocations),
                'records_count': len(match_records)
            }):
                with conn.cursor() as cur:
                    # 使用PostgreSQL FROM VALUES语法批量更新蓝票行
                    logger.debug(f"批量更新 {len(all_allocations)} 条蓝票行")

                    # 构建参数列表用于executemany
                    update_params = []
                    for line_id, amount_used in all_allocations:
                        update_params.append((amount_used, line_id, amount_used))

                    # 使用executemany进行批量更新（保持原子性但更可靠）
                    cur.executemany("""
                        UPDATE blue_lines
                        SET remaining = remaining - %s,
                            last_update = CURRENT_TIMESTAMP
                        WHERE line_id = %s
                          AND remaining >= %s
                    """, update_params)

                    updated_count = cur.rowcount

                    # 检查是否所有行都成功更新（防止并发冲突）
                    if updated_count != len(all_allocations):
                        # 查询哪些行的余额不足
                        line_ids = [str(line_id) for line_id, _ in all_allocations]
                        cur.execute(f"""
                            SELECT line_id, remaining
                            FROM blue_lines
                            WHERE line_id IN ({','.join(line_ids)})
                        """)

                        actual_remaining = {row[0]: row[1] for row in cur.fetchall()}
                        failed_lines = []

                        for line_id, amount_used in all_allocations:
                            if line_id not in actual_remaining or actual_remaining[line_id] < amount_used:
                                failed_lines.append(line_id)

                        raise Exception(f"并发冲突: {len(failed_lines)} 条记录更新失败, "
                                      f"失败行: {failed_lines}")

                    # 批量插入匹配记录
                    if match_records:
                        logger.debug(f"批量插入 {len(match_records)} 条匹配记录")
                        cur.executemany("""
                            INSERT INTO match_records
                            (batch_id, negative_invoice_id, blue_line_id, amount_used)
                            VALUES (%s, %s, %s, %s)
                        """, match_records)

            with timer.measure("database_transaction_commit"):
                conn.commit()

            logger.info(f"成功保存匹配结果: 更新 {updated_count} 条蓝票行, "
                       f"插入 {len(match_records)} 条匹配记录")
            return True

        except Exception as e:
            with timer.measure("database_transaction_rollback"):
                conn.rollback()
            logger.error(f"保存匹配结果失败: {e}")
            return False
        finally:
            with timer.measure("database_connection_release"):
                try:
                    if conn and not conn.closed:
                        self.pool.putconn(conn)
                except Exception as e:
                    logger.error(f"连接释放错误: {e}")
                    try:
                        if conn and not conn.closed:
                            conn.close()
                    except:
                        pass
    
    def get_statistics(self) -> Dict:
        """获取统计信息"""
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                # 碎片分布
                cur.execute("""
                    SELECT 
                        CASE 
                            WHEN remaining = 0 THEN '0_depleted'
                            WHEN remaining < 50 THEN '1_fragment'
                            WHEN remaining < 100 THEN '2_small'
                            WHEN remaining < 500 THEN '3_medium'
                            ELSE '4_large'
                        END as category,
                        COUNT(*) as count,
                        SUM(remaining) as total_amount
                    FROM blue_lines
                    GROUP BY category
                    ORDER BY category
                """)
                
                distribution = {}
                for row in cur.fetchall():
                    distribution[row[0]] = {
                        'count': row[1],
                        'amount': float(row[2]) if row[2] else 0
                    }
                
                return {'distribution': distribution}
                
        finally:
            self.pool.putconn(conn)

    def close(self):
        """关闭数据库连接池"""
        if self.pool:
            self.pool.closeall()

class CandidateProvider:
    """候选提供器，供匹配引擎使用"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def get_candidates(self, tax_rate: int, buyer_id: int, seller_id: int):
        """获取候选蓝票行"""
        return self.db_manager.get_candidates(tax_rate, buyer_id, seller_id)