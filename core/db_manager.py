import psycopg2
from psycopg2.pool import SimpleConnectionPool
from typing import List, Optional, Dict
from decimal import Decimal
from .matching_engine import BlueLineItem, NegativeInvoice, MatchResult
from .performance_monitor import get_performance_timer
import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    """数据库管理器，负责所有数据库操作"""
    
    def __init__(self, db_config: dict, pool_size: int = 10):
        """初始化数据库连接池"""
        self.pool = SimpleConnectionPool(
            1, pool_size,
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
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
                self.pool.putconn(conn)

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
                self.pool.putconn(conn)
    
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

class CandidateProvider:
    """候选提供器，供匹配引擎使用"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def get_candidates(self, tax_rate: int, buyer_id: int, seller_id: int):
        """获取候选蓝票行"""
        return self.db_manager.get_candidates(tax_rate, buyer_id, seller_id)