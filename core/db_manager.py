import psycopg2
from psycopg2.pool import SimpleConnectionPool
from typing import List, Optional, Dict
from decimal import Decimal
from .matching_engine import BlueLineItem, NegativeInvoice, MatchResult
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
        conn = self.pool.getconn()
        try:
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
            self.pool.putconn(conn)
    
    def save_match_results(self, 
                          results: List[MatchResult],
                          batch_id: str) -> bool:
        """
        保存匹配结果到数据库
        使用事务确保原子性
        """
        conn = self.pool.getconn()
        try:
            conn.autocommit = False
            with conn.cursor() as cur:
                for result in results:
                    if not result.success:
                        continue
                    
                    # 更新蓝票行remaining
                    for alloc in result.allocations:
                        cur.execute("""
                            UPDATE blue_lines 
                            SET remaining = remaining - %s,
                                last_update = CURRENT_TIMESTAMP
                            WHERE line_id = %s 
                              AND remaining >= %s
                        """, (
                            alloc.amount_used,
                            alloc.blue_line_id,
                            alloc.amount_used
                        ))
                        
                        if cur.rowcount == 0:
                            raise Exception(f"并发冲突: line_id={alloc.blue_line_id}")
                    
                    # 插入匹配记录
                    for alloc in result.allocations:
                        cur.execute("""
                            INSERT INTO match_records 
                            (batch_id, negative_invoice_id, blue_line_id, amount_used)
                            VALUES (%s, %s, %s, %s)
                        """, (
                            batch_id,
                            result.negative_invoice_id,
                            alloc.blue_line_id,
                            alloc.amount_used
                        ))
                
                conn.commit()
                return True
                
        except Exception as e:
            conn.rollback()
            logger.error(f"保存匹配结果失败: {e}")
            return False
        finally:
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
        self.cache = {}  # 简单缓存
    
    def get_candidates(self, tax_rate: int, buyer_id: int, seller_id: int):
        """获取候选蓝票行"""
        cache_key = f"{tax_rate}_{buyer_id}_{seller_id}"
        
        if cache_key not in self.cache:
            self.cache[cache_key] = self.db_manager.get_candidates(
                tax_rate, buyer_id, seller_id
            )
        
        return self.cache[cache_key]