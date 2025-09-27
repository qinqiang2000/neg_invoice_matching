"""
负数发票匹配系统 - 测试数据生成器
优化版本：支持参数控制、模块化操作和幂等性设计

创建日期: 2025-09-27
作者: 系统

主要功能:
- 创建测试数据库表结构
- 生成大量蓝票行测试数据（支持断点续传）
- 创建数据库索引
- 生成示例负数发票
- 批次管理和数据追踪
- 提供统计分析功能

使用方式示例:
    # 完整设置（首次使用）
    python test_data_generator.py --setup-db --generate-blue-lines --total-lines 1000000 --create-indexes

    # 大数据量生成（支持断点续传）
    python test_data_generator.py --generate-blue-lines --total-lines 10000000 --batch-id prod_001

    # 查看批次状态
    python test_data_generator.py --list-batches

    # 清理特定批次
    python test_data_generator.py --clear-batch prod_001

    # 生成测试负数发票
    python test_data_generator.py --generate-negatives --scenario mixed --count 500
"""

import sys
import os
import argparse
from datetime import datetime
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2.extras import execute_values
import random
import numpy as np
import time
from tqdm import tqdm
from typing import List, Dict, Optional
from decimal import Decimal

# 导入核心模块的数据模型
from core.matching_engine import NegativeInvoice

# SQL工具函数
def load_sql_file(filename: str) -> str:
    """
    加载SQL文件内容

    Args:
        filename: SQL文件名，相对于项目根目录的sql/路径

    Returns:
        str: SQL文件内容
    """
    sql_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'sql', filename
    )

    try:
        with open(sql_path, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"SQL文件未找到: {sql_path}")
    except Exception as e:
        raise Exception(f"读取SQL文件失败 {sql_path}: {e}")

class TestDataGenerator:
    """
    测试数据生成器
    可复用于：
    1. 初始化测试数据库
    2. 生成性能测试数据
    3. 生成特定场景的测试用例
    """
    
    def __init__(self, db_config: Dict, config: Optional[Dict] = None, seed: Optional[int] = None):
        """
        初始化数据生成器

        Args:
            db_config: 数据库配置
            config: 测试配置（可选，用于覆盖默认配置）
            seed: 随机种子（可选，用于生成可重复的测试数据）
        """
        self.conn = psycopg2.connect(**db_config)
        self.cur = self.conn.cursor()

        # 设置随机种子（用于可重复测试）
        if seed is not None:
            random.seed(seed)
            np.random.seed(seed)
            self.seed = seed
            print(f"🌱 已设置随机种子: {seed} (数据将完全可重复)")
        else:
            self.seed = None

        # 使用传入的配置或默认配置
        if config:
            self.total_lines = config.get('total_lines', 10_000_000)
            self.batch_size = config.get('batch_size', 10000)
        else:
            self.total_lines = 10_000_000  # 1000万条
            self.batch_size = 10000  # 批量插入大小

        # 业务分布参数
        self.tax_rates = [13, 6, 3, 0]
        self.tax_weights = [0.6, 0.25, 0.1, 0.05]

        # 买卖方配置
        self._init_buyer_seller_config()
        
    def _init_buyer_seller_config(self):
        """
        初始化买卖方配置（优化版）
        减少组合数量，增加数据密度，提高匹配率
        """
        # 优化后的配置：更少的买卖方，更高的密度
        self.hot_buyers = list(range(1, 11))      # Top 10买方 (40%概率)
        self.hot_sellers = list(range(1, 11))     # Top 10卖方
        self.regular_buyers = list(range(11, 51))  # Top 50买方 (40%概率，原来是100)
        self.regular_sellers = list(range(11, 51)) # Top 50卖方
        self.all_buyers = list(range(1, 101))     # 所有100个买方 (20%概率，原来是1000)
        self.all_sellers = list(range(1, 101))    # 所有100个卖方

        print(f"📊 数据分布配置:")
        print(f"  热门买卖方: {len(self.hot_buyers)}x{len(self.hot_sellers)} = {len(self.hot_buyers)*len(self.hot_sellers)} 组合")
        print(f"  常规买卖方: {len(self.regular_buyers)}x{len(self.regular_sellers)} = {len(self.regular_buyers)*len(self.regular_sellers)} 组合")
        print(f"  全部买卖方: {len(self.all_buyers)}x{len(self.all_sellers)} = {len(self.all_buyers)*len(self.all_sellers)} 组合")
    
    def setup_database(self):
        """设置数据库：创建表和索引"""
        print("创建数据库表和索引...")

        # 优先使用合并的SQL文件（包含表和索引）
        try:
            combined_sql = load_sql_file('schema/create_tables_with_indexes.sql')
            self.cur.execute(combined_sql)
            self.conn.commit()
            print("✓ 数据库表和索引创建完成（使用合并文件）")
        except FileNotFoundError:
            # 回退到分别创建表和索引
            print("  使用分离文件创建表和索引...")

            # 创建表
            create_tables_sql = load_sql_file('schema/create_tables.sql')
            self.cur.execute(create_tables_sql)
            self.conn.commit()
            print("  ✓ 数据库表创建完成")

            # 自动创建索引
            try:
                self.create_indexes()
                print("  ✓ 索引自动创建完成")
            except Exception as e:
                print(f"  ⚠️ 索引创建失败，请手动执行: python tests/test_data_generator.py --create-indexes")
                print(f"     错误: {e}")
        except Exception as e:
            print(f"❌ 数据库设置失败: {e}")
            raise
    
    def generate_blue_lines(self, total_lines: Optional[int] = None,
                           batch_id: Optional[str] = None,
                           resume_from: Optional[int] = None):
        """
        生成蓝票行数据（支持断点续传和幂等性）

        Args:
            total_lines: 总行数
            batch_id: 批次ID（默认生成时间戳）
            resume_from: 从第N条开始（断点续传，自动检测）
        """
        if total_lines is None:
            total_lines = self.total_lines

        # 生成批次ID
        if batch_id is None:
            batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        print(f"开始生成{total_lines:,}条蓝票行数据（批次ID: {batch_id}）...")

        # 获取当前最大ticket_id，确保不冲突
        self.cur.execute("SELECT COALESCE(MAX(ticket_id), 0) FROM blue_lines")
        max_ticket_id = self.cur.fetchone()[0]

        # 检查批次状态
        resume_from = self._check_batch_status(batch_id, total_lines, resume_from)

        if resume_from and resume_from >= total_lines:
            print(f"批次 {batch_id} 已完成，无需继续生成")
            return batch_id
        insert_sql = """
            INSERT INTO blue_lines (
                ticket_id, tax_rate, buyer_id, seller_id,
                product_name, original_amount, remaining, batch_id
            ) VALUES %s
        """

        batch_data = []
        ticket_id = max_ticket_id + 1  # 从最大ID开始，避免冲突

        # 调整起始位置
        start_from = resume_from or 0
        actual_lines = total_lines - start_from
        
        with tqdm(total=actual_lines, initial=0) as pbar:
            for i in range(start_from, total_lines):
                # 每100行属于同一张票据
                if i % 100 == 0:
                    ticket_id += 1

                # 生成数据（包含batch_id）
                data = self._generate_single_blue_line(i, ticket_id, batch_id)
                batch_data.append(data)

                # 批量插入
                if len(batch_data) >= self.batch_size:
                    execute_values(self.cur, insert_sql, batch_data)
                    self._update_batch_progress(batch_id, len(batch_data))
                    batch_data = []
                    pbar.update(self.batch_size)
            # 插入剩余数据
            if batch_data:
                execute_values(self.cur, insert_sql, batch_data)
                self._update_batch_progress(batch_id, len(batch_data))
                pbar.update(len(batch_data))

        self.conn.commit()

        # 标记批次完成
        self._mark_batch_completed(batch_id)
        print(f"✓ {total_lines:,}条蓝票行数据生成完成（批次ID: {batch_id}）")
        return batch_id
    
    def _generate_single_blue_line(self, index: int, ticket_id: int, batch_id: str):
        """
        生成单条蓝票行数据
        复用之前的数据生成逻辑，增加batch_id
        """
        tax_rate = int(np.random.choice(self.tax_rates, p=self.tax_weights))  # 转换为Python int
        buyer_id, seller_id = self.generate_buyer_seller()
        remaining = self.generate_remaining_amount()
        original_amount = remaining * random.uniform(1.2, 2.0) if remaining > 0 else random.uniform(100, 1000)
        product_name = f"Product_{index % 1000}"

        return (
            ticket_id, tax_rate, buyer_id, seller_id,
            product_name, round(original_amount, 2), remaining, batch_id
        )
    
    def generate_remaining_amount(self):
        """
        生成更贴近真实场景的remaining金额分布
        减少完全用完的比例，增加有效剩余金额
        """
        rand = random.random()
        if rand < 0.60:  # 60% remaining = 0 (从70%降低)
            return 0
        elif rand < 0.75:  # 15% 小额 1-100 (从12%增加)
            return round(random.uniform(1, 100), 2)
        elif rand < 0.85:  # 10% 中额 100-500 (从6%增加)
            return round(random.uniform(100, 500), 2)
        elif rand < 0.95:  # 10% 大额 500-2000 (从3%大幅增加)
            return round(random.uniform(500, 2000), 2)
        else:  # 5% 超大额 2000-10000 (从1%增加且金额范围扩大)
            return round(random.uniform(2000, 10000), 2)
    
    def generate_buyer_seller(self):
        """
        生成买卖方组合（优化版）
        调整概率分布，增加热门组合密度
        """
        rand = random.random()
        if rand < 0.40:  # 40% 热门组合（提高从30%）
            buyer = random.choice(self.hot_buyers)
            seller = random.choice(self.hot_sellers)
        elif rand < 0.80:  # 40% 常规组合（保持40%）
            buyer = random.choice(self.regular_buyers)
            seller = random.choice(self.regular_sellers)
        else:  # 20% 长尾组合（保持20%）
            buyer = random.choice(self.all_buyers)
            seller = random.choice(self.all_sellers)
        return buyer, seller
    
    def create_indexes(self):
        """创建索引（包括部分索引）"""
        print("\n创建索引...")

        # 从SQL文件加载索引语句
        indexes_sql = load_sql_file('schema/create_indexes.sql')

        # 按分号分割多个SQL语句
        statements = [stmt.strip() for stmt in indexes_sql.split(';') if stmt.strip()]

        for stmt in statements:
            # 跳过注释行
            if stmt.startswith('--') or not stmt.strip():
                continue

            if 'CREATE INDEX' in stmt.upper():
                # 提取索引名（用于显示进度）
                try:
                    idx_name = stmt.split()[2] if len(stmt.split()) > 2 else 'unknown'
                    print(f"  创建索引 {idx_name}...")
                    start_time = time.time()
                    self.cur.execute(stmt)
                    self.conn.commit()
                    elapsed = time.time() - start_time
                    print(f"    ✓ 完成 (耗时: {elapsed:.2f}秒)")
                except Exception as e:
                    print(f"    ❌ 创建失败: {e}")
            elif stmt.strip().upper().startswith('ANALYZE'):
                print("  更新统计信息...")
                try:
                    self.cur.execute(stmt)
                    self.conn.commit()
                    print("    ✓ 统计信息更新完成")
                except Exception as e:
                    print(f"    ❌ 统计更新失败: {e}")

        print("✓ 索引创建完成")
    
    def generate_negative_invoices_objects(self, scenario="mixed", count: Optional[int] = None) -> List[NegativeInvoice]:
        """
        生成负数发票对象（供核心模块使用）

        Args:
            scenario: 场景类型
            count: 生成数量（可选，覆盖场景默认数量）

        Returns:
            List[NegativeInvoice]: 负数发票对象列表
        """
        invoice_data = self.generate_negative_invoices_data(scenario, count)

        return [
            NegativeInvoice(
                invoice_id=data['id'],
                amount=Decimal(str(data['amount'])),
                tax_rate=data['tax_rate'],
                buyer_id=data['buyer_id'],
                seller_id=data['seller_id'],
                priority=data.get('priority', 0)
            )
            for data in invoice_data
        ]
    
    def generate_negative_invoices_data(self, scenario="mixed", count: Optional[int] = None) -> List[Dict]:
        """
        生成负数发票测试数据（原始字典格式）
        保留这个方法用于向后兼容

        Args:
            scenario: 场景类型 (small/mixed/stress/custom)
            count: 生成数量（可选，覆盖场景默认数量）
        """
        negative_data = []

        if scenario == "small":
            # 小额场景：默认200条，10-100元
            total_count = count if count is not None else 200
            for i in range(total_count):
                amount = random.uniform(10, 100)
                tax_rate = random.choice([13, 6])
                buyer_id = random.choice(self.hot_buyers)
                seller_id = random.choice(self.hot_sellers)
                negative_data.append({
                    'id': i + 1,
                    'amount': round(amount, 2),
                    'tax_rate': tax_rate,
                    'buyer_id': buyer_id,
                    'seller_id': seller_id
                })

        elif scenario == "mixed":
            # 混合场景：不同金额范围
            if count is not None:
                # 如果指定了数量，按比例分配
                ranges = [
                    (int(count * 0.5), 10, 100),    # 50% 10-100元
                    (int(count * 0.3), 100, 500),   # 30% 100-500元
                    (int(count * 0.15), 500, 1000), # 15% 500-1000元
                    (int(count * 0.05), 1000, 5000),# 5% 1000-5000元
                ]
            else:
                # 默认配置
                ranges = [
                    (50, 10, 100),    # 50条 10-100元
                    (30, 100, 500),   # 30条 100-500元
                    (15, 500, 1000),  # 15条 500-1000元
                    (5, 1000, 5000),  # 5条 1000-5000元
                ]

            id_counter = 1
            for count_in_range, min_amt, max_amt in ranges:
                for _ in range(count_in_range):
                    amount = random.uniform(min_amt, max_amt)
                    tax_rate = int(np.random.choice(self.tax_rates, p=self.tax_weights))
                    buyer_id, seller_id = self.generate_buyer_seller()
                    negative_data.append({
                        'id': id_counter,
                        'amount': round(amount, 2),
                        'tax_rate': tax_rate,
                        'buyer_id': buyer_id,
                        'seller_id': seller_id
                    })
                    id_counter += 1

        elif scenario == "stress":
            # 压力测试：默认1000条随机
            total_count = count if count is not None else 1000
            for i in range(total_count):
                amount = random.uniform(10, 5000)
                tax_rate = int(np.random.choice(self.tax_rates, p=self.tax_weights))
                buyer_id, seller_id = self.generate_buyer_seller()
                negative_data.append({
                    'id': i + 1,
                    'amount': round(amount, 2),
                    'tax_rate': tax_rate,
                    'buyer_id': buyer_id,
                    'seller_id': seller_id
                })

        elif scenario == "custom":
            # 自定义场景：完全随机
            total_count = count if count is not None else 100
            for i in range(total_count):
                amount = random.uniform(1, 10000)
                tax_rate = int(np.random.choice(self.tax_rates, p=self.tax_weights))
                buyer_id, seller_id = self.generate_buyer_seller()
                negative_data.append({
                    'id': i + 1,
                    'amount': round(amount, 2),
                    'tax_rate': tax_rate,
                    'buyer_id': buyer_id,
                    'seller_id': seller_id
                })

        # 按金额降序排序（大额优先）
        negative_data.sort(key=lambda x: x['amount'], reverse=True)

        return negative_data
    
    def _print_statistics(self):
        """打印数据统计信息"""
        print("\n数据分布统计：")

        # 从SQL文件加载统计查询
        stats_sql = load_sql_file('test/stats_queries.sql')

        # 按分号分割多个查询，去除注释行
        queries = []
        current_query = []

        for line in stats_sql.split('\n'):
            line = line.strip()
            if line.startswith('--') or not line:
                continue
            current_query.append(line)
            if line.endswith(';'):
                queries.append(' '.join(current_query))
                current_query = []

        # 执行第一个查询：余额分布
        if len(queries) >= 1:
            self.cur.execute(queries[0])
            print("\nRemaining分布：")
            for row in self.cur.fetchall():
                print(f"  {row[0]}: {row[1]:,} ({row[2]}%)")

        # 执行第二个查询：税率分布
        if len(queries) >= 2:
            self.cur.execute(queries[1])
            print("\n税率分布：")
            for row in self.cur.fetchall():
                print(f"  {row[0]}%: {row[1]:,} ({row[2]}%)")

        # 执行第三个查询：活跃数据统计
        if len(queries) >= 3:
            self.cur.execute(queries[2])
            row = self.cur.fetchone()
            print(f"\n活跃数据：{row[0]:,} / {row[1]:,} ({row[2]}%)")

    # ========== 批次管理方法 ==========

    def _check_batch_status(self, batch_id: str, total_lines: int, resume_from: Optional[int] = None) -> Optional[int]:
        """
        检查批次状态，支持断点续传

        Args:
            batch_id: 批次ID
            total_lines: 总行数
            resume_from: 指定的续传位置

        Returns:
            int: 续传位置（如果需要续传）
        """
        # 检查是否存在批次记录
        self.cur.execute("""
            SELECT total_lines, inserted_lines, status, start_time
            FROM batch_metadata WHERE batch_id = %s
        """, (batch_id,))

        result = self.cur.fetchone()

        if result:
            existing_total, existing_inserted, status, start_time = result
            print(f"发现批次 {batch_id}：")
            print(f"  状态: {status}")
            print(f"  开始时间: {start_time}")
            print(f"  进度: {existing_inserted:,} / {existing_total:,}")

            if status == 'completed':
                print(f"  批次已完成，无需继续")
                return existing_inserted

            if status == 'running':
                # 检查实际数据库中的记录数
                self.cur.execute("""
                    SELECT COUNT(*) FROM blue_lines WHERE batch_id = %s
                """, (batch_id,))
                actual_count = self.cur.fetchone()[0]

                if actual_count < existing_inserted:
                    # 数据不一致，从实际数量开始
                    print(f"  数据不一致，从实际数量 {actual_count:,} 继续")
                    self._update_batch_metadata(batch_id, total_lines, actual_count, 'running')
                    return actual_count
                else:
                    print(f"  从上次中断位置 {existing_inserted:,} 继续")
                    return existing_inserted
        else:
            # 创建新的批次记录
            self._create_batch_metadata(batch_id, total_lines)
            print(f"创建新批次 {batch_id}")

        return resume_from

    def _create_batch_metadata(self, batch_id: str, total_lines: int):
        """创建批次元数据记录"""
        self.cur.execute("""
            INSERT INTO batch_metadata (batch_id, total_lines, inserted_lines, status)
            VALUES (%s, %s, 0, 'running')
        """, (batch_id, total_lines))
        self.conn.commit()

    def _update_batch_progress(self, batch_id: str, increment: int):
        """更新批次进度"""
        self.cur.execute("""
            UPDATE batch_metadata
            SET inserted_lines = inserted_lines + %s,
                resumed_at = CURRENT_TIMESTAMP
            WHERE batch_id = %s
        """, (increment, batch_id))

    def _update_batch_metadata(self, batch_id: str, total_lines: int, inserted_lines: int, status: str):
        """更新批次元数据"""
        self.cur.execute("""
            UPDATE batch_metadata
            SET total_lines = %s, inserted_lines = %s, status = %s,
                resumed_at = CURRENT_TIMESTAMP
            WHERE batch_id = %s
        """, (total_lines, inserted_lines, status, batch_id))
        self.conn.commit()

    def _mark_batch_completed(self, batch_id: str):
        """标记批次完成"""
        self.cur.execute("""
            UPDATE batch_metadata
            SET status = 'completed', end_time = CURRENT_TIMESTAMP
            WHERE batch_id = %s
        """, (batch_id,))
        self.conn.commit()

    def list_batches(self):
        """列出所有批次信息"""
        self.cur.execute("""
            SELECT batch_id, table_name, total_lines, inserted_lines, status,
                   start_time, end_time,
                   CASE
                       WHEN total_lines > 0 THEN ROUND(inserted_lines * 100.0 / total_lines, 2)
                       ELSE 0
                   END as progress_percent
            FROM batch_metadata
            ORDER BY start_time DESC
        """)

        results = self.cur.fetchall()
        if not results:
            print("暂无批次记录")
            return

        print("\n批次列表：")
        print("=" * 100)
        print(f"{'批次ID':<20} {'表名':<12} {'总数':<10} {'已插入':<10} {'进度':<8} {'状态':<10} {'开始时间':<19} {'结束时间'}")
        print("-" * 100)

        for row in results:
            batch_id, table_name, total, inserted, status, start_time, end_time, progress = row
            end_str = end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else '-'
            print(f"{batch_id:<20} {table_name:<12} {total:<10,} {inserted:<10,} {progress:<7.1f}% {status:<10} {start_time.strftime('%Y-%m-%d %H:%M:%S')} {end_str}")

    def clear_batch(self, batch_id: str):
        """清理指定批次的数据"""
        # 检查批次是否存在
        self.cur.execute("SELECT COUNT(*) FROM batch_metadata WHERE batch_id = %s", (batch_id,))
        if self.cur.fetchone()[0] == 0:
            print(f"批次 {batch_id} 不存在")
            return

        # 删除数据
        self.cur.execute("DELETE FROM blue_lines WHERE batch_id = %s", (batch_id,))
        deleted_count = self.cur.rowcount

        # 删除元数据
        self.cur.execute("DELETE FROM batch_metadata WHERE batch_id = %s", (batch_id,))

        self.conn.commit()
        print(f"✓ 已清理批次 {batch_id}，删除 {deleted_count:,} 条数据")

    def reset_test_data(self):
        """重置测试数据（用于重复测试）"""
        print("重置测试数据...")

        # 从SQL文件加载重置语句
        reset_sql = load_sql_file('test/reset_data.sql')

        # 按分号分割并执行每个语句
        statements = [stmt.strip() for stmt in reset_sql.split(';') if stmt.strip() and not stmt.strip().startswith('--')]

        for stmt in statements:
            if stmt.upper().startswith('SELECT'):
                # 对于验证查询，显示结果
                self.cur.execute(stmt)
                result = self.cur.fetchone()
                if result:
                    total, restored, inconsistent, avg_remaining, avg_original = result
                    print(f"  数据验证: 总行数={total:,}, 已恢复={restored:,}, 异常={inconsistent}, 平均余额={avg_remaining}, 平均原始={avg_original}")
            else:
                self.cur.execute(stmt)

        self.conn.commit()
        print("✓ 测试数据已重置")

    def force_reset_to_fresh_state(self):
        """强制重置所有数据到完全可用状态（用于性能测试）"""
        print("强制重置数据到完全可用状态...")

        # 从SQL文件加载强制重置语句
        force_reset_sql = load_sql_file('test/force_reset_data.sql')

        # 按分号分割并执行每个语句
        statements = [stmt.strip() for stmt in force_reset_sql.split(';') if stmt.strip() and not stmt.strip().startswith('--')]

        for stmt in statements:
            if stmt.upper().startswith('SELECT'):
                # 对于验证查询，显示结果
                self.cur.execute(stmt)
                result = self.cur.fetchone()
                if result:
                    total, available, exhausted, avg_remaining, avg_original, availability = result
                    print(f"  数据验证: 总行数={total:,}, 完全可用={available:,}, 已用完={exhausted:,}")
                    print(f"  平均余额={avg_remaining}, 平均原始={avg_original}, 可用性={availability}%")
            else:
                self.cur.execute(stmt)

        self.conn.commit()
        print("✓ 数据已强制重置到完全可用状态")

    def create_data_snapshot(self, snapshot_name: str = None):
        """创建数据快照（保存 remaining 值）"""
        if snapshot_name is None:
            snapshot_name = f"snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        print(f"创建数据快照: {snapshot_name}")

        # 创建快照表（如果不存在）
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS data_snapshots (
                snapshot_name VARCHAR(100),
                line_id BIGINT,
                remaining_value DECIMAL(15,2),
                snapshot_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (snapshot_name, line_id)
            )
        """)

        # 删除同名快照（如果存在）
        self.cur.execute("DELETE FROM data_snapshots WHERE snapshot_name = %s", (snapshot_name,))

        # 保存当前 remaining 值
        self.cur.execute("""
            INSERT INTO data_snapshots (snapshot_name, line_id, remaining_value)
            SELECT %s, line_id, remaining FROM blue_lines
        """, (snapshot_name,))

        affected_rows = self.cur.rowcount
        self.conn.commit()
        print(f"✓ 快照已创建，保存了 {affected_rows:,} 条记录")
        return snapshot_name

    def restore_from_snapshot(self, snapshot_name: str):
        """从快照恢复数据"""
        print(f"从快照恢复数据: {snapshot_name}")

        # 检查快照是否存在
        self.cur.execute("SELECT COUNT(*) FROM data_snapshots WHERE snapshot_name = %s", (snapshot_name,))
        snapshot_count = self.cur.fetchone()[0]

        if snapshot_count == 0:
            raise ValueError(f"快照不存在: {snapshot_name}")

        # 清空匹配记录
        self.cur.execute("TRUNCATE TABLE match_records CASCADE")

        # 从快照恢复 remaining 值
        self.cur.execute("""
            UPDATE blue_lines
            SET remaining = ds.remaining_value,
                last_update = CURRENT_TIMESTAMP
            FROM data_snapshots ds
            WHERE blue_lines.line_id = ds.line_id
              AND ds.snapshot_name = %s
        """, (snapshot_name,))

        updated_rows = self.cur.rowcount
        self.conn.commit()
        print(f"✓ 数据已恢复，更新了 {updated_rows:,} 条记录")

        # 验证恢复状态
        self._verify_data_consistency()

    def list_snapshots(self):
        """列出所有可用快照"""
        self.cur.execute("""
            SELECT snapshot_name, COUNT(*) as record_count,
                   MIN(snapshot_time) as created_time
            FROM data_snapshots
            GROUP BY snapshot_name
            ORDER BY created_time DESC
        """)

        results = self.cur.fetchall()
        if not results:
            print("暂无数据快照")
            return

        print("\n可用数据快照：")
        print("=" * 60)
        print(f"{'快照名称':<25} {'记录数':<10} {'创建时间'}")
        print("-" * 60)

        for snapshot_name, count, created_time in results:
            print(f"{snapshot_name:<25} {count:<10,} {created_time.strftime('%Y-%m-%d %H:%M:%S')}")

    def delete_snapshot(self, snapshot_name: str):
        """删除指定快照"""
        self.cur.execute("DELETE FROM data_snapshots WHERE snapshot_name = %s", (snapshot_name,))
        deleted_count = self.cur.rowcount
        self.conn.commit()

        if deleted_count > 0:
            print(f"✓ 已删除快照 {snapshot_name}，清理了 {deleted_count:,} 条记录")
        else:
            print(f"快照 {snapshot_name} 不存在")

    def _verify_data_consistency(self):
        """验证数据一致性"""
        self.cur.execute("""
            SELECT
                COUNT(*) as total_lines,
                COUNT(CASE WHEN remaining < 0 THEN 1 END) as negative_remaining,
                COUNT(CASE WHEN remaining > original_amount THEN 1 END) as excess_remaining,
                ROUND(AVG(remaining), 2) as avg_remaining,
                ROUND(SUM(remaining), 2) as total_remaining
            FROM blue_lines
        """)

        result = self.cur.fetchone()
        if result:
            total, negative, excess, avg_remaining, total_remaining = result
            print(f"  数据验证: 总行数={total:,}, 负数余额={negative}, 超额余额={excess}")
            print(f"  平均余额={avg_remaining}, 总余额={total_remaining:,}")

            if negative > 0 or excess > 0:
                print(f"  ⚠️  发现数据异常: 负数余额={negative}, 超额余额={excess}")

    def get_data_utilization_stats(self):
        """获取数据利用率统计"""
        self.cur.execute("""
            SELECT
                COUNT(*) as total_lines,
                COUNT(CASE WHEN remaining = 0 THEN 1 END) as exhausted_lines,
                COUNT(CASE WHEN remaining = original_amount THEN 1 END) as unused_lines,
                COUNT(CASE WHEN remaining > 0 AND remaining < original_amount THEN 1 END) as partial_used_lines,
                ROUND(AVG(remaining / original_amount * 100), 2) as avg_utilization_percent,
                ROUND(SUM(remaining), 2) as total_remaining,
                ROUND(SUM(original_amount), 2) as total_original
            FROM blue_lines
            WHERE original_amount > 0
        """)

        result = self.cur.fetchone()
        if result:
            total, exhausted, unused, partial, avg_util, total_remaining, total_original = result

            # 处理 None 值
            total = total or 0
            exhausted = exhausted or 0
            unused = unused or 0
            partial = partial or 0
            avg_util = avg_util or 0
            total_remaining = total_remaining or 0
            total_original = total_original or 0

            utilization_rate = (1 - total_remaining / total_original) * 100 if total_original > 0 else 0

            print(f"\n📊 数据利用率统计:")
            print(f"  总行数: {total:,}")
            if total > 0:
                print(f"  已用完: {exhausted:,} ({exhausted/total*100:.1f}%)")
                print(f"  未使用: {unused:,} ({unused/total*100:.1f}%)")
                print(f"  部分使用: {partial:,} ({partial/total*100:.1f}%)")
            else:
                print(f"  已用完: {exhausted:,} (0.0%)")
                print(f"  未使用: {unused:,} (0.0%)")
                print(f"  部分使用: {partial:,} (0.0%)")
            print(f"  平均利用率: {avg_util:.1f}%")
            print(f"  总体利用率: {utilization_rate:.1f}%")
            print(f"  剩余金额: {total_remaining:,} / {total_original:,}")

            return {
                'total_lines': total,
                'exhausted_lines': exhausted,
                'unused_lines': unused,
                'partial_used_lines': partial,
                'avg_utilization_percent': avg_util,
                'total_utilization_percent': utilization_rate,
                'total_remaining': total_remaining,
                'total_original': total_original
            }
    
    def close(self):
        """关闭数据库连接"""
        self.cur.close()
        self.conn.close()


def run_generator(args):
    """
    根据参数运行数据生成器
    """
    from config.config import get_db_config

    # 获取配置
    db_config = get_db_config(args.env)
    # test_config = get_test_config()  # 暂时保留，可能用于未来扩展

    # 自定义配置覆盖
    config_overrides = {}
    if args.total_lines:
        config_overrides['total_lines'] = args.total_lines
    if args.batch_size:
        config_overrides['batch_size'] = args.batch_size

    # 初始化生成器
    generator = TestDataGenerator(db_config, config_overrides)

    try:
        print(f"使用环境: {args.env}")
        print(f"数据库: {db_config['database']}")

        # 1. 设置数据库（如果需要）
        if args.setup_db:
            print("\n=== 设置数据库 ===")
            generator.setup_database()

        # 2. 生成蓝票行数据（如果需要）
        if args.generate_blue_lines:
            print("\n=== 生成蓝票行数据 ===")
            total_lines = args.total_lines or generator.total_lines
            batch_id = args.batch_id
            resume_from = args.resume_from
            result_batch_id = generator.generate_blue_lines(total_lines, batch_id, resume_from)
            print(f"批次ID: {result_batch_id}")

        # 批次管理操作
        if args.list_batches:
            print("\n=== 批次列表 ===")
            generator.list_batches()

        if args.clear_batch:
            print(f"\n=== 清理批次 {args.clear_batch} ===")
            generator.clear_batch(args.clear_batch)

        # 数据快照管理操作
        if args.create_snapshot:
            print(f"\n=== 创建数据快照 ===")
            snapshot_name = generator.create_data_snapshot(args.snapshot_name)
            print(f"快照创建完成: {snapshot_name}")

        if args.list_snapshots:
            print("\n=== 快照列表 ===")
            generator.list_snapshots()

        if args.restore_snapshot:
            print(f"\n=== 恢复快照 {args.restore_snapshot} ===")
            generator.restore_from_snapshot(args.restore_snapshot)

        if args.delete_snapshot:
            print(f"\n=== 删除快照 {args.delete_snapshot} ===")
            generator.delete_snapshot(args.delete_snapshot)

        if args.data_stats:
            print("\n=== 数据利用率统计 ===")
            generator.get_data_utilization_stats()

        # 3. 创建索引（如果需要）
        if args.create_indexes:
            print("\n=== 创建索引 ===")
            generator.create_indexes()

        # 4. 生成示例负数发票（如果需要）
        if args.generate_negatives:
            print("\n=== 生成负数发票 ===")
            scenario = args.scenario or 'mixed'
            count = args.negative_count

            test_invoices = generator.generate_negative_invoices_objects(scenario, count)
            print(f"\n生成了 {len(test_invoices)} 条测试负数发票 (场景: {scenario})")

            if args.show_samples:
                print("前5条：")
                for inv in test_invoices[:5]:
                    print(f"  ID:{inv.invoice_id}, 金额:{inv.amount}, "
                          f"税率:{inv.tax_rate}%, 买方:{inv.buyer_id}, 卖方:{inv.seller_id}")

        # 5. 重置测试数据（如果需要）
        if args.reset_data:
            print("\n=== 重置测试数据 ===")
            generator.reset_test_data()

        if args.force_reset:
            print("\n=== 强制重置到完全可用状态 ===")
            generator.force_reset_to_fresh_state()

        print("\n✓ 操作完成")

    except Exception as e:
        print(f"\n❌ 执行失败: {e}")
        raise
    finally:
        generator.close()


def parse_args():
    """
    解析命令行参数
    """
    parser = argparse.ArgumentParser(
        description='负数发票匹配系统测试数据生成器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:

基础操作:
  # 完整初始化（首次使用）
  python test_data_generator.py --setup-db --generate-blue-lines --total-lines 1000000 --create-indexes

  # 只设置数据库
  python test_data_generator.py --setup-db

  # 生成1千万条数据（支持断点续传）
  python test_data_generator.py --generate-blue-lines --total-lines 10000000 --batch-id prod_001

批次管理:
  # 查看所有批次状态
  python test_data_generator.py --list-batches

  # 继续未完成的批次（自动检测断点）
  python test_data_generator.py --generate-blue-lines --batch-id prod_001

  # 清理特定批次数据
  python test_data_generator.py --clear-batch prod_001

测试数据:
  # 生成500条混合场景负数发票
  python test_data_generator.py --generate-negatives --scenario mixed --negative-count 500

  # 重置测试数据
  python test_data_generator.py --reset-data

环境配置:
  # 使用开发环境
  python test_data_generator.py --generate-blue-lines --env dev --total-lines 100000

性能优化:
  # 调整批次大小以优化性能
  python test_data_generator.py --generate-blue-lines --total-lines 1000000 --batch-size 50000
        """
    )

    # 环境配置
    parser.add_argument('--env', default='test', choices=['test', 'dev', 'prod'],
                       help='数据库环境 (默认: test)')

    # 操作选项
    parser.add_argument('--all', action='store_true',
                       help='执行所有操作（设置数据库、生成数据、创建索引、生成负数发票）')
    parser.add_argument('--setup-db', action='store_true',
                       help='设置数据库（创建表）')
    parser.add_argument('--generate-blue-lines', action='store_true',
                       help='生成蓝票行数据')
    parser.add_argument('--create-indexes', action='store_true',
                       help='创建索引')
    parser.add_argument('--generate-negatives', action='store_true',
                       help='生成负数发票测试数据')
    parser.add_argument('--reset-data', action='store_true',
                       help='重置测试数据')
    parser.add_argument('--force-reset', action='store_true',
                       help='强制重置数据到完全可用状态（用于性能测试）')

    # 数据生成参数
    parser.add_argument('--total-lines', type=int,
                       help='蓝票行总数（默认: 10,000,000）')
    parser.add_argument('--batch-size', type=int,
                       help='批量插入大小（默认: 10,000）')

    # 负数发票参数
    parser.add_argument('--scenario', choices=['small', 'mixed', 'stress', 'custom'],
                       help='负数发票场景类型（默认: mixed）')
    parser.add_argument('--negative-count', type=int,
                       help='负数发票数量（覆盖场景默认值）')
    parser.add_argument('--show-samples', action='store_true',
                       help='显示生成的负数发票样例')

    # 批次管理参数
    parser.add_argument('--batch-id', type=str,
                       help='批次ID（用于断点续传和数据追踪）')
    parser.add_argument('--resume-from', type=int,
                       help='从指定位置继续生成（通常由系统自动检测）')
    parser.add_argument('--list-batches', action='store_true',
                       help='列出所有批次信息')
    parser.add_argument('--clear-batch', type=str,
                       help='清理指定批次的数据')

    # 数据快照管理参数
    parser.add_argument('--create-snapshot', action='store_true',
                       help='创建数据快照（保存当前 remaining 值）')
    parser.add_argument('--snapshot-name', type=str,
                       help='快照名称（可选，默认生成时间戳）')
    parser.add_argument('--list-snapshots', action='store_true',
                       help='列出所有可用快照')
    parser.add_argument('--restore-snapshot', type=str,
                       help='从指定快照恢复数据')
    parser.add_argument('--delete-snapshot', type=str,
                       help='删除指定快照')
    parser.add_argument('--data-stats', action='store_true',
                       help='显示数据利用率统计')

    args = parser.parse_args()

    # 如果使用 --all，则启用所有操作
    if args.all:
        args.setup_db = True
        args.generate_blue_lines = True
        args.create_indexes = True
        args.generate_negatives = True
        args.show_samples = True

    # 如果没有指定任何操作，默认只生成数据（不删除表）
    if not any([
        args.setup_db, args.generate_blue_lines, args.create_indexes,
        args.generate_negatives, args.reset_data, args.force_reset, args.list_batches, args.clear_batch,
        args.create_snapshot, args.list_snapshots, args.restore_snapshot,
        args.delete_snapshot, args.data_stats
    ]):
        # 只生成数据，不执行破坏性操作
        args.generate_blue_lines = True
        args.create_indexes = True  # 确保索引存在
        # 注意：不自动设置 setup_db = True，避免意外删除表
        args.generate_negatives = True
        args.show_samples = True

    return args


# ========== 常用操作说明 ==========
"""
千万级数据生成最佳实践：

1. 首次设置：
   python test_data_generator.py --setup-db

2. 大数据量生成（推荐使用batch_id）：
   python test_data_generator.py --generate-blue-lines --total-lines 10000000 --batch-id prod_001

3. 断点续传（如果中断）：
   python test_data_generator.py --generate-blue-lines --batch-id prod_001
   # 系统自动检测进度并续传

4. 监控进度：
   python test_data_generator.py --list-batches

5. 清理数据：
   python test_data_generator.py --clear-batch prod_001

6. 性能调优：
   - 调整 --batch-size 参数
   - 使用适当的 batch_id 命名
   - 定期查看批次状态

注意事项：
- 重复执行相同命令会导致数据累加
- 使用不同的batch_id避免数据混乱
- 大数据量操作建议在低峰期进行
"""


# 独立运行时的初始化脚本
if __name__ == "__main__":
    args = parse_args()
    run_generator(args)
    