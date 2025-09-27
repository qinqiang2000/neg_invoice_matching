#!/usr/bin/env python3
"""
增强版数据生成进度监控脚本 - 显示批次元数据中的中间进度
"""
import time
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.db_manager import DatabaseManager
import config.config as config

def check_enhanced_progress():
    """检查增强进度（包含批次元数据）"""
    try:
        db_config = config.get_db_config('test')
        db_manager = DatabaseManager(db_config)

        conn = db_manager.pool.getconn()
        try:
            with conn.cursor() as cur:
                # 总数
                cur.execute('SELECT COUNT(*) FROM blue_lines')
                total = cur.fetchone()[0]
                progress = total / 10000000 * 100
                print(f'数据库总记录数: {total:,} / 10,000,000 ({progress:.1f}%)')

                # 批次元数据（显示中间进度）
                cur.execute('''
                    SELECT batch_id, total_lines, inserted_lines, status,
                           CASE
                               WHEN total_lines > 0 THEN ROUND(inserted_lines * 100.0 / total_lines, 1)
                               ELSE 0
                           END as progress_percent,
                           start_time
                    FROM batch_metadata
                    WHERE batch_id LIKE 'opt_batch_%'
                    ORDER BY start_time
                ''')
                batches = cur.fetchall()

                if batches:
                    print('\n批次详细进度:')
                    print(f'{"批次ID":<15} {"总数":<8} {"已处理":<8} {"进度":<7} {"状态":<10} {"开始时间"}')
                    print('-' * 70)

                    active_batches = 0
                    for batch_id, total_lines, inserted_lines, status, progress_pct, start_time in batches:
                        if status == 'running':
                            active_batches += 1
                        status_display = status
                        if status == 'running' and progress_pct < 100:
                            status_display = f'{status} ⚡'
                        elif status == 'completed':
                            status_display = f'{status} ✓'

                        print(f'{batch_id:<15} {total_lines:<8,} {inserted_lines:<8,} {progress_pct:<6.1f}% {status_display:<10} {start_time.strftime("%H:%M:%S")}')

                    if active_batches > 0:
                        print(f'\n🔄 当前有 {active_batches} 个批次正在运行')
                    else:
                        print(f'\n✅ 所有批次已完成')

                # 预估完成时间
                completed_batches = len([b for b in batches if b[3] == 'completed'])
                if completed_batches > 0:
                    avg_time_per_batch = 3.5  # 分钟，基于观察
                    remaining_batches = max(0, 10 - completed_batches)  # 假设总共需要10个批次
                    estimated_minutes = remaining_batches * avg_time_per_batch
                    print(f'\n📊 预估剩余时间: {estimated_minutes:.0f}分钟 ({remaining_batches}个批次)')

                return total
        finally:
            db_manager.pool.putconn(conn)
    except Exception as e:
        print(f'错误: {e}')
        return 0

def monitor_enhanced_loop(interval=9):
    """增强版循环监控"""
    print(f"增强版进度监控（每{interval}秒刷新，显示批次内部进度）\n")
    print("按Ctrl+C停止监控\n")

    try:
        while True:
            print(f"=== {time.strftime('%Y-%m-%d %H:%M:%S')} ===")

            total = check_enhanced_progress()

            if total >= 10000000:
                print("\n🎉🎉🎉 数据生成完成！总计10,000,000条记录 🎉🎉🎉")
                break

            print("=" * 70)
            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n\n👋 监控已停止")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='增强版数据生成进度监控')
    parser.add_argument('--once', action='store_true', help='只检查一次，不循环')
    parser.add_argument('--interval', type=int, default=9, help='刷新间隔（秒），默认9秒')

    args = parser.parse_args()

    if args.once:
        check_enhanced_progress()
    else:
        monitor_enhanced_loop(args.interval)