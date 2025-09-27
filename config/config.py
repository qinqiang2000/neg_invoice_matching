"""
配置管理模块
"""

def get_db_config(env='test'):
    """
    获取数据库配置
    
    Args:
        env: 环境标识 (test/dev/prod)
    """
    configs = {
        'test': {
            'host': 'gz-postgres-qm0mmn7p.sql.tencentcdb.com',
            'port': 24732,
            'database': 'test_matching',
            'user': 'root',
            'password': 'Fapiaoyun@2025'
        },
        'dev': {
            'host': 'gz-postgres-qm0mmn7p.sql.tencentcdb.com',
            'port': 24732,
            'database': 'test_matching',
            'user': 'root',
            'password': 'Fapiaoyun@2025'
        },
        'prod': {
            'host': 'gz-postgres-qm0mmn7p.sql.tencentcdb.com',
            'port': 24732,
            'database': 'test_matching',
            'user': 'root',
            'password': 'Fapiaoyun@2025'
        }
    }
    
    return configs.get(env, configs['test'])

def get_test_config():
    """获取测试配置"""
    return {
        'scenarios': {
            'small': {
                'count': 200,
                'amount_range': (10, 100),
                'description': '小额退货场景'
            },
            'mixed': {
                'count': 100,
                'amount_range': (10, 5000),
                'description': '混合场景'
            },
            'stress': {
                'count': 1000,
                'amount_range': (10, 5000),
                'description': '压力测试场景'
            }
        },
        'performance_targets': {
            'success_rate': 0.90,  # 最低成功率
            'max_latency_ms': 100,  # 最大延迟
            'fragment_rate': 0.15   # 最大碎片率
        }
    }