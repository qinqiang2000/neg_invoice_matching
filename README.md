# 负数发票匹配系统

这是一个用于处理负数发票匹配的Python项目。

## 环境设置

### 1. 激活虚拟环境

```bash
# 方法1: 使用便捷脚本
./activate.sh

# 方法2: 手动激活
source venv/bin/activate
```

### 2. 验证安装

```bash
# 检查Python版本
python --version

# 检查已安装的包
pip list

# 测试导入
python -c "import psycopg2; import numpy; import pandas; print('所有依赖导入成功！')"
```

## 项目结构

```
neg_invoice_matching/
├── api/                    # API服务层
├── config/                 # 配置管理
├── core/                   # 核心业务逻辑
│   ├── db_manager.py      # 数据库管理
│   ├── matching_engine.py # 匹配算法引擎
│   └── models.py          # 数据模型
├── sql/                   # SQL脚本
│   ├── schema/           # 数据库结构
│   └── test/             # 测试数据
├── tests/                 # 测试文件
├── venv/                  # 虚拟环境
├── requirements.txt       # 依赖库列表
└── activate.sh           # 环境激活脚本
```

## 主要功能

1. **匹配引擎**: 实现贪婪算法进行负数发票与蓝票行的匹配
2. **数据库管理**: 提供连接池和事务管理
3. **性能测试**: 包含基准测试和压力测试
4. **数据生成**: 自动生成测试数据

## 运行测试

```bash
# 激活环境
source venv/bin/activate

# 运行基本测试
python tests/test_basic_matching.py

# 运行性能测试
python tests/benchmark.py

# 生成测试数据
python tests/test_data_generator.py
```

## 依赖库

- **psycopg2-binary**: PostgreSQL数据库连接
- **numpy**: 数值计算
- **pandas**: 数据处理
- **matplotlib**: 数据可视化
- **tqdm**: 进度条显示

## 退出虚拟环境

```bash
deactivate
```

## 注意事项

- 确保PostgreSQL数据库服务正在运行
- 检查`config/config.py`中的数据库连接配置
- 首次运行前可能需要执行SQL脚本创建表结构
