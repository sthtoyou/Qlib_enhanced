# Qlib指标计算器 (qlib_indicators.py) 完整用法说明

## 概述

**QlibIndicatorsEnhancedCalculator** 是一个专业的量化金融指标计算器，集成了多种指标体系，支持高性能并行计算，专为量化研究和策略开发设计。

### 核心特性

- 🚀 **多线程并行计算**：支持股票级别和指标类型级别的并行计算
- 📊 **完整指标体系**：覆盖 Alpha158、Alpha360、技术指标、财务指标等 650+ 指标
- 🔄 **智能去重**：避免重复计算，确保指标唯一性
- 🌐 **多语言支持**：字段名支持中文标签，兼容 SAS 等分析软件
- 🔒 **线程安全**：采用线程本地存储和锁机制确保数据一致性

## 功能模块

### 1. 指标类型分类

| 指标类型 | 数量 | 说明 |
|----------|------|------|
| **Alpha158** | 159个 | 经典量化因子，包含 KBAR、价格、成交量等滚动统计指标 |
| **Alpha360** | 360个 | 时序特征，过去60天的标准化价格和成交量数据 |
| **技术指标** | 60个 | 移动平均、MACD、RSI、布林带、KDJ等经典技术指标 |
| **蜡烛图形态** | 61个 | 锤子线、十字星、吞没形态等K线形态识别 |
| **财务指标** | 78个 | 市净率、换手率、ROE、ROA、托宾Q值等基本面指标 |
| **波动率指标** | 8个 | 已实现波动率、正负半变差等风险衡量指标 |

### 2. 数据源支持

- **Qlib 二进制数据**：标准的 Qlib 格式数据文件
- **财务数据**：支持多种财务数据格式（CSV）
- **实时数据**：支持动态数据加载和处理

## 安装依赖

```bash
# 必需依赖
pip install pandas numpy talib loguru pathlib
pip install concurrent.futures threading multiprocessing

# 可选依赖（用于扩展功能）
pip install yfinance akshare tushare
```

## 基本用法

### 1. 命令行运行

```bash
# 基本用法 - 计算所有股票的所有指标
python scripts/qlib_indicators.py

# 指定数据目录
python scripts/qlib_indicators.py --data-dir "D:\stk_data\trd\us_data"

# 限制处理股票数量（用于测试）
python scripts/qlib_indicators.py --max-stocks 10

# 自定义输出文件
python scripts/qlib_indicators.py --output "my_indicators.csv"

# 禁用并行处理（调试用）
python scripts/qlib_indicators.py --disable-parallel

# 自定义线程数
python scripts/qlib_indicators.py --max-workers 16

# 调试模式
python scripts/qlib_indicators.py --log-level DEBUG --max-stocks 5
```

### 2. 代码调用

```python
from scripts.qlib_indicators import QlibIndicatorsEnhancedCalculator

# 创建计算器实例
calculator = QlibIndicatorsEnhancedCalculator(
    data_dir="D:/stk_data/trd/us_data",
    financial_data_dir="D:/stk_data/financial_data",
    max_workers=16,
    enable_parallel=True
)

# 运行完整计算流程
calculator.run(
    max_stocks=50,
    output_filename="enhanced_indicators.csv"
)
```

## 高级用法

### 1. 单只股票计算

```python
# 计算单只股票的所有指标
stock_data = calculator.calculate_all_indicators_for_stock("AAPL")
print(f"AAPL 指标数量: {len(stock_data.columns)}")
```

### 2. 指定指标类型

```python
# 只计算技术指标
data = calculator.read_qlib_binary_data("AAPL")
if data is not None:
    tech_indicators = calculator.calculate_all_technical_indicators(data)
    print(f"技术指标: {len(tech_indicators.columns)} 个")
```

### 3. 自定义标签

```python
# 获取字段中文标签
columns = ['Close', 'Volume', 'SMA_20', 'RSI_14']
labels = calculator.get_field_labels(columns)
for col, label in zip(columns, labels):
    print(f"{col}: {label}")
```

### 4. 批量处理

```python
# 获取可用股票列表
available_stocks = calculator.get_available_stocks()
print(f"可用股票数量: {len(available_stocks)}")

# 批量计算前10只股票
results = calculator.calculate_all_indicators(max_stocks=10)
print(f"结果形状: {results.shape}")
```

## 配置参数

### 1. 初始化参数

```python
QlibIndicatorsEnhancedCalculator(
    data_dir="D:/stk_data/trd/us_data",        # Qlib数据目录
    financial_data_dir=None,                   # 财务数据目录
    max_workers=None,                          # 最大线程数
    enable_parallel=True                       # 是否启用并行计算
)
```

### 2. 命令行参数

```bash
--data-dir         # Qlib数据目录路径
--financial-dir    # 财务数据目录路径
--max-stocks       # 最大股票数量限制
--output           # 输出文件名
--log-level        # 日志级别 (DEBUG/INFO/WARNING/ERROR)
--disable-parallel # 禁用多线程
--max-workers      # 最大线程数
```

## 输出格式

### 1. CSV 文件结构

```csv
# 第一行：英文字段名
Date,Symbol,Close,Volume,SMA_20,RSI_14,...

# 第二行：中文标签
日期,股票代码,收盘价,成交量,20日移动平均,14日RSI,...

# 第三行开始：数据内容
2024-01-01,AAPL,150.25,1000000,148.30,65.2,...
```

### 2. 特殊处理

- **空值处理**：NaN 值被转换为空字符串，兼容 SAS 导入
- **编码格式**：UTF-8-BOM 编码，确保中文正确显示
- **数据类型**：数值型数据保持精度，日期格式标准化

## 性能优化

### 1. 多线程配置

```python
# 自动配置（推荐）
max_workers = min(32, (CPU核心数 + 4))

# 手动配置
calculator = QlibIndicatorsEnhancedCalculator(
    max_workers=16,        # 16个线程
    enable_parallel=True   # 启用并行
)
```

### 2. 内存管理

- **数据缓存**：财务数据自动缓存到内存
- **线程本地存储**：避免线程间数据竞争
- **分批处理**：大数据集自动分批处理

### 3. 性能监控

```python
# 运行时会显示详细的性能统计
"""
🚀 开始运行增强版Qlib指标计算器
⚙️ 多线程模式: 启用
🧵 最大线程数: 20
📊 总共计算了 695 个指标
📈 包含 3 只股票
⏱️ 总耗时: 45.23 秒
"""
```

## 故障排除

### 1. 常见问题

**问题：数据目录不存在**
```bash
FileNotFoundError: Data directory does not exist
```
**解决方案：**
```python
# 检查数据目录路径
import os
print(os.path.exists("D:/stk_data/trd/us_data"))

# 创建必要目录
os.makedirs("D:/stk_data/trd/us_data/features", exist_ok=True)
```

**问题：财务数据缺失**
```bash
WARNING: 财务数据目录不存在，将使用估算值
```
**解决方案：**
```python
# 指定财务数据目录或使用估算值
calculator = QlibIndicatorsEnhancedCalculator(
    financial_data_dir="D:/stk_data/financial_data"
)
```

### 2. 调试模式

```python
# 开启详细日志
calculator = QlibIndicatorsEnhancedCalculator(
    enable_parallel=False,  # 禁用并行便于调试
)

# 设置日志级别
import logging
logging.basicConfig(level=logging.DEBUG)
```

### 3. 数据验证

```python
# 验证数据完整性
stocks = calculator.get_available_stocks()
print(f"可用股票: {len(stocks)}")

for stock in stocks[:5]:  # 检查前5只股票
    data = calculator.read_qlib_binary_data(stock)
    if data is not None:
        print(f"{stock}: {data.shape}")
    else:
        print(f"{stock}: 数据读取失败")
```

## 扩展开发

### 1. 自定义指标

```python
class MyCustomCalculator(QlibIndicatorsEnhancedCalculator):
    def calculate_custom_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """自定义指标计算"""
        indicators = {}
        
        # 自定义指标逻辑
        indicators['MY_CUSTOM'] = data['Close'].rolling(10).mean()
        
        return pd.DataFrame(indicators, index=data.index)
```

### 2. 数据源集成

```python
# 集成外部数据源
def integrate_external_data(calculator, symbol):
    """集成外部数据"""
    # 获取外部数据
    external_data = fetch_external_data(symbol)
    
    # 合并到指标计算
    return calculator.calculate_all_indicators_for_stock(symbol, external_data)
```

## 最佳实践

### 1. 生产环境配置

```python
# 生产环境推荐配置
calculator = QlibIndicatorsEnhancedCalculator(
    data_dir="D:/stk_data/trd/us_data",
    financial_data_dir="D:/stk_data/financial_data",
    max_workers=min(32, multiprocessing.cpu_count() * 2),
    enable_parallel=True
)
```

### 2. 内存优化

```python
# 分批处理大数据集
def process_large_dataset(calculator, stock_list, batch_size=50):
    """分批处理大数据集"""
    results = []
    
    for i in range(0, len(stock_list), batch_size):
        batch = stock_list[i:i+batch_size]
        batch_results = calculator.calculate_all_indicators(max_stocks=len(batch))
        results.append(batch_results)
        
        # 清理内存
        del batch_results
        
    return pd.concat(results, ignore_index=True)
```

### 3. 监控和日志

```python
# 配置详细日志
from loguru import logger

logger.add(
    "qlib_indicators.log",
    rotation="10 MB",
    retention="7 days",
    format="{time} | {level} | {message}"
)
```

## 技术支持

### 更新日志

- **v1.0.0**：基础功能实现
- **v1.1.0**：添加多线程支持
- **v1.2.0**：集成财务指标
- **v1.3.0**：添加中文标签支持
- **v1.4.0**：优化性能和内存管理

### 联系方式

如有问题或建议，请通过以下方式联系：
- 提交 Issue 到项目仓库
- 查看项目文档和示例
- 参与社区讨论

---

**注意**：本工具仅供学习和研究使用，不构成投资建议。使用前请确保了解相关风险。 