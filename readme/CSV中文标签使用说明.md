# CSV文件中文标签功能使用说明

## 📋 功能概述

现在 `scripts/qlib_indicators.py` 脚本已经支持在输出的CSV文件中为每个字段添加中文标签！

输出的CSV文件格式为：
- **第一行**：字段名（英文列名）
- **第二行**：中文标签
- **第三行开始**：具体数据

## 🎯 输出格式示例

```csv
Date,Symbol,Open,High,Low,Close,Volume,SMA_5,RSI_14,MACD,ALPHA158_KMID,ALPHA158_ROC_5,CDL2CROWS
日期,股票代码,开盘价,最高价,最低价,收盘价,成交量,5日简单移动平均,14日相对强弱指数,MACD主线,K线中点价格,5日收益率,两只乌鸦
2024-01-01,AAPL,100.0,105.0,99.0,104.0,1000000,102.0,50.0,0.5,102.0,0.01,0
2024-01-02,AAPL,101.0,106.0,100.0,105.0,1100000,103.0,55.0,0.8,103.0,0.02,0
2024-01-03,AAPL,102.0,107.0,101.0,106.0,1200000,104.0,60.0,1.0,104.0,0.03,1
```

## 📚 中文标签覆盖范围

### 基础字段
- `Date` → 日期
- `Symbol` → 股票代码
- `Open` → 开盘价
- `High` → 最高价
- `Low` → 最低价
- `Close` → 收盘价
- `Volume` → 成交量

### Alpha158指标体系 (~158个指标)
- `ALPHA158_KMID` → K线中点价格
- `ALPHA158_KLEN` → K线长度
- `ALPHA158_ROC_5` → 5日收益率
- `ALPHA158_MA_20` → 20日移动平均
- `ALPHA158_STD_10` → 10日标准差
- `ALPHA158_BETA_30` → 30日贝塔值
- ... 等约158个指标

### Alpha360指标体系 (~360个指标)
- `ALPHA360_open_0_0` → 开盘价标准化T-0期第0维
- `ALPHA360_close_1_1` → 收盘价标准化T-1期第1维
- `ALPHA360_high_59_5` → 最高价标准化T-59期第5维
- ... 等约360个指标

### 技术指标 (~60个指标)
- `SMA_5` → 5日简单移动平均
- `EMA_20` → 20日指数移动平均
- `RSI_14` → 14日相对强弱指数
- `MACD` → MACD主线
- `BB_Upper` → 布林带上轨
- `ATR_14` → 14日平均真实范围
- ... 等约60个技术指标

### 蜡烛图形态指标 (61个指标)
- `CDL2CROWS` → 两只乌鸦
- `CDL3BLACKCROWS` → 三只黑乌鸦
- `CDLDOJI` → 十字
- `CDLHAMMER` → 锤头
- `CDLENGULFING` → 吞噬模式
- ... 等61个蜡烛图形态

### 财务指标 (~15个指标)
- `MarketCap` → 市值
- `PE` → 市盈率
- `PriceToBook` → 市净率
- `ROE` → 净资产收益率
- `turnover_20` → 20日换手率
- `TobinsQ` → 托宾Q值
- ... 等约15个财务指标

### 波动率指标 (~8个指标)
- `RealizedVolatility_20` → 20日已实现波动率
- `SemiDeviation_10` → 10日半变差
- ... 等约8个波动率指标

## 🚀 使用方法

### 方法1：直接运行脚本
```bash
python scripts/qlib_indicators.py
```

### 方法2：使用命令行参数
```bash
# 计算前10只股票
python scripts/qlib_indicators.py --max-stocks 10

# 自定义输出文件名
python scripts/qlib_indicators.py --output my_indicators_with_labels.csv

# 指定数据目录
python scripts/qlib_indicators.py --data-dir ./data
```

### 方法3：在代码中使用
```python
from scripts.qlib_indicators import QlibIndicatorsEnhancedCalculator

# 创建计算器
calculator = QlibIndicatorsEnhancedCalculator(
    data_dir=r"D:\stk_data\trd\us_data"
)

# 计算指标
results_df = calculator.calculate_all_indicators(max_stocks=5)

# 保存带中文标签的CSV文件
output_path = calculator.save_results(results_df, "indicators_with_labels.csv")
print(f"文件已保存到: {output_path}")
```

## 📊 智能标签生成

脚本会为每个字段自动生成合适的中文标签：

1. **预定义标签**：对于常见指标使用预设的中文标签
2. **动态生成**：对于Alpha360等指标自动生成描述性标签
3. **分类标签**：对于未预定义的指标，根据前缀生成分类标签：
   - `ALPHA158_` → Alpha158指标_xxx
   - `ALPHA360_` → Alpha360指标_xxx
   - `CDL` → 蜡烛图形态_xxx
4. **保持原名**：对于无法分类的指标保持原英文名

## 🔍 文件编码

- 使用 **UTF-8-BOM** 编码保存
- 确保中文字符在Excel等软件中正确显示
- 支持各种数据分析工具导入

## ✨ 特色功能

1. **完整覆盖**：涵盖所有650+个指标的中文标签
2. **智能映射**：自动识别指标类型并生成合适标签
3. **格式规范**：标准的三行格式便于数据处理
4. **编码兼容**：支持Excel、Python、R等工具直接读取

## 📝 使用建议

1. **Excel导入**：直接用Excel打开CSV文件，第二行的中文标签便于理解
2. **数据分析**：在Python中可以跳过第二行读取数据，或将其作为列描述
3. **报告制作**：中文标签便于制作中文分析报告
4. **团队协作**：中文标签降低了沟通成本

## 🎉 测试验证

运行以下命令可以快速测试功能：
```bash
python quick_test_labels.py
```

这将生成一个小的测试文件，验证中文标签功能是否正常工作。

---

**总计约650+个指标，全部支持中文标签！** 🎊 