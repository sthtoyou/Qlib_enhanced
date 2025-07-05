# 财务数据下载功能使用指南

## 📋 功能概述

`scripts/get_data.py` 现已支持完整的财务数据下载，包括：

### 🏢 基本面信息
- **公司基本信息** (Company Info)
- **股息分红数据** (Dividends)
- **股票拆分记录** (Stock Splits)
- **分析师推荐** (Analyst Recommendations)
- **机构持股情况** (Institutional Holdings)

### 📊 财务报表
- **损益表** (Income Statement/Financials)
- **资产负债表** (Balance Sheet)
- **现金流量表** (Cash Flow Statement)
- **盈利数据** (Earnings)

### 📈 财务比率计算
- **估值比率**：市净率、市盈率、市销率
- **盈利能力**：净利润率、ROE、ROA
- **偿债能力**：资产负债率、流动比率、速动比率
- **运营效率**：资产周转率、库存周转率
- **托宾Q值**：市值/资产总额

## 📊 **各市场股票数量说明**

### 🇺🇸 **美股市场**
- **数据来源**: 标普500成分股（S&P 500）
- **获取方式**: 动态从Wikipedia获取，失败时使用内置列表
- **股票数量**: 约250只精选成分股
- **包含行业**: 科技、金融、医疗、消费、工业、能源等11个行业

### 🇭🇰 **港股市场**  
- **数据来源**: 恒生指数成分股
- **股票数量**: 约80只
- **代码格式**: xxxx.HK (如：0700.HK为腾讯)

### 🇨🇳 **A股市场**
- **数据来源**: 沪深300 + 中证500成分股
- **获取方式**: 通过baostock动态获取，失败时使用内置列表
- **股票数量**: 约800只（300+500）
- **代码格式**: xxxxxx.SS/.SZ (如：000001.SZ为平安银行)

## ⚙️ **max_stocks 参数使用说明**

### 🎯 **推荐设置**

| 使用场景 | 推荐设置 | 预计时间 | 说明 |
|---------|---------|---------|------|
| **首次测试** | `max_stocks=5` | 1-2分钟 | 验证功能，快速完成 |
| **小规模研究** | `max_stocks=20-50` | 5-15分钟 | 适合个人研究分析 |
| **中规模分析** | `max_stocks=100-200` | 30-60分钟 | 行业或主题研究 |
| **完整数据集** | `max_stocks=None` | 2-6小时 | 下载所有可用股票 |

### ⚠️ **重要限制说明**

**之前版本问题**: 旧版本硬编码限制股票数量：
- 美股：50只 ❌
- 港股：30只 ❌  
- A股：30只 ❌

**现在已修复**: 新版本完全支持：
- 美股：250只标普500成分股 ✅
- 港股：80只恒生指数成分股 ✅
- A股：800只沪深300+中证500 ✅

## 🚀 使用方法

### 1. 基础财务数据下载

```bash
# 下载所有美股财务数据（约250只标普500成分股）
python scripts/get_data.py download_financial_data --region=us

# 限制下载数量（建议首次使用）
python scripts/get_data.py download_financial_data --region=us --max_stocks=10

# 下载港股财务数据（约80只恒生指数成分股）
python scripts/get_data.py download_financial_data --region=hk --target_dir="./hk_financial_data"

# 下载中国A股财务数据（约800只沪深300+中证500）
python scripts/get_data.py download_financial_data --region=cn --target_dir="./cn_financial_data" --max_stocks=50
```

### 2. 指定数据类型

```bash
# 只下载基本信息和财务报表
python scripts/get_data.py download_financial_data \
    --region=us \
    --data_types="['info', 'financials']" \
    --target_dir="./basic_data"

# 下载完整财务数据集
python scripts/get_data.py download_financial_data \
    --region=us \
    --data_types="['info', 'financials', 'balance_sheet', 'cashflow', 'dividends', 'splits']" \
    --save_format=csv \
    --include_ratios=True
```

### 3. 指定股票列表

```bash
# 下载指定股票的财务数据
python scripts/get_data.py download_financial_data \
    --region=us \
    --stock_symbols="['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']" \
    --target_dir="./tech_stocks_financial"
```

### 4. 高级配置

```bash
# 高性能并行下载
python scripts/get_data.py download_financial_data \
    --region=us \
    --max_workers=10 \
    --save_format=pickle \
    --include_ratios=True \
    --target_dir="./financial_data_advanced"
```

### 5. 基本面分析数据

```bash
# 下载并分析基本面数据
python scripts/get_data.py download_fundamental_analysis_data \
    --region=us \
    --analysis_types="['valuation', 'profitability', 'liquidity']" \
    --include_technical_indicators=True
```

## 📁 数据目录结构

下载完成后，数据将按以下结构组织：

```
~/.qlib/financial_data/
├── info/                    # 公司基本信息
│   ├── AAPL.csv
│   ├── MSFT.csv
│   └── ...
├── financials/              # 财务报表（损益表）
│   ├── AAPL.csv
│   ├── MSFT.csv
│   └── ...
├── balance_sheet/           # 资产负债表
│   ├── AAPL.csv
│   ├── MSFT.csv
│   └── ...
├── cashflow/               # 现金流量表
│   ├── AAPL.csv
│   ├── MSFT.csv
│   └── ...
├── dividends/              # 股息数据
│   ├── AAPL.csv
│   ├── MSFT.csv
│   └── ...
├── financial_ratios/       # 计算的财务比率
│   ├── AAPL.csv
│   ├── MSFT.csv
│   └── ...
└── splits/                 # 股票拆分记录
    ├── AAPL.csv
    ├── MSFT.csv
    └── ...
```

## 📊 支持的数据类型详解

### 1. **info** - 公司基本信息
```python
# 包含字段示例：
{
    'marketCap': 3000000000000,      # 市值
    'enterpriseValue': 2950000000000, # 企业价值
    'trailingPE': 25.5,              # 市盈率
    'forwardPE': 23.2,               # 预期市盈率
    'priceToBook': 45.8,             # 市净率
    'priceToSalesTrailing12Months': 7.8, # 市销率
    'profitMargins': 0.258,          # 利润率
    'operatingMargins': 0.302,       # 营业利润率
    'returnOnAssets': 0.203,         # 资产回报率
    'returnOnEquity': 1.504,         # 净资产回报率
    'revenueGrowth': 0.081,          # 收入增长率
    'earningsGrowth': 0.252,         # 盈利增长率
    'currentRatio': 1.067,           # 流动比率
    'quickRatio': 0.825,             # 速动比率
    'debtToEquity': 261.446,         # 资产负债率
    'totalCashPerShare': 3.84,       # 每股现金
    'bookValue': 4.17,               # 每股净资产
    'sharesOutstanding': 15550061568, # 流通股数
    'floatShares': 15550061568,      # 自由流通股
}
```

### 2. **financials** - 财务报表（损益表）
```python
# 主要字段：
{
    'Total Revenue': 394328000000,        # 总收入
    'Cost Of Revenue': 223546000000,      # 收入成本
    'Gross Profit': 170782000000,         # 毛利润
    'Operating Income': 119437000000,     # 营业收入
    'Net Income': 99803000000,            # 净利润
    'EBITDA': 130541000000,               # 息税折旧摊销前利润
    'Operating Revenue': 394328000000,     # 营业收入
    'Total Operating Expenses': 274891000000, # 总营业费用
    'Interest Expense': 2931000000,        # 利息费用
    'Income Tax Expense': 19300000000,     # 所得税费用
}
```

### 3. **balance_sheet** - 资产负债表
```python
# 主要字段：
{
    'Total Assets': 365725000000,          # 总资产
    'Current Assets': 143566000000,        # 流动资产
    'Non Current Assets': 222159000000,    # 非流动资产
    'Total Liabilities Net Minority Interest': 302083000000, # 总负债
    'Current Liabilities': 134836000000,   # 流动负债
    'Long Term Debt': 106550000000,        # 长期债务
    'Stockholders Equity': 63090000000,    # 股东权益
    'Retained Earnings': 5562000000,       # 留存收益
    'Treasury Shares Number': -2674000000, # 库存股数量
    'Ordinary Shares Number': 15550061568, # 普通股数量
}
```

### 4. **cashflow** - 现金流量表
```python
# 主要字段：
{
    'Operating Cash Flow': 122151000000,   # 经营现金流
    'Investing Cash Flow': -22354000000,   # 投资现金流
    'Financing Cash Flow': -113579000000,  # 筹资现金流
    'End Cash Position': 29965000000,      # 期末现金余额
    'Free Cash Flow': 99584000000,         # 自由现金流
    'Capital Expenditure': -22085000000,   # 资本支出
    'Issuance Of Debt': 5985000000,        # 债务发行
    'Repayment Of Debt': -11151000000,     # 债务偿还
    'Repurchase Of Capital Stock': -85971000000, # 股票回购
}
```

### 5. **financial_ratios** - 计算的财务比率
```python
# 自动计算的财务比率：
{
    'MarketCap': 3000000000000,            # 市值
    'PriceToBook': 45.8,                   # 市净率
    'SharesOutstanding': 15550061568,      # 流通股数
    'NetProfitMargin': 0.253,              # 净利润率
    'PriceToSales': 7.61,                  # 市销率
    'PriceToEarnings': 30.07,              # 市盈率
    'DebtToAssets': 0.827,                 # 资产负债率
    'DebtToEquity': 1.690,                 # 债务权益比
    'CurrentRatio': 1.065,                 # 流动比率
    'TobinsQ': 8.208,                      # 托宾Q值
    'CalculationDate': '2025-01-05 10:30:00' # 计算时间
}
```

## ⚡ 性能优化建议

### 1. 并行下载优化
```bash
# 根据网络状况调整并行线程数
--max_workers=5    # 网络较慢时使用
--max_workers=10   # 网络良好时使用  
--max_workers=15   # 高速网络时使用
```

### 2. 数据格式选择
```bash
# CSV格式：人类可读，占用空间大
--save_format=csv

# Pickle格式：Python专用，速度快，占用空间小
--save_format=pickle

# JSON格式：跨平台兼容，中等大小
--save_format=json
```

### 3. 分批下载
```python
# 对于大量股票，建议分批下载
stock_batches = [
    ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'],
    ['META', 'NVDA', 'NFLX', 'ADBE', 'CRM'],
    # ... 更多批次
]

for i, batch in enumerate(stock_batches):
    print(f"下载第 {i+1} 批股票...")
    # 执行下载命令
```

## 🔧 Python API 使用

```python
from tests.data import GetDataEnhanced

# 创建数据下载器
downloader = GetDataEnhanced()

# 下载财务数据
success = downloader.download_financial_data(
    target_dir="./my_financial_data",
    region="us", 
    data_types=['info', 'financials', 'balance_sheet', 'cashflow'],
    stock_symbols=['AAPL', 'MSFT', 'GOOGL'],
    max_workers=5,
    save_format="csv",
    include_ratios=True
)

# 下载基本面分析数据
success = downloader.download_fundamental_analysis_data(
    target_dir="./fundamental_analysis",
    region="us",
    analysis_types=['valuation', 'profitability', 'liquidity'],
    stock_symbols=['AAPL', 'MSFT'],
    include_technical_indicators=True
)
```

## ⚠️ 注意事项

### 1. API限制
- **Yahoo Finance API** 有访问频率限制
- 建议在请求间添加适当延迟
- 避免在短时间内大量请求

### 2. 数据质量
- 财务数据来源于Yahoo Finance
- 数据可能存在延迟或错误
- 建议与官方财务报告进行交叉验证

### 3. 存储空间
- 完整财务数据集可能占用大量磁盘空间
- 建议定期清理旧数据
- 考虑使用数据压缩

### 4. 网络依赖
- 需要稳定的网络连接
- 下载大量数据时建议使用有线网络
- 支持断点续传和增量更新

## 🎯 实际应用案例

### 案例1：价值投资筛选
```bash
# 下载大盘股财务数据用于价值投资分析
python scripts/get_data.py download_financial_data \
    --region=us \
    --data_types="['info', 'financials', 'balance_sheet']" \
    --include_ratios=True \
    --target_dir="./value_investing_data"
```

### 案例2：行业分析
```bash
# 下载科技股财务数据进行行业对比
python scripts/get_data.py download_financial_data \
    --region=us \
    --stock_symbols="['AAPL', 'MSFT', 'GOOGL', 'META', 'NVDA', 'AMZN']" \
    --data_types="['info', 'financials', 'balance_sheet', 'cashflow']" \
    --target_dir="./tech_sector_analysis"
```

### 案例3：股息投资策略
```bash
# 专门下载股息数据进行收益投资分析
python scripts/get_data.py download_financial_data \
    --region=us \
    --data_types="['info', 'dividends', 'financials']" \
    --target_dir="./dividend_investing_data"
```

## 🔄 数据更新策略

### 定期更新
```bash
# 设置cron任务定期更新财务数据
# 每周日凌晨2点更新
0 2 * * 0 python /path/to/scripts/get_data.py download_financial_data --region=us
```

### 增量更新
```python
# 使用增量更新模式，只下载新数据
downloader.download_financial_data(
    target_dir="./financial_data",
    region="us",
    incremental_update=True  # 启用增量更新
)
```

---

## 📞 技术支持

如果您在使用过程中遇到问题，请：

1. 检查网络连接状态
2. 确认yfinance库已正确安装
3. 验证股票代码格式是否正确
4. 查看日志文件了解详细错误信息

**祝您投资顺利！** 🚀📈 