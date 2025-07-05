# Qlib数据下载增强版说明

## 概述

已成功将美股数据格式修正功能整合到 `scripts/get_data.py` 中，现在支持：

### ✅ 新增功能

1. **自动标准格式转换**：美股数据自动转换为标准qlib格式
2. **增量更新支持**：支持中国和美股数据的增量更新
3. **统一接口**：中美股票数据使用相同的命令行接口

## 使用方法

### 中国股票数据下载

```bash
# 基本下载
python scripts/get_data.py qlib_data --target_dir D:\stk_data\trd\cn_data --interval 1d --region cn

# 增量更新模式
python scripts/get_data.py qlib_data --target_dir D:\stk_data\trd\cn_data --interval 1d --region cn --trading_date 20240101 --end_date 20250601 --incremental_update True
```

### 美股数据下载（新功能）

```bash
# 基本下载
python scripts/get_data.py qlib_data --target_dir D:\stk_data\trd\us_data --interval 1d --region us --trading_date 19900101 --end_date 20250601

# 增量更新模式
python scripts/get_data.py qlib_data --target_dir D:\stk_data\trd\us_data --interval 1d --region us --trading_date 19900101 --end_date 20250601 --incremental_update True
```

## 新增参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--trading_date` | str | None | 开始日期 (YYYYMMDD格式) |
| `--end_date` | str | None | 结束日期 (YYYYMMDD格式) |
| `--incremental_update` | bool | False | 启用增量更新模式 |

## 数据格式

### 标准qlib格式（已修正）

```
target_dir/
├── calendars/
│   └── day.txt
├── features/
│   ├── aapl/                    # 每个股票一个目录
│   │   ├── open.day.bin         # 开盘价
│   │   ├── close.day.bin        # 收盘价
│   │   ├── high.day.bin         # 最高价
│   │   ├── low.day.bin          # 最低价
│   │   ├── volume.day.bin       # 成交量
│   │   ├── change.day.bin       # 涨跌幅
│   │   └── factor.day.bin       # 复权因子
│   └── msft/
│       └── ...
└── instruments/
    └── all.txt
```

## 增量更新机制

- **智能检测**：自动检测现有数据的最新日期
- **增量补充**：只下载缺失的数据段
- **数据合并**：自动合并新旧数据并去重
- **格式保持**：保持标准qlib二进制格式

## 支持的股票

### 美股
- 标准普尔500指数成分股（约145只）
- 包括：AAPL, MSFT, AMZN, GOOGL, META, TSLA等

### 中国股票
- 沪深A股（通过原有qlib数据源）

## 备份说明

- 原有 `tests/data.py` 已备份为 `tests/data_backup.py`
- 美股原始数据备份到 `features_backup/` 目录

## 依赖要求

```bash
pip install yfinance pandas numpy loguru tqdm
```

## 注意事项

1. **网络要求**：美股数据需要访问Yahoo Finance API
2. **速度控制**：自动添加延迟避免API限制
3. **错误处理**：自动跳过无法下载的股票
4. **格式验证**：自动验证生成的数据格式

## 故障排除

### yfinance未安装
```bash
pip install yfinance
```

### 网络连接问题
- 检查防火墙设置
- 尝试设置代理
- 减少并发下载数量

### 数据格式问题
- 增强版会自动修正为标准格式
- 可以手动运行格式验证

## 版本兼容性

- ✅ 向后兼容原有功能
- ✅ 保持原有API接口
- ✅ 支持所有原有参数
- ✅ 新增功能可选使用 