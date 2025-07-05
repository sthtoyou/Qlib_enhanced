# 🚀 三大指数实时数据下载功能

## 📊 支持的指数和数据下载方式

### 🎯 全面升级：三大核心指数全覆盖

| 市场 | 指数 | 股票数量 | 获取方式 | 数据源 |
|------|------|----------|----------|--------|
| 🇨🇳 **中国A股** | **沪深300** | **300只** | **实时获取** | **baostock API** |
| 🇺🇸 **美股** | **标普500** | **503只** | **实时获取** | **Wikipedia** |
| 🇭🇰 港股 | 恒生指数 | 81只 | 精选列表 | 固定列表 |

---

## 🇺🇸 美股标普500实时下载（新功能）

### 1️⃣ **标普500实时获取**

**特点**：
- ✅ **503只完整成分股**：覆盖标普500指数全部成分股
- ✅ **实时成分股更新**：通过Wikipedia自动获取最新调整
- ✅ **智能备用方案**：260+只精选美股确保稳定性
- ✅ **标准格式转换**：自动生成qlib标准格式

**使用方法**：
```bash
# 下载完整标普500成分股
python scripts/get_data.py qlib_data --target_dir D:\sp500_data --region us --trading_date 20240101 --end_date 20241231
```

### 📈 标普500股票范围

**🎉 实时获取标普500全部成分股！**

#### 🏢 覆盖全行业板块（503只）
- **科技巨头**：AAPL, MSFT, GOOGL, AMZN, META, TSLA, NVDA等
- **金融服务**：BRK.B, JPM, BAC, V, MA, AXP等
- **医疗保健**：UNH, JNJ, PFE, ABBV, LLY等  
- **消费品牌**：WMT, HD, MCD, NKE, COST等
- **工业制造**：BA, CAT, GE, MMM, HON等
- **能源公司**：XOM, CVX, COP, EOG等
- **公用事业**：NEE, DUK, SO等
- **房地产**：AMT, PLD, CCI等

#### 📊 获取机制
1. **Wikipedia实时获取**：从官方维基百科页面获取最新成分股
2. **智能清理验证**：自动处理股票代码格式（支持BRK.B等特殊格式）
3. **多重验证**：验证知名股票确保数据质量
4. **备用方案**：网络异常时使用260+只精选美股列表

---

## 🇨🇳 中国A股沪深300实时下载

### 1️⃣ 官方数据包（默认方式）

**特点**：
- ✅ **数据质量高**：经过专业清洗和验证
- ✅ **下载快速**：预打包ZIP文件，一次性下载
- ✅ **完整覆盖**：包含全市场A股数据
- ❌ **更新滞后**：取决于数据包发布周期
- ❌ **无增量更新**：每次全量替换
- ❌ **版本依赖**：与qlib版本绑定

**使用方法**：
```bash
# 默认方式：使用官方数据包
python scripts/get_data.py qlib_data --target_dir D:\stk_data\trd\cn_data --region cn
```

### 2️⃣ 沪深300实时下载（推荐）

**特点**：
- ✅ **实时性强**：获取最新交易数据
- ✅ **增量更新**：智能补充缺失数据
- ✅ **灵活时间范围**：可指定具体日期范围
- ✅ **标准格式**：自动转换为qlib标准格式
- ✅ **完整覆盖**：300只沪深300成分股
- ✅ **智能更新**：成分股调整自动同步

**使用方法**：
```bash
# 推荐方式：沪深300实时下载
python scripts/get_data.py qlib_data --target_dir D:\hs300_data --region cn --cn_realtime True --incremental_update True
```

## 🔧 详细参数说明

### 基础参数
- `--target_dir`: 数据存储目录
- `--region`: 市场选择 (`cn`中国, `us`美国, `hk`港股)
- `--trading_date`: 开始日期（YYYYMMDD格式）
- `--end_date`: 结束日期（YYYYMMDD格式）
- `--incremental_update`: 启用增量更新

### 中国A股专用参数
- `--cn_realtime True`: 启用A股实时下载（默认False使用官方数据包）

**🎉 最新升级：现在支持完整的沪深300成分股！**

### 🚀 实时获取沪深300
- ✅ **300只完整成分股**：自动获取官方最新沪深300指数成分股
- ✅ **智能成分股更新**：使用baostock实时获取，确保成分股变更时自动更新
- ✅ **覆盖全板块**：包含192只沪市股票 + 108只深市股票

### 🏛️ 沪市成分股（192只）
**大盘蓝筹**：
- 贵州茅台(600519.SS)、中国平安(601318.SS)、招商银行(600036.SS)
- 工商银行(601398.SS)、建设银行(601939.SS)、农业银行(601288.SS)
- 中国银行(601988.SS)、中国石化(600028.SS)、中国石油(601857.SS)

**科技龙头**：
- 恒瑞医药(600276.SS)、长江电力(600900.SS)、上汽集团(600104.SS)
- 浦发银行(600000.SS)、上海机场(600009.SS)、华能国际(600011.SS)

### 🏢 深市成分股（108只）
**创新企业**：
- 万科A(000002.SZ)、平安银行(000001.SZ)、五粮液(000858.SZ)
- 美的集团(000333.SZ)、格力电器(000651.SZ)、海康威视(002415.SZ)

**创业板重点**：
- 宁德时代(300750.SZ)、爱尔眼科(300015.SZ)、东方财富(300059.SZ)
- 智飞生物(300122.SZ)、迈瑞医疗(300760.SZ)、温氏股份(300498.SZ)

### 📊 获取机制
1. **baostock实时获取**：调用官方API获取最新沪深300成分股
2. **自动格式转换**：sz.000001 → 000001.SZ，sh.600000 → 600000.SS
3. **智能备用方案**：网络异常时使用280+只精选A股列表

## ⚡ 使用示例

### 场景1：下载完整指数数据

#### 美股标普500
```bash
# 🎯 推荐：下载完整标普500成分股（503只）
python scripts/get_data.py qlib_data --target_dir D:\sp500_data --region us --trading_date 20240101 --end_date 20241231

# 增量更新标普500
python scripts/get_data.py qlib_data --target_dir D:\sp500_data --region us --incremental_update True
```

#### 中国沪深300
```bash
# 🎯 推荐：下载完整沪深300成分股（300只）
python scripts/get_data.py qlib_data --target_dir D:\hs300_data --region cn --cn_realtime True --trading_date 20240101 --end_date 20241231

# 或使用官方数据包（如果可用）
python scripts/get_data.py qlib_data --target_dir D:\stk_data\trd\cn_data --region cn
```

### 场景2：增量更新指数数据

#### 美股标普500
```bash
# ✅ 智能增量更新（只下载缺失的新数据）
python scripts/get_data.py qlib_data --target_dir D:\sp500_data --region us --incremental_update True
```

#### 中国沪深300
```bash
# ✅ 智能增量更新（只下载缺失的新数据）
python scripts/get_data.py qlib_data --target_dir D:\hs300_data --region cn --cn_realtime True --incremental_update True

# ❌ 官方数据包不支持增量更新，每次全量替换
```

### 场景3：获取特定时间段数据
```bash
# 下载2024年的标普500数据
python scripts/get_data.py qlib_data --target_dir D:\sp500_data --region us --trading_date 20240101 --end_date 20241231

# 下载2024年的沪深300数据
python scripts/get_data.py qlib_data --target_dir D:\hs300_data --region cn --cn_realtime True --trading_date 20240101 --end_date 20241231
```

## 🔄 数据更新频率对比

| 下载方式 | 数据来源 | 更新频率 | 最新程度 | 股票数量 |
|---------|----------|----------|----------|----------|
| A股官方数据包 | GitHub Release | 不定期 | 可能滞后数月 | 全市场 |
| **沪深300实时下载** | **baostock + Yahoo Finance** | **每日更新** | **T+1日可获得** | **300只沪深300** |
| **标普500实时下载** | **Wikipedia + Yahoo Finance** | **每日更新** | **T+1日可获得** | **503只标普500** |

## 💡 推荐使用策略

### 🎯 推荐方案：指数实时下载
**最佳选择**：专注于核心指数投资和研究

#### 美股投资策略
- ✅ **标普500覆盖**：503只成分股，代表美国股市80%+市值
- ✅ **行业全覆盖**：科技、金融、医疗、消费、工业等11大板块
- ✅ **数据新鲜**：T+1日获取最新交易数据
- ✅ **成分股同步**：标普调整自动更新

#### 中国A股策略  
- ✅ **沪深300覆盖**：300只成分股，代表中国A股市场70%+市值
- ✅ **数据新鲜**：T+1日获取最新交易数据
- ✅ **智能更新**：成分股调整自动同步
- ✅ **质量保证**：多重数据源验证

### 🔄 日常使用建议
#### 美股
- **首次下载**：`python scripts/get_data.py qlib_data --target_dir D:\sp500_data --region us`
- **增量更新**：`python scripts/get_data.py qlib_data --target_dir D:\sp500_data --region us --incremental_update True`

#### A股
- **首次下载**：`python scripts/get_data.py qlib_data --target_dir D:\hs300_data --region cn --cn_realtime True`
- **增量更新**：`python scripts/get_data.py qlib_data --target_dir D:\hs300_data --region cn --cn_realtime True --incremental_update True`

### 📊 不同需求建议
- **标普500策略研究**：使用标普500实时下载（推荐）
- **沪深300策略研究**：使用沪深300实时下载（推荐）
- **跨市场配置**：同时下载标普500和沪深300
- **指数增强策略**：使用对应指数实时下载
- **因子挖掘**：使用指数下载（覆盖主要市值）

## ⚠️ 注意事项

1. **网络要求**：实时下载需要稳定的网络连接（国内外均可）
2. **时间成本**：
   - 标普500（503只）：约60-90分钟
   - 沪深300（300只）：约40-60分钟
3. **存储空间**：实时下载会同时保存CSV和二进制格式
   - 标普500：约3-4GB
   - 沪深300：约2-3GB
4. **数据范围**：指数覆盖对应市场主要市值，适合大部分投资策略

## 🆘 故障排除

### 问题1：实时下载失败
```bash
# 检查网络连接
ping wikipedia.org
ping finance.yahoo.com

# 重新尝试
python scripts/get_data.py qlib_data --target_dir D:\data --region us --incremental_update True
```

### 问题2：官方数据包404错误（A股）
```bash
# 使用沪深300实时下载替代
python scripts/get_data.py qlib_data --target_dir D:\hs300_data --region cn --cn_realtime True --trading_date 20200101
```

### 问题3：数据格式问题
```bash
# 检查目录结构
ls -la D:\sp500_data\features\
ls -la D:\hs300_data\features\

# 应该看到如下结构：
# features/
# ├── aapl/               # 苹果（美股）
# │   ├── open.day.bin
# │   ├── close.day.bin
# │   └── ...
# ├── 600000_ss/          # 浦发银行（A股）  
# │   ├── open.day.bin
# │   └── ...
# └── ...
```

---

**🎉 现在您可以选择最适合的指数数据下载方式，覆盖中美两大核心市场！** 