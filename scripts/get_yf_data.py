import yfinance as yf
import pandas as pd
import numpy as np
import talib
import datetime
import os 
import logging
import argparse
import sys
from typing import Dict, List, Optional, Tuple, Union
from requests.exceptions import HTTPError
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 忽略警告
warnings.filterwarnings('ignore')
data_dir = 'E:\\tushare_data' 
data_dir_daily = 'E:\\tushare_data\\daily' 
data_dir_index = 'E:\\tushare_data\\index' 


#####################################################################################
# 计算技术指标
def calculate_technical_indicators(data, market_data=None, other_data=None):
    indicators = {}
    
    # 使用 finta 库计算技术指标
    indicators['SMA'] = talib.SMA(data['Close'], timeperiod=20)
    indicators['EMA'] = talib.EMA(data['Close'], timeperiod=20)
    indicators['RSI'] = talib.RSI(data['Close'], timeperiod=14)
    indicators['Upper Band'], data['Middle Band'], data['Lower Band'] = talib.BBANDS(data['Close'], timeperiod=20)
    indicators['HT_DCPERIOD'] = talib.HT_DCPERIOD(data['Close'])
    indicators['HT_DCPHASE'] = talib.HT_DCPHASE(data['Close'])
    indicators['INPHASE'], data['QUADRATURE'] = talib.HT_PHASOR(data['Close'])
    indicators['SINE'], data['LEADSINE'] = talib.HT_SINE(data['Close'])
    indicators['HT_TRENDMODE'] = talib.HT_TRENDMODE(data['Close'])
    indicators['MAXINDEX'] = talib.MAXINDEX(data['Close'], timeperiod=30)
    indicators['MININDEX'] = talib.MININDEX(data['Close'], timeperiod=30)
    indicators['ADX'] = talib.ADX(data['High'], data['Low'], data['Close'], timeperiod=14)
    indicators['ADXR'] = talib.ADXR(data['High'], data['Low'], data['Close'], timeperiod=14)
    indicators['APO'] = talib.APO(data['Close'], fastperiod=12, slowperiod=26)
    indicators['AROON_DOWN'], data['AROON_UP'] = talib.AROON(data['High'], data['Low'], timeperiod=14)
    indicators['AROONOSC'] = talib.AROONOSC(data['High'], data['Low'], timeperiod=14)
    indicators['BOP'] = talib.BOP(data['Open'], data['High'], data['Low'], data['Close'])
    indicators['CCI'] = talib.CCI(data['High'], data['Low'], data['Close'], timeperiod=14)
    indicators['CMO'] = talib.CMO(data['Close'], timeperiod=14)
    indicators['DX'] = talib.DX(data['High'], data['Low'], data['Close'], timeperiod=14)
    indicators['MACD'], data['MACDSIGNAL'], data['MACDHIST'] = talib.MACD(data['Close'], fastperiod=12, slowperiod=26, signalperiod=9)
    indicators['MACDEXT'], _, _ = talib.MACDEXT(data['Close'], fastperiod=12, slowperiod=26, signalperiod=9)
    indicators['MACDFIX'], _, _ = talib.MACDFIX(data['Close'], signalperiod=9)
    indicators['MFI'] = talib.MFI(data['High'], data['Low'], data['Close'], data['Volume'], timeperiod=14)
    indicators['MINUS_DI'] = talib.MINUS_DI(data['High'], data['Low'], data['Close'], timeperiod=14)
    indicators['MINUS_DM'] = talib.MINUS_DM(data['High'], data['Low'], timeperiod=14)
    indicators['MOM'] = talib.MOM(data['Close'], timeperiod=10)
    indicators['PLUS_DI'] = talib.PLUS_DI(data['High'], data['Low'], data['Close'], timeperiod=14)
    indicators['PLUS_DM'] = talib.PLUS_DM(data['High'], data['Low'], timeperiod=14)
    indicators['PPO'] = talib.PPO(data['Close'], fastperiod=12, slowperiod=26)
    indicators['ROC'] = talib.ROC(data['Close'], timeperiod=10)
    indicators['ROCP'] = talib.ROCP(data['Close'], timeperiod=10)
    indicators['ROCR'] = talib.ROCR(data['Close'], timeperiod=10)
    indicators['ROCR100'] = talib.ROCR100(data['Close'], timeperiod=10)
    indicators['STOCH_SLOWK'], data['STOCH_SLOWD'] = talib.STOCH(data['High'], data['Low'], data['Close'])
    indicators['STOCHF_FASTK'], data['STOCHF_FASTD'] = talib.STOCHF(data['High'], data['Low'], data['Close'])
    indicators['STOCHRSI_FASTK'], data['STOCHRSI_FASTD'] = talib.STOCHRSI(data['Close'], timeperiod=14)
    indicators['TRIX'] = talib.TRIX(data['Close'], timeperiod=30)
    indicators['ULTOSC'] = talib.ULTOSC(data['High'], data['Low'], data['Close'], timeperiod1=7, timeperiod2=14, timeperiod3=28)
    indicators['WILLR'] = talib.WILLR(data['High'], data['Low'], data['Close'], timeperiod=14)
    indicators['DEMA'] = talib.DEMA(data['Close'], timeperiod=30)
    indicators['HT_TRENDLINE'] = talib.HT_TRENDLINE(data['Close'])
    indicators['KAMA'] = talib.KAMA(data['Close'], timeperiod=30)
    indicators['MA'] = talib.MA(data['Close'], timeperiod=30)
    indicators['MAMA'], data['FAMA'] = talib.MAMA(data['Close'])
    indicators['MIDPOINT'] = talib.MIDPOINT(data['Close'], timeperiod=14)
    indicators['MIDPRICE'] = talib.MIDPRICE(data['High'], data['Low'], timeperiod=14)
    
    indicators['AVGPRICE'] = talib.AVGPRICE(data['Open'], data['High'], data['Low'], data['Close'])
    indicators['MEDPRICE'] = talib.MEDPRICE(data['High'], data['Low'])
    indicators['TYPPRICE'] = talib.TYPPRICE(data['High'], data['Low'], data['Close'])
    indicators['WCLPRICE'] = talib.WCLPRICE(data['High'], data['Low'], data['Close'])
    
    if market_data is not None:
        indicators['BETA'] = talib.BETA(data['Close'], market_data['Close'], timeperiod=5)
    
    if other_data is not None:
        indicators['CORREL'] = talib.CORREL(data['Close'], other_data['Close'], timeperiod=30)
    
    indicators['LINEARREG'] = talib.LINEARREG(data['Close'], timeperiod=14)
    indicators['LINEARREG_ANGLE'] = talib.LINEARREG_ANGLE(data['Close'], timeperiod=14)
    indicators['LINEARREG_INTERCEPT'] = talib.LINEARREG_INTERCEPT(data['Close'], timeperiod=14)
    indicators['LINEARREG_SLOPE'] = talib.LINEARREG_SLOPE(data['Close'], timeperiod=14)
    indicators['STDDEV'] = talib.STDDEV(data['Close'], timeperiod=30)
    indicators['TSF'] = talib.TSF(data['Close'], timeperiod=14)
    indicators['VAR'] = talib.VAR(data['Close'], timeperiod=30)
    
    indicators['ATR'] = talib.ATR(data['High'], data['Low'], data['Close'], timeperiod=14)
    indicators['NATR'] = talib.NATR(data['High'], data['Low'], data['Close'], timeperiod=14)
    indicators['TRANGE'] = talib.TRANGE(data['High'], data['Low'], data['Close'])
    
    indicators['AD']= talib.AD(data['High'], data['Low'], data['Close'] , data['Volume'])
    indicators['ADOSC']= talib.ADOSC(data['High'], data['Low'], data['Close'] , data['Volume'], fastperiod=3, slowperiod=10)
    indicators['OBV']= talib.OBV(data['Close'], data['Volume'])
    
    
    # 计算所有的蜡烛图形态指标
    candle_patterns = [
        'CDL2CROWS', 'CDL3BLACKCROWS', 'CDL3INSIDE', 'CDL3LINESTRIKE', 'CDL3OUTSIDE',
        'CDL3STARSINSOUTH', 'CDL3WHITESOLDIERS', 'CDLABANDONEDBABY', 'CDLADVANCEBLOCK',
        'CDLBELTHOLD', 'CDLBREAKAWAY', 'CDLCLOSINGMARUBOZU', 'CDLCONCEALBABYSWALL',
        'CDLCOUNTERATTACK', 'CDLDARKCLOUDCOVER', 'CDLDOJI', 'CDLDOJISTAR', 'CDLDRAGONFLYDOJI',
        'CDLENGULFING', 'CDLEVENINGDOJISTAR', 'CDLEVENINGSTAR', 'CDLGAPSIDESIDEWHITE',
        'CDLGRAVESTONEDOJI', 'CDLHAMMER', 'CDLHANGINGMAN', 'CDLHARAMI', 'CDLHARAMICROSS',
        'CDLHIGHWAVE', 'CDLHIKKAKE', 'CDLHIKKAKEMOD', 'CDLHOMINGPIGEON', 'CDLIDENTICAL3CROWS',
        'CDLINNECK', 'CDLINVERTEDHAMMER', 'CDLKICKING', 'CDLKICKINGBYLENGTH', 'CDLLADDERBOTTOM',
        'CDLLONGLEGGEDDOJI', 'CDLLONGLINE', 'CDLMARUBOZU', 'CDLMATCHINGLOW', 'CDLMATHOLD',
        'CDLMORNINGDOJISTAR', 'CDLMORNINGSTAR', 'CDLONNECK', 'CDLPIERCING', 'CDLRICKSHAWMAN',
        'CDLRISEFALL3METHODS', 'CDLSEPARATINGLINES', 'CDLSHOOTINGSTAR', 'CDLSHORTLINE',
        'CDLSPINNINGTOP', 'CDLSTALLEDPATTERN', 'CDLSTICKSANDWICH', 'CDLTAKURI', 'CDLTASUKIGAP',
        'CDLTHRUSTING', 'CDLTRISTAR', 'CDLUNIQUE3RIVER', 'CDLUPSIDEGAP2CROWS', 'CDLXSIDEGAP3METHODS'
    ]
    for pattern in candle_patterns:
        indicators[pattern] = getattr(talib, pattern)(data['Open'], data['High'], data['Low'], data['Close'])
    
    result = pd.concat([data, pd.DataFrame(indicators)], axis=1)
    
    return result

#计算普通股获利率
def calculate_common_stock_yield(history_data):
    """
    此函数用于计算普通股获利率

    参数:
    history_data (DataFrame): 包含收盘价和股息等数据的DataFrame

    返回:
    Series: 计算得到的普通股获利率
    """
    dividend = history_data['Dividends']  # 获取股息数据
    close_price = history_data['Close']  # 获取收盘价数据
    common_stock_yield = dividend / close_price  # 计算普通股获利率
    return common_stock_yield

# 计算市净率
def calculate_price_to_book_ratio(history_data, info_data):
    """
    此函数用于计算市净率

    参数:
    history_data (DataFrame): 包含收盘价等数据的DataFrame
    info_data (DataFrame): 包含每股净资产等数据的DataFrame

    返回:
    Series: 计算得到的市净率
    """
    
    # 提取每只股票对应的唯一bookValue值
    book_values = info_data.groupby('Ticker')['bookValue'].first()
    # 根据股票代码将bookValue值与history_data进行合并
    history_data = history_data.merge(book_values, on='Ticker', how='left')
    close_price = history_data['Close']  # 获取收盘价
    book_value_per_share = history_data['bookValue']  # 获取每股净资产
    price_to_book_ratio = close_price / book_value_per_share  # 计算市净率
    price_to_book_ratio.index = history_data.index
    return price_to_book_ratio

# 计算指定窗口天数的累计换手率的函数
def calculate_turnover(history_data, info_data, window_length):
    # 提取每只股票对应的唯一流通股数量
    float_shares = info_data.groupby('Ticker')['floatShares'].first()
    # 根据股票代码将流通股数量与 history_data 进行合并
    history_data = history_data.merge(float_shares, on='Ticker', how='left')
    history_data['TurnoverRate'] = history_data['Volume'] / history_data['floatShares']  # 计算每日换手率
    history_data['AccumulatedTurnover'] = history_data['TurnoverRate'].rolling(window=window_length).sum()  # 计算指定窗口长度的累计换手率
    history_data['AvgTurnover'] = history_data['TurnoverRate'].rolling(window=window_length).mean()  # 计算指定窗口长度的平均换手率
    
    return history_data

# 优化的换手率计算函数 - 计算所有窗口期的换手率指标
def calculate_all_turnover_metrics(history_data: pd.DataFrame, info_data: pd.DataFrame) -> pd.DataFrame:
    """
    计算所有窗口期的换手率指标，避免重复计算
    """
    try:
        result_data = history_data.copy()
        
        # 检查必要的数据
        if 'floatShares' not in info_data.columns or len(info_data['floatShares'].dropna()) == 0:
            logger.warning("缺少流通股数据，无法计算换手率")
            return result_data
        
        float_shares = info_data['floatShares'].iloc[0]
        if pd.isna(float_shares) or float_shares <= 0:
            logger.warning("流通股数据无效")
            return result_data
        
        # 计算日换手率
        result_data['DailyTurnover'] = result_data['Volume'] / float_shares
        
        # 计算不同窗口的累计和平均换手率
        windows = [1, 5, 10, 20, 30]
        for window in windows:
            result_data[f'turnover_c{window}d'] = result_data['DailyTurnover'].rolling(window=window).sum()
            if window > 1:  # 1天平均换手率就是日换手率本身
                result_data[f'turnover_m{window}d'] = result_data['DailyTurnover'].rolling(window=window).mean()
        
        return result_data
        
    except Exception as e:
        logger.error(f"计算换手率指标失败: {e}")
    return history_data

#正/负收益次数    
def calculate_return_counts(data):
    days_list = [5, 10, 30, 60, 90]
    for days in days_list:
        data['Return'] = data['Close'].pct_change()
        for date in data.index:
            current_data = data.loc[:date]
            negative_dates = current_data[current_data['Return'] < 0].index
            positive_dates = current_data[current_data['Return'] > 0].index
            if len(current_data) >= days and len(current_data) > 0:  # 增加对数据长度大于 0 的检查
                negative_count = len([d for d in negative_dates if d >= current_data.index[-days]])
                positive_count = len([d for d in positive_dates if d >= current_data.index[-days]])
            else:
                negative_count = 0
                positive_count = 0
            data.at[date, f'NegativeReturnCount_{days}d'] = negative_count
            data.at[date, f'PositiveReturnCount_{days}d'] = positive_count
    return data

#市值有形资产比
def calculate_market_to_tangible_assets_ratio(history_data, info_data, balance_sheet_data):
    """
    此函数用于计算市值有形资产比
    参数:
    history_data (DataFrame): 包含收盘价等数据的DataFrame
    info_data (DataFrame): 包含流通股数量等数据的DataFrame
    balance_sheet_data (DataFrame): 包含有形资产数据的DataFrame
    返回:
    Series: 计算得到的市值有形资产比
    """
    # 提取每只股票对应的唯一流通股数量
    shares_outstanding = info_data.groupby('Ticker')['sharesOutstanding'].first()
    # 根据股票代码将流通股数量与history_data进行合并
    history_data = history_data.merge(shares_outstanding, on='Ticker', how='left')
    market_value = history_data['Close'] * history_data['sharesOutstanding']  # 计算市值
    
    # 提取每只股票对应的唯一有形资产值
    tangible_asset_values = balance_sheet_data.groupby('Ticker')['Net Tangible Assets'].first()
    # 根据股票代码将有形资产值与history_data进行合并
    history_data = history_data.merge(tangible_asset_values, on='Ticker', how='left')
    tangible_asset_value = history_data['Net Tangible Assets']  # 获取有形资产值
    market_to_tangible_assets_ratio = market_value / tangible_asset_value  # 计算市值有形资产比
    return market_to_tangible_assets_ratio

#已实现波动率\已实现负半变差\已实现连续波动率\已实现正半变差 等指标
def calculate_realized_volatility(history_data, window_length):
    if history_data.empty:
        return pd.DataFrame()  # 返回一个空的 DataFrame
    # 计算实现波动率
    price_diff = history_data['Close'].diff()
    # 已实现波动率
    realized_volatility = price_diff.rolling(window=window_length).std() * (252**0.5)
    # 已实现负半变差
    negative_semi_deviation = price_diff[price_diff < 0].rolling(window=window_length).std() * (252**0.5)
    # 已实现连续波动率
    log_returns = np.log(history_data['Close'] / history_data['Close'].shift(1))
    continuous_volatility = log_returns.rolling(window=window_length).std() * (252**0.5)
    # 已实现正半变差
    positive_semi_deviation = price_diff[price_diff > 0].rolling(window=window_length).std() * (252**0.5)
    realized_volatility_metrics = pd.DataFrame({
        'RealizedVolatility': realized_volatility,
        'NegativeSemiDeviation': negative_semi_deviation,
        'ContinuousVolatility': continuous_volatility,
        'PositiveSemiDeviation': positive_semi_deviation
    })
    return realized_volatility_metrics

#托宾Q值 
def calculate_tobins_q(info_data, balance_sheet_data):
    # 确保 balance_sheet_data 包含所需的列
    if not balance_sheet_data.empty and 'Total Assets' in balance_sheet_data.columns and 'Total Debt' in balance_sheet_data.columns:
        # 提取公司市值
        market_value = info_data.get('marketCap', None)
        # 提取公司资产重置成本，假设用总资产来近似表示
        asset_replacement_cost = balance_sheet_data['Total Assets'].iloc[0] if not balance_sheet_data['Total Assets'].empty else None
        # 提取公司净资产，通常为总资产减去总负债
        total_assets = balance_sheet_data['Total Assets'].iloc[0] if not balance_sheet_data['Total Assets'].empty else None
        total_liabilities = balance_sheet_data['Total Debt'].iloc[0] if not balance_sheet_data['Total Debt'].empty else None
        net_assets = total_assets - total_liabilities if total_assets is not None and total_liabilities is not None else None
        # 计算托宾Q值C
        if market_value is not None and asset_replacement_cost is not None and asset_replacement_cost != 0:
            tobins_q_c = market_value / asset_replacement_cost
        else:
            tobins_q_c = None  # 如果任一数据缺失或资产重置成本为零，返回None
        # 计算托宾Q值D
        if market_value is not None and net_assets is not None and net_assets != 0:
            tobins_q_d = market_value / net_assets
        else:
            tobins_q_d = None  # 如果任一数据缺失或净资产为零，返回None
        return tobins_q_c, tobins_q_d
    else:
        print("Error: balance_sheet_data does not contain required columns or is empty")
        return None, None

#计算账面市值比      
def calculate_book_to_market_ratio(history_data, info_data):
    """
    此函数用于计算账面市值比

    参数:
    history_data (DataFrame): 包含历史收盘价（Close）和股票代码（Ticker）的数据
    info_data (DataFrame): 包含公司账面价值（bookValue）和股票代码（Ticker）的数据

    返回:
    book_to_market_ratio (Series): 计算得到的账面市值比的 Series
    """
    if history_data.empty:
        print("历史数据为空")
        return None
    if info_data.empty:
        print("信息数据为空")
        return None
    # 提取每只股票对应的唯一bookValue  每股净资产
    bookValue = info_data.groupby('Ticker')['bookValue'].first()
    history_data = history_data.merge(bookValue, on='Ticker', how='left')
    if 'bookValue' not in history_data.columns:
        print("合并后的数据中没有 bookValue 列")
        return None
    close_prices = history_data['Close']  
    book_values = history_data['bookValue']  
    # 处理可能的 NaN 值
    valid_indices = ~(np.isnan(close_prices) | np.isnan(book_values))
    close_prices = close_prices[valid_indices]
    book_values = book_values[valid_indices]
    if len(close_prices) == 0 or len(book_values) == 0:
        print("没有有效数据用于计算")
        return None
    book_to_market_ratio = pd.Series(close_prices / book_values, name='BookToMarketRatio')
    return book_to_market_ratio


#####################################################################################

# 获取美股列表（支持多种编码）
def get_us_stocks(us_stk_lst):
    """
    读取美股列表文件，自动处理编码问题
    """
    file_path = os.path.join(data_dir, us_stk_lst)
    
    # 尝试多种编码方式
    encodings_to_try = ['utf-8', 'gbk', 'gb2312', 'utf-8-sig', 'latin1']
    
    us_stocks = None
    used_encoding = None
    
    for encoding in encodings_to_try:
        try:
            logger.info(f"尝试使用 {encoding} 编码读取文件")
            us_stocks = pd.read_csv(file_path, encoding=encoding)
            used_encoding = encoding
            logger.info(f"成功使用 {encoding} 编码读取文件")
            break
        except UnicodeDecodeError:
            logger.warning(f"使用 {encoding} 编码读取失败，尝试下一种编码")
            continue
        except Exception as e:
            logger.error(f"使用 {encoding} 编码时发生其他错误: {e}")
            continue
    
    if us_stocks is None:
        raise ValueError(f"无法读取文件 {file_path}，已尝试的编码: {encodings_to_try}")
    
    # 显示文件信息
    logger.info(f"文件编码: {used_encoding}")
    logger.info(f"文件总行数: {len(us_stocks)}")
    logger.info(f"文件列名: {list(us_stocks.columns)}")
    
    # 过滤符合条件的股票
    try:
        # 检查可能的列名映射
        column_mappings = [
            # 方案1：标准格式 (StatusID, ExchangeCode)
            {'status': 'StatusID', 'exchange': 'ExchangeCode', 'symbol': 'Symbol', 'name': 'ENName'},
            # 方案2：当前文件格式 (symbol, exchange)
            {'status': None, 'exchange': 'exchange', 'symbol': 'symbol', 'name': 'shortName'},
            # 方案3：tushare格式
            {'status': None, 'exchange': 'exchange', 'symbol': 'ts_code', 'name': 'shortName'},
            # 方案4：通用格式
            {'status': None, 'exchange': 'exchange', 'symbol': 'Ticker', 'name': 'longName'}
        ]
        
        selected_mapping = None
        for mapping in column_mappings:
            required_cols = [col for col in mapping.values() if col is not None]
            if all(col in us_stocks.columns for col in required_cols):
                selected_mapping = mapping
                logger.info(f"使用列映射: {mapping}")
                break
        
        if selected_mapping is None:
            # 如果没有找到标准映射，尝试智能识别
            logger.warning("未找到标准列映射，尝试智能识别...")
            
            # 查找股票代码列
            symbol_candidates = ['symbol', 'Symbol', 'ts_code', 'Ticker', 'ticker']
            symbol_col = None
            for col in symbol_candidates:
                if col in us_stocks.columns:
                    symbol_col = col
                    break
            
            # 查找交易所列
            exchange_candidates = ['exchange', 'Exchange', 'ExchangeCode', 'market']
            exchange_col = None
            for col in exchange_candidates:
                if col in us_stocks.columns:
                    exchange_col = col
                    break
            
            # 查找名称列
            name_candidates = ['shortName', 'longName', 'ENName', 'name', 'Name']
            name_col = None
            for col in name_candidates:
                if col in us_stocks.columns:
                    name_col = col
                    break
            
            if symbol_col:
                selected_mapping = {
                    'status': None,
                    'exchange': exchange_col,
                    'symbol': symbol_col,
                    'name': name_col or symbol_col  # 如果没有名称列，使用symbol列作为名称
                }
                logger.info(f"智能识别的列映射: {selected_mapping}")
            else:
                logger.error("无法识别股票代码列")
                logger.info("建议的列名格式：")
                logger.info("- 股票代码列: 'symbol', 'Symbol', 'ts_code', 'Ticker'")
                logger.info("- 交易所列: 'exchange', 'Exchange', 'ExchangeCode'")
                logger.info("- 名称列: 'shortName', 'longName', 'ENName'")
                raise ValueError("无法识别文件格式")
        
        # 应用过滤条件
        us_stocks_filtered = us_stocks.copy()
        
        # 如果有状态列，过滤状态为0的股票
        if selected_mapping['status'] and selected_mapping['status'] in us_stocks.columns:
            us_stocks_filtered = us_stocks_filtered[us_stocks_filtered[selected_mapping['status']] == 0]
            logger.info(f"按状态过滤后的股票数量: {len(us_stocks_filtered)}")
        
        # 如果有交易所列，尝试过滤NASDAQ和NYSE
        if selected_mapping['exchange'] and selected_mapping['exchange'] in us_stocks.columns:
            # 先查看交易所分布
            exchange_counts = us_stocks_filtered[selected_mapping['exchange']].value_counts()
            logger.info(f"交易所列数据样本: {us_stocks_filtered[selected_mapping['exchange']].head(10).tolist()}")
            
            # 检查exchange列是否包含文本数据
            sample_values = us_stocks_filtered[selected_mapping['exchange']].dropna().astype(str).head(10)
            is_numeric_exchange = all(
                val.replace('.', '').replace('-', '').replace('e', '').replace('E', '').replace('+', '').isdigit() 
                for val in sample_values if val != 'nan'
            )
            
            if is_numeric_exchange:
                logger.warning("检测到exchange列包含数值数据，不是交易所名称，跳过交易所过滤")
                logger.info("将使用所有股票进行下载")
            else:
                logger.info("检测到exchange列包含文本数据，尝试按交易所过滤")
                
                # 灵活匹配交易所名称
                nasdaq_patterns = ['NASDAQ', 'nasdaq', 'XNAS', 'NMS']
                nyse_patterns = ['NYSE', 'nyse', 'XNYS', 'NYQ']
                
                exchange_values = us_stocks_filtered[selected_mapping['exchange']].astype(str).str.upper()
                nasdaq_mask = exchange_values.isin([p.upper() for p in nasdaq_patterns])
                nyse_mask = exchange_values.isin([p.upper() for p in nyse_patterns])
                
                before_filter = len(us_stocks_filtered)
                us_stocks_filtered = us_stocks_filtered[nasdaq_mask | nyse_mask]
                after_filter = len(us_stocks_filtered)
                
                logger.info(f"按交易所过滤：{before_filter} -> {after_filter} 只股票")
                
                if after_filter == 0:
                    logger.warning("交易所过滤后没有股票，使用所有股票")
                    us_stocks_filtered = us_stocks.copy()  # 恢复原始数据
        
        # 确保有Symbol列
        if selected_mapping['symbol'] and selected_mapping['symbol'] in us_stocks_filtered.columns:
            if selected_mapping['symbol'] != 'Symbol':
                us_stocks_filtered['Symbol'] = us_stocks_filtered[selected_mapping['symbol']]
            # 如果原来就是Symbol列，不需要重命名
        else:
            raise ValueError("无法找到股票代码列")
        
        # 确保有ENName列
        if (selected_mapping['name'] and 
            selected_mapping['name'] in us_stocks_filtered.columns and 
            selected_mapping['name'] != selected_mapping['symbol']):  # 避免重复使用同一列
            if selected_mapping['name'] != 'ENName':
                us_stocks_filtered['ENName'] = us_stocks_filtered[selected_mapping['name']]
        else:
            # 如果没有单独的名称列，使用Symbol列作为ENName
            us_stocks_filtered['ENName'] = us_stocks_filtered['Symbol']
        
        # 过滤掉空值
        us_stocks_filtered = us_stocks_filtered.dropna(subset=['Symbol'])
        us_stocks_filtered = us_stocks_filtered[us_stocks_filtered['Symbol'].astype(str).str.strip() != '']
        
        # 如果过滤后没有股票，但原始数据有symbol列，则使用前100只股票进行测试
        if len(us_stocks_filtered) == 0 and 'symbol' in us_stocks.columns:
            logger.warning("过滤后没有股票，使用前100只股票进行测试下载")
            us_stocks_filtered = us_stocks.head(100).copy()
            
            # 重新应用列映射
            if selected_mapping['symbol']:
                us_stocks_filtered['Symbol'] = us_stocks_filtered[selected_mapping['symbol']]
            if selected_mapping['name'] and selected_mapping['name'] in us_stocks_filtered.columns:
                us_stocks_filtered['ENName'] = us_stocks_filtered[selected_mapping['name']]
            else:
                us_stocks_filtered['ENName'] = us_stocks_filtered['Symbol']
            
            # 再次过滤空值
            us_stocks_filtered = us_stocks_filtered.dropna(subset=['Symbol'])
            us_stocks_filtered = us_stocks_filtered[us_stocks_filtered['Symbol'].astype(str).str.strip() != '']
        
        logger.info(f"最终过滤后的股票数量: {len(us_stocks_filtered)}")
        
        if len(us_stocks_filtered) == 0:
            logger.error("没有找到任何有效的股票代码")
            logger.info("请检查数据文件格式和过滤条件")
            logger.info("确保文件包含有效的股票代码列")
        else:
            logger.info(f"前5只股票: {us_stocks_filtered[['Symbol', 'ENName']].head().to_dict('records')}")
        
    return us_stocks_filtered
        
    except Exception as e:
        logger.error(f"过滤股票时发生错误: {e}")
        logger.info(f"文件前5行数据:\n{us_stocks.head()}")
        raise

def download_data(stock_list, data_type, start_date=None):
    stock_data = {}
    base_data = {}
    for index, row in stock_list.iterrows():
        ticker = row['Symbol']
#        print(f"开始处理{data_type}数据-股票:", ticker)
        if isinstance(ticker, str):
            ticker = ticker.split('.')[0]  # 提取ticker部分
            name = row['ENName']
            try:
                stock = yf.Ticker(ticker)
                # 先下载基础数据
                base_data['info'] = pd.DataFrame([stock.info]) if stock.info else pd.DataFrame()
                if not base_data['info'].empty:
                base_data['info']['Ticker'] = ticker  # 添加 Ticker 列到基础数据的 info 部分
                base_data['financials'] = stock.financials.transpose()
                base_data['financials']['Ticker'] = ticker  # 添加 Ticker 列
                base_data['balance_sheet'] = stock.balance_sheet.transpose()
                base_data['balance_sheet']['Ticker'] = ticker  # 添加 Ticker 列
                base_data['cashflow'] = stock.cashflow.transpose()
                base_data['cashflow']['Ticker'] = ticker  # 添加 Ticker 列
                if data_type == 'history':
                    data = stock.history(start=start_date) if start_date else stock.history(period='max')
                    data['Ticker'] = ticker  # 添加 Ticker 列到历史数据
                    if not data.empty:
                        price_to_book_ratio = calculate_price_to_book_ratio(history_data=data, info_data=base_data['info'])
                        price_to_book_ratio.index = data.index
                        data['PriceToBookRatio'] = price_to_book_ratio
                        #市值有形资产比
                        market_to_tangible_ratio = calculate_market_to_tangible_assets_ratio(history_data=data, info_data=base_data['info'], balance_sheet_data=base_data['balance_sheet'])
                        market_to_tangible_ratio.index = data.index
                        data['MarketToTangibleRatio'] = market_to_tangible_ratio
                        # 计算并添加 Realized Volatility 等指标
                        realized_volatility_metrics = calculate_realized_volatility(history_data=data, window_length=5)
                        if realized_volatility_metrics is not None:
                            data = data.join(realized_volatility_metrics, how='left')
                        else:
                            logger.warning(f"No realized volatility metrics returned for {ticker}")    
                        # 计算托宾Q值C和D（修复版）
                        tobins_q_c, tobins_q_d = calculate_tobins_q(base_data['info'], base_data['balance_sheet'])
                        if not base_data['info'].empty:
                            # 安全地赋值托宾Q值
                            if tobins_q_c is not None:
                                data['TobinsQC'] = tobins_q_c
                            if tobins_q_d is not None:
                                data['TobinsQD'] = tobins_q_d 
                        #计算账面市值比
                        book_to_market_ratio = calculate_book_to_market_ratio(history_data=data, info_data=base_data['info'])
                        book_to_market_ratio.index = data.index
                        data['BookToMarketRatio'] = book_to_market_ratio  
                        # 计算不同窗口长度的负收益次数
#                        return_counts = calculate_return_counts(data)
#                        for key, value in return_counts.items():
#                            data[key] = value  
                        # 计算技术指标                     	  	                    		                    		                	
                        data = calculate_technical_indicators(data)  
                        data = data.copy()                    	                  	
                        # 计算所有换手率指标（优化版）
                        data = calculate_all_turnover_metrics(data, base_data['info'])             	
                elif data_type == 'financials':
                    data = stock.financials.transpose()
                elif data_type == 'balance_sheet':
                    data = stock.balance_sheet.transpose()
                elif data_type == 'cashflow':
                    data = stock.cashflow.transpose()
                elif data_type == 'dividends':
                    data = stock.dividends
                elif data_type == 'splits':
                    data = stock.splits
                elif data_type == 'recommendations':
                    data = stock.recommendations
                    if not data.empty:
                        data.reset_index(inplace=True)  # 重置索引，将日期列转换为普通列
                        data.rename(columns={'index': 'Date'}, inplace=True)  # 将索引列重命名为 'Date'
                elif data_type == 'institutional_holders':
                    data = stock.institutional_holders
                    if not data.empty:
                        data.reset_index(inplace=True)  # 重置索引，将日期列转换为普通列
                        data.rename(columns={'index': 'Date'}, inplace=True)  # 将索引列重命名为 'Date'
                elif data_type == 'mutualfund_holders':
                    data = stock.mutualfund_holders
                    if not data.empty:
                        data.reset_index(inplace=True)  # 重置索引，将日期列转换为普通列
                        data.rename(columns={'index': 'Date'}, inplace=True)  # 将索引列重命名为 'Date'
                elif data_type == 'sustainability':
                    try:
                        data = stock.sustainability
                        if not data.empty:
                            data.reset_index(inplace=True)  # 重置索引，将日期列转换为普通列
                            data.rename(columns={'index': 'Date'}, inplace=True)  # 将索引列重命名为 'Date'                        
                    except Exception as e:
                        logger.warning(f"Sustainability data for {ticker} ({name}) is not available: {e}")
                        continue
                elif data_type == 'actions':
                    data = stock.actions
                    if not data.empty:
                        data.reset_index(inplace=True)  # 重置索引，将日期列转换为普通列
                        data.rename(columns={'index': 'Date'}, inplace=True)  # 将索引列重命名为 'Date'
                elif data_type == 'earnings':
                    data = stock.earnings.transpose()
                elif data_type == 'info':
                    data = pd.DataFrame([stock.info]) if stock.info else pd.DataFrame()  # 将info转换为DataFrame                
                else:
                    continue
                    
                if not data.empty:
                    if isinstance(data, pd.Series):
                        data = data.to_frame(name=data_type)
                    data['Stock Name'] = ticker
                    stock_data[ticker] = data
            except Exception as e:
                if "YFDataException" in str(type(e)) or "YFinanceException" in str(type(e)):
                    logger.warning(f"YFinance数据异常 {ticker} ({name}): {e}")
                elif isinstance(e, KeyError):
                    logger.warning(f"数据键值错误 {ticker} ({name}): {e}")
                elif isinstance(e, HTTPError):
                if e.response.status_code == 404:
                        logger.warning(f"股票代码未找到 {ticker} ({name}): HTTP 404")
                    else:
                        logger.error(f"HTTP错误 {ticker} ({name}): {e}")
                        raise
                else:
                    logger.error(f"处理 {ticker} ({name}) 时发生未知错误: {e}")
    return stock_data

# 将数据保存为CSV文件（优化版）
def save_data(stock_data: Dict[str, pd.DataFrame], data_type: str, out_dir: str, start_date: Optional[str] = None) -> None:
    """
    保存股票数据为CSV文件
    """
    if not stock_data:
        logger.warning("没有数据需要保存")
        return
    
    try:
        # 确保输出目录存在
        os.makedirs(out_dir, exist_ok=True)
        
        # 合并所有数据
    combined_data = pd.concat(
        [df.assign(Ticker=ticker) if isinstance(df, pd.DataFrame) else df.to_frame().assign(Ticker=ticker)
         for ticker, df in stock_data.items()],
            ignore_index=True
    )
        
        # 重新排列列的顺序
    if 'Stock Name' in combined_data.columns:
        columns = ['Stock Name'] + [col for col in combined_data.columns if col != 'Stock Name']
        combined_data = combined_data[columns]
        
        # 生成文件名
    file_name = f'us_{data_type}_{start_date}.csv' if start_date else f'us_{data_type}.csv'
        file_path = os.path.join(out_dir, file_name)
        
        # 保存文件
        combined_data.to_csv(file_path, index=True, encoding='utf-8')
        
        logger.info(f"数据已保存到: {file_path}")
        logger.info(f"总共保存了 {len(combined_data)} 条记录，来自 {len(stock_data)} 只股票")
        
    except Exception as e:
        logger.error(f"保存数据失败: {e}")
        raise

# 主函数（优化版）
def yf_dwload_main(us_stk_lst: str, data_type: str, out_dir: str, start_date: Optional[str] = None) -> None:
    """
    主函数：下载美股数据并保存
    
    参数:
        us_stk_lst: 股票列表文件名
        data_type: 数据类型
        out_dir: 输出目录
        start_date: 开始日期（可选）
    """
    try:
        logger.info(f"开始下载 {data_type} 数据")
        logger.info(f"股票列表文件: {us_stk_lst}")
        logger.info(f"输出目录: {out_dir}")
        
        # 获取股票列表
    us_stocks = get_us_stocks(us_stk_lst=us_stk_lst)
        if us_stocks.empty:
            logger.error("没有找到有效的股票列表")
            return
        
        logger.info(f"找到 {len(us_stocks)} 只符合条件的股票")
        
        # 下载数据
    stock_data = download_data(us_stocks, data_type, start_date)
        
        if not stock_data:
            logger.warning("没有成功下载任何股票数据")
            return
        
        logger.info(f"成功下载 {len(stock_data)} 只股票的数据")
        
        # 保存数据
        save_data(stock_data, data_type, start_date=start_date, out_dir=out_dir)
        
        logger.info("数据下载和保存完成")
        
    except Exception as e:
        logger.error(f"主函数执行失败: {e}")
        raise


# 支持并行下载的新版本主函数
def yf_download_parallel(us_stk_lst: str, data_type: str, out_dir: str, 
                        start_date: Optional[str] = None, max_workers: int = 5) -> None:
    """
    并行下载版本的主函数，提高下载效率
    
    参数:
        us_stk_lst: 股票列表文件名
        data_type: 数据类型
        out_dir: 输出目录
        start_date: 开始日期（可选）
        max_workers: 最大并行工作线程数
    """
    def download_single_stock(ticker_info):
        """下载单只股票的数据"""
        ticker, name = ticker_info
        try:
            ticker = str(ticker).split('.')[0]  # 提取ticker部分
            stock = yf.Ticker(ticker)
            
            # 获取基础信息
            info_data = pd.DataFrame([stock.info]) if stock.info else pd.DataFrame()
            
            if data_type == 'history':
                data = stock.history(start=start_date, period='max' if not start_date else None)
                if data.empty:
                    return None, None
                
                data['Ticker'] = ticker
                
                # 计算所有指标
                if not info_data.empty:
                    # 基础财务指标
                    financials_data = stock.financials.T if hasattr(stock, 'financials') and not stock.financials.empty else pd.DataFrame()
                    balance_sheet_data = stock.balance_sheet.T if hasattr(stock, 'balance_sheet') and not stock.balance_sheet.empty else pd.DataFrame()
                    
                    data = calculate_financial_indicators(data, info_data, balance_sheet_data)
                    data = calculate_all_turnover_metrics(data, info_data)
                
                # 计算技术指标
                data = calculate_technical_indicators(data)
                
                # 计算波动率指标
                volatility_metrics = calculate_realized_volatility(data, window_length=20)
                if not volatility_metrics.empty:
                    data = data.join(volatility_metrics, how='left')
                
                return ticker, data
            else:
                # 处理其他数据类型
                data_methods = {
                    'info': lambda: info_data,
                    'financials': lambda: stock.financials.T if hasattr(stock, 'financials') and not stock.financials.empty else pd.DataFrame(),
                    'balance_sheet': lambda: stock.balance_sheet.T if hasattr(stock, 'balance_sheet') and not stock.balance_sheet.empty else pd.DataFrame(),
                    'cashflow': lambda: stock.cashflow.T if hasattr(stock, 'cashflow') and not stock.cashflow.empty else pd.DataFrame(),
                    'dividends': lambda: stock.dividends,
                    'splits': lambda: stock.splits,
                    'actions': lambda: stock.actions
                }
                
                if data_type in data_methods:
                    data = data_methods[data_type]()
                    if not data.empty:
                        if isinstance(data, pd.Series):
                            data = data.to_frame(name=data_type)
                        data['Ticker'] = ticker
                        return ticker, data
                
                return None, None
                
        except Exception as e:
            logger.error(f"下载 {ticker} 数据失败: {e}")
            return None, None
    
    try:
        logger.info(f"开始并行下载 {data_type} 数据（最大 {max_workers} 线程）")
        
        # 获取股票列表
        us_stocks = get_us_stocks(us_stk_lst=us_stk_lst)
        if us_stocks.empty:
            logger.error("没有找到有效的股票列表")
            return
        
        logger.info(f"找到 {len(us_stocks)} 只符合条件的股票")
        
        # 准备股票信息列表
        ticker_list = [(row['Symbol'], row['ENName']) for _, row in us_stocks.iterrows()]
        
        # 并行下载
        stock_data = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_ticker = {executor.submit(download_single_stock, ticker_info): ticker_info[0] 
                              for ticker_info in ticker_list}
            
            # 收集结果
            for future in as_completed(future_to_ticker):
                ticker_name = future_to_ticker[future]
                try:
                    ticker, data = future.result()
                    if ticker and data is not None and not data.empty:
                        stock_data[ticker] = data
                        logger.info(f"成功下载 {ticker} 的 {data_type} 数据")
                except Exception as e:
                    logger.error(f"处理 {ticker_name} 数据时出错: {e}")
        
        if not stock_data:
            logger.warning("没有成功下载任何股票数据")
            return
        
        logger.info(f"成功下载 {len(stock_data)} 只股票的数据")
        
        # 保存数据
        save_data(stock_data, data_type, start_date=start_date, out_dir=out_dir)
        
        logger.info("并行数据下载和保存完成")
        
    except Exception as e:
        logger.error(f"并行下载失败: {e}")
        raise


def calculate_financial_indicators(history_data: pd.DataFrame, info_data: pd.DataFrame, 
                                 balance_sheet_data: pd.DataFrame) -> pd.DataFrame:
    """
    计算财务指标（整合版本）
    """
    try:
        result_data = history_data.copy()
        
        # 市净率计算
        if 'bookValue' in info_data.columns and len(info_data['bookValue'].dropna()) > 0:
            book_value = info_data['bookValue'].iloc[0]
            if not pd.isna(book_value) and book_value > 0:
                result_data['PriceToBookRatio'] = result_data['Close'] / book_value
        
        # 市值计算
        if 'sharesOutstanding' in info_data.columns and len(info_data['sharesOutstanding'].dropna()) > 0:
            shares_outstanding = info_data['sharesOutstanding'].iloc[0]
            if not pd.isna(shares_outstanding) and shares_outstanding > 0:
                result_data['MarketCap'] = result_data['Close'] * shares_outstanding
        
        # 托宾Q值计算
        market_cap_series = info_data.get('marketCap', pd.Series([0]))
        market_cap = market_cap_series.iloc[0] if len(market_cap_series) > 0 else 0
        if (not balance_sheet_data.empty and 
            'Total Assets' in balance_sheet_data.columns and 
            market_cap is not None and market_cap > 0):
            
            total_assets_series = balance_sheet_data['Total Assets']
            total_assets = total_assets_series.iloc[0] if len(total_assets_series.dropna()) > 0 else 0
            if total_assets > 0:
                tobins_q = market_cap / total_assets
                result_data['TobinsQ'] = tobins_q
        
        return result_data
        
    except Exception as e:
        logger.error(f"计算财务指标失败: {e}")
        return history_data




#####################################################################################


def parse_arguments():
    """
    解析命令行参数
    """
    parser = argparse.ArgumentParser(
        description='美股数据下载工具 - 使用Yahoo Finance API下载美股历史数据和基础数据',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
使用示例:
  # 下载历史价格数据（使用并行下载）
  python get_yf_data.py history --parallel --max-workers 10
  
  # 下载财务数据
  python get_yf_data.py financials -o ./output
  
  # 下载指定日期范围的历史数据
  python get_yf_data.py history --start-date 2023-01-01 -l custom_stocks.csv
  
  # 显示支持的数据类型
  python get_yf_data.py --list-types

支持的数据类型:
  history              - 历史价格数据（推荐使用并行下载）
  financials          - 财务报表数据
  balance_sheet       - 资产负债表
  cashflow            - 现金流量表
  info                - 基本信息
  dividends           - 股息数据
  splits              - 股票分割数据
  recommendations     - 分析师推荐
  institutional_holders - 机构持股
  mutualfund_holders  - 基金持股
  actions             - 公司行动
  earnings            - 收益数据
        '''
    )
    
    # 必需参数
    parser.add_argument(
        'data_type',
        nargs='?',
        help='要下载的数据类型',
        choices=[
            'history', 'financials', 'balance_sheet', 'cashflow', 'info',
            'dividends', 'splits', 'recommendations', 'institutional_holders',
            'mutualfund_holders', 'actions', 'earnings'
        ]
    )
    
    # 可选参数
    parser.add_argument(
        '-l', '--stock-list',
        default='TRDA_StockInfo.csv',
        help='股票列表CSV文件名 (默认: TRDA_StockInfo.csv)'
    )
    
    parser.add_argument(
        '-o', '--output-dir',
        default=data_dir_daily,
        help=f'输出目录 (默认: {data_dir_daily})'
    )
    
    parser.add_argument(
        '-s', '--start-date',
        help='开始日期 (格式: YYYY-MM-DD, 仅适用于history数据类型)'
    )
    
    parser.add_argument(
        '--parallel',
        action='store_true',
        help='使用并行下载（推荐用于大量股票）'
    )
    
    parser.add_argument(
        '--max-workers',
        type=int,
        default=5,
        help='并行下载的最大工作线程数 (默认: 5)'
    )
    
    parser.add_argument(
        '--list-types',
        action='store_true',
        help='显示所有支持的数据类型并退出'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='日志级别 (默认: INFO)'
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s 2.0 (优化版)'
    )
    
    return parser.parse_args()


def list_supported_types():
    """
    显示支持的数据类型
    """
    print("支持的数据类型:")
    print("=" * 50)
    
    types_info = {
        'history': '历史价格数据（包含技术指标和财务比率）',
        'financials': '财务报表数据',
        'balance_sheet': '资产负债表',
        'cashflow': '现金流量表',
        'info': '基本信息（市值、股本等）',
        'dividends': '股息分红数据',
        'splits': '股票分割数据',
        'recommendations': '分析师推荐',
        'institutional_holders': '机构持股数据',
        'mutualfund_holders': '基金持股数据',
        'actions': '公司行动（分红、分割等）',
        'earnings': '收益数据'
    }
    
    for data_type, description in types_info.items():
        print(f"  {data_type:<20} - {description}")
    
    print("\n注意：history数据类型建议使用 --parallel 参数以提高下载速度")


def main():
    """
    主函数 - 处理命令行参数并执行相应操作
    """
    args = parse_arguments()
    
    # 设置日志级别
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # 显示支持的数据类型
    if args.list_types:
        list_supported_types()
        sys.exit(0)
    
    # 检查必需参数
    if not args.data_type:
        print("错误：需要指定数据类型")
        print("使用 --help 查看帮助信息")
        print("使用 --list-types 查看支持的数据类型")
        sys.exit(1)
    
    # 验证输入参数
    if args.start_date and args.data_type != 'history':
        logger.warning("--start-date 参数仅适用于 history 数据类型")
    
    if args.max_workers < 1 or args.max_workers > 20:
        logger.warning("建议 max-workers 设置在 1-20 之间")
    
    # 显示执行信息
    logger.info("=" * 60)
    logger.info("美股数据下载工具启动")
    logger.info("=" * 60)
    logger.info(f"数据类型: {args.data_type}")
    logger.info(f"股票列表文件: {args.stock_list}")
    logger.info(f"输出目录: {args.output_dir}")
    if args.start_date:
        logger.info(f"开始日期: {args.start_date}")
    if args.parallel:
        logger.info(f"并行下载: 开启 (最大 {args.max_workers} 个工作线程)")
    else:
        logger.info("并行下载: 关闭")
    logger.info("=" * 60)
    
    try:
        # 检查股票列表文件是否存在
        stock_list_path = os.path.join(data_dir, args.stock_list)
        if not os.path.exists(stock_list_path):
            logger.error(f"股票列表文件不存在: {stock_list_path}")
            sys.exit(1)
        
        # 执行下载
        if args.parallel:
            logger.info("使用并行下载模式")
            yf_download_parallel(
                us_stk_lst=args.stock_list,
                data_type=args.data_type,
                out_dir=args.output_dir,
                start_date=args.start_date,
                max_workers=args.max_workers
            )
        else:
            logger.info("使用标准下载模式")
            yf_dwload_main(
                us_stk_lst=args.stock_list,
                data_type=args.data_type,
                out_dir=args.output_dir,
                start_date=args.start_date
            )
        
        logger.info("=" * 60)
        logger.info("数据下载完成！")
        logger.info("=" * 60)
        
    except KeyboardInterrupt:
        logger.info("用户中断下载")
        sys.exit(0)
    except Exception as e:
        logger.error(f"下载过程中发生错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()



