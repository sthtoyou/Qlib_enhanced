import yfinance as yf
import pandas as pd
import numpy as np
import talib
import datetime
import os 
import logging
from typing import Dict, List, Optional, Tuple, Union
from requests.exceptions import HTTPError
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 数据目录配置
DATA_DIR = 'E:\\tushare_data' 
DATA_DIR_DAILY = 'E:\\tushare_data\\daily' 
DATA_DIR_INDEX = 'E:\\tushare_data\\index'

# 技术指标配置
TECHNICAL_INDICATORS_CONFIG = {
    'trend': ['SMA', 'EMA', 'HT_TRENDLINE', 'KAMA', 'MA', 'DEMA'],
    'momentum': ['RSI', 'MOM', 'ROC', 'ROCP', 'ROCR', 'ROCR100', 'CMO', 'TRIX'],
    'volatility': ['ATR', 'NATR', 'TRANGE', 'STDDEV'],
    'volume': ['AD', 'ADOSC', 'OBV', 'MFI'],
    'cycle': ['HT_DCPERIOD', 'HT_DCPHASE', 'HT_TRENDMODE'],
    'price': ['AVGPRICE', 'MEDPRICE', 'TYPPRICE', 'WCLPRICE'],
    'overlap': ['BBANDS', 'MIDPOINT', 'MIDPRICE'],
    'pattern': ['MACD', 'STOCH', 'STOCHF', 'STOCHRSI'],
    'statistic': ['BETA', 'CORREL', 'LINEARREG', 'LINEARREG_ANGLE', 'LINEARREG_INTERCEPT', 'LINEARREG_SLOPE', 'TSF', 'VAR']
}

TURNOVER_WINDOWS = [1, 5, 10, 20, 30]


class StockDataProcessor:
    """股票数据处理器"""
    
    def __init__(self, data_dir: str = DATA_DIR):
        self.data_dir = data_dir
        
    def get_us_stocks(self, us_stk_lst: str) -> pd.DataFrame:
        """获取美股列表"""
        try:
            us_stocks = pd.read_csv(os.path.join(self.data_dir, us_stk_lst))
            us_stocks_filtered = us_stocks[
                (us_stocks['StatusID'] == 0) & 
                (us_stocks['ExchangeCode'].isin(['NASDAQ', 'NYSE']))
            ]
            logger.info(f"加载了 {len(us_stocks_filtered)} 只美股")
            return us_stocks_filtered
        except Exception as e:
            logger.error(f"加载股票列表失败: {e}")
            raise
    
    def calculate_technical_indicators(self, data: pd.DataFrame, 
                                     market_data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """计算技术指标"""
        try:
            indicators = {}
            
            # 基础价格数据检查
            required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            if not all(col in data.columns for col in required_columns):
                logger.warning("缺少必要的价格数据列")
                return data
            
            # 趋势指标
            indicators['SMA_20'] = talib.SMA(data['Close'], timeperiod=20)
            indicators['EMA_20'] = talib.EMA(data['Close'], timeperiod=20)
            indicators['HT_TRENDLINE'] = talib.HT_TRENDLINE(data['Close'])
            indicators['KAMA_30'] = talib.KAMA(data['Close'], timeperiod=30)
            indicators['DEMA_30'] = talib.DEMA(data['Close'], timeperiod=30)
            
            # 动量指标
            indicators['RSI_14'] = talib.RSI(data['Close'], timeperiod=14)
            indicators['MOM_10'] = talib.MOM(data['Close'], timeperiod=10)
            indicators['ROC_10'] = talib.ROC(data['Close'], timeperiod=10)
            indicators['CMO_14'] = talib.CMO(data['Close'], timeperiod=14)
            
            # 波动性指标
            indicators['ATR_14'] = talib.ATR(data['High'], data['Low'], data['Close'], timeperiod=14)
            indicators['NATR_14'] = talib.NATR(data['High'], data['Low'], data['Close'], timeperiod=14)
            indicators['STDDEV_30'] = talib.STDDEV(data['Close'], timeperiod=30)
            
            # 成交量指标
            indicators['AD'] = talib.AD(data['High'], data['Low'], data['Close'], data['Volume'])
            indicators['OBV'] = talib.OBV(data['Close'], data['Volume'])
            indicators['MFI_14'] = talib.MFI(data['High'], data['Low'], data['Close'], data['Volume'], timeperiod=14)
            
            # 布林带
            upper, middle, lower = talib.BBANDS(data['Close'], timeperiod=20)
            indicators['BB_UPPER'] = upper
            indicators['BB_MIDDLE'] = middle
            indicators['BB_LOWER'] = lower
            
            # MACD
            macd, macd_signal, macd_hist = talib.MACD(data['Close'])
            indicators['MACD'] = macd
            indicators['MACD_SIGNAL'] = macd_signal
            indicators['MACD_HIST'] = macd_hist
            
            # 随机指标
            stoch_k, stoch_d = talib.STOCH(data['High'], data['Low'], data['Close'])
            indicators['STOCH_K'] = stoch_k
            indicators['STOCH_D'] = stoch_d
            
            # 价格指标
            indicators['AVGPRICE'] = talib.AVGPRICE(data['Open'], data['High'], data['Low'], data['Close'])
            indicators['MEDPRICE'] = talib.MEDPRICE(data['High'], data['Low'])
            indicators['TYPPRICE'] = talib.TYPPRICE(data['High'], data['Low'], data['Close'])
            
            # 如果有市场数据，计算贝塔
            if market_data is not None and len(market_data) == len(data):
                indicators['BETA_5'] = talib.BETA(data['Close'], market_data['Close'], timeperiod=5)
            
            # 线性回归指标
            indicators['LINEARREG_14'] = talib.LINEARREG(data['Close'], timeperiod=14)
            indicators['LINEARREG_SLOPE_14'] = talib.LINEARREG_SLOPE(data['Close'], timeperiod=14)
            
            # 添加主要的蜡烛图形态
            candlestick_patterns = [
                'CDL2CROWS', 'CDL3BLACKCROWS', 'CDL3WHITESOLDIERS', 'CDLDOJI',
                'CDLENGULFING', 'CDLHAMMER', 'CDLHANGINGMAN', 'CDLHARAMI',
                'CDLMORNINGSTAR', 'CDLEVENINGSTAR', 'CDLSHOOTINGSTAR'
            ]
            
            for pattern in candlestick_patterns:
                try:
                    indicators[pattern] = getattr(talib, pattern)(data['Open'], data['High'], data['Low'], data['Close'])
                except:
                    logger.warning(f"无法计算蜡烛图形态: {pattern}")
            
            # 合并结果
            result_df = pd.concat([data, pd.DataFrame(indicators, index=data.index)], axis=1)
            return result_df
            
        except Exception as e:
            logger.error(f"计算技术指标失败: {e}")
            return data
    
    def calculate_financial_ratios(self, history_data: pd.DataFrame, 
                                 info_data: pd.DataFrame, 
                                 balance_sheet_data: pd.DataFrame) -> pd.DataFrame:
        """计算财务比率"""
        try:
            result_data = history_data.copy()
            
            # 市净率 (P/B Ratio)
            if 'bookValue' in info_data.columns and not info_data['bookValue'].empty:
                book_value = info_data['bookValue'].iloc[0] if not pd.isna(info_data['bookValue'].iloc[0]) else 1
                result_data['PB_Ratio'] = result_data['Close'] / book_value
            
            # 市值计算相关指标
            if 'sharesOutstanding' in info_data.columns and not info_data['sharesOutstanding'].empty:
                shares_outstanding = info_data['sharesOutstanding'].iloc[0]
                if not pd.isna(shares_outstanding) and shares_outstanding > 0:
                    result_data['MarketCap'] = result_data['Close'] * shares_outstanding
            
            # 托宾Q值
            market_cap = info_data.get('marketCap', 0)
            if (not balance_sheet_data.empty and 
                'Total Assets' in balance_sheet_data.columns and 
                market_cap > 0):
                
                total_assets = balance_sheet_data['Total Assets'].iloc[0] if not balance_sheet_data['Total Assets'].empty else 0
                if total_assets > 0:
                    tobins_q = market_cap / total_assets
                    result_data['TobinsQ'] = tobins_q
            
            return result_data
            
        except Exception as e:
            logger.error(f"计算财务比率失败: {e}")
            return history_data
    
    def calculate_turnover_metrics(self, history_data: pd.DataFrame, 
                                 info_data: pd.DataFrame) -> pd.DataFrame:
        """计算换手率相关指标"""
        try:
            result_data = history_data.copy()
            
            if 'floatShares' not in info_data.columns or info_data['floatShares'].empty:
                logger.warning("缺少流通股数据，无法计算换手率")
                return result_data
            
            float_shares = info_data['floatShares'].iloc[0]
            if pd.isna(float_shares) or float_shares <= 0:
                logger.warning("流通股数据无效")
                return result_data
            
            # 计算日换手率
            result_data['DailyTurnover'] = result_data['Volume'] / float_shares
            
            # 计算不同窗口的累计和平均换手率
            for window in TURNOVER_WINDOWS:
                result_data[f'TurnoverCum_{window}d'] = result_data['DailyTurnover'].rolling(window=window).sum()
                result_data[f'TurnoverAvg_{window}d'] = result_data['DailyTurnover'].rolling(window=window).mean()
            
            return result_data
            
        except Exception as e:
            logger.error(f"计算换手率指标失败: {e}")
            return history_data
    
    def calculate_volatility_metrics(self, history_data: pd.DataFrame, 
                                   window_length: int = 20) -> pd.DataFrame:
        """计算波动率相关指标"""
        try:
            if history_data.empty or len(history_data) < window_length:
                return pd.DataFrame()
            
            # 计算收益率
            returns = history_data['Close'].pct_change()
            log_returns = np.log(history_data['Close'] / history_data['Close'].shift(1))
            
            # 波动率指标
            volatility_metrics = pd.DataFrame(index=history_data.index)
            
            # 已实现波动率 (年化)
            volatility_metrics['RealizedVol'] = returns.rolling(window=window_length).std() * np.sqrt(252)
            
            # 对数收益率波动率
            volatility_metrics['LogReturnVol'] = log_returns.rolling(window=window_length).std() * np.sqrt(252)
            
            # 上行和下行半偏差
            positive_returns = returns.where(returns > 0, 0)
            negative_returns = returns.where(returns < 0, 0)
            
            volatility_metrics['UpsideVol'] = positive_returns.rolling(window=window_length).std() * np.sqrt(252)
            volatility_metrics['DownsideVol'] = negative_returns.rolling(window=window_length).std() * np.sqrt(252)
            
            return volatility_metrics
            
        except Exception as e:
            logger.error(f"计算波动率指标失败: {e}")
            return pd.DataFrame()
    
    def download_single_stock(self, ticker: str, name: str, data_type: str, 
                            start_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        """下载单只股票数据"""
        try:
            stock = yf.Ticker(ticker)
            
            # 获取基础信息
            info_data = pd.DataFrame([stock.info]) if stock.info else pd.DataFrame()
            financials_data = stock.financials.T if hasattr(stock, 'financials') and not stock.financials.empty else pd.DataFrame()
            balance_sheet_data = stock.balance_sheet.T if hasattr(stock, 'balance_sheet') and not stock.balance_sheet.empty else pd.DataFrame()
            
            if data_type == 'history':
                data = stock.history(start=start_date, period='max' if not start_date else None)
                if data.empty:
                    return None
                
                data['Ticker'] = ticker
                
                # 计算技术指标
                data = self.calculate_technical_indicators(data)
                
                # 计算财务比率
                if not info_data.empty:
                    data = self.calculate_financial_ratios(data, info_data, balance_sheet_data)
                
                # 计算换手率指标
                if not info_data.empty:
                    data = self.calculate_turnover_metrics(data, info_data)
                
                # 计算波动率指标
                volatility_metrics = self.calculate_volatility_metrics(data)
                if not volatility_metrics.empty:
                    data = data.join(volatility_metrics, how='left')
                
                return data
                
            elif data_type == 'info':
                return info_data
            elif data_type == 'financials':
                return financials_data
            elif data_type == 'balance_sheet':
                return balance_sheet_data
            elif data_type == 'cashflow':
                cashflow_data = stock.cashflow.T if hasattr(stock, 'cashflow') and not stock.cashflow.empty else pd.DataFrame()
                return cashflow_data
            else:
                # 处理其他数据类型
                data_methods = {
                    'dividends': lambda: stock.dividends,
                    'splits': lambda: stock.splits,
                    'recommendations': lambda: stock.recommendations,
                    'institutional_holders': lambda: stock.institutional_holders,
                    'mutualfund_holders': lambda: stock.mutualfund_holders,
                    'actions': lambda: stock.actions,
                    'earnings': lambda: stock.earnings.T if hasattr(stock, 'earnings') and not stock.earnings.empty else pd.DataFrame()
                }
                
                if data_type in data_methods:
                    data = data_methods[data_type]()
                    if isinstance(data, pd.DataFrame) and not data.empty:
                        data['Ticker'] = ticker
                        return data
                    elif isinstance(data, pd.Series) and not data.empty:
                        df = data.to_frame(name=data_type)
                        df['Ticker'] = ticker
                        return df
                
                return None
                
        except Exception as e:
            logger.error(f"下载 {ticker} 数据失败: {e}")
            return None
    
    def download_data_parallel(self, stock_list: pd.DataFrame, data_type: str, 
                             start_date: Optional[str] = None, max_workers: int = 5) -> Dict[str, pd.DataFrame]:
        """并行下载数据"""
        stock_data = {}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_ticker = {}
            for index, row in stock_list.iterrows():
                ticker = str(row['Symbol']).split('.')[0]  # 提取ticker部分
                name = row['ENName']
                future = executor.submit(self.download_single_stock, ticker, name, data_type, start_date)
                future_to_ticker[future] = ticker
            
            # 收集结果
            for future in as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    data = future.result()
                    if data is not None and not data.empty:
                        stock_data[ticker] = data
                        logger.info(f"成功下载 {ticker} 的 {data_type} 数据")
                    else:
                        logger.warning(f"{ticker} 没有有效的 {data_type} 数据")
                except Exception as e:
                    logger.error(f"处理 {ticker} 数据时出错: {e}")
        
        return stock_data
    
    def save_data(self, stock_data: Dict[str, pd.DataFrame], data_type: str, 
                 out_dir: str, start_date: Optional[str] = None) -> None:
        """保存数据为CSV文件"""
        if not stock_data:
            logger.warning("没有数据需要保存")
            return
        
        try:
            # 合并所有数据
            combined_data = pd.concat(
                [df.assign(Ticker=ticker) for ticker, df in stock_data.items()],
                ignore_index=True
            )
            
            # 确保输出目录存在
            os.makedirs(out_dir, exist_ok=True)
            
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


def yf_download_main(us_stk_lst: str, data_type: str, out_dir: str, 
                    start_date: Optional[str] = None, max_workers: int = 5) -> None:
    """主函数"""
    try:
        processor = StockDataProcessor()
        
        # 获取股票列表
        us_stocks = processor.get_us_stocks(us_stk_lst)
        if us_stocks.empty:
            logger.error("没有找到有效的股票列表")
            return
        
        # 下载数据
        logger.info(f"开始下载 {len(us_stocks)} 只股票的 {data_type} 数据")
        stock_data = processor.download_data_parallel(us_stocks, data_type, start_date, max_workers)
        
        # 保存数据
        processor.save_data(stock_data, data_type, out_dir, start_date)
        
        logger.info("数据下载和保存完成")
        
    except Exception as e:
        logger.error(f"主函数执行失败: {e}")
        raise


if __name__ == "__main__":
    # 示例调用
    # yf_download_main('TRDA_StockInfo.csv', 'history', DATA_DIR_DAILY)
    pass 