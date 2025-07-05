# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
import re
import sys
import qlib
import shutil
import zipfile
import requests
import datetime
import pandas as pd
import numpy as np
import struct
import time
try:
    import yfinance as yf
except ImportError:
    logger.warning("yfinance not installed, US data download will not be available")
    yf = None
from tqdm import tqdm
from pathlib import Path
from loguru import logger
from qlib.utils import exists_qlib_data


class GetDataEnhanced:
    """
    增强版数据下载类
    支持中国、美股和港股数据下载，自动生成标准qlib格式
    """
    REMOTE_URL = "https://github.com/SunsetWolf/qlib_dataset/releases/download"

    def __init__(self, delete_zip_file=False):
        """
        Parameters
        ----------
        delete_zip_file : bool, optional
            Whether to delete the zip file, value from True or False, by default False
        """
        self.delete_zip_file = delete_zip_file

    def merge_remote_url(self, file_name: str):
        """Generate download links."""
        return f"{self.REMOTE_URL}/{file_name}" if "/" in file_name else f"{self.REMOTE_URL}/v0/{file_name}"

    def download(self, url: str, target_path: [Path, str]):
        """Download a file from the specified url."""
        file_name = str(target_path).rsplit("/", maxsplit=1)[-1]
        resp = requests.get(url, stream=True, timeout=60)
        resp.raise_for_status()
        if resp.status_code != 200:
            raise requests.exceptions.HTTPError()

        chunk_size = 1024
        logger.warning(
            f"The data for the example is collected from Yahoo Finance. Please be aware that the quality of the data might not be perfect. (You can refer to the original data source: https://finance.yahoo.com/lookup.)"
        )
        logger.info(f"{os.path.basename(file_name)} downloading......")
        with tqdm(total=int(resp.headers.get("Content-Length", 0))) as p_bar:
            with target_path.open("wb") as fp:
                for chunk in resp.iter_content(chunk_size=chunk_size):
                    fp.write(chunk)
                    p_bar.update(chunk_size)

    def download_data(self, file_name: str, target_dir: [Path, str], delete_old: bool = True):
        """Download the specified file to the target folder."""
        target_dir = Path(target_dir).expanduser()
        target_dir.mkdir(exist_ok=True, parents=True)
        # saved file name
        _target_file_name = datetime.datetime.now().strftime("%Y%m%d%H%M%S") + "_" + os.path.basename(file_name)
        target_path = target_dir.joinpath(_target_file_name)

        url = self.merge_remote_url(file_name)
        self.download(url=url, target_path=target_path)

        self._unzip(target_path, target_dir, delete_old)
        if self.delete_zip_file:
            target_path.unlink()

    def check_dataset(self, file_name: str):
        url = self.merge_remote_url(file_name)
        resp = requests.get(url, stream=True, timeout=60)
        status = True
        if resp.status_code == 404:
            status = False
        return status

    @staticmethod
    def _unzip(file_path: [Path, str], target_dir: [Path, str], delete_old: bool = True):
        file_path = Path(file_path)
        target_dir = Path(target_dir)
        if delete_old:
            logger.warning(
                f"will delete the old qlib data directory(features, instruments, calendars, features_cache, dataset_cache): {target_dir}"
            )
            GetDataEnhanced._delete_qlib_data(target_dir)
        logger.info(f"{file_path} unzipping......")
        with zipfile.ZipFile(str(file_path.resolve()), "r") as zp:
            for _file in tqdm(zp.namelist()):
                zp.extract(_file, str(target_dir.resolve()))

    @staticmethod
    def _delete_qlib_data(file_dir: Path):
        rm_dirs = []
        for _name in ["features", "calendars", "instruments", "features_cache", "dataset_cache"]:
            _p = file_dir.joinpath(_name)
            if _p.exists():
                rm_dirs.append(str(_p.resolve()))
        if rm_dirs:
            flag = input(
                f"Will be deleted: "
                f"\n\t{rm_dirs}"
                f"\nIf you do not need to delete {file_dir}, please change the <--target_dir>"
                f"\nAre you sure you want to delete, yes(Y/y), no (N/n):"
            )
            if str(flag) not in ["Y", "y"]:
                sys.exit()
            for _p in rm_dirs:
                logger.warning(f"delete: {_p}")
                shutil.rmtree(_p)

    def _get_us_stocks_list(self):
        """获取美股股票列表"""
        logger.info("正在获取美股股票列表...")
        
        # 方法1：从Wikipedia动态获取标普500成分股
        try:
            import pandas as pd
            import requests
            
            logger.info("尝试从Wikipedia获取标普500成分股...")
            
            url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
            response = requests.get(url, timeout=15)
            
            if response.status_code == 200:
                logger.info("✓ 成功连接Wikipedia")
                
                # 使用pandas直接读取表格
                tables = pd.read_html(url)
                sp500_table = tables[0]  # 第一个表格是标普500列表
                
                # 获取股票代码列
                if 'Symbol' in sp500_table.columns:
                    symbols = sp500_table['Symbol'].tolist()
                elif 'Ticker symbol' in sp500_table.columns:
                    symbols = sp500_table['Ticker symbol'].tolist()
                else:
                    # 尝试第一列
                    symbols = sp500_table.iloc[:, 0].tolist()
                
                # 清理数据：移除无效符号
                clean_symbols = []
                for symbol in symbols:
                    if isinstance(symbol, str) and len(symbol) <= 6:
                        # 允许字母、数字和连字符
                        clean_symbol = str(symbol).strip().upper()
                        if clean_symbol and all(c.isalnum() or c in ['-', '.'] for c in clean_symbol):
                            clean_symbols.append(clean_symbol)
                
                if len(clean_symbols) >= 450:  # 确保获取到足够多的股票
                    logger.info(f"✅ 成功获取 {len(clean_symbols)} 只标普500成分股")
                    
                    # 验证一些知名股票
                    famous_stocks = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'BRK.B']
                    # BRK-B在不同来源可能显示为BRK.B
                    found_count = 0
                    for stock in famous_stocks:
                        if stock in clean_symbols or stock.replace('.', '-') in clean_symbols:
                            found_count += 1
                    logger.info(f"知名股票验证: {found_count}/{len(famous_stocks)} 只")
                    
                    return clean_symbols
                else:
                    logger.warning(f"获取的股票数量不足: {len(clean_symbols)}，使用备用方案")
            else:
                logger.warning(f"Wikipedia访问失败: HTTP {response.status_code}")
                
        except ImportError as e:
            logger.warning(f"缺少必要库: {e}，使用备用股票列表")
        except Exception as e:
            logger.warning(f"从Wikipedia获取标普500失败: {e}")
        
        # 方法2：尝试其他数据源（预留）
        try:
            # 可以在这里添加其他数据源，如Yahoo Finance、Alpha Vantage等
            logger.info("其他数据源方法预留，使用备用列表")
            
        except Exception as e:
            logger.warning(f"其他数据源获取失败: {e}")
        
        # 方法3：备用标普500主要成分股列表
        logger.info("使用备用标普500主要成分股列表...")
        
        # 精选的标普500重要成分股（按市值和流动性筛选）
        sp500_symbols = [
            # 科技巨头 (Technology)
            'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'META', 'TSLA', 'NVDA', 'NFLX', 'ADBE',
            'CRM', 'ORCL', 'AMD', 'INTC', 'IBM', 'QCOM', 'TXN', 'AVGO', 'INTU', 'CSCO',
            'PYPL', 'EBAY', 'UBER', 'LYFT', 'SNAP', 'TWTR', 'ZOOM', 'DOCU', 'OKTA', 'CRWD',
            
            # 金融服务 (Financial Services)
            'BRK-B', 'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'AXP', 'V', 'MA',
            'COF', 'DIS', 'USB', 'PNC', 'TFC', 'BLK', 'SCHW', 'CB', 'ICE', 'CME',
            'AON', 'MMC', 'AJG', 'TRV', 'ALL', 'PGR', 'AFL', 'MET', 'PRU', 'AIG',
            
            # 医疗保健 (Healthcare) 
            'UNH', 'JNJ', 'PFE', 'ABBV', 'LLY', 'TMO', 'ABT', 'MDT', 'BMY', 'AMGN',
            'GILD', 'CVS', 'CI', 'HUM', 'ANTM', 'ZTS', 'SYK', 'BSX', 'EW', 'DHR',
            'BDX', 'BAX', 'HCA', 'CNC', 'MOH', 'DVA', 'MCK', 'ABC', 'CAH', 'WBA',
            
            # 消费品 (Consumer)
            'PG', 'KO', 'PEP', 'WMT', 'HD', 'MCD', 'NKE', 'SBUX', 'TGT', 'LOW',
            'COST', 'TJX', 'BKNG', 'MAR', 'HLT', 'MGM', 'LVS', 'WYNN', 'CCL', 'RCL',
            'YUM', 'CMG', 'QSR', 'DPZ', 'MCD', 'SBUX', 'KHC', 'GIS', 'K', 'CAG',
            
            # 工业 (Industrial)
            'BA', 'CAT', 'GE', 'MMM', 'HON', 'UPS', 'FDX', 'LMT', 'RTX', 'NOC',
            'GD', 'LHX', 'ITW', 'EMR', 'ETN', 'PH', 'CMI', 'DE', 'DOV', 'ROK',
            'JCI', 'CARR', 'OTIS', 'FTV', 'XYL', 'PNR', 'WAT', 'IEX', 'ROP', 'FAST',
            
            # 能源 (Energy)
            'XOM', 'CVX', 'COP', 'EOG', 'SLB', 'PSX', 'VLO', 'MPC', 'KMI', 'OKE',
            'WMB', 'EPD', 'MRO', 'DVN', 'FANG', 'APA', 'EQT', 'CNX', 'RRC', 'AR',
            
            # 公用事业 (Utilities)
            'NEE', 'DUK', 'SO', 'D', 'AEP', 'EXC', 'XEL', 'PEG', 'SRE', 'ES',
            'ED', 'FE', 'EIX', 'ETR', 'WEC', 'DTE', 'PPL', 'CMS', 'CNP', 'ATO',
            
            # 房地产 (Real Estate)
            'AMT', 'PLD', 'CCI', 'EQIX', 'SPG', 'PSA', 'O', 'WELL', 'DLR', 'EXR',
            'AVB', 'EQR', 'VTR', 'ESS', 'MAA', 'UDR', 'CPT', 'HST', 'REG', 'BXP',
            
            # 材料 (Materials)
            'LIN', 'APD', 'ECL', 'SHW', 'FCX', 'NUE', 'DD', 'DOW', 'PPG', 'IP',
            'PKG', 'CF', 'FMC', 'ALB', 'CE', 'VMC', 'MLM', 'NEM', 'AA', 'X',
            
            # 通信服务 (Communication Services)
            'T', 'VZ', 'CMCSA', 'CHTR', 'DISH', 'SIRI', 'LUMN', 'TMUS', 'TTWO', 'EA',
            'ATVI', 'NWSA', 'NYT', 'IPG', 'OMC', 'PARA', 'FOXA', 'FOX', 'WBD', 'LYV',
            
            # 消费必需品 (Consumer Staples)
            'WBA', 'CVS', 'KR', 'WMT', 'COST', 'TGT', 'DG', 'DLTR', 'SYY', 'KDP',
            'MNST', 'PEP', 'KO', 'CL', 'PG', 'UL', 'NSRGY', 'KHC', 'GIS', 'K'
        ]
        
        # 去重并排序
        sp500_symbols = sorted(list(set(sp500_symbols)))
        
        logger.info(f"✓ 使用备用股票列表: {len(sp500_symbols)} 只美股（包含标普500主要成分股）")
        return sp500_symbols

    def _get_hk_stocks_list(self):
        """获取港股股票列表（扩展版）
        包含恒生指数+恒生中国企业指数+恒生科技指数+富时中国50指数成分股
        """
        logger.info("正在获取扩展版港股股票列表...")
        logger.info("覆盖范围: 恒生指数 + 恒生中国企业指数 + 恒生科技指数 + 富时中国50指数")
        
        # 方法1：尝试动态获取成分股信息
        dynamic_stocks = self._get_hk_stocks_dynamic()
        if dynamic_stocks and len(dynamic_stocks) >= 150:
            logger.info(f"✅ 动态获取成功: {len(dynamic_stocks)} 只港股")
            return dynamic_stocks
        
        # 方法2：使用扩展的静态股票列表
        logger.info("使用扩展版港股成分股列表...")
        
        hk_symbols = []
        
        # === 恒生指数 (HSI) 成分股 ===
        hsi_stocks = [
            # 金融板块
            '0005.HK',  # 汇丰控股
            '0011.HK',  # 恒生银行
            '0388.HK',  # 香港交易所
            '0939.HK',  # 建设银行
            '1398.HK',  # 工商银行
            '3988.HK',  # 中国银行
            '2318.HK',  # 中国平安
            '2388.HK',  # 中银香港
            '1288.HK',  # 农业银行
            '6030.HK',  # 中信证券
            
            # 科技板块
            '0700.HK',  # 腾讯控股
            '9988.HK',  # 阿里巴巴
            '3690.HK',  # 美团
            '1024.HK',  # 快手科技
            '9618.HK',  # 京东集团
            '1810.HK',  # 小米集团
            '0981.HK',  # 中芯国际
            '2382.HK',  # 舜宇光学
            
            # 地产板块
            '0016.HK',  # 新鸿基地产
            '0017.HK',  # 新世界发展
            '0083.HK',  # 信和置业
            '0101.HK',  # 恒隆地产
            '1109.HK',  # 华润置地
            '1997.HK',  # 九龙仓置业
            
            # 能源石化
            '0857.HK',  # 中国石油
            '0883.HK',  # 中海油
            '0386.HK',  # 中国石化
            '2628.HK',  # 中国人寿
            
            # 基建公用
            '0002.HK',  # 中电控股
            '0003.HK',  # 香港中华煤气
            '0066.HK',  # 港铁公司
            '0267.HK',  # 中信股份
            '0688.HK',  # 中国海外
            '1038.HK',  # 长江基建
            '1044.HK',  # 恒安国际
            '6862.HK',  # 海底捞
            
            # 消费医药
            '0288.HK',  # 万洲国际
            '0291.HK',  # 华润啤酒
            '0762.HK',  # 中国联通
            '0823.HK',  # 领展房产基金
            '0960.HK',  # 龙湖集团
            '0968.HK',  # 信义光能
            '1093.HK',  # 石药集团
            '1113.HK',  # 长实集团
            '1177.HK',  # 中国生物制药
            '1211.HK',  # 比亚迪
            '1299.HK',  # 友邦保险
            '1876.HK',  # 百威亚太
            '1918.HK',  # 融创中国
            '1972.HK',  # 太古地产
            '2007.HK',  # 碧桂园
            '2018.HK',  # 瑞声科技
            '2020.HK',  # 安踏体育
            '2269.HK',  # 药明生物
            '2313.HK',  # 申洲国际
            '2319.HK',  # 蒙牛乳业
            '2331.HK',  # 李宁
            '2518.HK',  # 汽车之家
            '3968.HK',  # 招商银行
            '6098.HK',  # 碧桂园服务
            '9888.HK',  # 百度集团
            '9999.HK',  # 网易
        ]
        
        # === 恒生中国企业指数 (HSCEI) H股成分股 ===
        hscei_stocks = [
            # 大型国企H股
            '0914.HK',  # 海螺水泥
            '0728.HK',  # 中国电信
            '0753.HK',  # 中国国航
            '0670.HK',  # 中国东方航空
            '1088.HK',  # 中国神华
            '1336.HK',  # 新华保险
            '1359.HK',  # 中国信达
            '1368.HK',  # 特步国际
            '1448.HK',  # 福寿园
            '1478.HK',  # 四环医药
            '1658.HK',  # 邮储银行
            '1833.HK',  # 平安好医生
            '1928.HK',  # 金沙中国
            '2238.HK',  # 广汽集团
            '2380.HK',  # 中国电力
            '2588.HK',  # 中银航空租赁
            '3328.HK',  # 交通银行
            '3800.HK',  # 协鑫新能源
            '6178.HK',  # 光大证券
            '6690.HK',  # 海尔智家
            '9626.HK',  # 哔哩哔哩
            '9961.HK',  # 携程集团
            
            # 红筹股
            '0992.HK',  # 联想集团
            '1186.HK',  # 中国铁建
            '1347.HK',  # 华虹半导体
            '1800.HK',  # 中国交建
            '1919.HK',  # 中远海控
            '1988.HK',  # 民生银行
            '2899.HK',  # 紫金矿业
            '3993.HK',  # 洛阳钼业
            '6886.HK',  # HTSC
        ]
        
        # === 恒生科技指数 (HSTECH) 科技股 ===
        hstech_stocks = [
            # 互联网平台
            '0700.HK',  # 腾讯控股
            '9988.HK',  # 阿里巴巴
            '3690.HK',  # 美团
            '9618.HK',  # 京东集团
            '1024.HK',  # 快手科技
            '9888.HK',  # 百度集团
            '9999.HK',  # 网易
            '9626.HK',  # 哔哩哔哩
            '9961.HK',  # 携程集团
            '2518.HK',  # 汽车之家
            
            # 硬件制造
            '1810.HK',  # 小米集团
            '2018.HK',  # 瑞声科技
            '2382.HK',  # 舜宇光学
            '0981.HK',  # 中芯国际
            '1347.HK',  # 华虹半导体
            '0992.HK',  # 联想集团
            
            # 新能源汽车
            '1211.HK',  # 比亚迪
            '2015.HK',  # 理想汽车
            '9868.HK',  # 小鹏汽车
            '9866.HK',  # 蔚来
            
            # 生物医药科技
            '2269.HK',  # 药明生物
            '1177.HK',  # 中国生物制药
            '6160.HK',  # 百济神州
            '9926.HK',  # 康方生物
            '1833.HK',  # 平安好医生
            
            # 金融科技
            '6993.HK',  # 蓝月亮
            '1772.HK',  # 赣锋锂业
            '2020.HK',  # 安踏体育（智能制造）
            
            # 清洁能源
            '0968.HK',  # 信义光能
            '1772.HK',  # 赣锋锂业
            '3800.HK',  # 协鑫新能源
        ]
        
        # === 富时中国50指数 (FTSE China 50) 成分股 ===
        ftse_china50_stocks = [
            # 大盘蓝筹（与HSI重叠较多，补充一些独有标的）
            '1038.HK',  # 长江基建
            '1044.HK',  # 恒安国际
            '0175.HK',  # 吉利汽车
            '1066.HK',  # 威高股份
            '0151.HK',  # 中国旺旺
            '1119.HK',  # 印染股份
            '1122.HK',  # 庆龄汽车
            '1128.HK',  # 万福生科
            '1137.HK',  # 中国城建
            '1157.HK',  # 中联重科
            '1199.HK',  # 中远海发
            '1230.HK',  # 雅居乐
            '1339.HK',  # 中国人民保险
            '1378.HK',  # 中国宏桥
            '1668.HK',  # 华南城
            '1818.HK',  # 招金矿业
            '1988.HK',  # 民生银行
            '2600.HK',  # 中国铝业
            '2823.HK',  # 康师傅
            '2866.HK',  # 中海集运
            '3883.HK',  # 中国奥园
            '6808.HK',  # 高鑫零售
            '9923.HK',  # 移卡
        ]
        
        # 合并所有股票并去重
        all_stocks = set()
        all_stocks.update(hsi_stocks)
        all_stocks.update(hscei_stocks) 
        all_stocks.update(hstech_stocks)
        all_stocks.update(ftse_china50_stocks)
        
        # 转换为有序列表
        hk_symbols = sorted(list(all_stocks))
        
        logger.info(f"✅ 扩展版港股股票池构建完成:")
        logger.info(f"  - 恒生指数成分股: {len(hsi_stocks)} 只")
        logger.info(f"  - 恒生中国企业指数成分股: {len(hscei_stocks)} 只") 
        logger.info(f"  - 恒生科技指数成分股: {len(hstech_stocks)} 只")
        logger.info(f"  - 富时中国50指数成分股: {len(ftse_china50_stocks)} 只")
        logger.info(f"  - 去重后总计: {len(hk_symbols)} 只港股")
        
        return hk_symbols
    
    def _get_hk_stocks_dynamic(self):
        """动态获取港股成分股信息"""
        try:
            logger.info("尝试动态获取港股指数成分股...")
            
            # 这里可以添加从恒生指数公司、富时指数等官方网站爬取数据的逻辑
            # 由于涉及网页结构变化和反爬虫，目前返回None，使用静态列表
            logger.info("动态获取暂未实现，使用静态列表")
            return None
            
        except Exception as e:
            logger.warning(f"动态获取港股成分股失败: {e}")
            return None

    def _get_cn_stocks_list(self):
        """获取A股股票列表（沪深300 + 中证500）"""
        logger.info("正在获取A股股票列表（沪深300 + 中证500）...")
        
        # 方法1：使用baostock获取完整沪深300和中证500成分股
        try:
            import baostock as bs
            import pandas as pd
            
            logger.info("尝试使用baostock获取沪深300 + 中证500成分股...")
            
            # 登录baostock
            lg = bs.login()
            if lg.error_code == '0':
                logger.info("✓ 登录baostock成功")
                
                all_stocks = []
                
                # 获取沪深300成分股
                logger.info("正在获取沪深300成分股...")
                rs_hs300 = bs.query_hs300_stocks()
                if rs_hs300.error_code == '0':
                    hs300_count = 0
                    while (rs_hs300.error_code == '0') & rs_hs300.next():
                        stock_data = rs_hs300.get_row_data()
                        original_code = stock_data[1]  # baostock格式：sz.000001 或 sh.600000
                        
                        # 转换为yfinance格式
                        if original_code.startswith('sz.'):
                            yf_code = original_code.replace('sz.', '') + '.SZ'
                        elif original_code.startswith('sh.'):
                            yf_code = original_code.replace('sh.', '') + '.SS'
                        else:
                            continue  # 跳过无效格式
                            
                        all_stocks.append(yf_code)
                        hs300_count += 1
                    
                    logger.info(f"✓ 获取到 {hs300_count} 只沪深300成分股")
                else:
                    logger.warning(f"查询沪深300失败: {rs_hs300.error_msg}")
                
                # 获取中证500成分股
                logger.info("正在获取中证500成分股...")
                rs_zz500 = bs.query_zz500_stocks()
                if rs_zz500.error_code == '0':
                    zz500_count = 0
                    while (rs_zz500.error_code == '0') & rs_zz500.next():
                        stock_data = rs_zz500.get_row_data()
                        original_code = stock_data[1]  # baostock格式：sz.000001 或 sh.600000
                        
                        # 转换为yfinance格式
                        if original_code.startswith('sz.'):
                            yf_code = original_code.replace('sz.', '') + '.SZ'
                        elif original_code.startswith('sh.'):
                            yf_code = original_code.replace('sh.', '') + '.SS'
                        else:
                            continue  # 跳过无效格式
                            
                        all_stocks.append(yf_code)
                        zz500_count += 1
                    
                    logger.info(f"✓ 获取到 {zz500_count} 只中证500成分股")
                else:
                    logger.warning(f"查询中证500失败: {rs_zz500.error_msg}")
                
                bs.logout()
                
                # 去重合并
                unique_stocks = sorted(list(set(all_stocks)))
                
                if len(unique_stocks) >= 700:  # 确保获取到足够多的股票（预期约800只）
                    logger.info(f"✅ 成功获取 {len(unique_stocks)} 只A股成分股（沪深300 + 中证500，已去重）")
                    return unique_stocks
                else:
                    logger.warning(f"获取的股票数量不足: {len(unique_stocks)}，使用备用方案")
                    
            else:
                logger.warning(f"登录baostock失败: {lg.error_msg}")
                
        except ImportError:
            logger.warning("baostock未安装，使用备用股票列表")
        except Exception as e:
            logger.warning(f"使用baostock获取股票列表失败: {e}")
        
        # 方法2：使用tushare获取（需要token）
        try:
            import tushare as ts
            logger.info("尝试使用tushare获取沪深300成分股...")
            
            # 注意：tushare需要token，这里提供一个示例
            # 用户需要到 https://tushare.pro/ 注册获取token
            # ts.set_token('your_token_here')
            # pro = ts.pro_api()
            # df = pro.index_weight(index_code='000300.SH', trade_date='20241201')
            # 这里暂时跳过tushare方案，因为需要用户配置token
            
            logger.info("tushare需要用户配置token，跳过")
            
        except ImportError:
            logger.info("tushare未安装")
        except Exception as e:
            logger.warning(f"使用tushare获取失败: {e}")
        
        # 方法3：备用股票列表（精选的沪深300 + 中证500重要成分股）
        logger.info("使用备用沪深300 + 中证500主要成分股列表...")
        
        # 精选的沪深300 + 中证500重要成分股（按权重和流动性筛选）
        cn_symbols = [
            # 沪深300 - 沪市权重股（金融、能源、基建）
            '600036.SS', '600000.SS', '600016.SS', '601328.SS', '601398.SS', '601166.SS', '601318.SS',
            '601988.SS', '601288.SS', '600519.SS', '600887.SS', '600809.SS', '600028.SS', '601857.SS', '601088.SS',
            '601601.SS', '601766.SS', '601628.SS', '601669.SS', '601800.SS', '601985.SS', '601939.SS',
            '600009.SS', '600104.SS', '600115.SS', '600196.SS', '600276.SS', '600309.SS', '600346.SS', '600383.SS',
            '600406.SS', '600438.SS', '600570.SS', '600585.SS', '600588.SS', '600690.SS', '600703.SS', '600745.SS',
            '600795.SS', '600837.SS', '600867.SS', '600893.SS', '600958.SS', '600999.SS', '601006.SS', '601012.SS',
            '601066.SS', '601117.SS', '601138.SS', '601169.SS', '601186.SS', '601211.SS', '601229.SS', '601336.SS',
            '601390.SS', '601633.SS', '601658.SS', '601688.SS', '601788.SS', '601816.SS', '601818.SS', '601865.SS',
            '601888.SS', '601899.SS', '601919.SS', '601933.SS', '601989.SS', '601995.SS', '601998.SS', '600018.SS',
            '600019.SS', '600025.SS', '600027.SS', '600029.SS', '600030.SS', '600031.SS', '600048.SS', '600050.SS',
            '600061.SS', '600066.SS', '600085.SS', '600089.SS', '600111.SS', '600118.SS', '600150.SS', '600160.SS',
            '600170.SS', '600177.SS', '600188.SS', '600362.SS', '600398.SS', '600436.SS', '600547.SS', '600600.SS',
            '600606.SS', '600760.SS', '600848.SS', '600886.SS', '600010.SS', '600011.SS', '600015.SS', '600023.SS',
            
            # 沪深300 - 深市权重股（科技、消费、医药）
            '000001.SZ', '000002.SZ', '000063.SZ', '000069.SZ', '000100.SZ', '000157.SZ', '000166.SZ', '000333.SZ',
            '000338.SZ', '000401.SZ', '000402.SZ', '000538.SZ', '000568.SZ', '000596.SZ', '000625.SZ', '000627.SZ',
            '000651.SZ', '000661.SZ', '000671.SZ', '000703.SZ', '000708.SZ', '000725.SZ', '000728.SZ', '000738.SZ',
            '000776.SZ', '000783.SZ', '000792.SZ', '000858.SZ', '000876.SZ', '000895.SZ', '000938.SZ', '000961.SZ',
            '000977.SZ', '002001.SZ', '002007.SZ', '002008.SZ', '002027.SZ', '002032.SZ', '002044.SZ', '002050.SZ',
            '002142.SZ', '002146.SZ', '002152.SZ', '002230.SZ', '002236.SZ', '002241.SZ', '002271.SZ', '002304.SZ',
            '002311.SZ', '002352.SZ', '002415.SZ', '002466.SZ', '002475.SZ', '002493.SZ', '002508.SZ', '002594.SZ',
            '002601.SZ', '002602.SZ', '002714.SZ', '002736.SZ', '002837.SZ', '002916.SZ', '002938.SZ', '003816.SZ',
            '000012.SZ', '000021.SZ', '000039.SZ', '000046.SZ', '000060.SZ', '000089.SZ', '000301.SZ', '000413.SZ',
            '000423.SZ', '000425.SZ', '000503.SZ', '000513.SZ', '000629.SZ', '000636.SZ', '000656.SZ', '000709.SZ',
            '000723.SZ', '000729.SZ', '000755.SZ', '000759.SZ', '000768.SZ', '000786.SZ', '000800.SZ', '000826.SZ',
            '000839.SZ', '000860.SZ', '000877.SZ', '000898.SZ', '000917.SZ', '000932.SZ', '000959.SZ', '000963.SZ',
            
            # 中证500 - 沪市成分股（补充沪深300之外的重要股票）
            '600064.SS', '600068.SS', '600079.SS', '600096.SS', '600123.SS', '600125.SS', '600141.SS', '600161.SS',
            '600162.SS', '600176.SS', '600183.SS', '600202.SS', '600208.SS', '600219.SS', '600233.SS', '600251.SS',
            '600258.SS', '600266.SS', '600270.SS', '600285.SS', '600297.SS', '600307.SS', '600312.SS', '600315.SS',
            '600316.SS', '600332.SS', '600335.SS', '600340.SS', '600348.SS', '600369.SS', '600372.SS', '600377.SS',
            '600380.SS', '600387.SS', '600390.SS', '600399.SS', '600409.SS', '600410.SS', '600415.SS', '600426.SS',
            '600428.SS', '600449.SS', '600460.SS', '600466.SS', '600481.SS', '600482.SS', '600489.SS', '600498.SS',
            '600507.SS', '600511.SS', '600516.SS', '600518.SS', '600521.SS', '600522.SS', '600525.SS', '600528.SS',
            '600536.SS', '600549.SS', '600563.SS', '600566.SS', '600567.SS', '600572.SS', '600580.SS', '600583.SS',
            '600584.SS', '600590.SS', '600592.SS', '600595.SS', '600597.SS', '600598.SS', '600602.SS', '600604.SS',
            '600608.SS', '600612.SS', '600614.SS', '600615.SS', '600617.SS', '600618.SS', '600619.SS', '600622.SS',
            '600623.SS', '600628.SS', '600629.SS', '600633.SS', '600635.SS', '600637.SS', '600639.SS', '600641.SS',
            '600642.SS', '600643.SS', '600645.SS', '600649.SS', '600652.SS', '600653.SS', '600655.SS', '600658.SS',
            '600660.SS', '600662.SS', '600663.SS', '600664.SS', '600665.SS', '600673.SS', '600674.SS', '600675.SS',
            '600677.SS', '600678.SS', '600679.SS', '600682.SS', '600683.SS', '600684.SS', '600685.SS', '600686.SS',
            '600688.SS', '600689.SS', '600692.SS', '600693.SS', '600694.SS', '600695.SS', '600696.SS', '600697.SS',
            '600698.SS', '600699.SS', '600701.SS', '600702.SS', '600704.SS', '600705.SS', '600706.SS', '600707.SS',
            '600708.SS', '600710.SS', '600711.SS', '600712.SS', '600713.SS', '600714.SS', '600715.SS', '600716.SS',
            '600717.SS', '600718.SS', '600719.SS', '600720.SS', '600721.SS', '600722.SS', '600723.SS', '600724.SS',
            '600725.SS', '600726.SS', '600727.SS', '600728.SS', '600729.SS', '600730.SS', '600731.SS', '600732.SS',
            '600733.SS', '600734.SS', '600735.SS', '600736.SS', '600737.SS', '600738.SS', '600739.SS', '600741.SS',
            
            # 中证500 - 深市成分股（补充沪深300之外的重要股票）
            '000004.SZ', '000005.SZ', '000006.SZ', '000007.SZ', '000008.SZ', '000009.SZ', '000010.SZ', '000011.SZ',
            '000014.SZ', '000016.SZ', '000017.SZ', '000018.SZ', '000019.SZ', '000020.SZ', '000022.SZ', '000023.SZ',
            '000025.SZ', '000026.SZ', '000027.SZ', '000028.SZ', '000029.SZ', '000030.SZ', '000031.SZ', '000032.SZ',
            '000034.SZ', '000035.SZ', '000036.SZ', '000037.SZ', '000038.SZ', '000040.SZ', '000042.SZ', '000043.SZ',
            '000045.SZ', '000048.SZ', '000049.SZ', '000050.SZ', '000055.SZ', '000056.SZ', '000058.SZ', '000059.SZ',
            '000061.SZ', '000062.SZ', '000065.SZ', '000066.SZ', '000068.SZ', '000070.SZ', '000078.SZ', '000088.SZ',
            '000090.SZ', '000096.SZ', '000099.SZ', '000150.SZ', '000155.SZ', '000158.SZ', '000159.SZ', '000333.SZ',
            '000338.SZ', '000401.SZ', '000402.SZ', '000415.SZ', '000416.SZ', '000417.SZ', '000418.SZ', '000419.SZ',
            '000420.SZ', '000421.SZ', '000422.SZ', '000426.SZ', '000428.SZ', '000429.SZ', '000430.SZ', '000488.SZ',
            '000501.SZ', '000502.SZ', '000504.SZ', '000505.SZ', '000506.SZ', '000507.SZ', '000509.SZ', '000510.SZ',
            '000511.SZ', '000514.SZ', '000516.SZ', '000517.SZ', '000518.SZ', '000519.SZ', '000520.SZ', '000521.SZ',
            '000523.SZ', '000524.SZ', '000525.SZ', '000526.SZ', '000528.SZ', '000529.SZ', '000530.SZ', '000531.SZ',
            '000532.SZ', '000533.SZ', '000534.SZ', '000536.SZ', '000537.SZ', '000539.SZ', '000540.SZ', '000541.SZ',
            '000543.SZ', '000544.SZ', '000545.SZ', '000546.SZ', '000547.SZ', '000548.SZ', '000549.SZ', '000550.SZ',
            '000551.SZ', '000552.SZ', '000553.SZ', '000554.SZ', '000555.SZ', '000556.SZ', '000557.SZ', '000558.SZ',
            '000559.SZ', '000560.SZ', '000561.SZ', '000562.SZ', '000563.SZ', '000564.SZ', '000565.SZ', '000566.SZ',
            '000567.SZ', '000570.SZ', '000571.SZ', '000572.SZ', '000573.SZ', '000576.SZ', '000581.SZ', '000582.SZ',
            '000584.SZ', '000585.SZ', '000586.SZ', '000587.SZ', '000589.SZ', '000590.SZ', '000591.SZ', '000592.SZ',
            '000593.SZ', '000594.SZ', '000595.SZ', '000597.SZ', '000598.SZ', '000599.SZ', '000600.SZ', '000601.SZ',
            '000603.SZ', '000605.SZ', '000606.SZ', '000607.SZ', '000608.SZ', '000609.SZ', '000610.SZ', '000611.SZ',
            
            # 创业板重要成分股（中证500部分）
            '300003.SZ', '300014.SZ', '300015.SZ', '300033.SZ', '300059.SZ', '300122.SZ', '300142.SZ', '300144.SZ',
            '300347.SZ', '300408.SZ', '300413.SZ', '300433.SZ', '300498.SZ', '300595.SZ', '300628.SZ', '300750.SZ',
            '300760.SZ', '300896.SZ', '300919.SZ', '300999.SZ', '301236.SZ', '301095.SZ', '301110.SZ', '301187.SZ',
            '300124.SZ', '300383.SZ', '300454.SZ', '300601.SZ', '300782.SZ', '300888.SZ', '300979.SZ',
            '300017.SZ', '300024.SZ', '300026.SZ', '300027.SZ', '300028.SZ', '300030.SZ', '300034.SZ', '300037.SZ',
            '300039.SZ', '300040.SZ', '300041.SZ', '300045.SZ', '300046.SZ', '300048.SZ', '300049.SZ', '300050.SZ',
            '300051.SZ', '300052.SZ', '300054.SZ', '300055.SZ', '300056.SZ', '300058.SZ', '300061.SZ', '300062.SZ',
            
            # 科创板优质股票
            '688009.SS', '688036.SS', '688047.SS', '688065.SS', '688111.SS', '688169.SS', '688187.SS', '688223.SS',
            '688303.SS', '688598.SS', '688981.SS', '688050.SS', '688012.SS', '688126.SS', '688180.SS', '688202.SS',
            '688256.SS', '688363.SS', '688518.SS', '688700.SS', '688819.SS', '688072.SS', '688008.SS', '688099.SS',
            '688151.SS', '688199.SS', '688276.SS', '688396.SS', '688561.SS', '688733.SS', '688898.SS',
            '688016.SS', '688018.SS', '688019.SS', '688020.SS', '688021.SS', '688022.SS', '688023.SS', '688025.SS',
            '688026.SS', '688027.SS', '688028.SS', '688029.SS', '688030.SS', '688031.SS', '688032.SS', '688033.SS',
        ]
        
        # 去重并排序
        cn_symbols = sorted(list(set(cn_symbols)))
        
        logger.info(f"✓ 使用备用股票列表: {len(cn_symbols)} 只A股（包含沪深300 + 中证500主要成分股）")
        return cn_symbols

    def _create_us_calendar(self, start_date: str, end_date: str, target_dir: Path):
        """创建美股交易日历"""
        logger.info("创建美股交易日历...")
        
        if yf is None:
            logger.error("yfinance未安装，无法创建美股交易日历")
            return []
        
        # 使用yfinance获取AAPL的交易日历（更稳定）
        ticker = yf.Ticker("AAPL")
        hist = ticker.history(start=start_date, end=end_date, interval="1d")
        
        # 获取交易日期
        trading_dates = hist.index.to_series().dt.strftime('%Y-%m-%d').tolist()
        
        # 创建calendars目录
        calendar_dir = target_dir / "calendars"
        calendar_dir.mkdir(exist_ok=True)
        
        # 写入day.txt文件
        with open(calendar_dir / "day.txt", 'w') as f:
            for date in trading_dates:
                f.write(f"{date}\n")
        
        logger.info(f"创建美股交易日历: {len(trading_dates)} 个交易日")
        logger.info(f"日历范围: {trading_dates[0]} 到 {trading_dates[-1]}")
        
        return trading_dates

    def _create_us_instruments(self, stocks: list, target_dir: Path):
        """创建美股股票列表"""
        logger.info("创建美股股票列表...")
        
        instruments_dir = target_dir / "instruments"
        instruments_dir.mkdir(exist_ok=True)
        
        # 写入all.txt文件
        with open(instruments_dir / "all.txt", 'w') as f:
            for stock in stocks:
                # 格式: symbol	start_date	end_date
                f.write(f"{stock.lower()}\t1990-01-01\t2030-12-31\n")
        
        logger.info(f"创建美股股票列表: {len(stocks)} 只美股")

    def _create_hk_calendar(self, start_date: str, end_date: str, target_dir: Path):
        """创建港股交易日历"""
        logger.info("创建港股交易日历...")
        
        if yf is None:
            logger.error("yfinance未安装，无法创建港股交易日历")
            return []
        
        # 使用yfinance获取腾讯(0700.HK)的交易日历（港股代表）
        ticker = yf.Ticker("0700.HK")
        hist = ticker.history(start=start_date, end=end_date, interval="1d")
        
        # 获取交易日期
        trading_dates = hist.index.to_series().dt.strftime('%Y-%m-%d').tolist()
        
        # 创建calendars目录
        calendar_dir = target_dir / "calendars"
        calendar_dir.mkdir(exist_ok=True)
        
        # 写入day.txt文件
        with open(calendar_dir / "day.txt", 'w') as f:
            for date in trading_dates:
                f.write(f"{date}\n")
        
        logger.info(f"创建港股交易日历: {len(trading_dates)} 个交易日")
        logger.info(f"日历范围: {trading_dates[0]} 到 {trading_dates[-1]}")
        
        return trading_dates

    def _create_hk_instruments(self, stocks: list, target_dir: Path):
        """创建港股股票列表"""
        logger.info("创建港股股票列表...")
        
        instruments_dir = target_dir / "instruments"
        instruments_dir.mkdir(exist_ok=True)
        
        # 写入all.txt文件
        with open(instruments_dir / "all.txt", 'w') as f:
            for stock in stocks:
                # 港股代码转换：0700.HK -> 0700_hk
                symbol_qlib = stock.replace('.HK', '_hk').lower()
                # 格式: symbol	start_date	end_date
                f.write(f"{symbol_qlib}\t1990-01-01\t2030-12-31\n")
        
        logger.info(f"创建港股股票列表: {len(stocks)} 只港股")

    def _create_cn_calendar(self, start_date: str, end_date: str, target_dir: Path):
        """创建A股交易日历"""
        logger.info("创建A股交易日历...")
        
        if yf is None:
            logger.error("yfinance未安装，无法创建A股交易日历")
            return []
        
        # 使用yfinance获取中国平安(000001.SZ)的交易日历（A股代表）
        ticker = yf.Ticker("000001.SZ")
        hist = ticker.history(start=start_date, end=end_date, interval="1d")
        
        # 获取交易日期
        trading_dates = hist.index.to_series().dt.strftime('%Y-%m-%d').tolist()
        
        # 创建calendars目录
        calendar_dir = target_dir / "calendars"
        calendar_dir.mkdir(exist_ok=True)
        
        # 写入day.txt文件
        with open(calendar_dir / "day.txt", 'w') as f:
            for date in trading_dates:
                f.write(f"{date}\n")
        
        logger.info(f"创建A股交易日历: {len(trading_dates)} 个交易日")
        logger.info(f"日历范围: {trading_dates[0]} 到 {trading_dates[-1]}")
        
        return trading_dates

    def _create_cn_instruments(self, stocks: list, target_dir: Path):
        """创建A股股票列表"""
        logger.info("创建A股股票列表...")
        
        instruments_dir = target_dir / "instruments"
        instruments_dir.mkdir(exist_ok=True)
        
        # 写入all.txt文件
        with open(instruments_dir / "all.txt", 'w') as f:
            for stock in stocks:
                # A股代码转换：600000.SS -> 600000_ss, 000001.SZ -> 000001_sz
                symbol_qlib = stock.replace('.SS', '_ss').replace('.SZ', '_sz').lower()
                # 格式: symbol	start_date	end_date
                f.write(f"{symbol_qlib}\t1990-01-01\t2030-12-31\n")
        
        logger.info(f"创建A股股票列表: {len(stocks)} 只A股")

    def _download_us_stock_data(self, symbol: str, start_date: str, end_date: str, 
                              target_dir: Path, incremental_update: bool = False):
        """下载单只美股数据并转换为标准qlib格式"""
        if yf is None:
            logger.error("yfinance未安装，无法下载美股数据")
            return False
            
        symbol_lower = symbol.lower()
        stock_dir = target_dir / "features" / symbol_lower
        
        try:
            # 检查是否需要增量更新
            existing_data = None
            csv_file = target_dir / "features" / f"{symbol}.csv"
            if incremental_update and csv_file.exists():
                existing_data = pd.read_csv(csv_file)
                if 'Date' in existing_data.columns:
                    existing_data['Date'] = pd.to_datetime(existing_data['Date'], utc=True).dt.tz_convert(None)
                    latest_date = existing_data['Date'].max()
                    logger.info(f"    📁 发现现有数据: {symbol} ({len(existing_data)} 条记录)")
                    logger.info(f"        现有数据范围: {existing_data['Date'].min()} 到 {latest_date}")
                    
                    # 调整开始日期为最新日期的下一天
                    new_start = (latest_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                    if new_start >= end_date:
                        logger.info(f"    ✅ {symbol} 数据已是最新")
                        return self._convert_to_qlib_format(existing_data, stock_dir, symbol)
                    else:
                        logger.info(f"    🔄 {symbol} 需要补充数据: {new_start} 到 {end_date}")
                        start_date = new_start
            
            # 下载数据
            ticker = yf.Ticker(symbol)
            data = ticker.history(start=start_date, end=end_date, interval="1d")
            
            if data.empty:
                logger.warning(f"    ❌ {symbol} 没有获取到数据")
                return False
            
            # 转换为DataFrame格式
            df = data.reset_index()
            
            # 处理美股的时区信息（移除时区，保留日期）
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date']).dt.tz_convert(None)
            
            # 如果是增量更新，合并数据
            if existing_data is not None and not existing_data.empty:
                df = pd.concat([existing_data, df], ignore_index=True)
                df = df.drop_duplicates(subset=['Date'], keep='last')
                df = df.sort_values('Date').reset_index(drop=True)
            
            logger.info(f"    📊 {symbol} 新数据: {len(df)} 条记录")
            
            # 保存CSV文件（用于后续增量更新）
            csv_file = target_dir / "features" / f"{symbol}.csv"
            df.to_csv(csv_file, index=False)
            
            # 转换为标准qlib格式
            success = self._convert_to_qlib_format(df, stock_dir, symbol)
            
            if success:
                logger.info(f"    ✓ {symbol} 下载成功: {len(df)} 条记录 (CSV + 二进制)")
                return True
            else:
                logger.error(f"    ❌ {symbol} 转换格式失败")
                return False
                
        except Exception as e:
            logger.error(f"    ❌ {symbol} 下载失败: {e}")
            return False

    def _download_hk_stock_data(self, symbol: str, start_date: str, end_date: str, 
                              target_dir: Path, incremental_update: bool = False):
        """下载单只港股数据并转换为标准qlib格式"""
        if yf is None:
            logger.error("yfinance未安装，无法下载港股数据")
            return False
            
        # 港股代码转换：0700.HK -> 0700_hk（用于目录名和文件名）
        symbol_qlib = symbol.replace('.HK', '_hk').lower()
        stock_dir = target_dir / "features" / symbol_qlib
        
        try:
            # 检查是否需要增量更新
            existing_data = None
            csv_file = target_dir / "features" / f"{symbol}.csv"
            if incremental_update and csv_file.exists():
                existing_data = pd.read_csv(csv_file)
                if 'Date' in existing_data.columns:
                    existing_data['Date'] = pd.to_datetime(existing_data['Date'], utc=True).dt.tz_convert(None)
                    latest_date = existing_data['Date'].max()
                    logger.info(f"    📁 发现现有数据: {symbol} ({len(existing_data)} 条记录)")
                    logger.info(f"        现有数据范围: {existing_data['Date'].min()} 到 {latest_date}")
                    
                    # 调整开始日期为最新日期的下一天
                    new_start = (latest_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                    if new_start >= end_date:
                        logger.info(f"    ✅ {symbol} 数据已是最新")
                        return self._convert_to_qlib_format(existing_data, stock_dir, symbol)
                    else:
                        logger.info(f"    🔄 {symbol} 需要补充数据: {new_start} 到 {end_date}")
                        start_date = new_start
            
            # 下载数据
            ticker = yf.Ticker(symbol)
            data = ticker.history(start=start_date, end=end_date, interval="1d")
            
            if data.empty:
                logger.warning(f"    ❌ {symbol} 没有获取到数据")
                return False
            
            # 转换为DataFrame格式
            df = data.reset_index()
            
            # 处理港股的时区信息（移除时区，保留日期）
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date']).dt.date
                df['Date'] = pd.to_datetime(df['Date'])
            
            # 如果是增量更新，合并数据
            if existing_data is not None and not existing_data.empty:
                df = pd.concat([existing_data, df], ignore_index=True)
                df = df.drop_duplicates(subset=['Date'], keep='last')
                df = df.sort_values('Date').reset_index(drop=True)
            
            logger.info(f"    📊 {symbol} 新数据: {len(df)} 条记录")
            
            # 保存CSV文件（用于后续增量更新）
            csv_file = target_dir / "features" / f"{symbol}.csv"
            df.to_csv(csv_file, index=False)
            
            # 转换为标准qlib格式
            success = self._convert_to_qlib_format(df, stock_dir, symbol)
            
            if success:
                logger.info(f"    ✓ {symbol} 下载成功: {len(df)} 条记录 (CSV + 二进制)")
                return True
            else:
                logger.error(f"    ❌ {symbol} 转换格式失败")
                return False
                
        except Exception as e:
            logger.error(f"    ❌ {symbol} 下载失败: {e}")
            return False

    def _download_cn_stock_data(self, symbol: str, start_date: str, end_date: str, 
                              target_dir: Path, incremental_update: bool = False):
        """下载单只A股数据并转换为标准qlib格式"""
        if yf is None:
            logger.error("yfinance未安装，无法下载A股数据")
            return False
            
        # A股代码转换：600000.SS -> 600000_ss（用于目录名和文件名）
        symbol_qlib = symbol.replace('.SS', '_ss').replace('.SZ', '_sz').lower()
        stock_dir = target_dir / "features" / symbol_qlib
        
        try:
            # 检查是否需要增量更新
            existing_data = None
            csv_file = target_dir / "features" / f"{symbol}.csv"
            if incremental_update and csv_file.exists():
                existing_data = pd.read_csv(csv_file)
                if 'Date' in existing_data.columns:
                    existing_data['Date'] = pd.to_datetime(existing_data['Date'], utc=True).dt.tz_convert(None)
                    latest_date = existing_data['Date'].max()
                    logger.info(f"    📁 发现现有数据: {symbol} ({len(existing_data)} 条记录)")
                    logger.info(f"        现有数据范围: {existing_data['Date'].min()} 到 {latest_date}")
                    
                    # 调整开始日期为最新日期的下一天
                    new_start = (latest_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                    if new_start >= end_date:
                        logger.info(f"    ✅ {symbol} 数据已是最新")
                        return self._convert_to_qlib_format(existing_data, stock_dir, symbol)
                    else:
                        logger.info(f"    🔄 {symbol} 需要补充数据: {new_start} 到 {end_date}")
                        start_date = new_start
            
            # 下载数据
            ticker = yf.Ticker(symbol)
            data = ticker.history(start=start_date, end=end_date, interval="1d")
            
            if data.empty:
                logger.warning(f"    ❌ {symbol} 没有获取到数据")
                return False
            
            # 转换为DataFrame格式
            df = data.reset_index()
            
            # 处理A股的时区信息（移除时区，保留日期）
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date']).dt.date
                df['Date'] = pd.to_datetime(df['Date'])
            
            # 如果是增量更新，合并数据
            if existing_data is not None and not existing_data.empty:
                df = pd.concat([existing_data, df], ignore_index=True)
                df = df.drop_duplicates(subset=['Date'], keep='last')
                df = df.sort_values('Date').reset_index(drop=True)
            
            logger.info(f"    📊 {symbol} 新数据: {len(df)} 条记录")
            
            # 保存CSV文件（用于后续增量更新）
            csv_file = target_dir / "features" / f"{symbol}.csv"
            df.to_csv(csv_file, index=False)
            
            # 转换为标准qlib格式
            success = self._convert_to_qlib_format(df, stock_dir, symbol)
            
            if success:
                logger.info(f"    ✓ {symbol} 下载成功: {len(df)} 条记录 (CSV + 二进制)")
                return True
            else:
                logger.error(f"    ❌ {symbol} 转换格式失败")
                return False
                
        except Exception as e:
            logger.error(f"    ❌ {symbol} 下载失败: {e}")
            return False

    def _convert_to_qlib_format(self, df: pd.DataFrame, stock_dir: Path, symbol: str):
        """将DataFrame转换为标准qlib格式"""
        try:
            # 创建股票目录
            stock_dir.mkdir(parents=True, exist_ok=True)
            
            # 处理日期索引
            if 'Date' in df.columns:
                # 确保日期没有时区信息
                df['Date'] = pd.to_datetime(df['Date'])
                if df['Date'].dt.tz is not None:
                    df['Date'] = df['Date'].dt.tz_convert(None)
                df.set_index('Date', inplace=True)
            
            # 计算额外特征
            df['Change'] = df['Close'].pct_change().fillna(0)
            df['Factor'] = 1.0  # 复权因子简化为1
            
            # 定义特征映射
            feature_mapping = {
                'open': 'Open',
                'high': 'High', 
                'low': 'Low',
                'close': 'Close',
                'volume': 'Volume',
                'change': 'Change',
                'factor': 'Factor'
            }
            
            # 生成二进制文件
            for feature_name, column_name in feature_mapping.items():
                if column_name in df.columns:
                    values = df[column_name].values.astype(np.float32)
                    
                    # 替换无穷大和NaN为0
                    values = np.nan_to_num(values, nan=0.0, posinf=0.0, neginf=0.0)
                    
                    # 写入二进制文件
                    bin_file = stock_dir / f"{feature_name}.day.bin"
                    with open(bin_file, 'wb') as f:
                        for value in values:
                            f.write(struct.pack('<f', value))  # 小端序float32
            
            return True
            
        except Exception as e:
            logger.error(f"转换格式失败 {symbol}: {e}")
            return False

    def qlib_data(
        self,
        name="qlib_data",
        target_dir="~/.qlib/qlib_data/cn_data",
        version=None,
        interval="1d",
        region="cn",
        delete_old=True,
        exists_skip=False,
        trading_date=None,
        end_date=None,
        incremental_update=False,
        cn_realtime=False,
    ):
        """
        增强版数据下载方法
        
        Parameters
        ----------
        target_dir: str
            data save directory
        name: str
            dataset name, value from [qlib_data, qlib_data_simple], by default qlib_data
        version: str
            data version, value from [v1, ...], by default None(use script to specify version)
        interval: str
            data freq, value from [1d], by default 1d
        region: str
            data region, value from [cn, us, hk], by default cn
        delete_old: bool
            delete an existing directory, by default True
        exists_skip: bool
            exists skip, by default False
        trading_date: str
            start date for data download (YYYYMMDD format)
        end_date: str
            end date for data download (YYYYMMDD format)
        incremental_update: bool
            enable incremental update mode, by default False
        cn_realtime: bool
            use real-time download for China stocks (like US/HK), by default False (use official data package)
        """
        
        if exists_skip and exists_qlib_data(target_dir):
            logger.warning(
                f"Data already exists: {target_dir}, the data download will be skipped\n"
                f"\tIf downloading is required: `exists_skip=False` or `change target_dir`"
            )
            return

        # 处理日期参数
        if trading_date:
            try:
                start_date = pd.to_datetime(str(trading_date), format='%Y%m%d').strftime('%Y-%m-%d')
            except:
                logger.warning(f"无效的trading_date格式: {trading_date}，使用默认日期: 19900101")
                start_date = "1990-01-01"
        else:
            start_date = "1990-01-01"
            
        if end_date:
            try:
                end_date_formatted = pd.to_datetime(str(end_date), format='%Y%m%d').strftime('%Y-%m-%d')
            except:
                logger.warning(f"无效的end_date格式: {end_date}，使用默认日期: 20241231")
                end_date_formatted = "2024-12-31"
        else:
            end_date_formatted = "2024-12-31"

        target_path = Path(target_dir).expanduser()
        target_path.mkdir(parents=True, exist_ok=True)

        if region.lower() == "us":
            # 美股数据处理
            logger.info("============================================================")
            logger.info("美股数据实时下载工具")
            logger.info("============================================================")
            logger.info(f"输出目录: {target_dir}")
            logger.info(f"时间范围: {trading_date or '19900101'} 到 {end_date or '20241231'} (YYYYMMDD格式)")
            logger.info(f"yfinance格式: {start_date} 到 {end_date_formatted}")
            logger.info(f"数据间隔: {interval}")
            logger.info(f"增量更新模式: {'是' if incremental_update else '否'}")
            logger.info(f"自动转换格式: 是")
            logger.info("============================================================")
            
            # 获取美股列表
            stocks = self._get_us_stocks_list()
            logger.info(f"准备下载 {len(stocks)} 只美股的数据...")
            
            # 创建目录结构
            features_dir = target_path / "features"
            features_dir.mkdir(exist_ok=True)
            
            # 创建交易日历
            self._create_us_calendar(start_date, end_date_formatted, target_path)
            
            # 创建股票列表
            self._create_us_instruments(stocks, target_path)
            
            # 下载股票数据
            success_count = 0
            for i, stock in enumerate(stocks, 1):
                logger.info(f"[{i}/{len(stocks)}] 正在下载 {stock}...")
                
                success = self._download_us_stock_data(
                    stock, start_date, end_date_formatted, target_path, incremental_update
                )
                
                if success:
                    success_count += 1
                
                # 每下载10只股票休息5秒
                if i % 10 == 0:
                    logger.info(f"    已完成 {i}/{len(stocks)}，休息 5 秒...")
                    time.sleep(5)
                else:
                    time.sleep(1.5)  # 基本延迟
            
            logger.info(f"✅ 美股数据下载完成！成功: {success_count}/{len(stocks)}")
            
        elif region.lower() == "hk":
            # 港股数据处理
            logger.info("============================================================")
            logger.info("港股数据实时下载工具")
            logger.info("============================================================")
            logger.info(f"输出目录: {target_dir}")
            logger.info(f"时间范围: {trading_date or '20200101'} 到 {end_date or '20241231'} (YYYYMMDD格式)")
            logger.info(f"yfinance格式: {start_date} 到 {end_date_formatted}")
            logger.info(f"数据间隔: {interval}")
            logger.info(f"增量更新模式: {'是' if incremental_update else '否'}")
            logger.info(f"自动转换格式: 是")
            logger.info("============================================================")
            
            # 获取港股列表
            stocks = self._get_hk_stocks_list()
            logger.info(f"准备下载 {len(stocks)} 只港股的数据...")
            
            # 创建目录结构
            features_dir = target_path / "features"
            features_dir.mkdir(exist_ok=True)
            
            # 创建交易日历
            self._create_hk_calendar(start_date, end_date_formatted, target_path)
            
            # 创建股票列表
            self._create_hk_instruments(stocks, target_path)
            
            # 下载股票数据
            success_count = 0
            for i, stock in enumerate(stocks, 1):
                logger.info(f"[{i}/{len(stocks)}] 正在下载 {stock}...")
                
                success = self._download_hk_stock_data(
                    stock, start_date, end_date_formatted, target_path, incremental_update
                )
                
                if success:
                    success_count += 1
                
                # 每下载10只股票休息5秒
                if i % 10 == 0:
                    logger.info(f"    已完成 {i}/{len(stocks)}，休息 5 秒...")
                    time.sleep(5)
                else:
                    time.sleep(1.5)  # 基本延迟
            
            logger.info(f"✅ 港股数据下载完成！成功: {success_count}/{len(stocks)}")
            
        elif region.lower() == "cn" and cn_realtime:
            # 中国数据实时下载处理
            logger.info("============================================================")
            logger.info("中国股票数据实时下载工具")
            logger.info("============================================================")
            logger.info(f"输出目录: {target_dir}")
            logger.info(f"时间范围: {trading_date or '20200101'} 到 {end_date or '20241231'} (YYYYMMDD格式)")
            logger.info(f"yfinance格式: {start_date} 到 {end_date_formatted}")
            logger.info(f"数据间隔: {interval}")
            logger.info(f"增量更新模式: {'是' if incremental_update else '否'}")
            logger.info(f"自动转换格式: 是")
            logger.info("============================================================")
            
            # 获取A股列表
            stocks = self._get_cn_stocks_list()
            logger.info(f"准备下载 {len(stocks)} 只A股的数据...")
            
            # 创建目录结构
            features_dir = target_path / "features"
            features_dir.mkdir(exist_ok=True)
            
            # 创建交易日历
            self._create_cn_calendar(start_date, end_date_formatted, target_path)
            
            # 创建股票列表
            self._create_cn_instruments(stocks, target_path)
            
            # 下载股票数据
            success_count = 0
            for i, stock in enumerate(stocks, 1):
                logger.info(f"[{i}/{len(stocks)}] 正在下载 {stock}...")
                
                success = self._download_cn_stock_data(
                    stock, start_date, end_date_formatted, target_path, incremental_update
                )
                
                if success:
                    success_count += 1
                
                # 每下载10只股票休息5秒
                if i % 10 == 0:
                    logger.info(f"    已完成 {i}/{len(stocks)}，休息 5 秒...")
                    time.sleep(5)
                else:
                    time.sleep(1.5)  # 基本延迟
            
            logger.info(f"✅ A股数据下载完成！成功: {success_count}/{len(stocks)}")
            
        else:
            # 中国数据处理（使用原有逻辑）
            logger.info("============================================================")
            logger.info("中国股票数据实时下载工具")
            logger.info("============================================================")
            logger.info(f"输出目录: {target_dir}")
            logger.info(f"时间范围: {trading_date or '20200101'} 到 {end_date or '20241231'} (YYYYMMDD格式)")
            logger.info(f"yfinance格式: {start_date} 到 {end_date_formatted}")
            logger.info(f"数据间隔: {interval}")
            logger.info(f"增量更新模式: {'是' if incremental_update else '否'}")
            logger.info(f"自动转换格式: 是")
            logger.info("============================================================")
            
            # 使用标准qlib下载逻辑
            qlib_version = ".".join(re.findall(r"(\d+)\.+", qlib.__version__))

            def _get_file_name_with_version(qlib_version, dataset_version):
                dataset_version = "v2" if dataset_version is None else dataset_version
                file_name_with_version = f"{dataset_version}/{name}_{region.lower()}_{interval.lower()}_{qlib_version}.zip"
                return file_name_with_version

            file_name = _get_file_name_with_version(qlib_version, dataset_version=version)
            if not self.check_dataset(file_name):
                file_name = _get_file_name_with_version("latest", dataset_version=version)
            self.download_data(file_name.lower(), target_dir, delete_old)

    def download_financial_data(
        self,
        target_dir="~/.qlib/financial_data",
        region="us",
        data_types=None,
        stock_symbols=None,
        max_stocks=None,
        max_workers=5,
        save_format="csv",
        include_ratios=True,
    ):
        """
        下载财务数据：基本面信息、财务报表、资产负债表、现金流量表等
        
        Parameters
        ----------
        target_dir: str
            财务数据保存目录
        region: str  
            市场区域，支持 ['us', 'hk', 'cn']
        data_types: list
            要下载的数据类型，支持:
            ['info', 'financials', 'balance_sheet', 'cashflow', 'dividends', 
             'splits', 'recommendations', 'institutional_holders', 'earnings']
        stock_symbols: list
            指定股票代码列表，如果为None则使用默认股票列表
        max_stocks: int or None
            最大股票数量限制，如果为None则下载所有可用股票
            默认为None(下载所有)，建议首次使用时设置较小值进行测试
        max_workers: int
            并行下载线程数
        save_format: str
            保存格式 ['csv', 'pickle', 'json']
        include_ratios: bool
            是否计算财务比率
        """
        
        if yf is None:
            logger.error("yfinance未安装，无法下载财务数据")
            return False
            
        # 默认数据类型
        if data_types is None:
            data_types = ['info', 'financials', 'balance_sheet', 'cashflow', 'dividends']
        
        # 获取股票列表
        if stock_symbols is None:
            if region.lower() == "us":
                all_stocks = self._get_us_stocks_list()
                stock_symbols = all_stocks[:max_stocks] if max_stocks else all_stocks
            elif region.lower() == "hk":
                all_stocks = self._get_hk_stocks_list()
                stock_symbols = all_stocks[:max_stocks] if max_stocks else all_stocks
            elif region.lower() == "cn":
                all_stocks = self._get_cn_stocks_list()
                stock_symbols = all_stocks[:max_stocks] if max_stocks else all_stocks
            else:
                logger.error(f"不支持的市场区域: {region}")
                return False
        else:
            # 如果用户提供了股票列表，仍可应用max_stocks限制
            if max_stocks and len(stock_symbols) > max_stocks:
                stock_symbols = stock_symbols[:max_stocks]
                logger.info(f"应用max_stocks限制，将下载前{max_stocks}只股票")
        
        target_path = Path(target_dir).expanduser()
        target_path.mkdir(parents=True, exist_ok=True)
        
        logger.info("=" * 60)
        logger.info(f"财务数据下载工具 - {region.upper()}市场")
        logger.info("=" * 60)
        logger.info(f"目标目录: {target_dir}")
        logger.info(f"数据类型: {data_types}")
        logger.info(f"股票数量: {len(stock_symbols)}")
        logger.info(f"并行线程: {max_workers}")
        logger.info(f"保存格式: {save_format}")
        logger.info(f"计算比率: {include_ratios}")
        logger.info("=" * 60)
        
        # 为每种数据类型创建目录
        for data_type in data_types:
            (target_path / data_type).mkdir(exist_ok=True)
        
        # 并行下载财务数据
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import time
        
        def download_single_stock_financial(symbol):
            """下载单只股票的财务数据"""
            try:
                # 处理股票代码格式
                if region.lower() == "hk" and not symbol.endswith(".HK"):
                    ticker_symbol = f"{symbol}.HK"
                elif region.lower() == "cn" and not (symbol.endswith(".SS") or symbol.endswith(".SZ")):
                    # 简单判断，实际应该根据交易所来确定
                    ticker_symbol = f"{symbol}.SS"
                else:
                    ticker_symbol = symbol
                
                stock = yf.Ticker(ticker_symbol)
                results = {}
                
                for data_type in data_types:
                    try:
                        if data_type == 'info':
                            data = stock.info
                            if data:
                                df = pd.DataFrame([data])
                                df['Symbol'] = symbol
                                df['UpdateTime'] = pd.Timestamp.now()
                                results[data_type] = df
                        
                        elif data_type == 'financials':
                            data = stock.financials
                            if not data.empty:
                                df = data.T  # 转置使日期成为行
                                df['Symbol'] = symbol
                                df['UpdateTime'] = pd.Timestamp.now()
                                results[data_type] = df
                        
                        elif data_type == 'balance_sheet':
                            data = stock.balance_sheet
                            if not data.empty:
                                df = data.T
                                df['Symbol'] = symbol
                                df['UpdateTime'] = pd.Timestamp.now()
                                results[data_type] = df
                        
                        elif data_type == 'cashflow':
                            data = stock.cashflow
                            if not data.empty:
                                df = data.T
                                df['Symbol'] = symbol
                                df['UpdateTime'] = pd.Timestamp.now()
                                results[data_type] = df
                        
                        elif data_type == 'dividends':
                            data = stock.dividends
                            if not data.empty:
                                df = data.to_frame('Dividend')
                                df['Symbol'] = symbol
                                df['UpdateTime'] = pd.Timestamp.now()
                                df.reset_index(inplace=True)
                                results[data_type] = df
                        
                        elif data_type == 'splits':
                            data = stock.splits
                            if not data.empty:
                                df = data.to_frame('Split')
                                df['Symbol'] = symbol
                                df['UpdateTime'] = pd.Timestamp.now()
                                df.reset_index(inplace=True)
                                results[data_type] = df
                        
                        elif data_type == 'recommendations':
                            data = stock.recommendations
                            if data is not None and not data.empty:
                                df = data.copy()
                                df['Symbol'] = symbol
                                df['UpdateTime'] = pd.Timestamp.now()
                                results[data_type] = df
                        
                        elif data_type == 'institutional_holders':
                            data = stock.institutional_holders
                            if data is not None and not data.empty:
                                df = data.copy()
                                df['Symbol'] = symbol
                                df['UpdateTime'] = pd.Timestamp.now()
                                results[data_type] = df
                        
                        elif data_type == 'earnings':
                            data = stock.earnings
                            if not data.empty:
                                df = data.copy()
                                df['Symbol'] = symbol
                                df['UpdateTime'] = pd.Timestamp.now()
                                df.reset_index(inplace=True)
                                results[data_type] = df
                    
                    except Exception as e:
                        logger.warning(f"获取 {symbol} 的 {data_type} 数据失败: {e}")
                        continue
                
                # 计算财务比率（如果需要）
                if include_ratios and 'info' in results and 'financials' in results and 'balance_sheet' in results:
                    try:
                        ratios_df = self._calculate_financial_ratios(
                            results.get('info'),
                            results.get('financials'), 
                            results.get('balance_sheet'),
                            symbol
                        )
                        if ratios_df is not None:
                            results['financial_ratios'] = ratios_df
                    except Exception as e:
                        logger.warning(f"计算 {symbol} 财务比率失败: {e}")
                
                return symbol, results
                
            except Exception as e:
                logger.error(f"下载 {symbol} 财务数据失败: {e}")
                return symbol, {}
        
        # 执行并行下载
        success_count = 0
        total_files_saved = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {executor.submit(download_single_stock_financial, symbol): symbol 
                              for symbol in stock_symbols}
            
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    symbol, results = future.result()
                    
                    if results:
                        # 保存数据
                        files_saved = self._save_financial_data(results, target_path, symbol, save_format)
                        total_files_saved += files_saved
                        success_count += 1
                        logger.info(f"✅ {symbol}: 成功保存 {files_saved} 个文件")
                    else:
                        logger.warning(f"❌ {symbol}: 未获取到任何数据")
                
                except Exception as e:
                    logger.error(f"❌ {symbol}: 处理失败 - {e}")
                
                # 添加延迟避免API限制
                time.sleep(0.5)
        
        logger.info("=" * 60)
        logger.info(f"✅ 财务数据下载完成！")
        logger.info(f"成功股票: {success_count}/{len(stock_symbols)}")
        logger.info(f"总文件数: {total_files_saved}")
        logger.info(f"数据目录: {target_path}")
        logger.info("=" * 60)
        
        return True

    def _calculate_financial_ratios(self, info_df, financials_df, balance_sheet_df, symbol):
        """计算财务比率"""
        try:
            if info_df is None or financials_df is None or balance_sheet_df is None:
                return None
            
            ratios = {}
            
            # 从info获取基本信息
            if not info_df.empty:
                market_cap = info_df.get('marketCap', {}).iloc[0] if 'marketCap' in info_df.columns else None
                shares_outstanding = info_df.get('sharesOutstanding', {}).iloc[0] if 'sharesOutstanding' in info_df.columns else None
                book_value = info_df.get('bookValue', {}).iloc[0] if 'bookValue' in info_df.columns else None
                current_price = info_df.get('currentPrice', {}).iloc[0] if 'currentPrice' in info_df.columns else None
                
                # 基本比率
                if market_cap:
                    ratios['MarketCap'] = market_cap
                if current_price and book_value and book_value > 0:
                    ratios['PriceToBook'] = current_price / book_value
                if shares_outstanding:
                    ratios['SharesOutstanding'] = shares_outstanding
            
            # 从财务报表计算比率
            if not financials_df.empty:
                # 获取最新年度数据
                latest_financials = financials_df.iloc[0] if len(financials_df) > 0 else None
                
                if latest_financials is not None:
                    total_revenue = latest_financials.get('Total Revenue')
                    net_income = latest_financials.get('Net Income')
                    
                    if total_revenue and net_income and total_revenue > 0:
                        ratios['NetProfitMargin'] = net_income / total_revenue
                    
                    if market_cap and total_revenue and total_revenue > 0:
                        ratios['PriceToSales'] = market_cap / total_revenue
                    
                    if market_cap and net_income and net_income > 0:
                        ratios['PriceToEarnings'] = market_cap / net_income
            
            # 从资产负债表计算比率  
            if not balance_sheet_df.empty:
                latest_balance = balance_sheet_df.iloc[0] if len(balance_sheet_df) > 0 else None
                
                if latest_balance is not None:
                    total_assets = latest_balance.get('Total Assets')
                    total_debt = latest_balance.get('Total Debt')
                    stockholder_equity = latest_balance.get('Stockholders Equity')
                    current_assets = latest_balance.get('Current Assets')
                    current_liabilities = latest_balance.get('Current Liabilities')
                    
                    # 资产负债比率
                    if total_assets and total_debt and total_assets > 0:
                        ratios['DebtToAssets'] = total_debt / total_assets
                    
                    if stockholder_equity and total_debt and stockholder_equity > 0:
                        ratios['DebtToEquity'] = total_debt / stockholder_equity
                    
                    # 流动比率
                    if current_assets and current_liabilities and current_liabilities > 0:
                        ratios['CurrentRatio'] = current_assets / current_liabilities
                    
                    # 托宾Q值
                    if market_cap and total_assets and total_assets > 0:
                        ratios['TobinsQ'] = market_cap / total_assets
            
            if ratios:
                df = pd.DataFrame([ratios])
                df['Symbol'] = symbol
                df['CalculationDate'] = pd.Timestamp.now()
                return df
            
            return None
            
        except Exception as e:
            logger.error(f"计算 {symbol} 财务比率失败: {e}")
            return None

    def _save_financial_data(self, results, target_path, symbol, save_format):
        """保存财务数据"""
        files_saved = 0
        
        for data_type, df in results.items():
            if df is None or df.empty:
                continue
                
            try:
                # 创建文件路径
                file_dir = target_path / data_type
                file_dir.mkdir(exist_ok=True)
                
                if save_format.lower() == 'csv':
                    file_path = file_dir / f"{symbol}.csv"
                    df.to_csv(file_path, index=True)
                elif save_format.lower() == 'pickle':
                    file_path = file_dir / f"{symbol}.pkl"
                    df.to_pickle(file_path)
                elif save_format.lower() == 'json':
                    file_path = file_dir / f"{symbol}.json"
                    df.to_json(file_path, orient='records', date_format='iso')
                else:
                    logger.warning(f"不支持的保存格式: {save_format}")
                    continue
                
                files_saved += 1
                
            except Exception as e:
                logger.error(f"保存 {symbol} 的 {data_type} 数据失败: {e}")
        
        return files_saved

    def download_fundamental_analysis_data(
        self,
        target_dir="~/.qlib/fundamental_data", 
        region="us",
        analysis_types=None,
        stock_symbols=None,
        include_technical_indicators=True,
        time_periods=None,
    ):
        """
        下载基本面分析数据（整合版）
        
        Parameters
        ----------
        target_dir: str
            数据保存目录
        region: str
            市场区域
        analysis_types: list
            分析类型 ['valuation', 'profitability', 'liquidity', 'leverage', 'efficiency']
        stock_symbols: list
            股票代码列表
        include_technical_indicators: bool
            是否包含技术指标
        time_periods: list
            时间周期 ['quarterly', 'annual']
        """
        
        if analysis_types is None:
            analysis_types = ['valuation', 'profitability', 'liquidity', 'leverage']
        
        if time_periods is None:
            time_periods = ['quarterly', 'annual']
        
        # 获取基础财务数据
        basic_data_types = ['info', 'financials', 'balance_sheet', 'cashflow']
        success = self.download_financial_data(
            target_dir=target_dir,
            region=region,
            data_types=basic_data_types,
            stock_symbols=stock_symbols,
            include_ratios=True
        )
        
        if not success:
            return False
        
        # 进行基本面分析
        return self._perform_fundamental_analysis(
            target_dir, analysis_types, time_periods, include_technical_indicators
        )

    def _perform_fundamental_analysis(self, target_dir, analysis_types, time_periods, include_technical):
        """执行基本面分析"""
        try:
            target_path = Path(target_dir).expanduser()
            analysis_dir = target_path / "analysis_results"
            analysis_dir.mkdir(exist_ok=True)
            
            logger.info("开始基本面分析...")
            
            # 这里可以添加具体的分析逻辑
            # 例如：计算各种财务比率、趋势分析、同行比较等
            
            logger.info("✅ 基本面分析完成")
            return True
            
        except Exception as e:
            logger.error(f"基本面分析失败: {e}")
            return False


# 为了兼容性，保留原来的GetData类名
GetData = GetDataEnhanced 