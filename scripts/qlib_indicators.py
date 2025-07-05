#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pandas as pd
import numpy as np
import talib
import struct
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from loguru import logger
import warnings
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
from functools import partial
import multiprocessing

warnings.filterwarnings('ignore', category=RuntimeWarning)
warnings.filterwarnings('ignore', category=FutureWarning)


class QlibIndicatorsEnhancedCalculator:
    """
    增强版Qlib指标计算器
    集成Alpha158、Alpha360指标体系和多种技术分析指标
    支持多线程并行计算和指标去重功能
    """
    
    # 字段中文标签映射
    FIELD_LABELS = {
        # 基础字段
        'Date': '日期',
        'Symbol': '股票代码',
        
        # OHLCV基础数据
        'Open': '开盘价',
        'High': '最高价', 
        'Low': '最低价',
        'Close': '收盘价',
        'Volume': '成交量',
        
        # Alpha158指标体系
        'ALPHA158_KMID': 'K线中点价格',
        'ALPHA158_KLEN': 'K线长度',
        'ALPHA158_KMID2': 'K线中点价格平方',
        'ALPHA158_KUP': 'K线上影线',
        'ALPHA158_KUP2': 'K线上影线平方',
        'ALPHA158_KLOW': 'K线下影线',
        'ALPHA158_KLOW2': 'K线下影线平方',
        'ALPHA158_KSFT': 'K线偏移',
        'ALPHA158_KSFT2': 'K线偏移平方',
        'ALPHA158_ROC_1': '1日收益率',
        'ALPHA158_ROC_2': '2日收益率',
        'ALPHA158_ROC_3': '3日收益率',
        'ALPHA158_ROC_4': '4日收益率',
        'ALPHA158_ROC_5': '5日收益率',
        'ALPHA158_ROC_10': '10日收益率',
        'ALPHA158_ROC_20': '20日收益率',
        'ALPHA158_ROC_30': '30日收益率',
        'ALPHA158_ROC_60': '60日收益率',
        'ALPHA158_MA_5': '5日移动平均',
        'ALPHA158_MA_10': '10日移动平均',
        'ALPHA158_MA_20': '20日移动平均',
        'ALPHA158_MA_30': '30日移动平均',
        'ALPHA158_MA_60': '60日移动平均',
        'ALPHA158_STD_5': '5日标准差',
        'ALPHA158_STD_10': '10日标准差',
        'ALPHA158_STD_20': '20日标准差',
        'ALPHA158_STD_30': '30日标准差',
        'ALPHA158_STD_60': '60日标准差',
        'ALPHA158_BETA_5': '5日贝塔值',
        'ALPHA158_BETA_10': '10日贝塔值',
        'ALPHA158_BETA_20': '20日贝塔值',
        'ALPHA158_BETA_30': '30日贝塔值',
        'ALPHA158_BETA_60': '60日贝塔值',
        'ALPHA158_RSQR_5': '5日R平方',
        'ALPHA158_RSQR_10': '10日R平方',
        'ALPHA158_RSQR_20': '20日R平方',
        'ALPHA158_RSQR_30': '30日R平方',
        'ALPHA158_RSQR_60': '60日R平方',
        'ALPHA158_RESI_5': '5日残差',
        'ALPHA158_RESI_10': '10日残差',
        'ALPHA158_RESI_20': '20日残差',
        'ALPHA158_RESI_30': '30日残差',
        'ALPHA158_RESI_60': '60日残差',
        'ALPHA158_MAX_5': '5日最大值',
        'ALPHA158_MAX_10': '10日最大值',
        'ALPHA158_MAX_20': '20日最大值',
        'ALPHA158_MAX_30': '30日最大值',
        'ALPHA158_MAX_60': '60日最大值',
        'ALPHA158_MIN_5': '5日最小值',
        'ALPHA158_MIN_10': '10日最小值',
        'ALPHA158_MIN_20': '20日最小值',
        'ALPHA158_MIN_30': '30日最小值',
        'ALPHA158_MIN_60': '60日最小值',
        'ALPHA158_QTLU_5': '5日上四分位数',
        'ALPHA158_QTLU_10': '10日上四分位数',
        'ALPHA158_QTLU_20': '20日上四分位数',
        'ALPHA158_QTLU_30': '30日上四分位数',
        'ALPHA158_QTLU_60': '60日上四分位数',
        'ALPHA158_QTLD_5': '5日下四分位数',
        'ALPHA158_QTLD_10': '10日下四分位数',
        'ALPHA158_QTLD_20': '20日下四分位数',
        'ALPHA158_QTLD_30': '30日下四分位数',
        'ALPHA158_QTLD_60': '60日下四分位数',
        'ALPHA158_RANK_5': '5日排名',
        'ALPHA158_RANK_10': '10日排名',
        'ALPHA158_RANK_20': '20日排名',
        'ALPHA158_RANK_30': '30日排名',
        'ALPHA158_RANK_60': '60日排名',
        'ALPHA158_RSV_5': '5日RSV',
        'ALPHA158_RSV_10': '10日RSV',
        'ALPHA158_RSV_20': '20日RSV',
        'ALPHA158_RSV_30': '30日RSV',
        'ALPHA158_RSV_60': '60日RSV',
        'ALPHA158_IMAX_5': '5日最大值位置',
        'ALPHA158_IMAX_10': '10日最大值位置',
        'ALPHA158_IMAX_20': '20日最大值位置',
        'ALPHA158_IMAX_30': '30日最大值位置',
        'ALPHA158_IMAX_60': '60日最大值位置',
        'ALPHA158_IMIN_5': '5日最小值位置',
        'ALPHA158_IMIN_10': '10日最小值位置',
        'ALPHA158_IMIN_20': '20日最小值位置',
        'ALPHA158_IMIN_30': '30日最小值位置',
        'ALPHA158_IMIN_60': '60日最小值位置',
        'ALPHA158_IMXD_5': '5日最大最小值差位置',
        'ALPHA158_IMXD_10': '10日最大最小值差位置',
        'ALPHA158_IMXD_20': '20日最大最小值差位置',
        'ALPHA158_IMXD_30': '30日最大最小值差位置',
        'ALPHA158_IMXD_60': '60日最大最小值差位置',
        'ALPHA158_CORR_5': '5日相关性',
        'ALPHA158_CORR_10': '10日相关性',
        'ALPHA158_CORR_20': '20日相关性',
        'ALPHA158_CORR_30': '30日相关性',
        'ALPHA158_CORR_60': '60日相关性',
        'ALPHA158_CORD_5': '5日相关性差分',
        'ALPHA158_CORD_10': '10日相关性差分',
        'ALPHA158_CORD_20': '20日相关性差分',
        'ALPHA158_CORD_30': '30日相关性差分',
        'ALPHA158_CORD_60': '60日相关性差分',
        
        # Alpha158新增指标
        'ALPHA158_RESI_5': '5日线性回归残差',
        'ALPHA158_RESI_10': '10日线性回归残差',
        'ALPHA158_RESI_20': '20日线性回归残差',
        'ALPHA158_RESI_30': '30日线性回归残差',
        'ALPHA158_RESI_60': '60日线性回归残差',
        'ALPHA158_IMAX_5': '5日最高价位置指数',
        'ALPHA158_IMAX_10': '10日最高价位置指数',
        'ALPHA158_IMAX_20': '20日最高价位置指数',
        'ALPHA158_IMAX_30': '30日最高价位置指数',
        'ALPHA158_IMAX_60': '60日最高价位置指数',
        'ALPHA158_IMIN_5': '5日最低价位置指数',
        'ALPHA158_IMIN_10': '10日最低价位置指数',
        'ALPHA158_IMIN_20': '20日最低价位置指数',
        'ALPHA158_IMIN_30': '30日最低价位置指数',
        'ALPHA158_IMIN_60': '60日最低价位置指数',
        'ALPHA158_IMXD_5': '5日最高最低价位置差',
        'ALPHA158_IMXD_10': '10日最高最低价位置差',
        'ALPHA158_IMXD_20': '20日最高最低价位置差',
        'ALPHA158_IMXD_30': '30日最高最低价位置差',
        'ALPHA158_IMXD_60': '60日最高最低价位置差',
        'ALPHA158_CNTP_5': '5日上涨天数比例',
        'ALPHA158_CNTP_10': '10日上涨天数比例',
        'ALPHA158_CNTP_20': '20日上涨天数比例',
        'ALPHA158_CNTP_30': '30日上涨天数比例',
        'ALPHA158_CNTP_60': '60日上涨天数比例',
        'ALPHA158_CNTN_5': '5日下跌天数比例',
        'ALPHA158_CNTN_10': '10日下跌天数比例',
        'ALPHA158_CNTN_20': '20日下跌天数比例',
        'ALPHA158_CNTN_30': '30日下跌天数比例',
        'ALPHA158_CNTN_60': '60日下跌天数比例',
        'ALPHA158_CNTD_5': '5日涨跌天数差',
        'ALPHA158_CNTD_10': '10日涨跌天数差',
        'ALPHA158_CNTD_20': '20日涨跌天数差',
        'ALPHA158_CNTD_30': '30日涨跌天数差',
        'ALPHA158_CNTD_60': '60日涨跌天数差',
        'ALPHA158_SUMP_5': '5日总收益比例',
        'ALPHA158_SUMP_10': '10日总收益比例',
        'ALPHA158_SUMP_20': '20日总收益比例',
        'ALPHA158_SUMP_30': '30日总收益比例',
        'ALPHA158_SUMP_60': '60日总收益比例',
        'ALPHA158_SUMN_5': '5日总损失比例',
        'ALPHA158_SUMN_10': '10日总损失比例',
        'ALPHA158_SUMN_20': '20日总损失比例',
        'ALPHA158_SUMN_30': '30日总损失比例',
        'ALPHA158_SUMN_60': '60日总损失比例',
        'ALPHA158_SUMD_5': '5日收益损失差比例',
        'ALPHA158_SUMD_10': '10日收益损失差比例',
        'ALPHA158_SUMD_20': '20日收益损失差比例',
        'ALPHA158_SUMD_30': '30日收益损失差比例',
        'ALPHA158_SUMD_60': '60日收益损失差比例',
        'ALPHA158_VMA_5': '5日成交量移动平均',
        'ALPHA158_VMA_10': '10日成交量移动平均',
        'ALPHA158_VMA_20': '20日成交量移动平均',
        'ALPHA158_VMA_30': '30日成交量移动平均',
        'ALPHA158_VMA_60': '60日成交量移动平均',
        'ALPHA158_VSTD_5': '5日成交量标准差',
        'ALPHA158_VSTD_10': '10日成交量标准差',
        'ALPHA158_VSTD_20': '20日成交量标准差',
        'ALPHA158_VSTD_30': '30日成交量标准差',
        'ALPHA158_VSTD_60': '60日成交量标准差',
        'ALPHA158_WVMA_5': '5日成交量加权价格波动率',
        'ALPHA158_WVMA_10': '10日成交量加权价格波动率',
        'ALPHA158_WVMA_20': '20日成交量加权价格波动率',
        'ALPHA158_WVMA_30': '30日成交量加权价格波动率',
        'ALPHA158_WVMA_60': '60日成交量加权价格波动率',
        'ALPHA158_VSUMP_5': '5日成交量上升比例',
        'ALPHA158_VSUMP_10': '10日成交量上升比例',
        'ALPHA158_VSUMP_20': '20日成交量上升比例',
        'ALPHA158_VSUMP_30': '30日成交量上升比例',
        'ALPHA158_VSUMP_60': '60日成交量上升比例',
        'ALPHA158_VSUMN_5': '5日成交量下降比例',
        'ALPHA158_VSUMN_10': '10日成交量下降比例',
        'ALPHA158_VSUMN_20': '20日成交量下降比例',
        'ALPHA158_VSUMN_30': '30日成交量下降比例',
        'ALPHA158_VSUMN_60': '60日成交量下降比例',
        'ALPHA158_VSUMD_5': '5日成交量涨跌差比例',
        'ALPHA158_VSUMD_10': '10日成交量涨跌差比例',
        'ALPHA158_VSUMD_20': '20日成交量涨跌差比例',
        'ALPHA158_VSUMD_30': '30日成交量涨跌差比例',
        'ALPHA158_VSUMD_60': '60日成交量涨跌差比例',
        
        # 技术指标
        'SMA_5': '5日简单移动平均',
        'SMA_10': '10日简单移动平均',
        'SMA_20': '20日简单移动平均',
        'SMA_50': '50日简单移动平均',
        'EMA_5': '5日指数移动平均',
        'EMA_10': '10日指数移动平均',
        'EMA_20': '20日指数移动平均',
        'EMA_50': '50日指数移动平均',
        'DEMA_20': '20日双重指数移动平均',
        'TEMA_20': '20日三重指数移动平均',
        'KAMA_30': '30日自适应移动平均',
        'WMA_20': '20日加权移动平均',
        'MACD': 'MACD主线',
        'MACD_Signal': 'MACD信号线',
        'MACD_Histogram': 'MACD柱状图',
        'MACDEXT': 'MACD扩展',
        'MACDFIX': 'MACD固定',
        'RSI_14': '14日相对强弱指数',
        'CCI_14': '14日商品通道指数',
        'CMO_14': '14日钱德动量摆动指标',
        'MFI_14': '14日资金流量指数',
        'WILLR_14': '14日威廉指标',
        'ULTOSC': '终极震荡指标',
        'ADX_14': '14日平均趋向指数',
        'ADXR_14': '14日平均趋向指数评级',
        'APO': '绝对价格震荡指标',
        'AROON_DOWN': '阿隆下降指标',
        'AROON_UP': '阿隆上升指标',
        'AROONOSC_14': '14日阿隆震荡指标',
        'BOP': '均势指标',
        'DX_14': '14日趋向指数',
        'MINUS_DI_14': '14日负趋向指标',
        'MINUS_DM_14': '14日负向运动',
        'PLUS_DI_14': '14日正趋向指标',
        'PLUS_DM_14': '14日正向运动',
        'PPO': '价格震荡百分比',
        'TRIX_30': '30日三重指数平均趋势',
        'MOM_10': '10日动量指标',
        'ROC_10': '10日变动率',
        'ROCP_10': '10日变动率百分比',
        'ROCR_10': '10日变动率比率',
        'ROCR100_10': '10日变动率比率100',
        'BB_Upper': '布林带上轨',
        'BB_Middle': '布林带中轨',
        'BB_Lower': '布林带下轨',
        'STOCH_K': '随机指标K值',
        'STOCH_D': '随机指标D值',
        'STOCHF_K': '快速随机指标K值',
        'STOCHF_D': '快速随机指标D值',
        'STOCHRSI_K': '随机RSI K值',
        'STOCHRSI_D': '随机RSI D值',
        'ATR_14': '14日平均真实范围',
        'NATR_14': '14日标准化平均真实范围',
        'TRANGE': '真实范围',
        'AD': '累积/派发线',
        'ADOSC': '累积/派发震荡指标',
        'OBV': '能量潮指标',
        'HT_DCPERIOD': '希尔伯特变换主导周期',
        'HT_DCPHASE': '希尔伯特变换主导相位',
        'HT_PHASOR_INPHASE': '希尔伯特变换相位器同相分量',
        'HT_PHASOR_QUADRATURE': '希尔伯特变换相位器正交分量',
        'HT_SINE_SINE': '希尔伯特变换正弦波',
        'HT_SINE_LEADSINE': '希尔伯特变换前导正弦波',
        'HT_TRENDMODE': '希尔伯特变换趋势模式',
        'AVGPRICE': '平均价格',
        'MEDPRICE': '中位价格',
        'TYPPRICE': '典型价格',
        'WCLPRICE': '加权收盘价',
        'BETA': 'Beta系数',
        'CORREL': '相关系数',
        'LINEARREG': '线性回归',
        'LINEARREG_ANGLE': '线性回归角度',
        'LINEARREG_INTERCEPT': '线性回归截距',
        'LINEARREG_SLOPE': '线性回归斜率',
        'STDDEV': '标准差',
        'TSF': '时间序列预测',
        'VAR': '方差',
        
        # 蜡烛图形态指标
        'CDL2CROWS': '两只乌鸦',
        'CDL3BLACKCROWS': '三只黑乌鸦',
        'CDL3INSIDE': '三内部上升和下降',
        'CDL3LINESTRIKE': '三线打击',
        'CDL3OUTSIDE': '三外部上升和下降',
        'CDL3STARSINSOUTH': '南方三星',
        'CDL3WHITESOLDIERS': '三白兵',
        'CDLABANDONEDBABY': '弃婴',
        'CDLADVANCEBLOCK': '大敌当前',
        'CDLBELTHOLD': '捉腰带线',
        'CDLBREAKAWAY': '脱离',
        'CDLCLOSINGMARUBOZU': '收盘缺影线',
        'CDLCONCEALBABYSWALL': '藏婴吞没',
        'CDLCOUNTERATTACK': '反击线',
        'CDLDARKCLOUDCOVER': '乌云压顶',
        'CDLDOJI': '十字',
        'CDLDOJISTAR': '十字星',
        'CDLDRAGONFLYDOJI': '蜻蜓十字',
        'CDLENGULFING': '吞噬模式',
        'CDLEVENINGDOJISTAR': '黄昏十字星',
        'CDLEVENINGSTAR': '黄昏星',
        'CDLGAPSIDESIDEWHITE': '向上/下跳空并列阳线',
        'CDLGRAVESTONEDOJI': '墓碑十字',
        'CDLHAMMER': '锤头',
        'CDLHANGINGMAN': '上吊线',
        'CDLHARAMI': '母子线',
        'CDLHARAMICROSS': '十字孕线',
        'CDLHIGHWAVE': '长腿十字',
        'CDLHIKKAKE': 'Hikkake模式',
        'CDLHIKKAKEMOD': '修正Hikkake模式',
        'CDLHOMINGPIGEON': '家鸽',
        'CDLIDENTICAL3CROWS': '三胞胎乌鸦',
        'CDLINNECK': '颈内线',
        'CDLINVERTEDHAMMER': '倒锤头',
        'CDLKICKING': '反冲',
        'CDLKICKINGBYLENGTH': '由较长缺影线决定的反冲',
        'CDLLADDERBOTTOM': '梯底',
        'CDLLONGLEGGEDDOJI': '长腿十字',
        'CDLLONGLINE': '长蜡烛',
        'CDLMARUBOZU': '光头光脚',
        'CDLMATCHINGLOW': '相同低价',
        'CDLMATHOLD': '铺垫',
        'CDLMORNINGDOJISTAR': '早晨十字星',
        'CDLMORNINGSTAR': '早晨星',
        'CDLONNECK': '颈上线',
        'CDLPIERCING': '刺透',
        'CDLRICKSHAWMAN': '黄包车夫',
        'CDLRISEFALL3METHODS': '上升/下降三法',
        'CDLSEPARATINGLINES': '分离线',
        'CDLSHOOTINGSTAR': '射击之星',
        'CDLSHORTLINE': '短蜡烛',
        'CDLSPINNINGTOP': '纺锤',
        'CDLSTALLEDPATTERN': '停顿形态',
        'CDLSTICKSANDWICH': '条形三明治',
        'CDLTAKURI': 'Takuri线',
        'CDLTASUKIGAP': 'Tasuki跳空',
        'CDLTHRUSTING': '插入',
        'CDLTRISTAR': '三星',
        'CDLUNIQUE3RIVER': '奇特三河床',
        'CDLUPSIDEGAP2CROWS': '向上跳空的两只乌鸦',
        'CDLXSIDEGAP3METHODS': 'X侧跳空三法',
        
        # 财务指标
        'MarketCap': '市值',
        'PriceToBook': '市净率',
        'PE': '市盈率',
        'ROE': '净资产收益率',
        'ROA': '总资产收益率',
        'CurrentRatio': '流动比率',
        'QuickRatio': '速动比率',
        'DebtToEquity': '资产负债率',
        'GrossMargin': '毛利率',
        'NetMargin': '净利率',
        'TobinsQ': '托宾Q值',
        'turnover_5': '5日换手率',
        'turnover_10': '10日换手率',
        'turnover_20': '20日换手率',
        'turnover_60': '60日换手率',
        
        # 波动率指标
        'RealizedVolatility_5': '5日已实现波动率',
        'RealizedVolatility_10': '10日已实现波动率',
        'RealizedVolatility_20': '20日已实现波动率',
        'RealizedVolatility_60': '60日已实现波动率',
        'SemiDeviation_5': '5日半变差',
        'SemiDeviation_10': '10日半变差',
        'SemiDeviation_20': '20日半变差',
        'SemiDeviation_60': '60日半变差',
    }
    
    # 为Alpha360指标生成标签
    @classmethod
    def _generate_alpha360_labels(cls):
        """生成Alpha360指标的中文标签"""
        labels = {}
        features = ['open', 'close', 'high', 'low', 'volume']
        feature_names = {'open': '开盘价', 'close': '收盘价', 'high': '最高价', 'low': '最低价', 'volume': '成交量'}
        
        for i in range(60):
            for feature in features:
                for j in range(6):
                    col_name = f"ALPHA360_{feature}_{i}_{j}"
                    labels[col_name] = f"{feature_names[feature]}标准化T-{i}期第{j}维"
        
        return labels
    
    def get_field_labels(self, columns):
        """获取所有字段的中文标签"""
        # 合并静态标签和动态生成的Alpha360标签
        all_labels = self.FIELD_LABELS.copy()
        all_labels.update(self._generate_alpha360_labels())
        
        # 为每个列名生成标签，如果没有预定义标签则使用默认标签
        labels = []
        for col in columns:
            if col in all_labels:
                labels.append(all_labels[col])
            elif col.startswith('ALPHA360_'):
                # 对于未预定义的Alpha360指标，生成默认标签
                labels.append(f"Alpha360指标_{col.replace('ALPHA360_', '')}")
            elif col.startswith('ALPHA158_'):
                # 对于未预定义的Alpha158指标，生成默认标签
                labels.append(f"Alpha158指标_{col.replace('ALPHA158_', '')}")
            elif col.startswith('CDL'):
                # 对于未预定义的蜡烛图形态，生成默认标签
                labels.append(f"蜡烛图形态_{col.replace('CDL', '')}")
            else:
                # 其他未预定义的指标，使用原字段名
                labels.append(col)
        
        return labels
    
    def __init__(self, data_dir: str = r"D:\stk_data\trd\us_data", financial_data_dir: str = None, 
                 max_workers: int = None, enable_parallel: bool = True):
        self.data_dir = Path(data_dir)
        self.features_dir = self.data_dir / "features"
        self.output_dir = self.data_dir
        
        # 多线程配置
        self.enable_parallel = enable_parallel
        self.max_workers = max_workers or min(32, (multiprocessing.cpu_count() or 1) + 4)
        
        # 财务数据目录
        if financial_data_dir:
            self.financial_data_dir = Path(financial_data_dir)
        else:
            # 默认从用户的财务数据目录查找
            self.financial_data_dir = Path.home() / ".qlib" / "financial_data"
        
        logger.info(f"数据目录: {self.data_dir}")
        logger.info(f"财务数据目录: {self.financial_data_dir}")
        logger.info(f"多线程配置: {'启用' if self.enable_parallel else '禁用'} (最大线程数: {self.max_workers})")
        
        if not self.data_dir.exists():
            logger.error(f"Data directory does not exist: {self.data_dir}")
            raise FileNotFoundError(f"Data directory does not exist: {self.data_dir}")
        
        if not self.features_dir.exists():
            logger.warning(f"Features directory does not exist: {self.features_dir}")
        
        # 初始化财务数据缓存
        self.financial_cache = {}
        self._load_financial_data()
        
        # 线程本地存储，确保线程安全
        self._local = threading.local()
        
        # 线程锁，保护共享资源
        self._lock = threading.Lock()
    
    def _load_financial_data(self):
        """加载财务数据到内存缓存"""
        try:
            if self.financial_data_dir is None:
                logger.warning("未指定财务数据目录，将使用估算值")
                return
                
            logger.info(f"正在加载财务数据从: {self.financial_data_dir}")
            
            # 加载各种财务数据
            data_types = ['info', 'financials', 'balance_sheet', 'cashflow', 'dividends', 'financial_ratios']
            
            for data_type in data_types:
                data_path = self.financial_data_dir / data_type
                if data_path.exists():
                    self.financial_cache[data_type] = {}
                    
                    # 读取CSV文件
                    csv_files = list(data_path.glob("*.csv"))
                    if csv_files:
                        for csv_file in csv_files:
                            symbol = csv_file.stem.upper()
                            try:
                                df = pd.read_csv(csv_file, index_col=0)
                                self.financial_cache[data_type][symbol] = df
                            except Exception as e:
                                logger.warning(f"Failed to load {data_type} for {symbol}: {e}")
                        
                        logger.info(f"✅ 加载 {data_type} 数据: {len(self.financial_cache[data_type])} 只股票")
                    else:
                        logger.warning(f"📁 {data_type} 目录为空")
                else:
                    logger.warning(f"📁 财务数据目录不存在: {data_path}")
                    
        except Exception as e:
            logger.error(f"加载财务数据失败: {e}")
            self.financial_cache = {}
    
    def read_qlib_binary_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """读取Qlib二进制数据"""
        symbol_dir = self.features_dir / symbol.lower()
        
        if not symbol_dir.exists():
            return None
        
        features = ['open', 'high', 'low', 'close', 'volume']
        data_dict = {}
        
        try:
            for feature in features:
                bin_file = symbol_dir / f"{feature}.day.bin"
                if bin_file.exists():
                    with open(bin_file, 'rb') as f:
                        values = []
                        while True:
                            data = f.read(4)
                            if not data:
                                break
                            value = struct.unpack('<f', data)[0]
                            if not np.isnan(value) and not np.isinf(value):
                                values.append(value)
                            else:
                                values.append(0.0)
                        data_dict[feature.title()] = values
            
            if not data_dict:
                return None
            
            # Ensure all arrays have the same length
            lengths = [len(v) for v in data_dict.values()]
            if len(set(lengths)) > 1:
                min_length = min(lengths)
                for key in data_dict:
                    data_dict[key] = data_dict[key][:min_length]
            
            df = pd.DataFrame(data_dict)
            
            # Read actual calendar dates from qlib
            calendar_file = self.data_dir / "calendars" / "day.txt"
            if calendar_file.exists():
                with open(calendar_file, 'r') as f:
                    calendar_dates = [line.strip() for line in f.readlines()]
                calendar_dates = [pd.to_datetime(date) for date in calendar_dates if date.strip()]
                
                # The data in qlib is typically aligned with the calendar in reverse order
                if len(df) <= len(calendar_dates):
                    dates = calendar_dates[-len(df):]
                else:
                    # If data is longer than calendar, extend backwards
                    latest_date = calendar_dates[-1] if calendar_dates else pd.to_datetime('2025-06-27')
                    all_dates = pd.bdate_range(end=latest_date, periods=len(df), freq='B')
                    dates = all_dates.tolist()
            else:
                # Fallback: generate business days ending at a reasonable date
                end_date = pd.to_datetime('2025-06-27')
                dates = pd.bdate_range(end=end_date, periods=len(df), freq='B')
            
            df.index = pd.DatetimeIndex(dates)
            
            return df
            
        except Exception as e:
            logger.warning(f"Failed to read binary data {symbol}: {e}")
            return None
    
    def get_available_stocks(self) -> List[str]:
        """获取可用股票列表"""
        stocks = []
        
        if not self.features_dir.exists():
            logger.error(f"Features directory does not exist: {self.features_dir}")
            return stocks
        
        for stock_dir in self.features_dir.iterdir():
            if stock_dir.is_dir():
                required_files = ['open.day.bin', 'high.day.bin', 'low.day.bin', 'close.day.bin', 'volume.day.bin']
                if all((stock_dir / file).exists() for file in required_files):
                    symbol = stock_dir.name.upper()
                    stocks.append(symbol)
        
        logger.info(f"Found {len(stocks)} stocks data")
        return sorted(stocks)
    
    def get_financial_data(self, symbol: str, data_type: str) -> Optional[pd.DataFrame]:
        """获取财务数据"""
        try:
            # 尝试多种符号格式
            symbol_variants = [
                symbol,                        # 原始符号
                symbol.replace('_', '.'),      # 下划线转点号 (0002_HK -> 0002.HK)
                symbol.replace('.', '_'),      # 点号转下划线 (0002.HK -> 0002_HK)
                symbol.upper(),                # 大写
                symbol.replace('_HK', '.HK'),  # 特定转换
                symbol.replace('.HK', '_HK')   # 特定转换
            ]
            
            if data_type in self.financial_cache:
                for variant in symbol_variants:
                    if variant in self.financial_cache[data_type]:
                        return self.financial_cache[data_type][variant]
            return None
        except Exception as e:
            logger.warning(f"Failed to get financial data for {symbol}, {data_type}: {e}")
            return None
    
    def _safe_divide(self, a, b, fill_value=0.0):
        """安全除法操作，避免除零错误"""
        return np.where(np.abs(b) > 1e-12, a / b, fill_value)
    
    def _get_calculated_indicators(self):
        """获取线程本地的指标集合"""
        if not hasattr(self._local, 'calculated_indicators'):
            self._local.calculated_indicators = set()
        return self._local.calculated_indicators
    
    def _add_indicator(self, indicators: dict, name: str, values):
        """添加指标到字典中，避免重复（线程安全）"""
        calculated_indicators = self._get_calculated_indicators()
        if name not in calculated_indicators:
            indicators[name] = values
            calculated_indicators.add(name)
        else:
            logger.debug(f"指标 {name} 已存在，跳过重复计算")
    
    def _reset_indicators_cache(self):
        """重置线程本地的指标缓存"""
        if hasattr(self._local, 'calculated_indicators'):
            self._local.calculated_indicators.clear()
    
    def calculate_all_technical_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """计算所有技术指标（共约60个）"""
        if data.empty or len(data) < 50:
            logger.warning("Insufficient data for calculating technical indicators")
            return pd.DataFrame()
        
        try:
            indicators = {}
            
            # Clean data and convert to numpy arrays for talib
            close = data['Close'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            high = data['High'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            low = data['Low'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            open_price = data['Open'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            volume = data['Volume'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(0).values
            
            # 1. Moving Averages (移动平均线类) - 12个
            self._add_indicator(indicators, 'SMA_5', talib.SMA(close, timeperiod=5))
            self._add_indicator(indicators, 'SMA_10', talib.SMA(close, timeperiod=10))
            self._add_indicator(indicators, 'SMA_20', talib.SMA(close, timeperiod=20))
            self._add_indicator(indicators, 'SMA_50', talib.SMA(close, timeperiod=50))
            
            self._add_indicator(indicators, 'EMA_5', talib.EMA(close, timeperiod=5))
            self._add_indicator(indicators, 'EMA_10', talib.EMA(close, timeperiod=10))
            self._add_indicator(indicators, 'EMA_20', talib.EMA(close, timeperiod=20))
            self._add_indicator(indicators, 'EMA_50', talib.EMA(close, timeperiod=50))
            
            self._add_indicator(indicators, 'DEMA_20', talib.DEMA(close, timeperiod=20))
            self._add_indicator(indicators, 'TEMA_20', talib.TEMA(close, timeperiod=20))
            self._add_indicator(indicators, 'KAMA_30', talib.KAMA(close, timeperiod=30))
            self._add_indicator(indicators, 'WMA_20', talib.WMA(close, timeperiod=20))
            
            # 2. MACD Family - 3个
            indicators['MACD'], indicators['MACD_Signal'], indicators['MACD_Histogram'] = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
            indicators['MACDEXT'], _, _ = talib.MACDEXT(close, fastperiod=12, slowperiod=26, signalperiod=9)
            indicators['MACDFIX'], _, _ = talib.MACDFIX(close, signalperiod=9)
            
            # 3. Momentum Oscillators (动量振荡器) - 6个
            indicators['RSI_14'] = talib.RSI(close, timeperiod=14)
            indicators['CCI_14'] = talib.CCI(high, low, close, timeperiod=14)
            indicators['CMO_14'] = talib.CMO(close, timeperiod=14)
            indicators['MFI_14'] = talib.MFI(high, low, close, volume, timeperiod=14)
            indicators['WILLR_14'] = talib.WILLR(high, low, close, timeperiod=14)
            indicators['ULTOSC'] = talib.ULTOSC(high, low, close, timeperiod1=7, timeperiod2=14, timeperiod3=28)
            
            # 4. Trend Indicators (趋势指标) - 13个
            indicators['ADX_14'] = talib.ADX(high, low, close, timeperiod=14)
            indicators['ADXR_14'] = talib.ADXR(high, low, close, timeperiod=14)
            indicators['APO'] = talib.APO(close, fastperiod=12, slowperiod=26)
            indicators['AROON_DOWN'], indicators['AROON_UP'] = talib.AROON(high, low, timeperiod=14)
            indicators['AROONOSC_14'] = talib.AROONOSC(high, low, timeperiod=14)
            indicators['BOP'] = talib.BOP(open_price, high, low, close)
            indicators['DX_14'] = talib.DX(high, low, close, timeperiod=14)
            indicators['MINUS_DI_14'] = talib.MINUS_DI(high, low, close, timeperiod=14)
            indicators['MINUS_DM_14'] = talib.MINUS_DM(high, low, timeperiod=14)
            indicators['PLUS_DI_14'] = talib.PLUS_DI(high, low, close, timeperiod=14)
            indicators['PLUS_DM_14'] = talib.PLUS_DM(high, low, timeperiod=14)
            indicators['PPO'] = talib.PPO(close, fastperiod=12, slowperiod=26)
            indicators['TRIX_30'] = talib.TRIX(close, timeperiod=30)
            
            # 5. Momentum Indicators (动量指标) - 5个
            indicators['MOM_10'] = talib.MOM(close, timeperiod=10)
            indicators['ROC_10'] = talib.ROC(close, timeperiod=10)
            indicators['ROCP_10'] = talib.ROCP(close, timeperiod=10)
            indicators['ROCR_10'] = talib.ROCR(close, timeperiod=10)
            indicators['ROCR100_10'] = talib.ROCR100(close, timeperiod=10)
            
            # 6. Bollinger Bands - 3个
            indicators['BB_Upper'], indicators['BB_Middle'], indicators['BB_Lower'] = talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2)
            
            # 7. Stochastic (随机指标) - 6个
            indicators['STOCH_K'], indicators['STOCH_D'] = talib.STOCH(high, low, close, fastk_period=14, slowk_period=3, slowd_period=3)
            indicators['STOCHF_K'], indicators['STOCHF_D'] = talib.STOCHF(high, low, close, fastk_period=14, fastd_period=3)
            indicators['STOCHRSI_K'], indicators['STOCHRSI_D'] = talib.STOCHRSI(close, timeperiod=14, fastk_period=5, fastd_period=3)
            
            # 8. Volatility Indicators (波动率指标) - 3个
            indicators['ATR_14'] = talib.ATR(high, low, close, timeperiod=14)
            indicators['NATR_14'] = talib.NATR(high, low, close, timeperiod=14)
            indicators['TRANGE'] = talib.TRANGE(high, low, close)
            
            # 9. Volume Indicators (成交量指标) - 3个
            indicators['OBV'] = talib.OBV(close, volume)
            indicators['AD'] = talib.AD(high, low, close, volume)
            indicators['ADOSC'] = talib.ADOSC(high, low, close, volume, fastperiod=3, slowperiod=10)
            
            # 10. Hilbert Transform (希尔伯特变换) - 7个
            indicators['HT_DCPERIOD'] = talib.HT_DCPERIOD(close)
            indicators['HT_DCPHASE'] = talib.HT_DCPHASE(close)
            indicators['HT_INPHASE'], indicators['HT_QUADRATURE'] = talib.HT_PHASOR(close)
            indicators['HT_SINE'], indicators['HT_LEADSINE'] = talib.HT_SINE(close)
            indicators['HT_TRENDMODE'] = talib.HT_TRENDMODE(close)
            indicators['HT_TRENDLINE'] = talib.HT_TRENDLINE(close)
            
            # 11. Math Transform (数学变换) - 8个
            indicators['AVGPRICE'] = talib.AVGPRICE(open_price, high, low, close)
            indicators['MEDPRICE'] = talib.MEDPRICE(high, low)
            indicators['TYPPRICE'] = talib.TYPPRICE(high, low, close)
            indicators['WCLPRICE'] = talib.WCLPRICE(high, low, close)
            indicators['MIDPOINT'] = talib.MIDPOINT(close, timeperiod=14)
            indicators['MIDPRICE'] = talib.MIDPRICE(high, low, timeperiod=14)
            indicators['MAMA'], indicators['FAMA'] = talib.MAMA(close)
            
            # 12. Statistical Functions (统计函数) - 7个
            indicators['LINEARREG'] = talib.LINEARREG(close, timeperiod=14)
            indicators['LINEARREG_ANGLE'] = talib.LINEARREG_ANGLE(close, timeperiod=14)
            indicators['LINEARREG_INTERCEPT'] = talib.LINEARREG_INTERCEPT(close, timeperiod=14)
            indicators['LINEARREG_SLOPE'] = talib.LINEARREG_SLOPE(close, timeperiod=14)
            indicators['STDDEV'] = talib.STDDEV(close, timeperiod=30)
            indicators['TSF'] = talib.TSF(close, timeperiod=14)
            indicators['VAR'] = talib.VAR(close, timeperiod=30)
            
            # 13. Min/Max Functions - 2个
            indicators['MAXINDEX'] = talib.MAXINDEX(close, timeperiod=30)
            indicators['MININDEX'] = talib.MININDEX(close, timeperiod=30)
            
            # 转换为DataFrame
            indicators_df = pd.DataFrame(indicators, index=data.index)
            
            logger.info(f"计算了 {len(indicators)} 个技术指标")
            return indicators_df
            
        except Exception as e:
            logger.error(f"计算技术指标失败: {e}")
            return pd.DataFrame()
    
    def calculate_alpha158_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        计算Alpha158指标体系 (158个指标)
        包括KBAR指标、价格指标、成交量指标、滚动技术指标
        """
        if data.empty or len(data) < 60:
            logger.warning("数据不足以计算Alpha158指标")
            return pd.DataFrame()
        
        try:
            indicators = {}
            
            # 清理数据
            open_price = data['Open'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            high = data['High'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            low = data['Low'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            close = data['Close'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            volume = data['Volume'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(0).values
            
            # 计算VWAP
            vwap = self._safe_divide(
                np.convolve(close * volume, np.ones(1), 'same'),
                np.convolve(volume, np.ones(1), 'same')
            )
            
            # 1. KBAR指标 (9个)
            self._add_indicator(indicators, 'ALPHA158_KMID', self._safe_divide(close - open_price, open_price))
            self._add_indicator(indicators, 'ALPHA158_KLEN', self._safe_divide(high - low, open_price))
            self._add_indicator(indicators, 'ALPHA158_KMID2', self._safe_divide(close - open_price, high - low + 1e-12))
            self._add_indicator(indicators, 'ALPHA158_KUP', self._safe_divide(high - np.maximum(open_price, close), open_price))
            self._add_indicator(indicators, 'ALPHA158_KUP2', self._safe_divide(high - np.maximum(open_price, close), high - low + 1e-12))
            self._add_indicator(indicators, 'ALPHA158_KLOW', self._safe_divide(np.minimum(open_price, close) - low, open_price))
            self._add_indicator(indicators, 'ALPHA158_KLOW2', self._safe_divide(np.minimum(open_price, close) - low, high - low + 1e-12))
            self._add_indicator(indicators, 'ALPHA158_KSFT', self._safe_divide(2 * close - high - low, open_price))
            self._add_indicator(indicators, 'ALPHA158_KSFT2', self._safe_divide(2 * close - high - low, high - low + 1e-12))
            
            # 2. 价格指标 (标准化到收盘价)
            features = ['OPEN', 'HIGH', 'LOW', 'VWAP']
            for feature in features:
                if feature == 'OPEN':
                    price_values = open_price
                elif feature == 'HIGH':
                    price_values = high
                elif feature == 'LOW':
                    price_values = low
                elif feature == 'VWAP':
                    price_values = vwap
                
                self._add_indicator(indicators, f'ALPHA158_{feature}0', self._safe_divide(price_values, close))
            
            # 3. 成交量指标
            self._add_indicator(indicators, 'ALPHA158_VOLUME0', self._safe_divide(volume, volume + 1e-12))
            
            # 4. 滚动技术指标
            windows = [5, 10, 20, 30, 60]
            
            # ROC - Rate of Change
            for d in windows:
                ref_close = np.roll(close, d)
                self._add_indicator(indicators, f'ALPHA158_ROC{d}', self._safe_divide(ref_close, close))
            
            # MA - Simple Moving Average
            for d in windows:
                ma_values = pd.Series(close).rolling(window=d, min_periods=1).mean().values
                self._add_indicator(indicators, f'ALPHA158_MA{d}', self._safe_divide(ma_values, close))
            
            # STD - Standard Deviation
            for d in windows:
                std_values = pd.Series(close).rolling(window=d, min_periods=1).std().fillna(0).values
                self._add_indicator(indicators, f'ALPHA158_STD{d}', self._safe_divide(std_values, close))
            
            # BETA - Slope
            for d in windows:
                beta_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    x = np.arange(d)
                    y = close[i-d+1:i+1]
                    if len(y) > 1:
                        try:
                            slope = np.polyfit(x, y, 1)[0]
                            beta_values[i] = slope
                        except:
                            beta_values[i] = 0
                self._add_indicator(indicators, f'ALPHA158_BETA{d}', self._safe_divide(beta_values, close))
            
            # RSQR - R-square
            for d in windows:
                rsqr_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    x = np.arange(d)
                    y = close[i-d+1:i+1]
                    if len(y) > 1:
                        try:
                            correlation_matrix = np.corrcoef(x, y)
                            rsqr_values[i] = correlation_matrix[0, 1] ** 2 if not np.isnan(correlation_matrix[0, 1]) else 0
                        except:
                            rsqr_values[i] = 0
                self._add_indicator(indicators, f'ALPHA158_RSQR{d}', rsqr_values)
            
            # MAX/MIN
            for d in windows:
                max_values = pd.Series(high).rolling(window=d, min_periods=1).max().values
                min_values = pd.Series(low).rolling(window=d, min_periods=1).min().values
                self._add_indicator(indicators, f'ALPHA158_MAX{d}', self._safe_divide(max_values, close))
                self._add_indicator(indicators, f'ALPHA158_MIN{d}', self._safe_divide(min_values, close))
            
            # QTLU/QTLD - Quantiles
            for d in windows:
                qtlu_values = pd.Series(close).rolling(window=d, min_periods=1).quantile(0.8).values
                qtld_values = pd.Series(close).rolling(window=d, min_periods=1).quantile(0.2).values
                self._add_indicator(indicators, f'ALPHA158_QTLU{d}', self._safe_divide(qtlu_values, close))
                self._add_indicator(indicators, f'ALPHA158_QTLD{d}', self._safe_divide(qtld_values, close))
            
            # RANK - Percentile rank
            for d in windows:
                rank_values = pd.Series(close).rolling(window=d, min_periods=1).rank(pct=True).values
                self._add_indicator(indicators, f'ALPHA158_RANK{d}', rank_values)
            
            # RSV - Relative Strength Value
            for d in windows:
                min_low = pd.Series(low).rolling(window=d, min_periods=1).min().values
                max_high = pd.Series(high).rolling(window=d, min_periods=1).max().values
                self._add_indicator(indicators, f'ALPHA158_RSV{d}', self._safe_divide(close - min_low, max_high - min_low + 1e-12))
            
            # RESI - Linear Regression Residual
            for d in windows:
                resi_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    x = np.arange(d)
                    y = close[i-d+1:i+1]
                    if len(y) > 1:
                        try:
                            slope, intercept = np.polyfit(x, y, 1)
                            predicted = slope * (d-1) + intercept
                            resi_values[i] = y[-1] - predicted
                        except:
                            resi_values[i] = 0
                self._add_indicator(indicators, f'ALPHA158_RESI{d}', self._safe_divide(resi_values, close))
            
            # IMAX - Index of Maximum
            for d in windows:
                imax_values = np.zeros_like(close)
                for i in range(d, len(high)):
                    window_high = high[i-d+1:i+1]
                    if len(window_high) > 0:
                        imax_values[i] = (len(window_high) - 1 - np.argmax(window_high)) / d
                self._add_indicator(indicators, f'ALPHA158_IMAX{d}', imax_values)
            
            # IMIN - Index of Minimum  
            for d in windows:
                imin_values = np.zeros_like(close)
                for i in range(d, len(low)):
                    window_low = low[i-d+1:i+1]
                    if len(window_low) > 0:
                        imin_values[i] = (len(window_low) - 1 - np.argmin(window_low)) / d
                self._add_indicator(indicators, f'ALPHA158_IMIN{d}', imin_values)
            
            # IMXD - Index Max - Index Min Difference
            for d in windows:
                imxd_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    window_high = high[i-d+1:i+1] 
                    window_low = low[i-d+1:i+1]
                    if len(window_high) > 0 and len(window_low) > 0:
                        idx_max = len(window_high) - 1 - np.argmax(window_high)
                        idx_min = len(window_low) - 1 - np.argmin(window_low)
                        imxd_values[i] = (idx_max - idx_min) / d
                self._add_indicator(indicators, f'ALPHA158_IMXD{d}', imxd_values)
            
            # CORR - Correlation between close and log(volume)
            for d in windows:
                corr_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    window_close = close[i-d+1:i+1]
                    window_volume = volume[i-d+1:i+1]
                    if len(window_close) > 1:
                        try:
                            log_volume = np.log(window_volume + 1)
                            if np.std(window_close) > 1e-8 and np.std(log_volume) > 1e-8:
                                corr_values[i] = np.corrcoef(window_close, log_volume)[0, 1]
                        except:
                            corr_values[i] = 0
                self._add_indicator(indicators, f'ALPHA158_CORR{d}', corr_values)
            
            # CORD - Correlation between price change and volume change
            for d in windows:
                cord_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    if i >= d:
                        close_change = close[i-d+2:i+1] / close[i-d+1:i]
                        volume_change = np.log((volume[i-d+2:i+1] / (volume[i-d+1:i] + 1e-12)) + 1)
                        if len(close_change) > 1:
                            try:
                                if np.std(close_change) > 1e-8 and np.std(volume_change) > 1e-8:
                                    cord_values[i] = np.corrcoef(close_change, volume_change)[0, 1]
                            except:
                                cord_values[i] = 0
                self._add_indicator(indicators, f'ALPHA158_CORD{d}', cord_values)
            
            # CNTP - Count of Positive returns
            for d in windows:
                cntp_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    window_returns = close[i-d+2:i+1] > close[i-d+1:i]
                    if len(window_returns) > 0:
                        cntp_values[i] = np.mean(window_returns)
                self._add_indicator(indicators, f'ALPHA158_CNTP{d}', cntp_values)
            
            # CNTN - Count of Negative returns
            for d in windows:
                cntn_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    window_returns = close[i-d+2:i+1] < close[i-d+1:i]
                    if len(window_returns) > 0:
                        cntn_values[i] = np.mean(window_returns)
                self._add_indicator(indicators, f'ALPHA158_CNTN{d}', cntn_values)
            
            # CNTD - Count Difference (CNTP - CNTN)
            for d in windows:
                cntd_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    window_pos = close[i-d+2:i+1] > close[i-d+1:i]
                    window_neg = close[i-d+2:i+1] < close[i-d+1:i]
                    if len(window_pos) > 0:
                        cntd_values[i] = np.mean(window_pos) - np.mean(window_neg)
                self._add_indicator(indicators, f'ALPHA158_CNTD{d}', cntd_values)
            
            # SUMP - Sum of Positive returns ratio
            for d in windows:
                sump_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    changes = close[i-d+2:i+1] - close[i-d+1:i]
                    if len(changes) > 0:
                        positive_sum = np.sum(np.maximum(changes, 0))
                        total_abs_sum = np.sum(np.abs(changes))
                        sump_values[i] = self._safe_divide(positive_sum, total_abs_sum + 1e-12)
                self._add_indicator(indicators, f'ALPHA158_SUMP{d}', sump_values)
            
            # SUMN - Sum of Negative returns ratio  
            for d in windows:
                sumn_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    changes = close[i-d+2:i+1] - close[i-d+1:i]
                    if len(changes) > 0:
                        negative_sum = np.sum(np.maximum(-changes, 0))
                        total_abs_sum = np.sum(np.abs(changes))
                        sumn_values[i] = self._safe_divide(negative_sum, total_abs_sum + 1e-12)
                self._add_indicator(indicators, f'ALPHA158_SUMN{d}', sumn_values)
            
            # SUMD - Sum Difference (SUMP - SUMN)
            for d in windows:
                sumd_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    changes = close[i-d+2:i+1] - close[i-d+1:i]
                    if len(changes) > 0:
                        positive_sum = np.sum(np.maximum(changes, 0))
                        negative_sum = np.sum(np.maximum(-changes, 0))
                        total_abs_sum = np.sum(np.abs(changes))
                        sumd_values[i] = self._safe_divide(positive_sum - negative_sum, total_abs_sum + 1e-12)
                self._add_indicator(indicators, f'ALPHA158_SUMD{d}', sumd_values)
            
            # VMA - Volume Moving Average
            for d in windows:
                vma_values = pd.Series(volume).rolling(window=d, min_periods=1).mean().values
                self._add_indicator(indicators, f'ALPHA158_VMA{d}', self._safe_divide(vma_values, volume + 1e-12))
            
            # VSTD - Volume Standard Deviation
            for d in windows:
                vstd_values = pd.Series(volume).rolling(window=d, min_periods=1).std().fillna(0).values
                self._add_indicator(indicators, f'ALPHA158_VSTD{d}', self._safe_divide(vstd_values, volume + 1e-12))
            
            # WVMA - Weighted Volume Moving Average (price change volatility weighted by volume)
            for d in windows:
                wvma_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    price_changes = np.abs(close[i-d+2:i+1] / close[i-d+1:i] - 1)
                    weights = volume[i-d+2:i+1]
                    if len(price_changes) > 0:
                        weighted_changes = price_changes * weights
                        mean_weighted = np.mean(weighted_changes)
                        std_weighted = np.std(weighted_changes)
                        wvma_values[i] = self._safe_divide(std_weighted, mean_weighted + 1e-12)
                self._add_indicator(indicators, f'ALPHA158_WVMA{d}', wvma_values)
            
            # VSUMP - Volume Sum Positive ratio
            for d in windows:
                vsump_values = np.zeros_like(close)
                for i in range(d, len(volume)):
                    vol_changes = volume[i-d+2:i+1] - volume[i-d+1:i] 
                    if len(vol_changes) > 0:
                        positive_sum = np.sum(np.maximum(vol_changes, 0))
                        total_abs_sum = np.sum(np.abs(vol_changes))
                        vsump_values[i] = self._safe_divide(positive_sum, total_abs_sum + 1e-12)
                self._add_indicator(indicators, f'ALPHA158_VSUMP{d}', vsump_values)
            
            # VSUMN - Volume Sum Negative ratio
            for d in windows:
                vsumn_values = np.zeros_like(close)
                for i in range(d, len(volume)):
                    vol_changes = volume[i-d+2:i+1] - volume[i-d+1:i]
                    if len(vol_changes) > 0:
                        negative_sum = np.sum(np.maximum(-vol_changes, 0))
                        total_abs_sum = np.sum(np.abs(vol_changes))
                        vsumn_values[i] = self._safe_divide(negative_sum, total_abs_sum + 1e-12)
                self._add_indicator(indicators, f'ALPHA158_VSUMN{d}', vsumn_values)
            
            # VSUMD - Volume Sum Difference (VSUMP - VSUMN)
            for d in windows:
                vsumd_values = np.zeros_like(close)
                for i in range(d, len(volume)):
                    vol_changes = volume[i-d+2:i+1] - volume[i-d+1:i]
                    if len(vol_changes) > 0:
                        positive_sum = np.sum(np.maximum(vol_changes, 0))
                        negative_sum = np.sum(np.maximum(-vol_changes, 0))
                        total_abs_sum = np.sum(np.abs(vol_changes))
                        vsumd_values[i] = self._safe_divide(positive_sum - negative_sum, total_abs_sum + 1e-12)
                self._add_indicator(indicators, f'ALPHA158_VSUMD{d}', vsumd_values)
            
            # 转换为DataFrame
            indicators_df = pd.DataFrame(indicators, index=data.index)
            
            logger.info(f"计算了Alpha158指标体系: {len(indicators)} 个指标")
            return indicators_df
            
        except Exception as e:
            logger.error(f"计算Alpha158指标失败: {e}")
            return pd.DataFrame()
    
    def calculate_alpha360_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        计算Alpha360指标体系 (360个指标)
        包括过去60天的标准化价格和成交量数据
        """
        if data.empty or len(data) < 60:
            logger.warning("数据不足以计算Alpha360指标")
            return pd.DataFrame()
        
        try:
            indicators = {}
            
            # 清理数据
            open_price = data['Open'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            high = data['High'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            low = data['Low'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            close = data['Close'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            volume = data['Volume'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(0).values
            
            # 计算VWAP
            vwap = self._safe_divide(
                np.convolve(close * volume, np.ones(1), 'same'),
                np.convolve(volume, np.ones(1), 'same')
            )
            
            # Alpha360: 过去60天的价格和成交量数据，除以当前收盘价标准化
            # 1. CLOSE 指标 (60个)
            for i in range(59, -1, -1):
                if i == 0:
                    self._add_indicator(indicators, f'ALPHA360_CLOSE{i}', self._safe_divide(close, close))
                else:
                    ref_close = np.roll(close, i)
                    self._add_indicator(indicators, f'ALPHA360_CLOSE{i}', self._safe_divide(ref_close, close))
            
            # 2. OPEN 指标 (60个)
            for i in range(59, -1, -1):
                if i == 0:
                    self._add_indicator(indicators, f'ALPHA360_OPEN{i}', self._safe_divide(open_price, close))
                else:
                    ref_open = np.roll(open_price, i)
                    self._add_indicator(indicators, f'ALPHA360_OPEN{i}', self._safe_divide(ref_open, close))
            
            # 3. HIGH 指标 (60个)
            for i in range(59, -1, -1):
                if i == 0:
                    self._add_indicator(indicators, f'ALPHA360_HIGH{i}', self._safe_divide(high, close))
                else:
                    ref_high = np.roll(high, i)
                    self._add_indicator(indicators, f'ALPHA360_HIGH{i}', self._safe_divide(ref_high, close))
            
            # 4. LOW 指标 (60个)
            for i in range(59, -1, -1):
                if i == 0:
                    self._add_indicator(indicators, f'ALPHA360_LOW{i}', self._safe_divide(low, close))
                else:
                    ref_low = np.roll(low, i)
                    self._add_indicator(indicators, f'ALPHA360_LOW{i}', self._safe_divide(ref_low, close))
            
            # 5. VWAP 指标 (60个)
            for i in range(59, -1, -1):
                if i == 0:
                    self._add_indicator(indicators, f'ALPHA360_VWAP{i}', self._safe_divide(vwap, close))
                else:
                    ref_vwap = np.roll(vwap, i)
                    self._add_indicator(indicators, f'ALPHA360_VWAP{i}', self._safe_divide(ref_vwap, close))
            
            # 6. VOLUME 指标 (60个)
            for i in range(59, -1, -1):
                if i == 0:
                    self._add_indicator(indicators, f'ALPHA360_VOLUME{i}', self._safe_divide(volume, volume + 1e-12))
                else:
                    ref_volume = np.roll(volume, i)
                    self._add_indicator(indicators, f'ALPHA360_VOLUME{i}', self._safe_divide(ref_volume, volume + 1e-12))
            
            # 转换为DataFrame
            indicators_df = pd.DataFrame(indicators, index=data.index)
            
            logger.info(f"计算了Alpha360指标体系: {len(indicators)} 个指标")
            return indicators_df
            
        except Exception as e:
            logger.error(f"计算Alpha360指标失败: {e}")
            return pd.DataFrame()
    
    def calculate_candlestick_patterns(self, data: pd.DataFrame) -> pd.DataFrame:
        """计算蜡烛图形态指标（共61个）"""
        if data.empty or len(data) < 10:
            logger.warning("Insufficient data for calculating candlestick patterns")
            return pd.DataFrame()
        
        try:
            patterns = {}
            
            # Clean data
            open_price = data['Open'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            high = data['High'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            low = data['Low'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            close = data['Close'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            
            # 所有61个蜡烛图形态
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
                try:
                    patterns[pattern] = getattr(talib, pattern)(open_price, high, low, close)
                except Exception as e:
                    logger.warning(f"Failed to calculate {pattern}: {e}")
                    patterns[pattern] = np.zeros(len(data))
            
            patterns_df = pd.DataFrame(patterns, index=data.index)
            
            logger.info(f"计算了 {len(patterns)} 个蜡烛图形态指标")
            return patterns_df
            
        except Exception as e:
            logger.error(f"计算蜡烛图形态失败: {e}")
            return pd.DataFrame()
    
    def calculate_financial_indicators(self, data: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """计算财务指标和换手率（约15个）- 使用估算值替代缺失数据"""
        try:
            result_data = data.copy()
            
            # 预先初始化所有财务指标列
            financial_columns = [
                'PriceToBookRatio', 'MarketCap', 'PERatio', 'PriceToSalesRatio',
                'ROE', 'ROA', 'ProfitMargins', 'CurrentRatio', 'QuickRatio', 
                'DebtToEquity', 'TobinsQ', 'DailyTurnover', 
                'turnover_c1d', 'turnover_c5d', 'turnover_c10d', 'turnover_c20d', 'turnover_c30d',
                'turnover_m5d', 'turnover_m10d', 'turnover_m20d', 'turnover_m30d'
            ]
            
            # 获取基本信息数据
            info_data = self.get_financial_data(symbol, 'info')
            balance_sheet_data = self.get_financial_data(symbol, 'balance_sheet')
            
            # 如果有真实财务数据，使用真实数据
            if info_data is not None and not info_data.empty:
                result_data = self._calculate_real_financial_indicators(result_data, info_data, balance_sheet_data)
            else:
                # 否则使用基于价格和成交量的估算指标
                result_data = self._calculate_estimated_financial_indicators(result_data, symbol)
            
            # 确保所有财务指标列都存在且有默认值
            result_data = self._ensure_financial_columns_exist(result_data, symbol)
            
            logger.info(f"✅ 完成财务指标计算 (包含估算值)")
            return result_data
            
        except Exception as e:
            logger.error(f"计算财务指标失败: {e}")
            # 即使失败也要确保列存在
            for col in financial_columns:
                if col not in data.columns:
                    data[col] = np.nan
            return data
    
    def _calculate_real_financial_indicators(self, data: pd.DataFrame, info_data: pd.DataFrame, balance_sheet_data: pd.DataFrame) -> pd.DataFrame:
        """使用真实财务数据计算指标"""
        result_data = data.copy()
        
        try:
            # 获取财务数据的第一行（通常包含最新数据）
            info_row = info_data.iloc[0] if not info_data.empty else pd.Series()
            
            # 1. 市净率 (Price to Book Ratio)
            if 'priceToBook' in info_row.index and not pd.isna(info_row['priceToBook']):
                # 直接使用已计算的市净率
                result_data['PriceToBookRatio'] = float(info_row['priceToBook'])
            elif 'bookValue' in info_row.index and not pd.isna(info_row['bookValue']):
                book_value = float(info_row['bookValue'])
                if book_value > 0:
                    result_data['PriceToBookRatio'] = result_data['Close'] / book_value
            
            # 2. 市值 (Market Cap)
            if 'marketCap' in info_row.index and not pd.isna(info_row['marketCap']):
                result_data['MarketCap'] = float(info_row['marketCap'])
            elif 'sharesOutstanding' in info_row.index and not pd.isna(info_row['sharesOutstanding']):
                shares_outstanding = float(info_row['sharesOutstanding'])
                if shares_outstanding > 0:
                    result_data['MarketCap'] = result_data['Close'] * shares_outstanding
            
            # 3. 市盈率 (PE Ratio)
            if 'trailingPE' in info_row.index and not pd.isna(info_row['trailingPE']):
                result_data['PERatio'] = float(info_row['trailingPE'])
            elif 'forwardPE' in info_row.index and not pd.isna(info_row['forwardPE']):
                result_data['PERatio'] = float(info_row['forwardPE'])
            
            # 4. 市销率 (Price to Sales Ratio)
            if 'priceToSalesTrailing12Months' in info_row.index and not pd.isna(info_row['priceToSalesTrailing12Months']):
                result_data['PriceToSalesRatio'] = float(info_row['priceToSalesTrailing12Months'])
            
            # 5. 净资产收益率 (ROE)
            if 'returnOnEquity' in info_row.index and not pd.isna(info_row['returnOnEquity']):
                result_data['ROE'] = float(info_row['returnOnEquity'])
            
            # 6. 资产收益率 (ROA)
            if 'returnOnAssets' in info_row.index and not pd.isna(info_row['returnOnAssets']):
                result_data['ROA'] = float(info_row['returnOnAssets'])
            
            # 7. 利润率
            if 'profitMargins' in info_row.index and not pd.isna(info_row['profitMargins']):
                result_data['ProfitMargins'] = float(info_row['profitMargins'])
            
            # 8. 流动比率 (如果有资产负债表数据)
            if balance_sheet_data is not None and not balance_sheet_data.empty:
                balance_row = balance_sheet_data.iloc[0]
                if 'currentRatio' in balance_row.index and not pd.isna(balance_row['currentRatio']):
                    result_data['CurrentRatio'] = float(balance_row['currentRatio'])
                elif 'Total Current Assets' in balance_row.index and 'Total Current Liabilities' in balance_row.index:
                    current_assets = balance_row.get('Total Current Assets', 0)
                    current_liabilities = balance_row.get('Total Current Liabilities', 1)
                    if current_liabilities > 0:
                        result_data['CurrentRatio'] = current_assets / current_liabilities
            
            # 9. 速动比率
            if 'quickRatio' in info_row.index and not pd.isna(info_row['quickRatio']):
                result_data['QuickRatio'] = float(info_row['quickRatio'])
            else:
                # 估算为流动比率的80%
                if 'CurrentRatio' in result_data.columns:
                    result_data['QuickRatio'] = result_data['CurrentRatio'] * 0.8
            
            # 10. 资产负债率
            if 'debtToEquity' in info_row.index and not pd.isna(info_row['debtToEquity']):
                result_data['DebtToEquity'] = float(info_row['debtToEquity'])
            elif 'totalDebt' in info_row.index and 'marketCap' in info_row.index:
                total_debt = info_row.get('totalDebt', 0)
                market_cap = info_row.get('marketCap', 1)
                if market_cap > 0:
                    result_data['DebtToEquity'] = total_debt / market_cap
            
            # 11. 托宾Q值
            if 'enterpriseValue' in info_row.index and balance_sheet_data is not None:
                enterprise_value = info_row.get('enterpriseValue', None)
                if enterprise_value and not balance_sheet_data.empty:
                    balance_row = balance_sheet_data.iloc[0]
                    total_assets = balance_row.get('Total Assets', balance_row.get('totalAssets', None))
                    if total_assets and total_assets > 0:
                        result_data['TobinsQ'] = enterprise_value / total_assets
            
            # 12. 换手率计算
            if 'floatShares' in info_row.index and not pd.isna(info_row['floatShares']):
                float_shares = float(info_row['floatShares'])
                if float_shares > 0:
                    result_data = self._calculate_real_turnover_indicators(result_data, float_shares)
            elif 'sharesOutstanding' in info_row.index and not pd.isna(info_row['sharesOutstanding']):
                # 使用总股本作为流通股的替代
                shares_outstanding = float(info_row['sharesOutstanding'])
                if shares_outstanding > 0:
                    result_data = self._calculate_real_turnover_indicators(result_data, shares_outstanding)
            
            logger.info("✅ 使用真实财务数据计算完成")
            
        except Exception as e:
            logger.error(f"使用真实财务数据计算失败: {e}")
            # 如果真实数据计算失败，回退到估算方法
            result_data = self._calculate_estimated_financial_indicators(result_data, "UNKNOWN")
        
        return result_data
    
    def _calculate_real_turnover_indicators(self, data: pd.DataFrame, shares_count: float) -> pd.DataFrame:
        """基于真实流通股数计算换手率指标"""
        try:
            result_data = data.copy()
            
            # 计算日换手率
            result_data['DailyTurnover'] = result_data['Volume'] / shares_count
            
            # 计算不同窗口的累计和平均换手率
            windows = [1, 5, 10, 20, 30]
            for window in windows:
                result_data[f'turnover_c{window}d'] = result_data['DailyTurnover'].rolling(window=window, min_periods=1).sum()
                if window > 1:
                    result_data[f'turnover_m{window}d'] = result_data['DailyTurnover'].rolling(window=window, min_periods=1).mean()
            
            return result_data
            
        except Exception as e:
            logger.error(f"计算真实换手率指标失败: {e}")
            return data
    
    def _calculate_estimated_financial_indicators(self, data: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """基于价格和成交量数据估算财务指标"""
        result_data = data.copy()
        
        # 获取基础数据
        close = result_data['Close'].values
        volume = result_data['Volume'].values
        high = result_data['High'].values
        low = result_data['Low'].values
        
        # 1. 估算市值 (假设流通股为平均成交量的某个倍数)
        avg_volume = np.mean(volume[volume > 0]) if len(volume[volume > 0]) > 0 else 1000000
        estimated_shares = avg_volume * 50  # 假设平均成交量是流通股的1/50
        result_data['MarketCap'] = close * estimated_shares
        
        # 2. 估算市净率 (基于价格波动性，高波动性通常对应高PB)
        price_volatility = pd.Series(close).rolling(20).std().fillna(0) / pd.Series(close).rolling(20).mean().fillna(1)
        result_data['PriceToBookRatio'] = 1.0 + price_volatility * 3  # 基准1倍，波动性每增加1，PB增加3
        
        # 3. 估算市盈率 (基于价格趋势，上涨趋势对应高PE)
        price_trend = pd.Series(close).rolling(20).apply(lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) > 1 else 0).fillna(0)
        base_pe = 15  # 基准PE
        result_data['PERatio'] = base_pe + (price_trend / np.mean(close) * 1000)
        result_data['PERatio'] = np.clip(result_data['PERatio'], 5, 50)  # 限制在合理范围
        
        # 4. 估算市销率 (基于成交量活跃度)
        volume_series = pd.Series(volume, index=result_data.index)
        volume_activity = volume_series.rolling(20).mean().fillna(avg_volume) / avg_volume
        result_data['PriceToSalesRatio'] = 1.0 + volume_activity * 2  # 成交活跃对应高PS
        
        # 5. 估算ROE (基于收益率)
        returns = pd.Series(close).pct_change(20).fillna(0)
        result_data['ROE'] = np.clip(returns * 4, -0.3, 0.5)  # 年化收益率作为ROE的代理
        
        # 6. 估算ROA (通常比ROE低)
        result_data['ROA'] = result_data['ROE'] * 0.6
        
        # 7. 估算利润率 (基于价格稳定性)
        price_stability = 1 / (1 + price_volatility)
        result_data['ProfitMargins'] = price_stability * 0.1  # 稳定的股票假设有更好的利润率
        
        # 8. 估算流动比率 (基于成交量流动性)
        volume_series = pd.Series(volume, index=result_data.index)
        liquidity = volume_series.rolling(5).mean() / volume_series.rolling(20).mean()
        result_data['CurrentRatio'] = 1.0 + liquidity.fillna(1) * 0.5
        
        # 9. 估算速动比率 (通常比流动比率低)
        result_data['QuickRatio'] = result_data['CurrentRatio'] * 0.8
        
        # 10. 估算资产负债率 (基于波动性，高波动可能意味着高杠杆)
        result_data['DebtToEquity'] = price_volatility * 2
        
        # 11. 估算托宾Q值
        market_value_ratio = (high + low) / (2 * close)  # 相对估值
        market_value_ratio = pd.Series(market_value_ratio, index=result_data.index).fillna(1)
        result_data['TobinsQ'] = market_value_ratio
        
        # 12. 估算换手率指标
        result_data = self._calculate_estimated_turnover_indicators(result_data, estimated_shares)
        
        logger.info(f"🔮 使用估算方法计算财务指标 (基于价格和成交量)")
        return result_data
    
    def _calculate_estimated_turnover_indicators(self, data: pd.DataFrame, estimated_shares: float) -> pd.DataFrame:
        """基于估算流通股计算换手率指标"""
        try:
            result_data = data.copy()
            
            # 计算日换手率
            result_data['DailyTurnover'] = result_data['Volume'] / estimated_shares
            
            # 计算不同窗口的累计和平均换手率
            windows = [1, 5, 10, 20, 30]
            for window in windows:
                result_data[f'turnover_c{window}d'] = result_data['DailyTurnover'].rolling(window=window, min_periods=1).sum()
                if window > 1:
                    result_data[f'turnover_m{window}d'] = result_data['DailyTurnover'].rolling(window=window, min_periods=1).mean()
            
            logger.info("✅ 完成换手率指标估算")
            return result_data
            
        except Exception as e:
            logger.error(f"估算换手率指标失败: {e}")
            return data
    
    def _ensure_financial_columns_exist(self, data: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """确保所有财务指标列都存在且有合理的默认值"""
        result_data = data.copy()
        
        # 定义所有必需的财务指标列和其默认值
        required_columns = {
            'PriceToBookRatio': 1.5,    # 默认市净率
            'MarketCap': None,          # 市值需要计算
            'PERatio': 15.0,            # 默认市盈率
            'PriceToSalesRatio': 2.0,   # 默认市销率
            'ROE': 0.1,                 # 默认10%的ROE
            'ROA': 0.05,                # 默认5%的ROA
            'ProfitMargins': 0.08,      # 默认8%的利润率
            'CurrentRatio': 1.2,        # 默认流动比率
            'QuickRatio': 1.0,          # 默认速动比率
            'DebtToEquity': 0.5,        # 默认资产负债率
            'TobinsQ': 1.0,             # 默认托宾Q值
            'DailyTurnover': None,      # 需要计算
            'turnover_c1d': None,       # 需要计算
            'turnover_c5d': None,       # 需要计算
            'turnover_c10d': None,      # 需要计算
            'turnover_c20d': None,      # 需要计算
            'turnover_c30d': None,      # 需要计算
            'turnover_m5d': None,       # 需要计算
            'turnover_m10d': None,      # 需要计算
            'turnover_m20d': None,      # 需要计算
            'turnover_m30d': None       # 需要计算
        }
        
        # 为缺失的列添加默认值
        for col_name, default_value in required_columns.items():
            # 检查列是否不存在或全部为NaN
            col_missing = col_name not in result_data.columns 
            col_all_nan = not col_missing and result_data[col_name].isna().all()
            col_all_zero = not col_missing and (result_data[col_name] == 0).all()
            
            # 对于换手率指标，额外检查是否全部为0（这通常表示计算有问题）
            needs_calculation = col_missing or col_all_nan or (col_name.startswith('turnover') and col_all_zero)
            
            if needs_calculation:
                if default_value is not None:
                    result_data[col_name] = default_value
                elif col_name == 'MarketCap':
                    # 估算市值：假设平均股价为当前股价，流通股为成交量的50倍
                    avg_volume = result_data['Volume'].mean() if 'Volume' in result_data.columns else 1000000
                    estimated_shares = avg_volume * 50
                    result_data['MarketCap'] = result_data['Close'] * estimated_shares
                elif col_name.startswith('turnover'):
                    # 只有在换手率指标确实缺失或有问题时才重新计算
                    logger.warning(f"换手率指标 {col_name} 缺失或异常，使用估算方法")
                    
                    # 估算换手率相关指标
                    if 'DailyTurnover' not in result_data.columns or result_data['DailyTurnover'].isna().all() or (result_data['DailyTurnover'] == 0).all():
                        avg_volume = result_data['Volume'].mean() if 'Volume' in result_data.columns else 1000000
                        estimated_shares = avg_volume * 50
                        result_data['DailyTurnover'] = result_data['Volume'] / estimated_shares
                    
                    # 计算累计和平均换手率
                    if col_name.startswith('turnover_c'):
                        window = int(col_name.split('d')[0].split('_c')[1])
                        result_data[col_name] = result_data['DailyTurnover'].rolling(window=window, min_periods=1).sum()
                    elif col_name.startswith('turnover_m'):
                        window = int(col_name.split('d')[0].split('_m')[1])
                        result_data[col_name] = result_data['DailyTurnover'].rolling(window=window, min_periods=1).mean()
        
        return result_data
    

    
    def calculate_volatility_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """计算波动率指标（约8个）"""
        try:
            volatility_data = {}
            
            # 计算价格变化
            price_diff = data['Close'].diff()
            log_returns = np.log(data['Close'] / data['Close'].shift(1))
            
            # 1. 已实现波动率 (20天窗口)
            volatility_data['RealizedVolatility_20'] = price_diff.rolling(window=20).std() * np.sqrt(252)
            
            # 2. 已实现负半变差
            negative_returns = price_diff[price_diff < 0]
            volatility_data['NegativeSemiDeviation_20'] = negative_returns.rolling(window=20).std() * np.sqrt(252)
            
            # 3. 已实现连续波动率
            volatility_data['ContinuousVolatility_20'] = log_returns.rolling(window=20).std() * np.sqrt(252)
            
            # 4. 已实现正半变差
            positive_returns = price_diff[price_diff > 0]
            volatility_data['PositiveSemiDeviation_20'] = positive_returns.rolling(window=20).std() * np.sqrt(252)
            
            # 5. 不同窗口的波动率
            for window in [10, 30, 60]:
                volatility_data[f'Volatility_{window}'] = price_diff.rolling(window=window).std() * np.sqrt(252)
            
            volatility_df = pd.DataFrame(volatility_data, index=data.index)
            
            logger.info("计算了波动率指标")
            return volatility_df
            
        except Exception as e:
            logger.error(f"计算波动率指标失败: {e}")
            return pd.DataFrame()
    
    def calculate_all_indicators_for_stock(self, symbol: str) -> Optional[pd.DataFrame]:
        """为单只股票计算所有指标（支持并行计算）"""
        try:
            # 读取历史价格数据
            price_data = self.read_qlib_binary_data(symbol)
            if price_data is None or price_data.empty:
                logger.warning(f"No price data found for {symbol}")
                return None
            
            # 使用并行计算或顺序计算
            if self.enable_parallel:
                return self._calculate_indicators_parallel(symbol, price_data)
            else:
                return self._calculate_indicators_sequential(symbol, price_data)
                
        except Exception as e:
            logger.error(f"❌ {symbol}: 计算指标失败 - {e}")
            return None
    
    def _calculate_indicators_parallel(self, symbol: str, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        并行计算单只股票的所有指标类型
        """
        if not self.enable_parallel:
            return self._calculate_indicators_sequential(symbol, price_data)
        
        try:
            logger.info(f"开始并行计算 {symbol} 的所有指标...")
            start_time = time.time()
            
            # 重置指标缓存
            self._reset_indicators_cache()
            
            # 保存原始日期信息
            original_dates = price_data.index
            
            # 定义各类指标计算任务
            indicator_tasks = [
                ('Alpha158', partial(self.calculate_alpha158_indicators, price_data)),
                ('Alpha360', partial(self.calculate_alpha360_indicators, price_data)),
                ('Technical', partial(self.calculate_all_technical_indicators, price_data)),
                ('Candlestick', partial(self.calculate_candlestick_patterns, price_data)),
                ('Financial', partial(self.calculate_financial_indicators, price_data, symbol)),
                ('Volatility', partial(self.calculate_volatility_indicators, price_data))
            ]
            
            # 使用线程池并行计算
            results = {}
            failed_tasks = []
            
            with ThreadPoolExecutor(max_workers=min(6, self.max_workers)) as executor:
                # 提交所有任务
                future_to_task = {
                    executor.submit(task_func): task_name 
                    for task_name, task_func in indicator_tasks
                }
                
                # 收集结果
                for future in as_completed(future_to_task):
                    task_name = future_to_task[future]
                    try:
                        result = future.result(timeout=300)  # 5分钟超时
                        if result is not None and not result.empty:
                            results[task_name] = result
                            logger.debug(f"✅ {symbol} - {task_name}: {result.shape[1]} 个指标")
                        else:
                            failed_tasks.append(task_name)
                            logger.warning(f"⚠️ {symbol} - {task_name}: 计算结果为空")
                    except Exception as e:
                        failed_tasks.append(task_name)
                        logger.error(f"❌ {symbol} - {task_name}: 计算失败 - {e}")
            
            if failed_tasks:
                logger.warning(f"{symbol}: 以下指标类型计算失败: {failed_tasks}")
            
            # 合并所有指标
            try:
                all_indicators = [price_data]
                all_indicators.extend(results.values())
                
                if all_indicators:
                    # 确保所有DataFrame都有一致的索引，但保留日期信息
                    aligned_indicators = []
                    base_length = len(price_data)
                    
                    for df in all_indicators:
                        if df is not None and not df.empty:
                            # 重置索引但保留原始索引作为Date列（如果还没有Date列的话）
                            df_reset = df.reset_index()
                            if 'index' in df_reset.columns and 'Date' not in df_reset.columns:
                                df_reset = df_reset.rename(columns={'index': 'Date'})
                            elif 'Date' not in df_reset.columns:
                                # 如果没有日期信息，使用原始日期
                                df_reset['Date'] = original_dates[:len(df_reset)]
                            
                            # 确保长度一致
                            if len(df_reset) != base_length:
                                # 重新采样或截断以匹配基准长度
                                if len(df_reset) > base_length:
                                    df_reset = df_reset.iloc[:base_length]
                                else:
                                    # 用NaN填充不足的行
                                    missing_rows = base_length - len(df_reset)
                                    padding_data = {}
                                    for col in df_reset.columns:
                                        if col == 'Date':
                                            # 为日期列生成合适的日期
                                            last_date = df_reset['Date'].iloc[-1] if len(df_reset) > 0 else original_dates[-1]
                                            if isinstance(last_date, str):
                                                last_date = pd.to_datetime(last_date)
                                            additional_dates = pd.bdate_range(start=last_date + pd.Timedelta(days=1), periods=missing_rows)
                                            padding_data[col] = additional_dates
                                        else:
                                            padding_data[col] = [np.nan] * missing_rows
                                    
                                    padding_df = pd.DataFrame(padding_data)
                                    df_reset = pd.concat([df_reset, padding_df], ignore_index=True)
                            aligned_indicators.append(df_reset)
                    
                    # 安全合并 - 保留Date列
                    combined_df = pd.concat(aligned_indicators, axis=1)
                    
                    # 处理重复的Date列
                    if 'Date' in combined_df.columns:
                        date_cols = [col for col in combined_df.columns if col == 'Date']
                        if len(date_cols) > 1:
                            # 保留第一个Date列，删除其余的
                            combined_df = combined_df.loc[:, ~combined_df.columns.duplicated(keep='first')]
                    
                    # 从财务数据中提取新增的列
                    if 'Financial' in results:
                        financial_data = results['Financial']
                        financial_cols = [col for col in financial_data.columns if col not in price_data.columns and col != 'Date']
                        if financial_cols:
                            for col in financial_cols:
                                if col not in combined_df.columns:
                                    # 重新索引财务数据以匹配基准索引
                                    financial_col_data = financial_data[col].reindex(range(base_length), method='ffill')
                                    combined_df[col] = financial_col_data
                    
                    # 添加股票代码
                    combined_df['Symbol'] = symbol
                    
                    # 重新排列列顺序：Date, Symbol, 然后是其他列
                    cols = []
                    if 'Date' in combined_df.columns:
                        cols.append('Date')
                    cols.append('Symbol')
                    cols.extend([col for col in combined_df.columns if col not in ['Date', 'Symbol']])
                    combined_df = combined_df[cols]
                    
                    # 重置数字索引，但保留Date列
                    combined_df = combined_df.reset_index(drop=True)
                    
                    elapsed_time = time.time() - start_time
                    logger.info(f"✅ {symbol}: 并行计算完成 {len(combined_df.columns)-2} 个指标 (耗时: {elapsed_time:.2f}s)")
                    return combined_df
                    
            except Exception as e:
                logger.error(f"❌ {symbol}: 合并指标时发生错误 - {e}")
                # 降级到顺序计算方法
                return self._calculate_indicators_sequential(symbol, price_data)
            else:
                logger.error(f"❌ {symbol}: 所有指标计算都失败了")
                return None
                
        except Exception as e:
            logger.error(f"❌ {symbol}: 并行计算失败 - {e}")
            return self._calculate_indicators_sequential(symbol, price_data)
    
    def _calculate_indicators_sequential(self, symbol: str, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        顺序计算单只股票的所有指标（备用方法）
        """
        try:
            logger.info(f"开始顺序计算 {symbol} 的所有指标...")
            
            # 重置指标跟踪器
            self._reset_indicators_cache()
            
            # 保存原始日期信息
            original_dates = price_data.index
            
            # 1. 计算Alpha158指标体系 (~158个)
            alpha158_indicators = self.calculate_alpha158_indicators(price_data)
            
            # 2. 计算Alpha360指标体系 (~360个)
            alpha360_indicators = self.calculate_alpha360_indicators(price_data)
            
            # 3. 计算技术指标 (~60个)
            technical_indicators = self.calculate_all_technical_indicators(price_data)
            
            # 4. 计算蜡烛图形态 (61个)
            candlestick_patterns = self.calculate_candlestick_patterns(price_data)
            
            # 5. 计算财务指标 (~15个)
            financial_data = self.calculate_financial_indicators(price_data, symbol)
            
            # 6. 计算波动率指标 (~8个)
            volatility_indicators = self.calculate_volatility_indicators(price_data)
            
            # 合并所有指标（确保索引一致性并保留日期信息）
            base_index = price_data.index
            indicator_dfs = [
                price_data,
                alpha158_indicators,
                alpha360_indicators,
                technical_indicators,
                candlestick_patterns,
                financial_data,  # 添加财务数据
                volatility_indicators
            ]
            
            # 重新索引所有DataFrame以确保一致性，并保留日期信息
            aligned_dfs = []
            for df in indicator_dfs:
                if df is not None and not df.empty:
                    if not df.index.equals(base_index):
                        df = df.reindex(base_index, method='ffill')
                    
                    # 将索引转换为Date列（如果还不是列的话）
                    df_with_date = df.reset_index()
                    if 'index' in df_with_date.columns and 'Date' not in df_with_date.columns:
                        df_with_date = df_with_date.rename(columns={'index': 'Date'})
                    elif 'Date' not in df_with_date.columns:
                        df_with_date['Date'] = original_dates
                    
                    aligned_dfs.append(df_with_date)
            
            all_indicators = pd.concat(aligned_dfs, axis=1)
            
            # 处理重复的Date列
            if 'Date' in all_indicators.columns:
                date_cols = [col for col in all_indicators.columns if col == 'Date']
                if len(date_cols) > 1:
                    # 保留第一个Date列，删除其余的
                    all_indicators = all_indicators.loc[:, ~all_indicators.columns.duplicated(keep='first')]
            
            # 财务数据已经在主合并中处理，无需额外操作
            
            # 添加股票代码
            all_indicators['Symbol'] = symbol
            
            # 重新排列列顺序：Date, Symbol, 然后是其他列
            cols = []
            if 'Date' in all_indicators.columns:
                cols.append('Date')
            cols.append('Symbol')
            cols.extend([col for col in all_indicators.columns if col not in ['Date', 'Symbol']])
            all_indicators = all_indicators[cols]
            
            # 重置数字索引，但保留Date列
            all_indicators = all_indicators.reset_index(drop=True)
            
            logger.info(f"✅ {symbol}: 顺序计算完成 {len(all_indicators.columns)-2} 个指标")
            return all_indicators
            
        except Exception as e:
            logger.error(f"❌ {symbol}: 顺序计算失败 - {e}")
            return None
    
    def calculate_all_indicators(self, max_stocks: Optional[int] = None) -> pd.DataFrame:
        """计算所有股票的所有指标（支持并行处理）"""
        stocks = self.get_available_stocks()
        
        if max_stocks:
            stocks = stocks[:max_stocks]
        
        if not stocks:
            logger.error("没有找到可用的股票数据")
            return pd.DataFrame()
        
        logger.info(f"开始计算 {len(stocks)} 只股票的指标...")
        start_time = time.time()
        
        if self.enable_parallel and len(stocks) > 1:
            return self._calculate_all_stocks_parallel(stocks)
        else:
            return self._calculate_all_stocks_sequential(stocks)
    
    def _calculate_all_stocks_parallel(self, stocks: List[str]) -> pd.DataFrame:
        """并行计算多只股票的指标"""
        logger.info(f"使用并行模式计算 {len(stocks)} 只股票 (最大线程数: {self.max_workers})")
        
        all_results = []
        success_count = 0
        failed_stocks = []
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有股票的计算任务
            future_to_symbol = {
                executor.submit(self.calculate_all_indicators_for_stock, symbol): symbol 
                for symbol in stocks
            }
            
            # 收集结果（带进度显示）
            completed = 0
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                completed += 1
                
                try:
                    result = future.result(timeout=600)  # 10分钟超时
                    if result is not None:
                        all_results.append(result)
                        success_count += 1
                        logger.info(f"✅ 进度 {completed}/{len(stocks)}: {symbol} 计算完成 ({len(result.columns)-1} 个指标)")
                    else:
                        failed_stocks.append(symbol)
                        logger.warning(f"⚠️ 进度 {completed}/{len(stocks)}: {symbol} 计算结果为空")
                        
                except Exception as e:
                    failed_stocks.append(symbol)
                    logger.error(f"❌ 进度 {completed}/{len(stocks)}: {symbol} 计算失败 - {e}")
        
        elapsed_time = time.time() - start_time
        
        if failed_stocks:
            logger.warning(f"计算失败的股票 ({len(failed_stocks)}): {failed_stocks[:5]}{'...' if len(failed_stocks) > 5 else ''}")
        
        if all_results:
            try:
                # 数据合并前的预处理和检查
                logger.info("开始合并多只股票的计算结果...")
                
                # 强化的DataFrame清理和标准化
                logger.info("开始强化的DataFrame清理和标准化...")
                
                # 第一步：初步清理和验证
                valid_dfs = []
                for i, df in enumerate(all_results):
                    if df is None or df.empty:
                        logger.warning(f"跳过空的DataFrame (索引: {i})")
                        continue
                    
                    # 完全重置索引，确保为连续数字索引
                    df_clean = df.copy()
                    df_clean = df_clean.reset_index(drop=True)
                    
                    # 检查索引唯一性
                    if df_clean.index.has_duplicates:
                        logger.warning(f"DataFrame {i} 存在重复索引，进行去重")
                        df_clean = df_clean.loc[~df_clean.index.duplicated(keep='first')]
                        df_clean = df_clean.reset_index(drop=True)
                    
                    valid_dfs.append((i, df_clean))
                
                if not valid_dfs:
                    logger.error("❌ 没有有效的计算结果可以处理")
                    return pd.DataFrame()
                
                # 第二步：统一列结构
                logger.info(f"统一 {len(valid_dfs)} 个DataFrame的列结构...")
                
                # 获取所有唯一的列名
                all_columns = set()
                for _, df in valid_dfs:
                    all_columns.update(df.columns)
                
                all_columns = sorted(list(all_columns))
                logger.info(f"发现 {len(all_columns)} 个唯一列")
                
                # 使用简单安全的合并方法
                cleaned_results = []
                for i, df in valid_dfs:
                    try:
                        # 简单复制和重置索引
                        clean_df = df.copy()
                        clean_df = clean_df.reset_index(drop=True)
                        
                        # 确保所有列都是数值类型或字符串，但保护重要的字符串列和日期列
                        # 需要保护的字符串列和日期列
                        protected_cols = ['Symbol', 'Ticker', 'Name', 'ENName', 'Code', 'Date']
                        
                        for col in clean_df.columns:
                            try:
                                # 跳过重要的字符串列和日期列，不进行数值转换
                                if col in protected_cols:
                                    continue
                                    
                                # 尝试转换为数值，如果失败就保持原样
                                if clean_df[col].dtype == 'object':
                                    try:
                                        clean_df[col] = pd.to_numeric(clean_df[col], errors='coerce')
                                    except:
                                        pass
                            except:
                                pass
                        
                        cleaned_results.append(clean_df)
                        logger.debug(f"✅ 成功清理DataFrame {i}")
                        
                    except Exception as e:
                        logger.error(f"❌ 清理DataFrame {i} 时发生错误: {e}")
                        continue
                
                if not cleaned_results:
                    logger.error("❌ 没有有效的计算结果可以合并")
                    return pd.DataFrame()
                
                # 安全合并DataFrame - 使用更保守的方法
                logger.info(f"正在合并 {len(cleaned_results)} 个有效结果...")
                try:
                    # 逐个合并，避免列不匹配的问题
                    combined_df = None
                    for i, df in enumerate(cleaned_results):
                        if combined_df is None:
                            combined_df = df.copy()
                        else:
                            # 使用outer join确保所有列都被保留
                            combined_df = pd.concat([combined_df, df], ignore_index=True, sort=False)
                        logger.debug(f"合并第 {i+1}/{len(cleaned_results)} 个DataFrame")
                    
                    if combined_df is None or combined_df.empty:
                        logger.error("❌ 合并后的DataFrame为空")
                        return pd.DataFrame()
                    
                except Exception as merge_error:
                    logger.error(f"❌ DataFrame合并失败: {merge_error}")
                    # 降级到最简单的方法
                    logger.info("尝试使用最简单的合并方法...")
                    try:
                        # 只保留列数最少的DataFrame的列
                        min_cols = min(len(df.columns) for df in cleaned_results)
                        logger.info(f"使用最小列数: {min_cols}")
                        
                        # 找到有最小列数的第一个DataFrame作为模板
                        template_df = next(df for df in cleaned_results if len(df.columns) == min_cols)
                        template_cols = template_df.columns.tolist()
                        
                        # 只保留公共列进行合并
                        aligned_dfs = []
                        for df in cleaned_results:
                            aligned_df = df[template_cols].copy()
                            aligned_dfs.append(aligned_df)
                        
                        combined_df = pd.concat(aligned_dfs, ignore_index=True)
                        logger.info(f"✅ 简化合并成功，保留 {len(template_cols)} 列")
                        
                    except Exception as e2:
                        logger.error(f"❌ 简化合并也失败: {e2}")
                        return pd.DataFrame()
                
                # 验证合并结果
                if combined_df.empty:
                    logger.error("❌ 合并后的DataFrame为空")
                    return pd.DataFrame()
                
                # 检查重复行
                initial_rows = len(combined_df)
                combined_df = combined_df.drop_duplicates()
                if len(combined_df) < initial_rows:
                    logger.info(f"移除了 {initial_rows - len(combined_df)} 行重复数据")
                
                logger.info(f"✅ 并行计算完成: {success_count}/{len(stocks)} 只股票成功 (耗时: {elapsed_time:.2f}s)")
                # 计算指标数量：总列数减去Date和Symbol列
                indicator_count = len(combined_df.columns) - (2 if 'Date' in combined_df.columns else 1)
                logger.info(f"📊 总指标数量: {indicator_count}")
                logger.info(f"📈 总数据行数: {len(combined_df)}")
                logger.info(f"⚡ 平均每只股票耗时: {elapsed_time/len(stocks):.2f}s")
                return combined_df
                
            except Exception as e:
                logger.error(f"❌ 合并计算结果时发生错误: {e}")
                logger.error(f"尝试降级为简单合并...")
                
                # 降级方案：最安全的逐个合并
                try:
                    logger.info("使用最安全的逐个合并方案...")
                    
                    # 只保留非空的结果
                    valid_results = [df for df in all_results if df is not None and not df.empty]
                    
                    if not valid_results:
                        logger.error("❌ 没有有效的计算结果")
                        return pd.DataFrame()
                    
                    logger.info(f"开始逐个合并 {len(valid_results)} 个DataFrame...")
                    
                    # 逐个安全合并
                    combined_df = None
                    successful_merges = 0
                    
                    for i, df in enumerate(valid_results):
                        try:
                            # 彻底清理单个DataFrame
                            clean_df = df.copy()
                            clean_df = clean_df.reset_index(drop=True)
                            
                            # 确保列名为字符串
                            clean_df.columns = [str(col) for col in clean_df.columns]
                            
                            # 去除任何可能的重复索引
                            if clean_df.index.has_duplicates:
                                clean_df = clean_df.loc[~clean_df.index.duplicated(keep='first')]
                                clean_df = clean_df.reset_index(drop=True)
                            
                            # 转换为字典再转回DataFrame（最彻底的清理）
                            # 保护重要的字符串列和日期列
                            protected_cols = ['Symbol', 'Ticker', 'Name', 'ENName', 'Code', 'Date']
                            data_dict = {}
                            for col in clean_df.columns:
                                try:
                                    if col in protected_cols:
                                        # 对于重要的字符串列和日期列，直接保留原值
                                        data_dict[col] = clean_df[col].tolist()
                                    else:
                                        data_dict[col] = clean_df[col].values.tolist()
                                except:
                                    if col in protected_cols:
                                        # 对于重要列，尝试保留原值而不是设为NaN
                                        try:
                                            data_dict[col] = clean_df[col].astype(str).tolist()
                                        except:
                                            data_dict[col] = [''] * len(clean_df)
                                    else:
                                        data_dict[col] = [np.nan] * len(clean_df)
                            
                            clean_df = pd.DataFrame(data_dict)
                            
                            if combined_df is None:
                                combined_df = clean_df
                            else:
                                # 逐个添加行
                                combined_df = pd.concat([combined_df, clean_df], ignore_index=True, sort=False)
                            
                            successful_merges += 1
                            logger.debug(f"成功合并第 {i+1} 个DataFrame")
                            
                        except Exception as merge_error:
                            logger.warning(f"跳过第 {i+1} 个DataFrame，合并失败: {merge_error}")
                            continue
                    
                    if combined_df is not None and not combined_df.empty:
                        logger.info(f"✅ 降级合并成功: {successful_merges}/{len(valid_results)} 个结果")
                        # 计算指标数量：总列数减去Date和Symbol列
                        indicator_count = len(combined_df.columns) - (2 if 'Date' in combined_df.columns else 1)
                        logger.info(f"📊 总指标数量: {indicator_count}")
                        logger.info(f"📈 总数据行数: {len(combined_df)}")
                        return combined_df
                    else:
                        logger.error("❌ 降级合并后结果为空")
                        return pd.DataFrame()
                    
                except Exception as e2:
                    logger.error(f"❌ 降级合并也失败: {e2}")
                    # 最后的备用方案：保存单个最大的结果
                    try:
                        if all_results:
                            largest_df = max(all_results, key=lambda x: len(x) if x is not None else 0)
                            if largest_df is not None and not largest_df.empty:
                                result_df = largest_df.copy().reset_index(drop=True)
                                logger.warning(f"⚠️ 使用最大的单个结果: {len(result_df)} 行")
                                return result_df
                    except:
                        pass
                    return pd.DataFrame()
        else:
            logger.error("❌ 没有成功计算任何股票的指标")
            return pd.DataFrame()
    
    def _calculate_all_stocks_sequential(self, stocks: List[str]) -> pd.DataFrame:
        """顺序计算多只股票的指标"""
        logger.info(f"使用顺序模式计算 {len(stocks)} 只股票")
        
        all_results = []
        success_count = 0
        start_time = time.time()
        
        for i, symbol in enumerate(stocks, 1):
            stock_start_time = time.time()
            logger.info(f"📈 处理第 {i}/{len(stocks)} 只股票: {symbol}")
            
            result = self.calculate_all_indicators_for_stock(symbol)
            if result is not None:
                all_results.append(result)
                success_count += 1
                stock_elapsed = time.time() - stock_start_time
                # 计算指标数量：总列数减去Date和Symbol列
                indicator_count = len(result.columns) - (2 if 'Date' in result.columns else 1)
                logger.info(f"✅ {symbol}: 完成 {indicator_count} 个指标 (耗时: {stock_elapsed:.2f}s)")
            else:
                logger.warning(f"⚠️ {symbol}: 计算失败")
        
        elapsed_time = time.time() - start_time
        
        if all_results:
            try:
                # 数据合并前的预处理和检查
                logger.info("开始合并多只股票的计算结果...")
                
                # 重置所有DataFrame的索引以避免冲突
                cleaned_results = []
                for i, df in enumerate(all_results):
                    if df is None or df.empty:
                        logger.warning(f"跳过空的DataFrame (索引: {i})")
                        continue
                    cleaned_results.append(df.reset_index(drop=True))
                
                if not cleaned_results:
                    logger.error("❌ 没有有效的计算结果可以合并")
                    return pd.DataFrame()
                
                # 安全合并DataFrame
                combined_df = pd.concat(cleaned_results, ignore_index=True, sort=False)
                
                # 检查重复行
                initial_rows = len(combined_df)
                combined_df = combined_df.drop_duplicates()
                if len(combined_df) < initial_rows:
                    logger.info(f"移除了 {initial_rows - len(combined_df)} 行重复数据")
                
                logger.info(f"✅ 顺序计算完成: {success_count}/{len(stocks)} 只股票成功 (总耗时: {elapsed_time:.2f}s)")
                # 计算指标数量：总列数减去Date和Symbol列
                indicator_count = len(combined_df.columns) - (2 if 'Date' in combined_df.columns else 1)
                logger.info(f"📊 总指标数量: {indicator_count}")
                logger.info(f"📈 总数据行数: {len(combined_df)}")
                logger.info(f"⏱️ 平均每只股票耗时: {elapsed_time/len(stocks):.2f}s")
                return combined_df
                
            except Exception as e:
                logger.error(f"❌ 顺序计算合并结果时发生错误: {e}")
                return pd.DataFrame()
        else:
            logger.error("❌ 没有成功计算任何股票的指标")
            return pd.DataFrame()
    
    def save_results(self, df: pd.DataFrame, filename: str = "enhanced_quantitative_indicators.csv") -> str:
        """保存结果到CSV文件，包含中文标签行，空值使用空字符串（兼容SAS）"""
        if df.empty:
            logger.warning("DataFrame为空，无法保存")
            return ""
        
        try:
            output_path = self.output_dir / filename
            
            # 获取列名和中文标签
            columns = df.columns.tolist()
            chinese_labels = self.get_field_labels(columns)
            
            # 处理空值：将NaN替换为空字符串，以兼容SAS
            df_clean = df.copy()
            df_clean = df_clean.fillna('')  # 将NaN值替换为空字符串
            
            logger.info("📝 空值处理: 将NaN值替换为空字符串以兼容SAS")
            
            # 使用手动方式写入CSV文件以包含中文标签行
            with open(output_path, 'w', encoding='utf-8-sig', newline='') as f:
                import csv
                writer = csv.writer(f)
                
                # 第一行：字段名（英文列名）
                writer.writerow(columns)
                
                # 第二行：中文标签
                writer.writerow(chinese_labels)
                
                # 第三行开始：具体数据
                for _, row in df_clean.iterrows():
                    writer.writerow(row.values)
            
            logger.info(f"结果已保存到: {output_path}")
            logger.info(f"数据形状: {df.shape}")
            logger.info(f"包含中文标签行的CSV格式:")
            logger.info(f"  第一行: 字段名 ({len(columns)} 个字段)")
            logger.info(f"  第二行: 中文标签")
            logger.info(f"  第三行开始: 具体数据 ({len(df)} 行数据)")
            logger.info(f"  空值处理: NaN → '' (空字符串，兼容SAS)")
            
            # 计算指标数量：总列数减去Date和Symbol列
            indicator_count = len(df.columns) - (2 if 'Date' in df.columns else 1)
            logger.info(f"指标数量: {indicator_count}")
            
            return str(output_path)
            
        except Exception as e:
            logger.error(f"保存结果失败: {e}")
            return ""
    
    def run(self, max_stocks: Optional[int] = None, output_filename: str = "enhanced_quantitative_indicators.csv"):
        """运行完整的指标计算流程"""
        logger.info("=" * 80)
        logger.info("🚀 开始运行增强版Qlib指标计算器")
        logger.info(f"⚙️ 多线程模式: {'启用' if self.enable_parallel else '禁用'}")
        if self.enable_parallel:
            logger.info(f"🧵 最大线程数: {self.max_workers}")
        logger.info("=" * 80)
        
        start_time = time.time()
        
        # 计算指标
        results_df = self.calculate_all_indicators(max_stocks=max_stocks)
        
        total_elapsed = time.time() - start_time
        
        if not results_df.empty:
            # 保存结果
            output_path = self.save_results(results_df, output_filename)
            
            logger.info("=" * 80)
            logger.info("✅ 指标计算完成！")
            # 计算指标数量：总列数减去Date和Symbol列
            indicator_count = len(results_df.columns) - (2 if 'Date' in results_df.columns else 1)
            logger.info(f"📊 总共计算了 {indicator_count} 个指标")
            logger.info(f"📈 包含 {results_df['Symbol'].nunique()} 只股票")
            logger.info(f"⏱️ 总耗时: {total_elapsed:.2f} 秒")
            logger.info(f"💾 结果保存至: {output_path}")
            logger.info("=" * 80)
            
            # 显示指标统计
            self._show_indicators_summary(results_df)
        else:
            logger.error(f"❌ 指标计算失败，没有生成任何结果 (耗时: {total_elapsed:.2f}s)")
    
    def _show_indicators_summary(self, df: pd.DataFrame):
        """显示指标统计摘要"""
        logger.info("📊 指标分类统计:")
        logger.info("-" * 50)
        
        # 统计各类指标数量
        alpha158_count = len([col for col in df.columns if col.startswith('ALPHA158_')])
        alpha360_count = len([col for col in df.columns if col.startswith('ALPHA360_')])
        technical_count = len([col for col in df.columns if any(prefix in col for prefix in ['SMA', 'EMA', 'RSI', 'MACD', 'ADX', 'ATR', 'BB_', 'STOCH']) and not col.startswith(('ALPHA158_', 'ALPHA360_'))])
        candlestick_count = len([col for col in df.columns if col.startswith('CDL')])
        financial_count = len([col for col in df.columns if any(prefix in col for prefix in ['PriceToBook', 'MarketCap', 'PE', 'ROE', 'ROA', 'turnover', 'TobinsQ'])])
        volatility_count = len([col for col in df.columns if 'Volatility' in col or 'SemiDeviation' in col])
        
        logger.info(f"Alpha158指标: {alpha158_count} 个")
        logger.info(f"Alpha360指标: {alpha360_count} 个")
        logger.info(f"技术指标: {technical_count} 个")
        logger.info(f"蜡烛图形态: {candlestick_count} 个")
        logger.info(f"财务指标: {financial_count} 个")
        logger.info(f"波动率指标: {volatility_count} 个")
        logger.info(f"总计: {alpha158_count + alpha360_count + technical_count + candlestick_count + financial_count + volatility_count} 个")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='增强版Qlib指标计算器 - 集成Alpha158、Alpha360指标体系',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
使用示例:
  # 计算所有股票的所有指标
  python qlib_indicators.py
  
  # 只计算前10只股票
  python qlib_indicators.py --max-stocks 10
  
  # 指定数据目录和财务数据目录
  python qlib_indicators.py --data-dir ./data --financial-dir ./financial_data
  
  # 自定义输出文件名
  python qlib_indicators.py --output indicators_2025.csv
  
  # 禁用多线程并行计算
  python qlib_indicators.py --disable-parallel
  
  # 自定义线程数量
  python qlib_indicators.py --max-workers 16
  
  # 调试模式
  python qlib_indicators.py --log-level DEBUG --max-stocks 5

多线程性能优化:
  - 🚀 多只股票并行计算: 显著提升处理速度
  - ⚡ 单只股票指标类型并行: Alpha158、Alpha360、技术指标等同时计算
  - 🧵 智能线程管理: 自动优化线程数量，避免资源竞争
  - 📊 实时进度显示: 详细的计算进度和性能统计
  - 🔒 线程安全: 确保指标去重和数据一致性

支持的指标类型:
  - Alpha158指标体系: ~158个 (KBAR、价格、成交量、滚动技术指标)
  - Alpha360指标体系: ~360个 (过去60天标准化价格和成交量数据)
  - 技术指标: ~60个 (移动平均、MACD、RSI、布林带等)
  - 蜡烛图形态: 61个 (锤子线、十字星、吞没形态等)
  - 财务指标: ~15个 (市净率、换手率、托宾Q值等)
  - 波动率指标: ~8个 (已实现波动率、半变差等)
  
总计约650+个指标，具备去重功能和多线程加速
        '''
    )
    
    parser.add_argument(
        '--data-dir',
        default=r"D:\stk_data\trd\us_data",
        help='Qlib数据目录路径'
    )
    
    parser.add_argument(
        '--financial-dir',
        help='财务数据目录路径 (默认: ~/.qlib/financial_data)'
    )
    
    parser.add_argument(
        '--max-stocks',
        type=int,
        help='最大股票数量限制'
    )
    
    parser.add_argument(
        '--output',
        default='enhanced_quantitative_indicators.csv',
        help='输出文件名'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='日志级别'
    )
    
    parser.add_argument(
        '--disable-parallel',
        action='store_true',
        help='禁用多线程并行计算'
    )
    
    parser.add_argument(
        '--max-workers',
        type=int,
        help='最大线程数 (默认: CPU核心数+4，最大32)'
    )
    
    args = parser.parse_args()
    
    # 设置日志级别
    logger.remove()
    logger.add(
        lambda msg: print(msg, end=""),
        level=args.log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    
    try:
        # 创建计算器并运行
        calculator = QlibIndicatorsEnhancedCalculator(
            data_dir=args.data_dir,
            financial_data_dir=args.financial_dir,
            enable_parallel=not args.disable_parallel,
            max_workers=args.max_workers
        )
        
        calculator.run(
            max_stocks=args.max_stocks,
            output_filename=args.output
        )
        
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        raise


if __name__ == "__main__":
    main()
