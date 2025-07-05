#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Qlib指标计算器快速使用示例
演示如何使用 QlibIndicatorsEnhancedCalculator 进行指标计算
"""

import os
import sys
from pathlib import Path
import pandas as pd
import time

# 添加项目根目录到路径
sys.path.append(str(Path(__file__).parent))

# 导入指标计算器
from scripts.qlib_indicators import QlibIndicatorsEnhancedCalculator

def example_basic_usage():
    """基础使用示例"""
    print("=" * 60)
    print("📊 基础使用示例")
    print("=" * 60)
    
    # 创建计算器实例
    calculator = QlibIndicatorsEnhancedCalculator(
        data_dir="D:/stk_data/trd/us_data",
        financial_data_dir="D:/stk_data/financial_data",
        max_workers=8,
        enable_parallel=True
    )
    
    # 获取可用股票
    stocks = calculator.get_available_stocks()
    print(f"📈 可用股票数量: {len(stocks)}")
    if stocks:
        print(f"📋 前10只股票: {stocks[:10]}")
    
    # 计算前3只股票的指标（用于演示）
    print(f"\n🚀 开始计算前3只股票的指标...")
    start_time = time.time()
    
    results = calculator.calculate_all_indicators(max_stocks=3)
    
    elapsed = time.time() - start_time
    print(f"⏱️ 计算完成，耗时: {elapsed:.2f}秒")
    
    if not results.empty:
        print(f"📊 结果统计:")
        print(f"   - 数据形状: {results.shape}")
        print(f"   - 股票数量: {results['Symbol'].nunique()}")
        print(f"   - 指标数量: {len(results.columns) - 2}")  # 减去Date和Symbol
        
        # 保存结果
        output_path = calculator.save_results(results, "quick_example_results.csv")
        print(f"💾 结果已保存到: {output_path}")
    else:
        print("❌ 没有生成任何结果")

def example_single_stock():
    """单只股票计算示例"""
    print("\n" + "=" * 60)
    print("📊 单只股票计算示例")
    print("=" * 60)
    
    calculator = QlibIndicatorsEnhancedCalculator(
        data_dir="D:/stk_data/trd/us_data",
        enable_parallel=False  # 单只股票可以禁用并行
    )
    
    # 获取第一只股票
    stocks = calculator.get_available_stocks()
    if not stocks:
        print("❌ 没有找到可用股票")
        return
    
    target_stock = stocks[0]
    print(f"🎯 计算股票: {target_stock}")
    
    # 计算单只股票的指标
    stock_data = calculator.calculate_all_indicators_for_stock(target_stock)
    
    if stock_data is not None:
        print(f"📊 {target_stock} 指标计算完成:")
        print(f"   - 数据记录数: {len(stock_data)}")
        print(f"   - 指标数量: {len(stock_data.columns)}")
        print(f"   - 时间范围: {stock_data.index.min()} ~ {stock_data.index.max()}")
        
        # 显示前5个指标
        print(f"   - 前5个指标: {list(stock_data.columns)[:5]}")
    else:
        print(f"❌ {target_stock} 指标计算失败")

def example_specific_indicators():
    """指定指标类型计算示例"""
    print("\n" + "=" * 60)
    print("📊 指定指标类型计算示例")
    print("=" * 60)
    
    calculator = QlibIndicatorsEnhancedCalculator(
        data_dir="D:/stk_data/trd/us_data"
    )
    
    stocks = calculator.get_available_stocks()
    if not stocks:
        print("❌ 没有找到可用股票")
        return
    
    target_stock = stocks[0]
    print(f"🎯 目标股票: {target_stock}")
    
    # 读取原始数据
    raw_data = calculator.read_qlib_binary_data(target_stock)
    if raw_data is None:
        print(f"❌ 无法读取 {target_stock} 的数据")
        return
    
    print(f"📊 原始数据形状: {raw_data.shape}")
    
    # 计算不同类型的指标
    indicator_types = [
        ("技术指标", calculator.calculate_all_technical_indicators),
        ("Alpha158指标", calculator.calculate_alpha158_indicators),
        ("Alpha360指标", calculator.calculate_alpha360_indicators),
        ("蜡烛图形态", calculator.calculate_candlestick_patterns),
        ("波动率指标", calculator.calculate_volatility_indicators),
    ]
    
    for name, func in indicator_types:
        try:
            indicators = func(raw_data)
            print(f"✅ {name}: {len(indicators.columns)} 个指标")
        except Exception as e:
            print(f"❌ {name}: 计算失败 - {e}")

def example_custom_labels():
    """自定义标签示例"""
    print("\n" + "=" * 60)
    print("📊 自定义标签示例")
    print("=" * 60)
    
    calculator = QlibIndicatorsEnhancedCalculator(
        data_dir="D:/stk_data/trd/us_data"
    )
    
    # 测试字段标签
    test_columns = [
        'Date', 'Symbol', 'Close', 'Volume',
        'SMA_20', 'RSI_14', 'MACD_12_26_9',
        'ALPHA158_ROC_5', 'ALPHA360_0',
        'MarketCap', 'PERatio'
    ]
    
    labels = calculator.get_field_labels(test_columns)
    
    print("📋 字段标签映射:")
    print("-" * 50)
    for col, label in zip(test_columns, labels):
        print(f"   {col:<20} -> {label}")

def example_performance_comparison():
    """性能对比示例"""
    print("\n" + "=" * 60)
    print("📊 性能对比示例")
    print("=" * 60)
    
    stocks_to_test = 5
    
    # 并行计算
    print(f"🚀 并行计算 {stocks_to_test} 只股票...")
    calculator_parallel = QlibIndicatorsEnhancedCalculator(
        data_dir="D:/stk_data/trd/us_data",
        enable_parallel=True,
        max_workers=8
    )
    
    start_time = time.time()
    results_parallel = calculator_parallel.calculate_all_indicators(max_stocks=stocks_to_test)
    parallel_time = time.time() - start_time
    
    print(f"   ⏱️ 并行计算耗时: {parallel_time:.2f}秒")
    print(f"   📊 结果形状: {results_parallel.shape}")
    
    # 顺序计算
    print(f"\n🐌 顺序计算 {stocks_to_test} 只股票...")
    calculator_sequential = QlibIndicatorsEnhancedCalculator(
        data_dir="D:/stk_data/trd/us_data",
        enable_parallel=False
    )
    
    start_time = time.time()
    results_sequential = calculator_sequential.calculate_all_indicators(max_stocks=stocks_to_test)
    sequential_time = time.time() - start_time
    
    print(f"   ⏱️ 顺序计算耗时: {sequential_time:.2f}秒")
    print(f"   📊 结果形状: {results_sequential.shape}")
    
    # 性能对比
    if sequential_time > 0:
        speedup = sequential_time / parallel_time
        print(f"\n🏃 性能提升: {speedup:.2f}x")
    
    # 验证结果一致性
    if results_parallel.shape == results_sequential.shape:
        print("✅ 并行和顺序计算结果形状一致")
    else:
        print("❌ 并行和顺序计算结果形状不一致")

def example_data_validation():
    """数据验证示例"""
    print("\n" + "=" * 60)
    print("📊 数据验证示例")
    print("=" * 60)
    
    calculator = QlibIndicatorsEnhancedCalculator(
        data_dir="D:/stk_data/trd/us_data"
    )
    
    # 检查数据目录
    print(f"📁 数据目录: {calculator.data_dir}")
    print(f"   存在: {calculator.data_dir.exists()}")
    
    print(f"📁 特征目录: {calculator.features_dir}")
    print(f"   存在: {calculator.features_dir.exists()}")
    
    # 检查可用股票
    stocks = calculator.get_available_stocks()
    print(f"\n📈 可用股票数量: {len(stocks)}")
    
    # 验证前几只股票的数据
    test_stocks = stocks[:3] if len(stocks) >= 3 else stocks
    print(f"\n🔍 验证前 {len(test_stocks)} 只股票的数据:")
    
    for stock in test_stocks:
        data = calculator.read_qlib_binary_data(stock)
        if data is not None:
            print(f"   ✅ {stock}: {data.shape[0]} 条记录, 时间范围: {data.index.min().date()} ~ {data.index.max().date()}")
        else:
            print(f"   ❌ {stock}: 数据读取失败")

def main():
    """主函数 - 运行所有示例"""
    print("🎯 Qlib指标计算器使用示例")
    print("=" * 60)
    
    try:
        # 1. 数据验证
        example_data_validation()
        
        # 2. 基础使用
        example_basic_usage()
        
        # 3. 单只股票计算
        example_single_stock()
        
        # 4. 指定指标类型计算
        example_specific_indicators()
        
        # 5. 自定义标签
        example_custom_labels()
        
        # 6. 性能对比（可选，耗时较长）
        run_performance_test = input("\n是否运行性能对比测试？(y/n): ").lower().strip()
        if run_performance_test == 'y':
            example_performance_comparison()
        
        print("\n" + "=" * 60)
        print("🎉 所有示例运行完成！")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ 示例运行失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 