#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import time
import pandas as pd
from pathlib import Path
from loguru import logger
import argparse

# 添加当前目录到Python路径
sys.path.insert(0, str(Path(__file__).parent))

from qlib_indicators import QlibIndicatorsEnhancedCalculator

class BatchIndicatorCalculator:
    """
    批处理指标计算器
    将大量股票分批处理，避免内存溢出和索引冲突
    """
    
    def __init__(self, data_dir: str = None, batch_size: int = 20, max_workers: int = 8):
        self.batch_size = batch_size
        self.max_workers = max_workers
        
        # 创建计算器
        if data_dir:
            self.calculator = QlibIndicatorsEnhancedCalculator(
                data_dir=data_dir,
                enable_parallel=True,
                max_workers=max_workers
            )
        else:
            self.calculator = QlibIndicatorsEnhancedCalculator(
                enable_parallel=True,
                max_workers=max_workers
            )
        
        logger.info(f"批处理配置: 批次大小={batch_size}, 线程数={max_workers}")
    
    def get_stock_batches(self, max_stocks: int = None):
        """将股票分批"""
        stocks = self.calculator.get_available_stocks()
        
        if max_stocks:
            stocks = stocks[:max_stocks]
        
        # 分批
        batches = []
        for i in range(0, len(stocks), self.batch_size):
            batch = stocks[i:i + self.batch_size]
            batches.append(batch)
        
        logger.info(f"总股票数: {len(stocks)}, 分为 {len(batches)} 个批次")
        return batches
    
    def process_single_batch(self, batch_stocks, batch_num, total_batches):
        """处理单个批次"""
        logger.info(f"🔄 开始处理批次 {batch_num}/{total_batches} ({len(batch_stocks)} 只股票)")
        
        start_time = time.time()
        
        try:
            # 临时修改股票列表
            original_get_stocks = self.calculator.get_available_stocks
            self.calculator.get_available_stocks = lambda: batch_stocks
            
            # 计算指标
            result = self.calculator.calculate_all_indicators()
            
            # 恢复原始方法
            self.calculator.get_available_stocks = original_get_stocks
            
            elapsed_time = time.time() - start_time
            
            if not result.empty:
                logger.info(f"✅ 批次 {batch_num} 完成: {len(result)} 行, {len(result.columns)-1} 个指标 (耗时: {elapsed_time:.2f}s)")
                return result
            else:
                logger.warning(f"⚠️ 批次 {batch_num} 结果为空")
                return None
                
        except Exception as e:
            logger.error(f"❌ 批次 {batch_num} 处理失败: {e}")
            return None
    
    def merge_batch_results(self, batch_results):
        """安全合并批次结果"""
        logger.info(f"开始合并 {len(batch_results)} 个批次结果...")
        
        if not batch_results:
            logger.error("没有有效的批次结果可以合并")
            return pd.DataFrame()
        
        try:
            # 最安全的逐个合并方式
            combined_df = None
            total_rows = 0
            
            for i, batch_df in enumerate(batch_results):
                if batch_df is None or batch_df.empty:
                    continue
                
                # 彻底清理DataFrame
                clean_df = batch_df.copy()
                clean_df = clean_df.reset_index(drop=True)
                
                # 确保列名一致
                if combined_df is not None:
                    # 统一列结构
                    all_cols = sorted(set(combined_df.columns) | set(clean_df.columns))
                    
                    # 为缺失的列添加NaN
                    for col in all_cols:
                        if col not in combined_df.columns:
                            combined_df[col] = pd.NA
                        if col not in clean_df.columns:
                            clean_df[col] = pd.NA
                    
                    # 重新排序列
                    combined_df = combined_df[all_cols]
                    clean_df = clean_df[all_cols]
                
                if combined_df is None:
                    combined_df = clean_df
                else:
                    # 使用最基本的append方式
                    combined_df = pd.concat([combined_df, clean_df], ignore_index=True, sort=False)
                
                total_rows += len(clean_df)
                logger.info(f"合并批次 {i+1}: +{len(clean_df)} 行 (总计: {total_rows} 行)")
            
            if combined_df is not None and not combined_df.empty:
                # 去重
                initial_rows = len(combined_df)
                combined_df = combined_df.drop_duplicates()
                final_rows = len(combined_df)
                
                if initial_rows != final_rows:
                    logger.info(f"去重: {initial_rows} -> {final_rows} 行")
                
                logger.info(f"✅ 合并完成: {final_rows} 行, {len(combined_df.columns)-1} 个指标")
                return combined_df
            else:
                logger.error("合并后结果为空")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"合并批次结果失败: {e}")
            return pd.DataFrame()
    
    def run_batch_calculation(self, max_stocks: int = None, output_file: str = "batch_indicators.csv"):
        """运行批处理计算"""
        logger.info("🚀 开始批处理指标计算")
        logger.info("=" * 60)
        
        start_time = time.time()
        
        # 获取股票批次
        batches = self.get_stock_batches(max_stocks)
        
        if not batches:
            logger.error("没有股票需要处理")
            return False
        
        # 处理各个批次
        batch_results = []
        successful_batches = 0
        
        for i, batch in enumerate(batches, 1):
            try:
                result = self.process_single_batch(batch, i, len(batches))
                if result is not None and not result.empty:
                    batch_results.append(result)
                    successful_batches += 1
                    
                    # 显示进度
                    progress = i / len(batches) * 100
                    logger.info(f"📊 整体进度: {progress:.1f}% ({i}/{len(batches)} 批次)")
                    
                else:
                    logger.warning(f"批次 {i} 无有效结果")
                    
            except Exception as e:
                logger.error(f"批次 {i} 处理异常: {e}")
                continue
        
        # 合并所有批次结果
        if batch_results:
            final_result = self.merge_batch_results(batch_results)
            
            if not final_result.empty:
                # 保存结果
                try:
                    output_path = Path(output_file)
                    final_result.to_csv(output_path, index=False, encoding='utf-8-sig')
                    
                    total_time = time.time() - start_time
                    
                    logger.info("=" * 60)
                    logger.info("🎉 批处理完成！")
                    logger.info(f"✅ 成功批次: {successful_batches}/{len(batches)}")
                    logger.info(f"📊 总股票数: {final_result['Symbol'].nunique()}")
                    logger.info(f"📈 总指标数: {len(final_result.columns)-1}")
                    logger.info(f"📋 总数据行数: {len(final_result)}")
                    logger.info(f"⏱️ 总耗时: {total_time:.2f} 秒")
                    logger.info(f"💾 结果保存至: {output_path.absolute()}")
                    logger.info("=" * 60)
                    
                    return True
                    
                except Exception as e:
                    logger.error(f"保存结果失败: {e}")
                    return False
            else:
                logger.error("最终合并结果为空")
                return False
        else:
            logger.error("没有成功的批次结果")
            return False

def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='批处理Qlib指标计算器 - 分批处理大量股票',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
使用示例:
  # 默认配置 (每批20只股票，8个线程)
  python batch_calculator.py
  
  # 自定义批次大小和线程数
  python batch_calculator.py --batch-size 10 --max-workers 4
  
  # 处理前100只股票
  python batch_calculator.py --max-stocks 100 --batch-size 25
  
  # 小批次处理（内存受限环境）
  python batch_calculator.py --batch-size 5 --max-workers 2
        '''
    )
    
    parser.add_argument('--data-dir', help='Qlib数据目录路径')
    parser.add_argument('--max-stocks', type=int, help='最大股票数量')
    parser.add_argument('--batch-size', type=int, default=20, help='每批处理的股票数量 (默认: 20)')
    parser.add_argument('--max-workers', type=int, default=8, help='最大线程数 (默认: 8)')
    parser.add_argument('--output', default='batch_indicators.csv', help='输出文件名')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                       default='INFO', help='日志级别')
    
    args = parser.parse_args()
    
    # 设置日志
    logger.remove()
    logger.add(
        lambda msg: print(msg, end=""),
        level=args.log_level,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"
    )
    
    try:
        # 创建批处理计算器
        batch_calc = BatchIndicatorCalculator(
            data_dir=args.data_dir,
            batch_size=args.batch_size,
            max_workers=args.max_workers
        )
        
        # 运行计算
        success = batch_calc.run_batch_calculation(
            max_stocks=args.max_stocks,
            output_file=args.output
        )
        
        if success:
            logger.info("🎯 批处理计算成功完成！")
        else:
            logger.error("❌ 批处理计算失败")
            
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 