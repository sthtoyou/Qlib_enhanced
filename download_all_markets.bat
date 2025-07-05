@echo off
chcp 65001 > nul
echo ========================================
echo      多市场股票数据下载工具 v2.0
echo ========================================
echo.
echo 正在下载全球三大核心指数数据...
echo - A股: 沪深300+中证500 (~800只)
echo - 美股: 标普500 (~503只)  
echo - 港股: 恒生指数 (~81只)
echo.

set "base_dir=D:\stk_data\trd"

echo [1/3] 下载中国A股数据 (沪深300+中证500)...
echo 目标目录: %base_dir%\cn_data
python scripts/get_data.py qlib_data --target_dir "%base_dir%\cn_data" --region cn --cn_realtime True --incremental_update True
if %errorlevel% neq 0 (
    echo 错误: A股数据下载失败
    pause
    exit /b 1
)
echo ✓ A股数据下载完成
echo.

echo [2/3] 下载美国标普500数据...
echo 目标目录: %base_dir%\us_data  
python scripts/get_data.py qlib_data --target_dir "%base_dir%\us_data" --region us --incremental_update True
if %errorlevel% neq 0 (
    echo 错误: 美股数据下载失败
    pause
    exit /b 1
)
echo ✓ 美股数据下载完成
echo.

echo [3/3] 下载香港恒生指数数据...
echo 目标目录: %base_dir%\hk_data
python scripts/get_data.py qlib_data --target_dir "%base_dir%\hk_data" --region hk --incremental_update True
if %errorlevel% neq 0 (
    echo 错误: 港股数据下载失败
    pause  
    exit /b 1
)
echo ✓ 港股数据下载完成
echo.

echo ========================================
echo           🎉 全部下载完成！
echo ========================================
echo 数据存储位置:
echo - A股数据: %base_dir%\cn_data
echo - 美股数据: %base_dir%\us_data  
echo - 港股数据: %base_dir%\hk_data
echo.
echo 总计: ~1,384只全球核心股票数据
echo ========================================
pause 