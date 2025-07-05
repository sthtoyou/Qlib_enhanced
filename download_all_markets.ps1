# 多市场股票数据下载工具 v2.0 - PowerShell版
# 支持Windows PowerShell和PowerShell Core

Write-Host "========================================" -ForegroundColor Green
Write-Host "     多市场股票数据下载工具 v2.0" -ForegroundColor Green  
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "正在下载全球三大核心指数数据..." -ForegroundColor Yellow
Write-Host "- A股: 沪深300+中证500 (~800只)" -ForegroundColor Cyan
Write-Host "- 美股: 标普500 (~503只)" -ForegroundColor Cyan
Write-Host "- 港股: 恒生指数 (~81只)" -ForegroundColor Cyan
Write-Host ""

$baseDir = "D:\stk_data\trd"

try {
    # 1. 下载中国A股数据
    Write-Host "[1/3] 下载中国A股数据 (沪深300+中证500)..." -ForegroundColor Magenta
    Write-Host "目标目录: $baseDir\cn_data" -ForegroundColor Gray
    
    & python scripts/get_data.py qlib_data --target_dir "$baseDir\cn_data" --region cn --cn_realtime True --incremental_update True
    if ($LASTEXITCODE -ne 0) { throw "A股数据下载失败" }
    
    Write-Host "✓ A股数据下载完成" -ForegroundColor Green
    Write-Host ""

    # 2. 下载美国标普500数据  
    Write-Host "[2/3] 下载美国标普500数据..." -ForegroundColor Magenta
    Write-Host "目标目录: $baseDir\us_data" -ForegroundColor Gray
    
    & python scripts/get_data.py qlib_data --target_dir "$baseDir\us_data" --region us --incremental_update True
    if ($LASTEXITCODE -ne 0) { throw "美股数据下载失败" }
    
    Write-Host "✓ 美股数据下载完成" -ForegroundColor Green
    Write-Host ""

    # 3. 下载香港恒生指数数据
    Write-Host "[3/3] 下载香港恒生指数数据..." -ForegroundColor Magenta  
    Write-Host "目标目录: $baseDir\hk_data" -ForegroundColor Gray
    
    & python scripts/get_data.py qlib_data --target_dir "$baseDir\hk_data" --region hk --incremental_update True
    if ($LASTEXITCODE -ne 0) { throw "港股数据下载失败" }
    
    Write-Host "✓ 港股数据下载完成" -ForegroundColor Green
    Write-Host ""

    # 成功完成
    Write-Host "========================================" -ForegroundColor Green
    Write-Host "          🎉 全部下载完成！" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
    Write-Host "数据存储位置:" -ForegroundColor Yellow
    Write-Host "- A股数据: $baseDir\cn_data" -ForegroundColor White
    Write-Host "- 美股数据: $baseDir\us_data" -ForegroundColor White  
    Write-Host "- 港股数据: $baseDir\hk_data" -ForegroundColor White
    Write-Host ""
    Write-Host "总计: ~1,384只全球核心股票数据" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Green
    
} catch {
    Write-Host ""
    Write-Host "❌ 错误: $_" -ForegroundColor Red
    Write-Host "请检查网络连接和Python环境" -ForegroundColor Yellow
    exit 1
}

Write-Host ""
Write-Host "按任意键继续..." -ForegroundColor Gray
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown") 