# pip install pandas  numpy pyreadstat SASPy 
import subprocess
import tushare as ts 
import pandas as pd
import time
import datetime
import os 

def split_dates_by_month(start_date_str, end_date_str,days_str):
    """
    将指定起止日期的时段按月划分为多个时段

    参数:
    start_date_str (str): 起始日期字符串，格式为 'yyyymmdd'
    end_date_str (str): 结束日期字符串，格式为 'yyyymmdd'

    返回:
    list: 包含多个时段的列表，每个时段为一个元组，格式为 (起始日期字符串, 结束日期字符串)
    """
    start_date = datetime.datetime.strptime(start_date_str, '%Y%m%d')
    end_date = datetime.datetime.strptime(end_date_str, '%Y%m%d')
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        next_month = current_date.replace(day=1) + datetime.timedelta(days=days_str)
        next_month = next_month.replace(day=1)
        if next_month > end_date:
            date_list.append((current_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')))
            break
        date_list.append((current_date.strftime('%Y%m%d'), (next_month - datetime.timedelta(days=1)).strftime('%Y%m%d')))
        current_date = next_month
    return date_list

# 构建ts_dwload_daily函数，用于下载个股的daily数据
def  ts_dwload_daily(ts_table,file_dir,is_update_all,start_date,end_date,is_tslst_src_online,mkt,visit_per_min):
	if mkt==2:# mkt=1 为H股市场
		# 获取所有H股的列表,并存放在变量stock_codes中
		file_path = os.path.join(data_dir, 'stock_hk_basic_info.csv') 
		if is_tslst_src_online==1:
			stock_basic_info = pro.hk_basic()
			stock_basic_info.to_csv(file_path, index=False,encoding='utf-8')
		else:
			stock_basic_info = pd.read_csv(file_path) 
	elif mkt==3:# mkt=3 为美股市场
		# 获取所有H股的列表,并存放在变量stock_codes中
		file_path = os.path.join(data_dir, 'stock_us_basic_info.csv') 
		if is_tslst_src_online==1:
			stock_basic_info = pro.us_basic()
			stock_basic_info.to_csv(file_path, index=False,encoding='utf-8')
		else:
			stock_basic_info = pd.read_csv(file_path) 
	else:# mkt=1或空 为A股市场
		# 获取所有A股的列表,并存放在变量stock_codes中
		file_path = os.path.join(data_dir, 'stock_cn_basic_info.csv') 
		if is_tslst_src_online==1:
			stock_basic_info = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
			stock_basic_info.to_csv(file_path, index=False,encoding='utf-8')
		else:
			stock_basic_info = pd.read_csv(file_path) 
						
		
	stock_codes = stock_basic_info['ts_code'].unique()	
	
	#设立计数器和计时器，当1分钟内访问频率过快时，sleep一定时间
	start_time = datetime.datetime.now()
	rd_cnt=0
	# by 股票id 循环下载
	for code in stock_codes:
		rd_cnt=rd_cnt+1
		sec_round=(datetime.datetime.now()-start_time).total_seconds()
		
		if sec_round<60 and rd_cnt<visit_per_min:
			print(f"Downloading data for {ts_table} - {code} 注：本数据表1分钟内已访问次数={rd_cnt}")
			try:
				if is_update_all==1:
					#A股日线行情
					if ts_table in ('daily'):
						df = pro.daily(ts_code=code)
				else:
					#A股日线行情
					if ts_table in ('daily'):
						df = pro.daily(ts_code=code,start_date=start_date, end_date=end_date)
				file_path = os.path.join(file_dir, f"{ts_table}_{code}.csv")
				df.to_csv(file_path, index=False,encoding='utf-8')	
			except Exception as e:
				print(f"Error downloading data for stock {code}: {e}")
				
		elif sec_round<60 and rd_cnt>=visit_per_min:
			sec_to_sleep=60-sec_round
			print(f"1分钟内访问频率过快,暂停{sec_to_sleep}秒")
			time.sleep(sec_to_sleep)  # 暂停 X 秒
			print("继续下载")
			
		elif sec_round>=60:
			print("重新计算访问频率")
			start_time = datetime.datetime.now()
			rd_cnt=0
						
	if is_update_all==1:
		os.system(f"copy /b {file_dir}\\*{ts_table}*.csv {data_dir}\\{ts_table}_tdr.csv")
	else:
		os.system(f"copy /b {file_dir}\\*{ts_table}*.csv {data_dir}\\{ts_table}_tdr_{start_date}_{end_date}.csv")
	
	os.system(f"del {file_dir}\\*{ts_table}*.csv")
	
	print(f"The Data Download task of {ts_table} finished")
	
# 构建ts_dwload_index_daily函数，用于下载index数据
def  ts_dwload_index_daily(ts_table,file_dir,is_update_all,start_date,end_date,is_tslst_src_online,visit_per_min):
	# 获取所有指数的列表,并存放在变量index_codes中
	file_path = os.path.join(data_dir, 'index_basic.csv') 
	if is_tslst_src_online==1:
		index_basic = pro.index_basic()
		index_basic.to_csv(file_path, index=False,encoding='utf-8')
	else:
		index_basic = pd.read_csv(file_path) 
						
		
	index_codes = index_basic['ts_code'].unique()	
	
	#设立计数器和计时器，当1分钟内访问频率过快时，sleep一定时间
	start_time = datetime.datetime.now()
	rd_cnt=0
	# by 股票id 循环下载
	for code in index_codes:
		rd_cnt=rd_cnt+1
		sec_round=(datetime.datetime.now()-start_time).total_seconds()
		
		if sec_round<60 and rd_cnt<visit_per_min:
			print(f"Downloading data for {ts_table} - {code} 注：本数据表1分钟内已访问次数={rd_cnt}")
			try:
				if is_update_all==1:
					#A股日线行情
					if ts_table in ('index_daily'):
						df = pro.index_daily(ts_code=code)
					#大盘指数每日指标
					if ts_table in ('index_dailybasic'):
						df = pro.index_dailybasic(ts_code=code)
				else:
					#A股日线行情
					if ts_table in ('index_daily'):
						df = pro.index_daily(ts_code=code,start_date=start_date, end_date=end_date)
				file_path = os.path.join(file_dir, f"{ts_table}_{code}.csv")
				df.to_csv(file_path, index=False,encoding='utf-8')	
			except Exception as e:
				print(f"Error downloading data for index {code}: {e}")
				
		elif sec_round<60 and rd_cnt>=visit_per_min:
			sec_to_sleep=60-sec_round
			print(f"1分钟内访问频率过快,暂停{sec_to_sleep}秒")
			time.sleep(sec_to_sleep)  # 暂停 X 秒
			print("继续下载")
			
		elif sec_round>=60:
			print("重新计算访问频率")
			start_time = datetime.datetime.now()
			rd_cnt=0
						
	if is_update_all==1:
		if ts_table in ('index_daily'):
			os.system(f"copy /b {file_dir}\\*{ts_table}*.csv {data_dir}\\{ts_table}_tdr.csv")
		else:
			os.system(f"copy /b {file_dir}\\*{ts_table}*.csv {data_dir}\\{ts_table}.csv")
	else:
		if ts_table in ('index_daily'):
			os.system(f"copy /b {file_dir}\\*{ts_table}*.csv {data_dir}\\{ts_table}_tdr_{start_date}_{end_date}.csv")
		else:
			os.system(f"copy /b {file_dir}\\*{ts_table}*.csv {data_dir}\\{ts_table}_{start_date}_{end_date}.csv")
		
	os.system(f"del {file_dir}\\*{ts_table}*.csv")
	print(f"The Data Download task of {ts_table} finished")

# 构建ts_dwload_bytsid函数，用于从tushare by 股票id 批量下载各类原始数据
def ts_dwload_bytsid(ts_table,file_dir,is_update_all,start_date,end_date,is_tslst_src_online,mkt,visit_per_min):
	if mkt==2:# mkt=1 为H股市场
		# 获取所有H股的列表,并存放在变量stock_codes中
		file_path = os.path.join(data_dir, 'stock_hk_basic_info.csv') 
		if is_tslst_src_online==1:
			stock_basic_info = pro.hk_basic()
			stock_basic_info.to_csv(file_path, index=False,encoding='utf-8')
		else:
			stock_basic_info = pd.read_csv(file_path) 
	elif mkt==3:# mkt=3 为美股市场
		# 获取所有H股的列表,并存放在变量stock_codes中
		file_path = os.path.join(data_dir, 'stock_us_basic_info.csv') 
		if is_tslst_src_online==1:
			stock_basic_info = pro.us_basic()
			stock_basic_info.to_csv(file_path, index=False,encoding='utf-8')
		else:
			stock_basic_info = pd.read_csv(file_path) 
	elif mkt==4:# mkt=4 为公募基金
		# 获取所有公募基金的列表,并存放在变量stock_codes中
		file_path = os.path.join(data_dir, 'fund_basic_info.csv') 
		if is_tslst_src_online==1:
			stock_basic_info = pro.fund_basic(market='E')
			stock_basic_info.to_csv(file_path, index=False,encoding='utf-8')
		else:
			stock_basic_info = pd.read_csv(file_path) 
	else:# mkt=1或空 为A股市场
		# 获取所有A股的列表,并存放在变量stock_codes中
		file_path = os.path.join(data_dir, 'stock_cn_basic_info.csv') 
		if is_tslst_src_online==1:
			stock_basic_info = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
			stock_basic_info.to_csv(file_path, index=False,encoding='utf-8')
		else:
			stock_basic_info = pd.read_csv(file_path) 
						
		
	stock_codes = stock_basic_info['ts_code'].unique()	
	
	#设立计数器和计时器，当1分钟内访问频率过快时，sleep一定时间
	start_time = datetime.datetime.now()
	rd_cnt=0
	# by 股票id 循环下载
	for code in stock_codes:
		rd_cnt=rd_cnt+1
		sec_round=(datetime.datetime.now()-start_time).total_seconds()
		
		if sec_round<60 and rd_cnt<visit_per_min:
			print(f"Downloading data for {ts_table} - {code} 注：本数据表1分钟内已访问次数={rd_cnt}")
			try:
				if is_update_all==1:
					#A股日线行情
					if ts_table in ('daily'):
						df = pro.daily(ts_code=code)
					#每日指标
					if ts_table in ('daily_basic'):
						df = pro.daily_basic(ts_code=code)
					#复权因子
					if ts_table in ('adj_factor'):
						df = pro.adj_factor(ts_code=code)
					#个股资金流向
					if ts_table in ('moneyflow'):
						df = pro.moneyflow(ts_code=code)
					#每日涨跌停价格
					if ts_table in ('stk_limit'):
						df = pro.stk_limit(ts_code=code)
					#财务审计意见
					if ts_table in ('fina_audit'):
						df = pro.fina_audit(ts_code=code)
					#财务指标数据
					if ts_table in ('fina_indicator'):
						df = pro.fina_indicator(ts_code=code,fields='ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,capital_rese_ps,surplus_rese_ps,undist_profit_ps,extra_item,profit_dedt,gross_margin,current_ratio,quick_ratio,cash_ratio,invturn_days,arturn_days,inv_turn,ar_turn,ca_turn,fa_turn,assets_turn,op_income,valuechange_income,interst_income,daa,ebit,ebitda,fcff,fcfe,current_exint,noncurrent_exint,interestdebt,netdebt,tangible_asset,working_capital,networking_capital,invest_capital,retained_earnings,diluted2_eps,bps,ocfps,retainedps,cfps,ebit_ps,fcff_ps,fcfe_ps,netprofit_margin,grossprofit_margin,cogs_of_sales,expense_of_sales,profit_to_gr,saleexp_to_gr,adminexp_of_gr,finaexp_of_gr,impai_ttm,gc_of_gr,op_of_gr,ebit_of_gr,roe,roe_waa,roe_dt,roa,npta,roic,roe_yearly,roa2_yearly,roe_avg,opincome_of_ebt,investincome_of_ebt,n_op_profit_of_ebt,tax_to_ebt,dtprofit_to_profit,salescash_to_or,ocf_to_or,ocf_to_opincome,capitalized_to_da,debt_to_assets,assets_to_eqt,dp_assets_to_eqt,ca_to_assets,nca_to_assets,tbassets_to_totalassets,int_to_talcap,eqt_to_talcapital,currentdebt_to_debt,longdeb_to_debt,ocf_to_shortdebt,debt_to_eqt,eqt_to_debt,eqt_to_interestdebt,tangibleasset_to_debt,tangasset_to_intdebt,tangibleasset_to_netdebt,ocf_to_debt,ocf_to_interestdebt,ocf_to_netdebt,ebit_to_interest,longdebt_to_workingcapital,ebitda_to_debt,turn_days,roa_yearly,roa_dp,fixed_assets,profit_prefin_exp,non_op_profit,op_to_ebt,nop_to_ebt,ocf_to_profit,cash_to_liqdebt,cash_to_liqdebt_withinterest,op_to_liqdebt,op_to_debt,roic_yearly,total_fa_trun,profit_to_op,q_opincome,q_investincome,q_dtprofit,q_eps,q_netprofit_margin,q_gsprofit_margin,q_exp_to_sales,q_profit_to_gr,q_saleexp_to_gr,q_adminexp_to_gr,q_finaexp_to_gr,q_impair_to_gr_ttm,q_gc_to_gr,q_op_to_gr,q_roe,q_dt_roe,q_npta,q_opincome_to_ebt,q_investincome_to_ebt,q_dtprofit_to_profit,q_salescash_to_or,q_ocf_to_sales,q_ocf_to_or,basic_eps_yoy,dt_eps_yoy,cfps_yoy,op_yoy,ebt_yoy,netprofit_yoy,dt_netprofit_yoy,ocf_yoy,roe_yoy,bps_yoy,assets_yoy,eqt_yoy,tr_yoy,or_yoy,q_gr_yoy,q_gr_qoq,q_sales_yoy,q_sales_qoq,q_op_yoy,q_op_qoq,q_profit_yoy,q_profit_qoq,q_netprofit_yoy,q_netprofit_qoq,equity_yoy,rd_exp,update_flag')
					#利润表
					if ts_table in ('income'):
						df = pro.income(ts_code=code)
					#资产负债表
					if ts_table in ('balancesheet'):
						df = pro.balancesheet(ts_code=code)
					#现金流量表
					if ts_table in ('cashflow'):
						df = pro.cashflow(ts_code=code)
					#业绩预告
					if ts_table in ('forecast'):
						df = pro.forecast(ts_code=code)
					#业绩快报
					if ts_table in ('express'):
						df = pro.express(ts_code=code)
					#分红送股
					if ts_table in ('dividend'):
						df = pro.dividend(ts_code=code)
					#主营业务构成
					if ts_table in ('fina_mainbz'):
						df = pro.fina_mainbz(ts_code=code)
					#卖方盈利预测数据
					if ts_table in ('report_rc'):
						df = pro.report_rc(ts_code=code)
					#每日筹码及胜率 单次最大5000条，可以分页或者循环提取
					if ts_table in ('cyq_perf'):
						df = pro.cyq_perf(ts_code=code)
					#每日筹码分布,限量：单次最大2000条，可以按股票代码和日期循环提取
					if ts_table in ('cyq_chips'):
						df = pro.cyq_chips(ts_code=code)
					#股票技术因子（量化因子）
					if ts_table in ('stk_factor'):
						df = pro.stk_factor(ts_code=code)
					#股票技术面因子(专业版)
					if ts_table in ('stk_factor_pro'):
						df = pro.stk_factor_pro(ts_code=code)
					#中央结算系统持股汇总
					if ts_table in ('ccass_hold'):
						df = pro.ccass_hold(ts_code=code)
					#中央结算系统持股明细
					if ts_table in ('ccass_hold_detail'):
						df = pro.ccass_hold_detail(ts_code=code)
					#公募基金持仓数据
					if ts_table in ('fund_portfolio'):
						df = pro.fund_portfolio(ts_code=code)
					#沪深港股通持股明细
					if ts_table in ('hk_hold'):
						df = pro.hk_hold(ts_code=code)
					#港股行情
					if ts_table in ('hk_daily'):
						df = pro.hk_daily(ts_code=code)
					#港股分钟行情 限量：单次最大8000行数据，可以通过股票代码和日期循环获取
					if ts_table in ('hk_mins'):
						df = pro.hk_mins(ts_code=code)
					#美股行情 限量：单次最大6000行数据，可根据日期参数循环提取，开通正式权限后也可支持分页提取全部历史
					if ts_table in ('us_daily'):
						df = pro.us_daily(ts_code=code)
				else:
					#A股日线行情
					if ts_table in ('daily'):
						df = pro.daily(ts_code=code,start_date=start_date, end_date=end_date)
					#每日指标
					if ts_table in ('daily_basic'):
						df = pro.daily_basic(ts_code=code,start_date=start_date, end_date=end_date)
					#复权因子
					if ts_table in ('adj_factor'):
						df = pro.adj_factor(ts_code=code,start_date=start_date, end_date=end_date)
					#个股资金流向
					if ts_table in ('moneyflow'):
						df = pro.moneyflow(ts_code=code,start_date=start_date, end_date=end_date)
					#每日涨跌停价格
					if ts_table in ('stk_limit'):
						df = pro.stk_limit(ts_code=code,start_date=start_date, end_date=end_date)
					#财务审计意见
					if ts_table in ('fina_audit'):
						df = pro.fina_audit(ts_code=code,start_date=start_date, end_date=end_date)
					#财务指标数据
					if ts_table in ('fina_indicator'):
						df = pro.fina_indicator(ts_code=code,fields='ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,capital_rese_ps,surplus_rese_ps,undist_profit_ps,extra_item,profit_dedt,gross_margin,current_ratio,quick_ratio,cash_ratio,invturn_days,arturn_days,inv_turn,ar_turn,ca_turn,fa_turn,assets_turn,op_income,valuechange_income,interst_income,daa,ebit,ebitda,fcff,fcfe,current_exint,noncurrent_exint,interestdebt,netdebt,tangible_asset,working_capital,networking_capital,invest_capital,retained_earnings,diluted2_eps,bps,ocfps,retainedps,cfps,ebit_ps,fcff_ps,fcfe_ps,netprofit_margin,grossprofit_margin,cogs_of_sales,expense_of_sales,profit_to_gr,saleexp_to_gr,adminexp_of_gr,finaexp_of_gr,impai_ttm,gc_of_gr,op_of_gr,ebit_of_gr,roe,roe_waa,roe_dt,roa,npta,roic,roe_yearly,roa2_yearly,roe_avg,opincome_of_ebt,investincome_of_ebt,n_op_profit_of_ebt,tax_to_ebt,dtprofit_to_profit,salescash_to_or,ocf_to_or,ocf_to_opincome,capitalized_to_da,debt_to_assets,assets_to_eqt,dp_assets_to_eqt,ca_to_assets,nca_to_assets,tbassets_to_totalassets,int_to_talcap,eqt_to_talcapital,currentdebt_to_debt,longdeb_to_debt,ocf_to_shortdebt,debt_to_eqt,eqt_to_debt,eqt_to_interestdebt,tangibleasset_to_debt,tangasset_to_intdebt,tangibleasset_to_netdebt,ocf_to_debt,ocf_to_interestdebt,ocf_to_netdebt,ebit_to_interest,longdebt_to_workingcapital,ebitda_to_debt,turn_days,roa_yearly,roa_dp,fixed_assets,profit_prefin_exp,non_op_profit,op_to_ebt,nop_to_ebt,ocf_to_profit,cash_to_liqdebt,cash_to_liqdebt_withinterest,op_to_liqdebt,op_to_debt,roic_yearly,total_fa_trun,profit_to_op,q_opincome,q_investincome,q_dtprofit,q_eps,q_netprofit_margin,q_gsprofit_margin,q_exp_to_sales,q_profit_to_gr,q_saleexp_to_gr,q_adminexp_to_gr,q_finaexp_to_gr,q_impair_to_gr_ttm,q_gc_to_gr,q_op_to_gr,q_roe,q_dt_roe,q_npta,q_opincome_to_ebt,q_investincome_to_ebt,q_dtprofit_to_profit,q_salescash_to_or,q_ocf_to_sales,q_ocf_to_or,basic_eps_yoy,dt_eps_yoy,cfps_yoy,op_yoy,ebt_yoy,netprofit_yoy,dt_netprofit_yoy,ocf_yoy,roe_yoy,bps_yoy,assets_yoy,eqt_yoy,tr_yoy,or_yoy,q_gr_yoy,q_gr_qoq,q_sales_yoy,q_sales_qoq,q_op_yoy,q_op_qoq,q_profit_yoy,q_profit_qoq,q_netprofit_yoy,q_netprofit_qoq,equity_yoy,rd_exp,update_flag',start_date=start_date, end_date=end_date)
					#利润表
					if ts_table in ('income'):
						df = pro.income(ts_code=code,start_date=start_date, end_date=end_date)
					#资产负债表
					if ts_table in ('balancesheet'):
						df = pro.balancesheet(ts_code=code,start_date=start_date, end_date=end_date)
					#现金流量表
					if ts_table in ('cashflow'):
						df = pro.cashflow(ts_code=code,start_date=start_date, end_date=end_date)
					#业绩预告
					if ts_table in ('forecast'):
						df = pro.forecast(ts_code=code,start_date=start_date, end_date=end_date)
					#业绩快报
					if ts_table in ('express'):
						df = pro.express(ts_code=code,start_date=start_date, end_date=end_date)
					#分红送股
					if ts_table in ('dividend'):
						df = pro.dividend(ts_code=code,start_date=start_date, end_date=end_date)
					#主营业务构成
					if ts_table in ('fina_mainbz'):
						df = pro.fina_mainbz(ts_code=code,start_date=start_date, end_date=end_date)
					#卖方盈利预测数据
					if ts_table in ('report_rc'):
						df = pro.report_rc(ts_code=code,start_date=start_date, end_date=end_date)
					#每日筹码及胜率 单次最大5000条，可以分页或者循环提取
					if ts_table in ('cyq_perf'):
						df = pro.cyq_perf(ts_code=code,start_date=start_date, end_date=end_date)
					#每日筹码分布,限量：单次最大2000条，可以按股票代码和日期循环提取
					if ts_table in ('cyq_chips'):
						df = pro.cyq_chips(ts_code=code,start_date=start_date, end_date=end_date)
					#股票技术因子（量化因子）
					if ts_table in ('stk_factor'):
						df = pro.stk_factor(ts_code=code,start_date=start_date, end_date=end_date)
					#股票技术面因子(专业版)
					if ts_table in ('stk_factor_pro'):
						df = pro.stk_factor_pro(ts_code=code,start_date=start_date, end_date=end_date)	
					#中央结算系统持股汇总
					if ts_table in ('ccass_hold'):
						df = pro.ccass_hold(ts_code=code,start_date=start_date, end_date=end_date)
					#中央结算系统持股明细
					if ts_table in ('ccass_hold_detail'):
						df = pro.ccass_hold_detail(ts_code=code,start_date=start_date, end_date=end_date)
					#公募基金持仓数据
					if ts_table in ('fund_portfolio'):
						df = pro.fund_portfolio(ts_code=code,start_date=start_date, end_date=end_date)
					#沪深港股通持股明细
					if ts_table in ('hk_hold'):
						df = pro.hk_hold(ts_code=code,start_date=start_date, end_date=end_date)					
					#港股行情
					if ts_table in ('hk_daily'):
						df = pro.hk_daily(ts_code=code,start_date=start_date, end_date=end_date)
					#港股分钟行情 限量：单次最大8000行数据，可以通过股票代码和日期循环获取
					if ts_table in ('hk_mins'):
						df = pro.hk_mins(ts_code=code,start_date=start_date, end_date=end_date)
					#美股行情 限量：单次最大6000行数据，可根据日期参数循环提取，开通正式权限后也可支持分页提取全部历史
					if ts_table in ('us_daily'):
						df = pro.us_daily(ts_code=code,start_date=start_date, end_date=end_date)
					
				file_path = os.path.join(file_dir, f"{ts_table}_{code}.csv")
				df.to_csv(file_path, index=False,encoding='utf-8')
					
			except Exception as e:
				print(f"Error downloading data for stock {code}: {e}")
				
		elif sec_round<60 and rd_cnt>=visit_per_min:
			sec_to_sleep=60-sec_round
			print(f"1分钟内访问频率过快,暂停{sec_to_sleep}秒")
			time.sleep(sec_to_sleep)  # 暂停 X 秒
			print("继续下载")
			
		elif sec_round>=60:
			print("重新计算访问频率")
			start_time = datetime.datetime.now()
			rd_cnt=0
						
	if is_update_all==1:
		os.system(f"copy /b {file_dir}\\*{ts_table}*.csv {data_dir}\\{ts_table}.csv")
	else:
		os.system(f"copy /b {file_dir}\\*{ts_table}*.csv {data_dir}\\{ts_table}_{start_date}_{end_date}.csv")
		
	os.system(f"del {file_dir}\\*{ts_table}*.csv")
	print(f"The Data Download task of {ts_table} finished")

# 构建ts_dwload_bydate函数，用于by trade_date下载数据， 如龙虎榜数据
def  ts_dwload_bydate(ts_table,file_dir,start_date,end_date,visit_per_min):
	# 获取所有A股交易日的列表,并存放在变量trade_date_lst中
	df=pro.trade_cal(exchange='', start_date=start_date, end_date=end_date,is_open='1',fields='cal_date')			
	trade_date_lst = df['cal_date'].unique()	
	
	#设立计数器和计时器，当1分钟内访问频率过快时，sleep一定时间
	start_time = datetime.datetime.now()
	rd_cnt=0
	# by trade_date 循环下载
	for tdate in trade_date_lst:
		rd_cnt=rd_cnt+1
		sec_round=(datetime.datetime.now()-start_time).total_seconds()
		if sec_round<60 and rd_cnt<visit_per_min:
			print(f"Downloading data for {ts_table} - {tdate} 注：本数据表1分钟内已访问次数={rd_cnt}")
			try:
				#龙虎榜每日明细
				if ts_table in ('top_list'):
					df = pro.top_list(trade_date=tdate)
				#龙虎榜机构明细
				if ts_table in ('top_inst'):
					df = pro.top_inst(trade_date=tdate)
				file_path = os.path.join(file_dir, f"{ts_table}_{tdate}.csv")
				df.to_csv(file_path, index=False,encoding='utf-8')	
			except Exception as e:
				print(f"Error downloading data for stock {tdate}: {e}")
		elif sec_round<60 and rd_cnt>=visit_per_min:
			sec_to_sleep=60-sec_round
			print(f"1分钟内访问频率过快,暂停{sec_to_sleep}秒")
			time.sleep(sec_to_sleep)  # 暂停 X 秒
			print("继续下载")
		elif sec_round>=60:
			print("重新计算访问频率")
			start_time = datetime.datetime.now()
			rd_cnt=0
	# 循环下载完成后的文件合并及清扫处理
	os.system(f"copy /b {file_dir}\\*{ts_table}*.csv {data_dir}\\{ts_table}_{start_date}_{end_date}.csv")
	os.system(f"del {file_dir}\\*{ts_table}*.csv")
	print(f"The Data Download task of {ts_table}_{start_date}_{end_date} finished")

		
# 构建ts_dwload_bytsid_and_months函数，用于同时按照ts_id及时间段来下载数据， 如筹码分布数据
def  ts_dwload_bytsid_and_months(ts_table,file_dir,start_date,end_date,is_tslst_src_online,days_str,visit_per_min):
	# 获取所有A股的列表,并存放在变量stock_codes中
	file_path = os.path.join(data_dir, 'stock_cn_basic_info.csv') 
	if is_tslst_src_online==1:
		stock_basic_info = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
		stock_basic_info.to_csv(file_path, index=False,encoding='utf-8')
	else:
		stock_basic_info = pd.read_csv(file_path)
	stock_codes = stock_basic_info['ts_code'].unique()
	
	# 将指定起止日期的时段按月划分为多个时段，并装载在date_tuples二元组中
	date_tuples = split_dates_by_month(start_date_str=start_date,end_date_str=end_date,days_str=days_str)
	
	#在开始循环前设立计数器和计时器，当1分钟内访问频率过快时，sleep一定时间
	start_time = datetime.datetime.now()
	rd_cnt=0
	
	# by code 循环下载
	for s_date,e_date in date_tuples:
		for code in stock_codes:
			rd_cnt=rd_cnt+1
			sec_round=(datetime.datetime.now()-start_time).total_seconds()
			
			# 建立访问频率限制框架
			if sec_round<60 and rd_cnt<visit_per_min:
				print(f"Downloading data for {ts_table}_{code}_{s_date}_{e_date}, 注：本数据表1分钟内已访问次数={rd_cnt}")
				try:
					#每日筹码分布
					if ts_table in ('cyq_chips'):
						df = pro.cyq_chips(ts_code=code, start_date=s_date, end_date=e_date)
					#每日筹码及胜率
					if ts_table in ('cyq_perf'):
						df = pro.cyq_perf(ts_code=code, start_date=s_date, end_date=e_date)
					#股票技术面因子(专业版)
					if ts_table in ('stk_factor_pro'):
						df = pro.stk_factor_pro(ts_code=code, start_date=s_date, end_date=e_date)
					#股票技术因子（量化因子）
					if ts_table in ('stk_factor'):
						df = pro.stk_factor(ts_code=code, start_date=s_date, end_date=e_date)
					#A股日线行情
					if ts_table in ('daily'):
						df = pro.daily(ts_code=code,start_date=s_date, end_date=e_date)
					#每日指标
					if ts_table in ('daily_basic'):
						df = pro.daily_basic(ts_code=code,start_date=s_date, end_date=e_date)
					#复权因子
					if ts_table in ('adj_factor'):
						df = pro.adj_factor(ts_code=code,start_date=s_date, end_date=e_date)
					#个股资金流向
					if ts_table in ('moneyflow'):
						df = pro.moneyflow(ts_code=code,start_date=s_date, end_date=e_date)
					#每日涨跌停价格
					if ts_table in ('stk_limit'):
						df = pro.stk_limit(ts_code=code,start_date=s_date, end_date=e_date)
					#财务审计意见
					if ts_table in ('fina_audit'):
						df = pro.fina_audit(ts_code=code,start_date=s_date, end_date=e_date)
					#财务指标数据
					if ts_table in ('fina_indicator'):
						df = pro.fina_indicator(ts_code=code,fields='ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,capital_rese_ps,surplus_rese_ps,undist_profit_ps,extra_item,profit_dedt,gross_margin,current_ratio,quick_ratio,cash_ratio,invturn_days,arturn_days,inv_turn,ar_turn,ca_turn,fa_turn,assets_turn,op_income,valuechange_income,interst_income,daa,ebit,ebitda,fcff,fcfe,current_exint,noncurrent_exint,interestdebt,netdebt,tangible_asset,working_capital,networking_capital,invest_capital,retained_earnings,diluted2_eps,bps,ocfps,retainedps,cfps,ebit_ps,fcff_ps,fcfe_ps,netprofit_margin,grossprofit_margin,cogs_of_sales,expense_of_sales,profit_to_gr,saleexp_to_gr,adminexp_of_gr,finaexp_of_gr,impai_ttm,gc_of_gr,op_of_gr,ebit_of_gr,roe,roe_waa,roe_dt,roa,npta,roic,roe_yearly,roa2_yearly,roe_avg,opincome_of_ebt,investincome_of_ebt,n_op_profit_of_ebt,tax_to_ebt,dtprofit_to_profit,salescash_to_or,ocf_to_or,ocf_to_opincome,capitalized_to_da,debt_to_assets,assets_to_eqt,dp_assets_to_eqt,ca_to_assets,nca_to_assets,tbassets_to_totalassets,int_to_talcap,eqt_to_talcapital,currentdebt_to_debt,longdeb_to_debt,ocf_to_shortdebt,debt_to_eqt,eqt_to_debt,eqt_to_interestdebt,tangibleasset_to_debt,tangasset_to_intdebt,tangibleasset_to_netdebt,ocf_to_debt,ocf_to_interestdebt,ocf_to_netdebt,ebit_to_interest,longdebt_to_workingcapital,ebitda_to_debt,turn_days,roa_yearly,roa_dp,fixed_assets,profit_prefin_exp,non_op_profit,op_to_ebt,nop_to_ebt,ocf_to_profit,cash_to_liqdebt,cash_to_liqdebt_withinterest,op_to_liqdebt,op_to_debt,roic_yearly,total_fa_trun,profit_to_op,q_opincome,q_investincome,q_dtprofit,q_eps,q_netprofit_margin,q_gsprofit_margin,q_exp_to_sales,q_profit_to_gr,q_saleexp_to_gr,q_adminexp_to_gr,q_finaexp_to_gr,q_impair_to_gr_ttm,q_gc_to_gr,q_op_to_gr,q_roe,q_dt_roe,q_npta,q_opincome_to_ebt,q_investincome_to_ebt,q_dtprofit_to_profit,q_salescash_to_or,q_ocf_to_sales,q_ocf_to_or,basic_eps_yoy,dt_eps_yoy,cfps_yoy,op_yoy,ebt_yoy,netprofit_yoy,dt_netprofit_yoy,ocf_yoy,roe_yoy,bps_yoy,assets_yoy,eqt_yoy,tr_yoy,or_yoy,q_gr_yoy,q_gr_qoq,q_sales_yoy,q_sales_qoq,q_op_yoy,q_op_qoq,q_profit_yoy,q_profit_qoq,q_netprofit_yoy,q_netprofit_qoq,equity_yoy,rd_exp,update_flag',start_date=s_date, end_date=e_date)
					#利润表
					if ts_table in ('income'):
						df = pro.income(ts_code=code,start_date=s_date, end_date=e_date)
					#资产负债表
					if ts_table in ('balancesheet'):
						df = pro.balancesheet(ts_code=code,start_date=s_date, end_date=e_date)
					#现金流量表
					if ts_table in ('cashflow'):
						df = pro.cashflow(ts_code=code,start_date=s_date, end_date=e_date)
					#业绩预告
					if ts_table in ('forecast'):
						df = pro.forecast(ts_code=code,start_date=s_date, end_date=e_date)
					#业绩快报
					if ts_table in ('express'):
						df = pro.express(ts_code=code,start_date=s_date, end_date=e_date)
					#分红送股
					if ts_table in ('dividend'):
						df = pro.dividend(ts_code=code,start_date=s_date, end_date=e_date)
					#主营业务构成
					if ts_table in ('fina_mainbz'):
						df = pro.fina_mainbz(ts_code=code,start_date=s_date, end_date=e_date)
					#卖方盈利预测数据
					if ts_table in ('report_rc'):
						df = pro.report_rc(ts_code=code,start_date=s_date, end_date=e_date)
					#每日筹码及胜率 单次最大5000条，可以分页或者循环提取
					if ts_table in ('cyq_perf'):
						df = pro.cyq_perf(ts_code=code,start_date=s_date, end_date=e_date)
					#每日筹码分布,限量：单次最大2000条，可以按股票代码和日期循环提取
					if ts_table in ('cyq_chips'):
						df = pro.cyq_chips(ts_code=code,start_date=s_date, end_date=e_date)
					#股票技术因子（量化因子）
					if ts_table in ('stk_factor'):
						df = pro.stk_factor(ts_code=code,start_date=s_date, end_date=e_date)
					#股票技术面因子(专业版)
					if ts_table in ('stk_factor_pro'):
						df = pro.stk_factor_pro(ts_code=code,start_date=s_date, end_date=e_date)	
					#中央结算系统持股汇总
					if ts_table in ('ccass_hold'):
						df = pro.ccass_hold(ts_code=code,start_date=s_date, end_date=e_date)
					#中央结算系统持股明细
					if ts_table in ('ccass_hold_detail'):
						df = pro.ccass_hold_detail(ts_code=code,start_date=s_date, end_date=e_date)
					#公募基金持仓数据
					if ts_table in ('fund_portfolio'):
						df = pro.fund_portfolio(ts_code=code,start_date=s_date, end_date=e_date)
					#沪深港股通持股明细
					if ts_table in ('hk_hold'):
						df = pro.hk_hold(ts_code=code,start_date=s_date, end_date=e_date)	
					file_path = os.path.join(file_dir, f"{ts_table}_{code}_{s_date}_{e_date}.csv")
					df.to_csv(file_path, index=False,encoding='utf-8')	
				except Exception as e:
					print(f"Error downloading data for {ts_table}_{code}_{s_date}_{e_date}: {e}")
			elif sec_round<60 and rd_cnt>=visit_per_min:
				sec_to_sleep=60-sec_round
				print(f"1分钟内访问频率过快,暂停{sec_to_sleep}秒")
				time.sleep(sec_to_sleep)  # 暂停 X 秒
				print("继续下载")
			elif sec_round>=60:
				print("重新计算访问频率")
				start_time = datetime.datetime.now()
				rd_cnt=0
	
	# 循环下载完成后的文件合并及清扫处理
	os.system(f"copy /b {file_dir}\\*{ts_table}*.csv {data_dir}\\{ts_table}_{start_date}_{end_date}.csv")
	os.system(f"del {file_dir}\\*{ts_table}*.csv")
	print(f"The Data Download task of {ts_table}_{start_date}_{end_date} finished")
		
	
"""
ts_dwload_daily(ts_table='daily',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)

ts_dwload_bytsid(ts_table='daily_basic',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='moneyflow',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='stk_limit',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='adj_factor',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='fina_audit',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='fina_indicator',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='income',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='balancesheet',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='cashflow',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='forecast',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='express',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='dividend',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='report_rc',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='ccass_hold_detail',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='ccass_hold',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='stk_factor',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='stk_factor_pro',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='hk_hold',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='fund_portfolio',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=1,mkt=4,visit_per_min=200)


ts_dwload_index_daily(ts_table='index_daily',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=1,visit_per_min=400)
ts_dwload_index_daily(ts_table='index_dailybasic',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=1,visit_per_min=400)

ts_dwload_bydate(ts_table='top_list',file_dir=data_dir_daily,start_date='20000101',end_date='20240531',visit_per_min=400)
ts_dwload_bydate(ts_table='top_inst',file_dir=data_dir_daily,start_date='20000101',end_date='20240531',visit_per_min=400)

ts_dwload_bytsid_and_months(ts_table='cyq_chips',file_dir=data_dir_daily,start_date='20100101'end_date='20240603',is_tslst_src_online=1,days_str=92,visit_per_min=200)
ts_dwload_bytsid_and_months(ts_table='cyq_perf',file_dir=data_dir_daily,start_date='20100101',end_date='20240603',is_tslst_src_online=1,days_str=392,visit_per_min=200)
ts_dwload_bytsid_and_months(ts_table='stk_factor_pro',file_dir=data_dir_daily,start_date='20100101',end_date='20240603',is_tslst_src_online=1,days_str=1832,visit_per_min=30)
ts_dwload_bytsid_and_months(ts_table='stk_factor',file_dir=data_dir_index,start_date='20100101',end_date='20240603',is_tslst_src_online=1,days_str=1832,visit_per_min=30)
ts_dwload_bytsid_and_months(ts_table='daily',file_dir=data_dir_daily,start_date='19980101',end_date='20240603',is_tslst_src_online=1,days_str=7202,visit_per_min=400)
"""