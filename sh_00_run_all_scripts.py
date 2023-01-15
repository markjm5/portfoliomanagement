import os
from datetime import datetime


def run_script(file_name):
    print("=================================================================================")
    print("* Started %s *" % (file_name))
    print("=================================================================================")

    os.system("python %s" % (file_name))

now_start = datetime.now()
start_time = now_start.strftime("%H:%M:%S")

run_script("001_Lagging_Indicator_YoY_Asset_Class_Performance.py")
run_script("002_Lagging_Indicator_US_GDP.py")
run_script("003_Lagging_Indicator_World_GDP.py")
run_script("005_Lagging_Indicator_Job_Market_US_World.py")
run_script("006_Lagging_Indicator_PCE.py")
run_script("007_Lagging_Indicator_US_Inflation.py")
run_script("008_Lagging_Indicator_Global_Inflation.py")
run_script("009_Lagging_Indicator_US_Industrial_Production.py")
run_script("010_Lagging_Indicator_World_Industrial_Production.py")
run_script("011_Lagging_Indicator_US_Durable_Goods.py")
run_script("011_Lagging_Indicator_US_Retail_Sales.py")
run_script("012_Central_Banks.py")
run_script("013_Interest_Rates.py")
run_script("013_Yield_Curve.py")
run_script("014_US_Global_Money_Supply.py")
run_script("014_FX_Commitment_of_Traders.py")
run_script("015_Leading_Indicator_US_LEI_Consumer_Confidence.py")
run_script("016_Leading_Indicator_US_ISM_Manufacturing.py")
run_script("017_Leading_Indicator_US_ISM_Services.py")
run_script("018_Leading_Indicator_PMI_Manufacturing_World.py")
run_script("019_Leading_Indicator_PMI_Services_World.py")
run_script("020_Leading_Indicator_US_Housing_Market.py")
run_script("022_Global_Macro_Data_Summary_Page.py")
run_script("024_Filtering_Process_ETF_Universe.py")
run_script("030_Filtering_Process_US_Stocks.py")
run_script("031_Filtering_Process_Idea_Generation_Bottom_Up.py")

now_finish = datetime.now()
finish_time = now_finish.strftime("%H:%M:%S")

difference = now_finish - now_start

seconds_in_day = 24 * 60 * 60

print("Start Time = %s" % (start_time))
print("End Time = %s" % (finish_time))
print(divmod(difference.days * seconds_in_day + difference.seconds, 60))
