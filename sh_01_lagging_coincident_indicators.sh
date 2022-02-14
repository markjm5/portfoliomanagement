#!/bin/bash

echo $(date) 
echo "****************************************"
echo "* Started 001_Lagging_Indicator_YoY_Asset_Class_Performance *"
echo "****************************************"
python 001_Lagging_Indicator_YoY_Asset_Class_Performance.py

echo $(date) 
echo "****************************************"
echo "* Started 002_Lagging_Indicator_US_GDP *"
echo "****************************************"
python 002_Lagging_Indicator_US_GDP.py

echo $(date) 
echo "*******************************************"
echo "* Started 003_Lagging_Indicator_World_GDP *"
echo "*******************************************"
python 003_Lagging_Indicator_World_GDP.py

echo $(date) 
echo "*****************************************************"
echo "* Started 005_Lagging_Indicator_Job_Market_US_World *"
echo "*****************************************************"
python 005_Lagging_Indicator_Job_Market_US_World.py

echo $(date) 
echo "*************************************"
echo "* Started 006_Lagging_Indicator_PCE *"
echo "*************************************"
python 006_Lagging_Indicator_PCE.py

echo $(date) 
echo "**********************************************"
echo "* Started 007_Lagging_Indicator_US_Inflation *"
echo "**********************************************"
python 007_Lagging_Indicator_US_Inflation.py

echo $(date) 
echo "**************************************************"
echo "* Started 008_Lagging_Indicator_Global_Inflation *"
echo "**************************************************"
python 008_Lagging_Indicator_Global_Inflation.py

echo $(date) 
echo "**********************************************************"
echo "* Started 009_Lagging_Indicator_US_Industrial_Production *"
echo "**********************************************************"
python 009_Lagging_Indicator_US_Industrial_Production.py

echo $(date) 
echo "*************************************************************"
echo "* Started 010_Lagging_Indicator_World_Industrial_Production *"
echo "*************************************************************"
python 010_Lagging_Indicator_World_Industrial_Production.py

echo $(date) 
echo "**********************************************"
echo "* Started 011_Lagging_Indicator_Retail_Sales *"
echo "**********************************************"
python 011_Lagging_Indicator_Retail_Sales.py

echo $(date) 
echo "***********************************************"
echo "* Started 011_Lagging_Indicator_Durable_Goods *"
echo "***********************************************"
python 011_Lagging_Indicator_Durable_Goods.py