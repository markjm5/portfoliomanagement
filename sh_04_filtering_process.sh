#!/bin/bash

echo $(date) 
echo "**********************************************"
echo "* Started 024_Filtering_Process_ETF_Universe *"
echo "**********************************************"
python ~/Documents/PythonProjects/PortfolioManagement/024_Filtering_Process_ETF_Universe.py

#echo $(date) 
#echo "********************************************"
#echo "* Started 030_Filtering_Process_US_Stocks *"
#echo "********************************************"
python ~/Documents/PythonProjects/PortfolioManagement/030_Filtering_Process_US_Stocks.py

echo $(date) 
echo "***************************************************"
echo "* Started 031_Filtering_Idea_Generation_Bottom_Up *"
echo "***************************************************"
python ~/Documents/PythonProjects/PortfolioManagement/031_Filtering_Process_Idea_Generation_Bottom_Up.py

echo $(date) 
echo "**************************************"
echo "* Started 039_Package_And_Distribute *"
echo "**************************************"
python ~/Documents/PythonProjects/PortfolioManagement/039_Package_And_Distribute.py