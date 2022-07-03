#!/bin/bash

echo $(date) 
echo "**********************************************"
echo "* Started 024_Filtering_Process_ETF_Universe *"
echo "**********************************************"
python ~/Documents/PythonProjects/PortfolioManagement/024_Filtering_Process_ETF_Universe.py

echo $(date) 
echo "********************************************"
echo "* Started 030_Filtering_Process_US_Stocks *"
echo "********************************************"
python ~/Documents/PythonProjects/PortfolioManagement/030_Filtering_Process_US_Stocks.py