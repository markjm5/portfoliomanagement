#!/bin/bash

echo $(date) 
echo "************************************************************"
echo "* Started 015_Leading_Indicator_US_LEI_Consumer_Confidence *"
echo "************************************************************"
python ~/Documents/PythonProjects/PortfolioManagement/015_Leading_Indicator_US_LEI_Consumer_Confidence.py

echo $(date) 
echo "******************************************************"
echo "* Started 016_Leading_Indicator_US_ISM_Manufacturing *"
echo "******************************************************"
python ~/Documents/PythonProjects/PortfolioManagement/016_Leading_Indicator_US_ISM_Manufacturing.py

echo $(date) 
echo "*************************************************"
echo "* Started 017_Leading_Indicator_US_ISM_Services *"
echo "*************************************************"
python ~/Documents/PythonProjects/PortfolioManagement/017_Leading_Indicator_US_ISM_Services.py

echo $(date) 
echo "*********************************************************"
echo "* Started 018_Leading_Indicator_PMI_Manufacturing_World *"
echo "*********************************************************"
python ~/Documents/PythonProjects/PortfolioManagement/018_Leading_Indicator_PMI_Manufacturing_World.py

echo $(date) 
echo "****************************************************"
echo "* Started 019_Leading_Indicator_PMI_Services_World *"
echo "****************************************************"
python ~/Documents/PythonProjects/PortfolioManagement/019_Leading_Indicator_PMI_Services_World.py

echo $(date) 
echo "***************************************************"
echo "* Started 020_Leading_Indicator_US_Housing_Market *"
echo "***************************************************"
python ~/Documents/PythonProjects/PortfolioManagement/020_Leading_Indicator_US_Housing_Market.py

echo $(date) 
echo "**********************************************"
echo "* Started 022_Global_Macro_Data_Summary_Page *"
echo "**********************************************"
#python ~/Documents/PythonProjects/PortfolioManagement/022_Global_Macro_Data_Summary_Page.py