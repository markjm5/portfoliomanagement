#!/bin/bash

echo $(date) 
echo "************************************************************"
echo "* Started 015_Leading_Indicator_US_LEI_Consumer_Confidence *"
echo "************************************************************"
python 015_Leading_Indicator_US_LEI_Consumer_Confidence.py

echo $(date) 
echo "******************************************************"
echo "* Started 016_Leading_Indicator_US_ISM_Manufacturing *"
echo "******************************************************"
python 016_Leading_Indicator_US_ISM_Manufacturing.py

echo $(date) 
echo "*************************************************"
echo "* Started 017_Leading_Indicator_US_ISM_Services *"
echo "*************************************************"
python 017_Leading_Indicator_US_ISM_Services.py

echo $(date) 
echo "*********************************************************"
echo "* Started 018_Leading_Indicator_PMI_Manufacturing_World *"
echo "*********************************************************"
python 018_Leading_Indicator_PMI_Manufacturing_World.py

echo $(date) 
echo "****************************************************"
echo "* Started 019_Leading_Indicator_PMI_Services_World *"
echo "****************************************************"
python 019_Leading_Indicator_PMI_Services_World.py

echo $(date) 
echo "***************************************************"
echo "* Started 020_Leading_Indicator_US_Housing_Market *"
echo "***************************************************"
python 020_Leading_Indicator_US_Housing_Market.py

echo $(date) 
echo "**********************************************"
echo "* Started 022_Global_Macro_Data_Summary_Page *"
echo "**********************************************"
python 022_Global_Macro_Data_Summary_Page.py