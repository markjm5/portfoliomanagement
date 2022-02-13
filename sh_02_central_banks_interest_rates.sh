#!/bin/bash

echo $(date) 
echo "*****************************"
echo "* Started 012_Central_Banks *"
echo "*****************************"
python 012_Central_Banks.py

echo $(date) 
echo "******************************"
echo "* Started 013_Interest_Rates *"
echo "******************************"
python 013_Interest_Rates.py

echo $(date) 
echo "***************************"
echo "* Started 013_Yield_Curve *"
echo "***************************"
python 013_Yield_Curve.py

echo $(date) 
echo "**************************************"
echo "* Started 014_US_Global_Money_Supply *"
echo "**************************************"
python 014_US_Global_Money_Supply.py