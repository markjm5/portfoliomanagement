#!/bin/bash

SECONDS=0
source ~/Documents/PythonProjects/PortfolioManagement/venv/bin/activate  
bash ~/Documents/PythonProjects/PortfolioManagement/sh_01_lagging_coincident_indicators.sh
bash ~/Documents/PythonProjects/PortfolioManagement/sh_02_central_banks_interest_rates.sh
bash ~/Documents/PythonProjects/PortfolioManagement/sh_03_leading_indicators.sh
bash ~/Documents/PythonProjects/PortfolioManagement/sh_04_filtering_process.sh

duration=$SECONDS
echo "Total Time: $(($duration / 60)) minutes and $(($duration % 60)) seconds"