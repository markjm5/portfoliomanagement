#!/bin/bash

SECONDS=0

bash sh_04_all.sh 2>&1 | tee output.txt

duration=$SECONDS
echo "Total Time: $(($duration / 60)) minutes and $(($duration % 60)) seconds"