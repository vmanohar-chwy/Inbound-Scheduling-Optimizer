# -*- coding: utf-8 -*-
"""
Created on Fri Nov 22 10:45:47 2019

@author: AA-VManohar
"""

from subprocess import call
import time
start = time.time()
call(["python", "ISO_EFC.py"])
time.sleep(10)
call(["python", "ISO_WFC.py"])
time.sleep(10)
call(["python", "ISO_PHX.py"])
time.sleep(10)
call(["python", "ISO_DFW.py"])
time.sleep(10)
call(["python", "csv_template.py"])
end = time.time()
execution = end-start
print(execution)