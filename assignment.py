# -*- coding: utf-8 -*-
"""
Created on Thu Oct 08 22:08:27 2015

@author: richardmcaleavey
"""

import numpy as np
import math
import pandas as pd
import sqlite3



conn = sqlite3.connect('renewable.db')
c = conn.cursor()

locationquery = "SELECT * FROM location;"

df = pd.read_sql(locationquery, con=conn)
print df.head()