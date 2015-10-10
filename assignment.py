# -*- coding: utf-8 -*-
"""
Created on Thu Oct 08 22:08:27 2015

@author: richardmcaleavey
"""

import numpy as np
import math
import pandas as pd
import pandasql as ps
from pandasql import sqldf
import sqlite3

def pysqldf(q):
    return sqldf(q, globals())
    

conn = sqlite3.connect('renewable.db')
c = conn.cursor()

locationquery = "SELECT * FROM location;"
portsquery = "SELECT * FROM ports;"

df_location = pd.read_sql(locationquery, con=conn)
df_ports = pd.read_sql(portsquery, con=conn)

"""Adding identifiers"""
df_location.insert(loc=0, column='ID', value=(range(1, len(df_location) + 1))) 
df_ports.insert(loc=0, column='Port_ID', value=(range(1, len(df_ports) + 1)))


a = map(list, df_location.values)
b = map(list, df_location.values)

mapping_list = []

"""matching plant locations against feeder locations"""
for x, y in [(x,y) for x in a for y in b]:
    "removing values where source and destination are same"
    if x[0] == y[0]:
        continue
    mapping_list.append(x+y)

mapping_df = pd.DataFrame(mapping_list, columns=['ID',
                  'Plant_Longitude', 
                  'Plant_Latitude', 
                  'Plant_Production',
                  'Raw_Site_ID',
                  'Raw_Site_Longitude',
                  'Raw_Site_Latitude',
                  'Raw_Site_Production'])

"""adding column calulating distance between locations"""

mapping_df['distance'] = np.sqrt((mapping_df['Raw_Site_Longitude']-mapping_df['Plant_Longitude'])**2+(mapping_df['Raw_Site_Latitude']-mapping_df['Plant_Latitude'])**2)


"""adding column calculating payload transported"""

mapping_df['delivery_payload'] = mapping_df['distance']*mapping_df['Raw_Site_Production']

"""The objective is to find the site with the smallest overall payload. Two stages: transportation to the plant, and then to the port"""

sum_query = """SELECT ID, Plant_Longitude, Plant_Latitude, Plant_Production, sum(payload)  FROM totals_df GROUP BY ID"""

totals = pysqldf(sum_query)

f = map(list,totals.values)
h = map(list,df_ports.values)

port_mapping_list = []

"""matching plant locations against feeder locations"""
for x, y in [(x,y) for x in f for y in h]:
    port_mapping_list.append(x+y)

port_mapping_df = pd.DataFrame(port_mapping_list, columns=['ID',
                  'Plant_Longitude', 
                  'Plant_Latitude', 
                  'Plant_Production',
                  'Plant_Payload',
                  'Port_ID',
                  'Port_Longitude',
                  'Port_Latitude'])

"""The crucial information at this point is
 the Location_ID with the lowest overall delivery_payload""" 

port_mapping_df['distance'] = np.sqrt((port_mapping_df['Port_Longitude']-port_mapping_df['Plant_Longitude'])**2+(port_mapping_df['Port_Longitude']-port_mapping_df['Plant_Latitude'])**2)


"""Then we need to find the row that has the lowest payload since we will use this for the second stage"""
 
port_mapping_df['port_delivery_payload'] = port_mapping_df['distance']*port_mapping_df['Plant_Production']
port_mapping_df['total_payload'] = port_mapping_df['Plant_Payload']+port_mapping_df['port_delivery_payload']


port_mapping_df.insert(loc=0, column='Option_ID', value=(range(1, len(port_mapping_df) + 1)))

port_query = """SELECT Option_ID, ID, Plant_Longitude, Plant_Latitude, Port_ID, Port_Longitude, Port_Latitude, total_payload  FROM port_mapping_df ORDER BY total_payload ASC"""
final_list = pysqldf(port_query)
best_candidate = final_list.head(1)

print best_candidate
    
    


