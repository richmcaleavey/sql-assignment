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
    
#opening database
conn = sqlite3.connect('renewable.db')
c = conn.cursor()

locationquery = "SELECT * FROM location;"
portsquery = "SELECT * FROM ports;"

location_df = pd.read_sql(locationquery, con=conn)
ports_df = pd.read_sql(portsquery, con=conn)

# Adding identifiers
location_df.insert(loc=0, column='ID', value=(range(1, len(location_df) + 1))) 
ports_df.insert(loc=0, column='Port_ID', value=(range(1, len(ports_df) + 1)))


# map potential processing plant sites against all other production sites
a = map(list, location_df.values)
b = map(list, location_df.values)

mapping_list = []
for x, y in [(x,y) for x in a for y in b]:
    # remove values where source and destination identifiers are identical
    if x[0] == y[0]:
        continue
    mapping_list.append(x+y)

# write list to dataframe
mapping_df = pd.DataFrame(mapping_list, columns=['ID',
                  'Plant_Latitude', 
                  'Plant_Longitude', 
                  'Plant_Production',
                  'Raw_Site_ID',
                  'Raw_Site_Latitude',
                  'Raw_Site_Longitude',
                  'Raw_Site_Payload_Distance'])


# add column calulating distance between locations using Pythagorean theorem
mapping_df['distance'] = np.sqrt((mapping_df['Raw_Site_Latitude']-mapping_df['Plant_Latitude'])**2+
(mapping_df['Raw_Site_Longitude']-mapping_df['Plant_Longitude'])**2)/1000

# add column calculating payload-distance of transportation between source and destination
mapping_df['delivery_payload'] = mapping_df['distance']*mapping_df['Raw_Site_Payload_Distance']

#The objective is to find the site with the smallest overall payload. 
sum_query = """SELECT ID, 
                Plant_Latitude, 
                Plant_Longitude, 
                Plant_Production, 
                sum(Raw_Site_Payload_Distance)  
                FROM mapping_df GROUP BY ID"""

totals = pysqldf(sum_query)


# match potential processor locations against port locations
f = map(list,totals.values)
h = map(list,ports_df.values)

port_mapping_list = []

for x, y in [(x,y) for x in f for y in h]:
    port_mapping_list.append(x+y)

port_mapping_df = pd.DataFrame(port_mapping_list, columns=['ID',
                  'Plant_Latitude', 
                  'Plant_Longitude', 
                  'Plant_Production',
                  'Plant_Payload',
                  'Port_ID',
                  'Port_Latitude',
                  'Port_Longitude'])

# calculate distance between potential processor locations and ports
port_mapping_df['distance'] = np.sqrt((port_mapping_df['Port_Latitude']-port_mapping_df['Plant_Latitude'])**2+
(port_mapping_df['Port_Longitude']-port_mapping_df['Plant_Longitude'])**2)/1000


# Calculate distance-payload of   1) processor site's output to reach port, 
#                                 2) processor site's output combined with rest of output
 
port_mapping_df['port_Payload_Distance'] = port_mapping_df['distance']*port_mapping_df['Plant_Production']
port_mapping_df['total_Payload_Distance'] = port_mapping_df['Plant_Payload']+port_mapping_df['port_Payload_Distance']

# Add idenfier for each option
port_mapping_df.insert(loc=0, column='Option_ID', value=(range(1, len(port_mapping_df) + 1)))

# Collect all potential options, in ascending order of distance-payload
port_query = """SELECT Option_ID, 
                ID, 
                Plant_Latitude, 
                Plant_Longitude, 
                Port_ID, 
                Port_Latitude, 
                Port_Longitude, 
                total_Payload_Distance  
                FROM port_mapping_df 
                ORDER BY total_Payload_Distance ASC"""

# select top option                
final_candidate = pysqldf(port_query).head(1)


# describe selected option
conclusion = list(final_candidate.values)

a = (conclusion[0])
Plant_Latitude = a[2]
Plant_Longitude = a[3]
Port_Latitude = a[5]
Port_Longitude = a[6]
Payload_Distance = a[7]

plantlatinfo = "The option with the lowest distance-payload is the site with latitude %.2f" % Plant_Latitude
plantlonginfo = "and longitude %.2f." % Plant_Longitude
portlatinfo = "Its output should be transported to the port with latitude %.2f" % Port_Latitude
portlonginfo = "and longitude %.2f." % Port_Longitude
distpayloadinfo = "This option will generate a payload distance of %.0f tonne-kilometres annually." % Payload_Distance 

print plantlatinfo, plantlonginfo, portlatinfo, portlonginfo, distpayloadinfo
 
