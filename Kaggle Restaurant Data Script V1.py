###
# Kaggle Competition Code
# Data Development script - not done! :0
# Created by: Andy Clapson
# Created on: May 25th, 2018
###

import os
import pandas as pd

os.chdir('/Users/Andy_iMac/Documents/Work & Resumés/Kaggle')


###
# Read in the raw CSV data and do some processing and renaming of variables
###

### Prep the Air data

air_reserve = pd.read_csv('air_reserve.csv')
# Convert date values from Object to datetime
air_reserve['reserve_datetime'] = pd.to_datetime(air_reserve['reserve_datetime'])
air_reserve['visit_datetime'] = pd.to_datetime(air_reserve['visit_datetime'])
# Then create a visit_date_key for joining (i.e. leave the time off)
air_reserve['visit_date_key'] = air_reserve['visit_datetime'].dt.strftime('%Y%m%d')
# Bounce results to CSV, in case you want to double-check it
air_reserve.to_csv('air_reserve_final.csv')

air_store_info = pd.read_csv('air_store_info.csv')

air_visit_data = pd.read_csv('air_visit_data.csv')
# Convert date values from Object to datetime
air_visit_data['visit_date'] = pd.to_datetime(air_visit_data['visit_date'])
# Then create a visit_date_key for joining (i.e. leave the time off)
air_visit_data['visit_date_key'] = air_visit_data['visit_date'].dt.strftime('%Y%m%d')
# Bounce results to CSV, in case you want to double-check it
air_visit_data.to_csv('air_visit_data_final.csv')


### Prep the HPG data (reservations only)

hpg_reserve = pd.read_csv('hpg_reserve.csv')
# Convert date values from Object to datetime
hpg_reserve['reserve_datetime'] = pd.to_datetime(hpg_reserve['reserve_datetime'])
hpg_reserve['visit_datetime'] = pd.to_datetime(hpg_reserve['visit_datetime'])
# Then create a visit_date_key for joining (i.e. leave the time off)
hpg_reserve['hpg_visit_date_key'] = hpg_reserve['visit_datetime'].dt.strftime('%Y%m%d')
hpg_reserve = hpg_reserve.rename(columns = {'visit_datetime' : 'hpg_visit_datetime'
                                            , 'reserve_datetime' : 'hpg_reserve_datetime'
                                            , 'reserve_visitors' : 'hpg_reserve_visitors'}
                                )
# Bounce results to CSV, in case you want to double-check it
hpg_reserve.to_csv('hpg_reserve_final.csv')

hpg_store_info = pd.read_csv('hpg_store_info.csv')
hpg_store_info = hpg_store_info.rename(columns = {'latitude' : 'hpg_latitude'
                                            , 'longitude' : 'hpg_longitude'}
                                )
# Bounce results to CSV, in case you want to double-check it
hpg_store_info.to_csv('hpg_store_info_final.csv')


### Then load the 'other' files - dates and store-id concordance

store_id_relation = pd.read_csv('store_id_relation.csv')

date_info = pd.read_csv('date_info.csv')
# Convert date values from Object to datetime
date_info['calendar_date'] = pd.to_datetime(date_info['calendar_date'])


###
# Now build *one possible* version Air data master set
# The 'inner' is the visits - to which matching reservations and store info is joined
###

air_master = air_visit_data.merge(air_store_info
                               , on = 'air_store_id'                                  
                               , how = 'inner')

### Note: the Reserve data has datetime - different level of granularity. 
# So we need to roll up/aggregate by day! Jeez!
air_reserve_by_day = air_reserve[['air_store_id', 'visit_date_key', 'reserve_visitors']]
air_reserve_by_day = air_reserve_by_day.groupby(['air_store_id', 'visit_date_key'])['reserve_visitors'].agg('sum').reset_index(name="sum")

air_master = air_master.merge(air_reserve_by_day
                               , on = ['air_store_id', 'visit_date_key']
                               , how = 'left')

air_master = air_master.merge(store_id_relation
                               , on = 'air_store_id'
                               , how = 'left')

air_master.to_csv('air_master.csv')


###
# Now build the HPG Reservations data master set
# The 'inner' is the visits - to which matching reservations and store info is joined
###

hpg_master = hpg_reserve.merge(hpg_store_info
                               , on = 'hpg_store_id'
                               , how = 'inner')

hpg_master = hpg_master.merge(store_id_relation
                               , on = 'hpg_store_id'
                               , how = 'inner')

hpg_master.head()
hpg_master.to_csv('hpg_master.csv')


###
# Now put a final master together and see how it looks
# good luck! :)
###
restaurant_master = air_master.merge(hpg_master
                               , left_on = ['air_store_id','visit_date_key']
                               , right_on = ['air_store_id','hpg_visit_date_key'] 
                               , how = 'left')
                                    

restaurant_master.head()
restaurant_master.to_csv('restaurant_master.csv')
