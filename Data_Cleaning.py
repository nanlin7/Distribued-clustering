
## Data Preprocessing
import pandas as pd
import pyspark
import os
import sys
#from pyspark import SparkContext, SparkConf
#from pyspark.sql import SparkSession
#from pyspark.sql.types import *
#from pyspark.sql.functions import asc, desc
import datetime as dt
import re
import numpy as np
#from pyspark.ml.feature import StringIndexer
#from pyspark.ml.feature import OneHotEncoder
#from pyspark.ml.feature import VectorAssembler
#from pyspark.ml.clustering import KMeans
#from pyspark.mllib.clustering import LDA, LDAModel
#from pyspark.mllib.linalg import Vectors
#import math
#import matplotlib.pyplot as plt

'''
placeholder for load data from mongodb 
'''

# from file content string to pandas DF
'''
if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO


parking_2017_str = StringIO(parking_2017_s3)
parking_2016_str = StringIO(parking_2016_s3)


parking_2017_df = pd.read_csv(parking_2017_str, sep=",")
parking_2016_df = pd.read_csv(parking_2016_str, sep=",")
'''

f_17 = 's3n://msan694/nyc_parking_tickets/Parking_Violations_Issued_-_Fiscal_Year_2017.csv'
#f_16 = 's3n://msan694/nyc_parking_tickets/Parking_Violations_Issued_-_Fiscal_Year_2016.csv'
#f_18 = 's3n://msan694/nyc_parking_tickets/Parking_Violations_Issued_-_Fiscal_Year_2018.csv'

parking_2017_df = pd.read_csv(f_17, sep=",")
#parking_2016_df = pd.read_csv(f_16, sep=",")

# parking tickets
viol_code_park = [6, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 27, 32,
                 35, 37, 38, 39, 40, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 55,
                 56, 57, 58, 59, 60, 61, 62, 63, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75,
                 76, 77, 78, 80, 82, 83, 84, 85, 86, 89, 91, 92, 93, 96, 97, 98, 99]

# features we need 
feature_list = ['Plate Type','Violation Precinct','Vehicle Body Type','Vehicle Make','Vehicle Color',
               'Registration State','Violation Code','Violation County','Violation In Front Of Or Opposite',
               'Issue Date', 'Violation Time','Time First Observed','Vehicle Year']

name_dic = {'Plate Type' : 'plate_type', 
                  'Vehicle Body Type' : 'veh_body_type', 
                  'Summons Number' : 'summons_numb', 
                  'Vehicle Make' : 'veh_make', 
                  'Vehicle Color' : 'veh_color', 
                  'Registration State' : 'registration_state',
                  'Violation Code' : 'violation_code', 
                  'Violation County' : 'violation_county', 
                  'Violation In Front Of Or Opposite' : 'violation_infront_oppos', 
                  'Issue Date' : 'issue_date', 
                  'Violation Time' : 'violation_time', 
                  'Time First Observed' : 'time_first_observed', 
                  'Vehicle Year' : 'veh_year',
                 'Violation Precinct' : 'violation_precinct'}



def data_clean(df,name_dictionary,violation_code,features):

    # subsetting and rename
    df_subset = df[df['Violation Code'].isin(violation_code)][features]
    df_subset = df_subset.rename(name_dictionary,axis='columns')

    #plate type
    plate_type_info = pd.DataFrame({'plate_type_class' : ['PAS','COM','OMT','OMS']})
    df_subset = pd.merge(df_subset,plate_type_info,how = 'left', left_on = 'plate_type',right_on = "plate_type_class")
    df_subset["plate_type_class"] = df_subset["plate_type_class"].fillna("OTHERS")

    # vehicle body type
    veh_btype_c = ['Convertible', 'Sedan', 'Sedan', 'Sedan', 'Motorcycle', 
               'Emergency', 'Emergency', 'Emergency', 'Bus', 'Taxi', 
               'Limousine', 'Trailer', 'Trailer', 'Trailer', 'Trailer', 
               'Trailer', 'Truck', 'Truck', 'Truck', 'Truck', 'Truck', 
               'Truck', 'Truck', 'Truck', 'Pick-up', 'Pick-up', 'Suburban']
    veh_btype = ['CONV', 'SEDN', '4DSD', '2DSD', 'MCY', 'FIRE', 'AMBU',
            'HRSE', 'BUS', 'TAXI', 'LIM', 'POLE', 'H/TR', 'SEMI',
            'TRLR', 'LTRL', 'LSTV', 'VAN','TOW', 'TANK', 'STAK',
            'FLAT', 'DUMP', 'DELV', 'PICK', 'P-U', 'SUBN']

    veh_btype_info = pd.DataFrame({'veh_body_type' : veh_btype,
                                'veh_body_type_class' : veh_btype_c})

    df_subset = pd.merge(df_subset, veh_btype_info, 
                                how = "left", on = "veh_body_type")

    df_subset["veh_body_type_class"] = df_subset["veh_body_type_class"].fillna("Others")

    #vehicle make
    # we create the dataframe that will hold the groups we want
    veh_make = df_subset["veh_make"].value_counts()[:30].reset_index()[["index"]]
    veh_make = veh_make.rename(columns={"index" : "veh_make_class"})

    df_subset = pd.merge(df_subset, veh_make, how = "left", left_on = "veh_make",right_on = "veh_make_class")
    df_subset["veh_make_class"] = df_subset["veh_make_class"].fillna("OTHERS")

    # vehicle color
    veh_color_c = ['White', 'White', 'White', 'Gray', 'Gray', 'Gray',
               'Gray', 'Black', 'Black', 'Black', 'Black', 
               'Red', 'Red', 'Brown', 'Brown', 'Silver', 
               'Blue', 'Green', 'Green', 'Yellow', 'Yellow', 
               'Gold', 'Gold', 'Orange', 'Orange']
    veh_color = ['WH', 'WHITE', 'WHT', 'GY', 'GREY', 'GRAY', 'GRY',
            'BK', 'BLACK', 'BL', 'BLK', 'RD', 'RED', 'BROWN',
            'BR', 'SILVE', 'BLUE', 'GR','GREEN', 'YW', 'YELLO',
            'GOLD', 'GL', 'ORANG', 'OR']

    veh_color_info = pd.DataFrame({'veh_color' : veh_color,'veh_color_group' : veh_color_c})
    df_subset = pd.merge(df_subset, veh_color_info, how = "left", on = "veh_color")
    df_subset["veh_color_group"] = df_subset["veh_color_group"].fillna("Others")

    # issue date, new columns: issue_date, issue_year, issue_month, issue_quarter, issue_weekday
    def regex_filter(val):
        if val:
            mo = re.match(r'[0-9]{2}/[0-9]{2}/[0-9]{4}',str(val))
            if mo:
                return True
            else:
                return False
        else:
            return False

    df_subset = df_subset[df_subset['issue_date'].apply(regex_filter)]
    #df_subset = df_subset[df_subset['issue_date'].map(re.match(r'[0-9]{2}/[0-9]{2}/[0-9]{4}'))]
    #df_subset = df_subset[np.isfinite(df_subset['issue_date'])]
    
    date = pd.to_datetime(df_subset.loc[:,'issue_date'], format="%m/%d/%Y")
    
    year = date.dt.year.astype(int)
    month = date.dt.month
    quarter = date.dt.quarter
    dow = date.dt.weekday 
    weekday = ((dow <= 5) & (dow >=1)).astype(int)
    df_subset['issue_date'] = date
    df_subset['issue_month'] = month.astype(int)
    df_subset['issue_year'] = year.astype(int)
    df_subset['issue_quarter'] = quarter.astype(int)
    df_subset['issue_weekday'] = weekday.astype(int)
    df_subset = df_subset.reset_index(drop=True)

    # violation time 
    df_subset['violation_time'] = df_subset['violation_time'].map(lambda x: np.nan \
        if (re.match(r'\d{4}(A|P)', str(x).upper()) is None) else str(x))
    
    hour = df_subset['violation_time'].map(lambda x: x if pd.isnull(x) else int(str(x)[:2]))
    minute = df_subset['violation_time'].map(lambda x: x if pd.isnull(x) else int(str(x)[2:4]))
    section = df_subset['violation_time'].map(lambda x: x if pd.isnull(x) else str(x)[4:])
    hour_24 = []
    for h, s in zip(hour, section):
        if (pd.isnull(s) or h > 12):
            h_new = np.nan
        else:
            if s == "A":
                if h == 12:
                    h_new = 0
                else:
                    h_new = h
            if s == "P":
                if h == 12:
                    h_new = 12
                else:
                    h_new = h + 12
        hour_24.append(h_new)
        
    df_subset['violation_time'] = pd.Series(hour_24)

    # vehicle year
    df_subset['veh_year'] = df_subset['veh_year'].map(lambda x: np.nan if (re.match(r'\d{4}', str(x)) is None) \
    else int(str(x)[:4]))
    
    # violation county
    vio_county = {'K':'Kings','Q':'Queens','NY':'Manhattan','BX':'Bronx','R':'Richmond'}
    df_subset["violation_county"].replace(vio_county,inplace=True)
    
    # registration state
    top_state = ['NY','NJ','PA','CT','FL','CT']
    df_subset.loc[(~df_subset['registration_state'].isin(top_state)),'registration_state'] = 'OTHERS'
    df_subset_final = df_subset.dropna()
    # final features
    df_subset_final = df_subset_final[['plate_type','violation_precinct','veh_color_group',\
                        'issue_month', 'issue_year', 'issue_quarter', 'issue_weekday',\
                       'registration_state','violation_county','violation_infront_oppos','violation_time',\
                       'veh_year','plate_type_class','veh_body_type_class','veh_make_class']]
        
    return df_subset_final

#df_2018_cleaned = data_clean(parking_2018_df,name_dic,viol_code_park,feature_list)
df_2017_cleaned = data_clean(parking_2017_df,name_dic,viol_code_park,feature_list)
#df_2016_cleaned = data_clean(parking_2016_df,name_dic,viol_code_park,feature_list)

#frames = [df_2018_cleaned, df_2017_cleaned, df_2016_cleaned]
#df_2016_2017_2018 = pd.concat(frames)

df_2017_cleaned.to_csv("cleaned_df.csv")
