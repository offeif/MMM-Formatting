#!/usr/bin/env python
# coding: utf-8

# In[1]:


import time
import re
import numpy as np
import pandas as pd
import datetime as dt
import snowflake.connector
import google.auth
import snowflake.connector
from google.cloud import secretmanager

import warnings
warnings.filterwarnings('ignore')


# In[2]:


#Grab snowflake login
details = pd.read_csv('details.csv')

user = details['usr'].iloc[0]
password = details['psw'].iloc[0]

ctx = snowflake.connector.connect(
    user=user,
    password=password,
    account='urbanoutfittersinc.us-central1.gcp',
    warehouse='BI_WH_PROD',
    role='RO_ROLE_PROD',
    database='EDW_PROD'
    )


# ## Reporting Data

# In[36]:


model = '4_39_58'
fymap_file = '2. FY Map FP.csv'
pareto_file = '1. pareto_alldecomp_matrix.csv'
rawdata_file = '6. raw_data.csv'


# In[37]:


# Accept user input for labels and convert to uppercase
display_labels_input = input("Enter labels for DISPLAY (comma-separated and without DISPLAY): ")
display_labels = [label.strip().upper() for label in display_labels_input.split(",")]


# In[38]:


display_labels


# In[39]:


# Chosen the model
selected_model = model

# ------- TO GET FY INFO -----------------------#
fymap = pd.read_csv(fymap_file)

#--------------------------------------------------------------------------#
# ----------------------- ALLDECOMP SUMMARIES -----------------------------#
#--------------------------------------------------------------------------#

alldecomp = pd.read_csv(pareto_file, low_memory=False)
alldecomp_filter = alldecomp[alldecomp['solID'] == selected_model]
alldecomp_filter = alldecomp_filter.drop(alldecomp_filter.columns[0], axis=1)

# FY_Summary = alldecomp_filter.copy()
# Mutate columns
FY_Summary = alldecomp_filter.assign(
    BASELINE=lambda x: x['intercept'] + x['trend'] + x['season'] + x['weekday'] + x.filter(regex='^BASE').sum(axis=1),
#     BASELINE=lambda x: x['intercept'] + x['trend'] + x['season'] + x['weekday'] + x.filter(regex='^BASE|^PROMO').sum(axis=1),
    TOTALINCREMENTAL=lambda x: x['depVarHat'] - x['BASELINE'],
    PROMOS=lambda x: x.filter(like="PROMO").sum(axis=1),
#     PAIDMEDIA=lambda x: x.filter(like="_S").sum(axis=1),
    PAIDMEDIA=lambda x: x.filter(regex='_S$').sum(axis=1),
    MKTGOUTBOUND=lambda x: x.filter(like="EMAIL").sum(axis=1) + x.filter(like="SMS").sum(axis=1) + x.filter(like="PUSH").sum(axis=1) + x.filter(like="PR_").sum(axis=1) + x.filter(like="PRINT").sum(axis=1),
    UNPAID=lambda x: x.filter(like="NATURAL").sum(axis=1) + x.filter(like="ORG").sum(axis=1)
)

# Select required columns
FY_Summary = FY_Summary[['ds', 'BASELINE', 'TOTALINCREMENTAL', 'PROMOS', 'PAIDMEDIA', 'MKTGOUTBOUND', 'UNPAID']]

# Pivot longer
FY_Summary = FY_Summary.melt(id_vars='ds', var_name='variablenames', value_name='attributeddemand')

# Left join with 'fymap'
FY_Summary = pd.merge(FY_Summary, fymap, left_on='ds', right_on='ORDER_DT', how='left')

# Group by and summarize
FY_Summary = FY_Summary.groupby(['FY', 'variablenames']).agg(Demand=('attributeddemand', 'sum')).reset_index()

# Pivot wider
FY_Summary = FY_Summary.pivot(index='variablenames', columns='FY', values='Demand').reset_index()

# Select columns ending with "_S" and 'ds'
FY_PM_Summary = alldecomp_filter.filter(like="_S").assign(ds=alldecomp_filter['ds'])

# Mutate columns
FY_PM_Summary = FY_PM_Summary.assign(
    PAIDSEARCH=lambda x: x.filter(like="PAIDSEARCH").sum(axis=1),
    DISPLAY=lambda x: x.filter(like="DISPLAY").sum(axis=1) + 
                      sum(x.filter(like=label).sum(axis=1) for label in display_labels),
    AFFILIATES=lambda x: x.filter(like="AFFILIATES").sum(axis=1),
    PAIDSOCIAL=lambda x: x.filter(regex=re.compile(r'(META|^FB)')).sum(axis=1) + 
                         x.filter(like="PINTEREST").sum(axis=1) + 
                         x.filter(like="TIKTOK").sum(axis=1) + 
                         x.filter(like="SNAP").sum(axis=1)
)

# Select columns not matching "*_S"
FY_PM_Summary = FY_PM_Summary.drop(FY_PM_Summary.filter(like="_S").columns, axis=1)

# Pivot longer
FY_PM_Summary = FY_PM_Summary.melt(id_vars='ds', var_name='variablenames', value_name='attributeddemand')

# Inner join with 'fymap'
FY_PM_Summary = pd.merge(FY_PM_Summary, fymap, left_on='ds', right_on='ORDER_DT', how='inner')

# Group by and summarize
FY_PM_Summary = FY_PM_Summary.groupby(['FY', 'variablenames']).agg(Demand=('attributeddemand', 'sum')).reset_index()

# Pivot wider
FY_PM_Summary = FY_PM_Summary.pivot(index='variablenames', columns='FY', values='Demand').reset_index()

# Select columns ending with "_S" and 'ds'
FY_PM_Detail = alldecomp_filter.filter(like="_S").assign(ds=alldecomp_filter['ds'])

# Pivot longer
FY_PM_Detail = FY_PM_Detail.melt(id_vars='ds', var_name='variablenames', value_name='attributeddemand')

# Inner join with 'fymap'
FY_PM_Detail = pd.merge(FY_PM_Detail, fymap, left_on='ds', right_on='ORDER_DT', how='inner')

# Group by and summarize
FY_PM_Detail = FY_PM_Detail.groupby(['FY', 'variablenames']).agg(Demand=('attributeddemand', 'sum')).reset_index()

# Pivot wider
FY_PM_Detail = FY_PM_Detail.pivot(index='variablenames', columns='FY', values='Demand').reset_index()

# Select columns ending with "_I" and 'ds'
FY_I_Detail = alldecomp_filter[[col for col in alldecomp_filter.columns if col.endswith('_I')]].assign(ds=alldecomp_filter['ds'])

# Pivot longer
FY_I_Detail = FY_I_Detail.melt(id_vars='ds', var_name='variablenames', value_name='attributeddemand')

# Inner join with 'fymap'
FY_I_Detail = pd.merge(FY_I_Detail, fymap, left_on='ds', right_on='ORDER_DT', how='inner')

# Group by and summarize
FY_I_Detail = FY_I_Detail.groupby(['FY', 'variablenames']).agg(Demand=('attributeddemand', 'sum')).reset_index()

# Pivot wider
FY_I_Detail = FY_I_Detail.pivot(index='variablenames', columns='FY', values='Demand').reset_index()

# #--------------------------------------------------------------------------#
# # ------------------------- RAWDATA SUMMARIES -----------------------------#
# #--------------------------------------------------------------------------#
rawdata = pd.read_csv(rawdata_file)

rawdata = rawdata.rename(columns={'ORDER_DT': 'Q_DT_ID'})

# Convert date columns to datetime objects and then to the desired format
# rawdata['Q_DT_ID'] = pd.to_datetime(rawdata['Q_DT_ID'], format='%m/%d/%y').dt.strftime('%Y-%m-%d')
if rawdata['Q_DT_ID'].str.contains('/').any():  # Assuming '/' indicates the format '%m/%d/%y'
    rawdata['Q_DT_ID'] = pd.to_datetime(rawdata['Q_DT_ID'], format='%m/%d/%y').dt.strftime('%Y-%m-%d')


# Assuming 'rawdata' and 'fymap' DataFrames are already defined

# Select columns ending with "_S" or "_ID"
FY_PM_Summary2 = rawdata.filter(regex='(_S$)|(_ID$)')

# Mutate columns
FY_PM_Summary2 = FY_PM_Summary2.assign(
    PAIDSEARCH=lambda x: x.filter(like="PAIDSEARCH").sum(axis=1),
    DISPLAY=lambda x: x.filter(like="DISPLAY").sum(axis=1) + 
                      sum(x.filter(like=label).sum(axis=1) for label in display_labels),
    AFFILIATES=lambda x: x.filter(like="AFFILIATES").sum(axis=1),
    PAIDSOCIAL=lambda x: x.filter(regex=re.compile(r'(META|^FB)')).sum(axis=1) + 
                         x.filter(like="PINTEREST").sum(axis=1) + 
                         x.filter(like="TIKTOK").sum(axis=1) + 
                         x.filter(like="SNAP").sum(axis=1)
)

# Select columns not ending with "_S"
FY_PM_Summary2 = FY_PM_Summary2.drop(FY_PM_Summary2.filter(regex='_S$').columns, axis=1)

# Pivot longer
FY_PM_Summary2 = FY_PM_Summary2.melt(id_vars='Q_DT_ID', var_name='variablenames', value_name='spend')

# Inner join with 'fymap'
FY_PM_Summary2 = pd.merge(FY_PM_Summary2, fymap, left_on='Q_DT_ID', right_on='ORDER_DT', how='inner')

# Group by and summarize
FY_PM_Summary2 = FY_PM_Summary2.groupby(['FY', 'variablenames']).agg(spend=('spend', 'sum')).reset_index()

# Pivot wider
FY_PM_Summary2 = FY_PM_Summary2.pivot(index='variablenames', columns='FY', values='spend').reset_index()

# Select columns ending with "_S" and "_ID"
FY_PM_Detail2 = rawdata.filter(regex='(_S$)|(_ID$)')

# Pivot longer
FY_PM_Detail2 = FY_PM_Detail2.melt(id_vars='Q_DT_ID', var_name='variablenames', value_name='spend')

# Inner join with 'fymap'
FY_PM_Detail2 = pd.merge(FY_PM_Detail2, fymap, left_on='Q_DT_ID', right_on='ORDER_DT', how='inner')

# Group by and summarize
FY_PM_Detail2 = FY_PM_Detail2.groupby(['FY', 'variablenames'])['spend'].sum().reset_index()

# Pivot wider
FY_PM_Detail2 = FY_PM_Detail2.pivot(index='variablenames', columns='FY', values='spend').reset_index()

# Select columns with specific prefixes and suffix
FY_I_Detail2 = rawdata.filter(regex='^EMAIL|^SMS|^PUSH|^ORG|^NATUR|^PR_|^PRINT|_ID$')

# Pivot longer
FY_I_Detail2 = FY_I_Detail2.melt(id_vars='Q_DT_ID', var_name='variablenames', value_name='spend')

# Inner join with 'fymap'
FY_I_Detail2 = pd.merge(FY_I_Detail2, fymap, left_on='Q_DT_ID', right_on='ORDER_DT', how='inner')

# Group by and summarize
FY_I_Detail2 = FY_I_Detail2.groupby(['FY', 'variablenames'])['spend'].sum().reset_index()

# Pivot wider
FY_I_Detail2 = FY_I_Detail2.pivot(index='variablenames', columns='FY', values='spend').reset_index()

# #--------------------------------------------------------------------------#
# # ---------------- COMBINED SUMMARIES FOR THE REPORT ----------------------#
# #--------------------------------------------------------------------------#
# Assuming FY_Summary, FY_PM_Summary, FY_PM_Summary2, FY_PM_Detail, FY_PM_Detail2, FY_I_Detail, FY_I_Detail2 DataFrames are already defined

# FY_PM_Summary3
FY_PM_Summary3 = pd.concat([FY_Summary, FY_PM_Summary]).merge(FY_PM_Summary2, on="variablenames", how="left")

# FY_PM_Detail3
FY_PM_Detail3 = FY_PM_Detail.merge(FY_PM_Detail2, on="variablenames", how="inner")

# FY_Detail3
FY_Detail3 = FY_I_Detail.merge(FY_I_Detail2, on="variablenames", how="inner")

# FY_Decomp
FY_Decomp = pd.concat([FY_PM_Summary3, FY_PM_Detail3, FY_Detail3])

# Write to CSV
FY_Decomp.to_csv("FYSummaryDecomp_r11.csv", index=False)


# In[40]:


FY_Decomp


# In[41]:


# Chosen the model
selected_model = model

# ------- TO GET FY INFO -----------------------#
fymap = pd.read_csv(fymap_file)

#--------------------------------------------------------------------------#
# ----------------------- ALLDECOMP SUMMARIES -----------------------------#
#--------------------------------------------------------------------------#
alldecomp = pd.read_csv(pareto_file, low_memory=False)
alldecomp_filter = alldecomp[alldecomp['solID'] == selected_model]
alldecomp_filter = alldecomp_filter.drop(alldecomp_filter.columns[0], axis=1)

# Mutate columns
FY_Summary = alldecomp_filter.assign(
    BASELINE=lambda x: x['intercept'] + x['trend'] + x['season'] + x['weekday'] + x.filter(regex='^BASE').sum(axis=1),
#     BASELINE=lambda x: x['intercept'] + x['trend'] + x['season'] + x['weekday'] + x.filter(regex='^BASE|^PROMO').sum(axis=1),
    TOTALINCREMENTAL=lambda x: x['depVarHat'] - x['BASELINE'],
    PROMOS=lambda x: x.filter(like="PROMO").sum(axis=1),
#     PAIDMEDIA=lambda x: x.filter(like="_S").sum(axis=1),
    PAIDMEDIA=lambda x: x.filter(regex='_S$').sum(axis=1),
    MKTGOUTBOUND=lambda x: x.filter(like="EMAIL").sum(axis=1) + x.filter(like="SMS").sum(axis=1) + x.filter(like="PUSH").sum(axis=1) + x.filter(like="PR_").sum(axis=1) + x.filter(like="PRINT").sum(axis=1),
    UNPAID=lambda x: x.filter(like="NATURAL").sum(axis=1) + x.filter(like="ORG").sum(axis=1)
)

# Select required columns
FY_Summary = FY_Summary[['ds', 'BASELINE', 'TOTALINCREMENTAL', 'PROMOS', 'PAIDMEDIA', 'MKTGOUTBOUND', 'UNPAID']]

# Pivot longer
FY_Summary = FY_Summary.melt(id_vars='ds', var_name='variablenames', value_name='attributeddemand')

# Left join with 'fymap'
FY_Summary = pd.merge(FY_Summary, fymap, left_on='ds', right_on='ORDER_DT', how='left')

# Group by and summarize
FY_Summary = FY_Summary.groupby(['YTD', 'variablenames']).agg(Demand=('attributeddemand', 'sum')).reset_index()

# Pivot wider
FY_Summary = FY_Summary.pivot(index='variablenames', columns='YTD', values='Demand').reset_index()

# Select columns ending with "_S" and 'ds'
FY_PM_Summary = alldecomp_filter.filter(like="_S").assign(ds=alldecomp_filter['ds'])

# Mutate columns
FY_PM_Summary = FY_PM_Summary.assign(
    PAIDSEARCH=lambda x: x.filter(like="PAIDSEARCH").sum(axis=1),
    DISPLAY=lambda x: x.filter(like="DISPLAY").sum(axis=1) + 
                      sum(x.filter(like=label).sum(axis=1) for label in display_labels),
    AFFILIATES=lambda x: x.filter(like="AFFILIATES").sum(axis=1),
    PAIDSOCIAL=lambda x: x.filter(regex=re.compile(r'(META|^FB)')).sum(axis=1) + 
                         x.filter(like="PINTEREST").sum(axis=1) + 
                         x.filter(like="TIKTOK").sum(axis=1) + 
                         x.filter(like="SNAP").sum(axis=1)
)

# Select columns not matching "*_S"
FY_PM_Summary = FY_PM_Summary.drop(FY_PM_Summary.filter(like="_S").columns, axis=1)

# Pivot longer
FY_PM_Summary = FY_PM_Summary.melt(id_vars='ds', var_name='variablenames', value_name='attributeddemand')

# Inner join with 'fymap'
FY_PM_Summary = pd.merge(FY_PM_Summary, fymap, left_on='ds', right_on='ORDER_DT', how='inner')

# Group by and summarize
FY_PM_Summary = FY_PM_Summary.groupby(['YTD', 'variablenames']).agg(Demand=('attributeddemand', 'sum')).reset_index()

# Pivot wider
FY_PM_Summary = FY_PM_Summary.pivot(index='variablenames', columns='YTD', values='Demand').reset_index()

# Select columns ending with "_S" and 'ds'
FY_PM_Detail = alldecomp_filter.filter(regex='_S$').assign(ds=alldecomp_filter['ds'])

# Pivot longer
FY_PM_Detail = FY_PM_Detail.melt(id_vars='ds', var_name='variablenames', value_name='attributeddemand')

# Inner join with 'fymap'
FY_PM_Detail = pd.merge(FY_PM_Detail, fymap, left_on='ds', right_on='ORDER_DT', how='inner')

# Group by and summarize
FY_PM_Detail = FY_PM_Detail.groupby(['YTD', 'variablenames']).agg(Demand=('attributeddemand', 'sum')).reset_index()

# Pivot wider
FY_PM_Detail = FY_PM_Detail.pivot(index='variablenames', columns='YTD', values='Demand').reset_index()

# Select columns ending with "_I" and 'ds'
FY_Detail = alldecomp_filter[[col for col in alldecomp_filter.columns if col.endswith('_I')]].assign(ds=alldecomp_filter['ds'])

# Pivot longer
FY_Detail = FY_Detail.melt(id_vars='ds', var_name='variablenames', value_name='attributeddemand')

# Inner join with 'fymap'
FY_Detail = pd.merge(FY_Detail, fymap, left_on='ds', right_on='ORDER_DT', how='inner')

# Group by and summarize
FY_Detail = FY_Detail.groupby(['YTD', 'variablenames']).agg(Demand=('attributeddemand', 'sum')).reset_index()

# Pivot wider
FY_Detail = FY_Detail.pivot(index='variablenames', columns='YTD', values='Demand').reset_index()

#--------------------------------------------------------------------------#
# ------------------------- RAWDATA SUMMARIES -----------------------------#
#--------------------------------------------------------------------------#
rawdata = pd.read_csv(rawdata_file)
rawdata = rawdata.rename(columns={'ORDER_DT': 'Q_DT_ID'})
# Convert date columns to datetime objects and then to the desired format
# rawdata['Q_DT_ID'] = pd.to_datetime(rawdata['Q_DT_ID'], format='%m/%d/%y').dt.strftime('%Y-%m-%d')
if rawdata['Q_DT_ID'].str.contains('/').any():  # Assuming '/' indicates the format '%m/%d/%y'
    rawdata['Q_DT_ID'] = pd.to_datetime(rawdata['Q_DT_ID'], format='%m/%d/%y').dt.strftime('%Y-%m-%d')

# Assuming 'rawdata' and 'fymap' DataFrames are already defined

# Select columns ending with "_S" or "_ID"
# FY_PM_Summary2 = rawdata.filter(regex='_S$|_ID$')
FY_PM_Summary2 = rawdata.filter(regex='(_S$)|(_ID$)')

# Mutate columns
FY_PM_Summary2 = FY_PM_Summary2.assign(
    PAIDSEARCH=lambda x: x.filter(like="PAIDSEARCH").sum(axis=1),
    DISPLAY=lambda x: x.filter(like="DISPLAY").sum(axis=1) + 
                      sum(x.filter(like=label).sum(axis=1) for label in display_labels),
    AFFILIATES=lambda x: x.filter(like="AFFILIATES").sum(axis=1),
    PAIDSOCIAL=lambda x: x.filter(regex=re.compile(r'(META|^FB)')).sum(axis=1) + 
                         x.filter(like="PINTEREST").sum(axis=1) + 
                         x.filter(like="TIKTOK").sum(axis=1) + 
                         x.filter(like="SNAP").sum(axis=1)
)

# Select columns not ending with "_S"
FY_PM_Summary2 = FY_PM_Summary2.drop(FY_PM_Summary2.filter(regex='_S$').columns, axis=1)

# Pivot longer
FY_PM_Summary2 = FY_PM_Summary2.melt(id_vars='Q_DT_ID', var_name='variablenames', value_name='spend')

# Inner join with 'fymap'
FY_PM_Summary2 = pd.merge(FY_PM_Summary2, fymap, left_on='Q_DT_ID', right_on='ORDER_DT', how='inner')

# Group by and summarize
FY_PM_Summary2 = FY_PM_Summary2.groupby(['YTD', 'variablenames']).agg(spend=('spend', 'sum')).reset_index()

# Pivot wider
FY_PM_Summary2 = FY_PM_Summary2.pivot(index='variablenames', columns='YTD', values='spend').reset_index()

# Select columns ending with "_S" and "_ID"
FY_PM_Detail2 = rawdata.filter(regex='_S$|_ID$')

# Pivot longer
FY_PM_Detail2 = FY_PM_Detail2.melt(id_vars='Q_DT_ID', var_name='variablenames', value_name='spend')

# Inner join with 'fymap'
FY_PM_Detail2 = pd.merge(FY_PM_Detail2, fymap, left_on='Q_DT_ID', right_on='ORDER_DT', how='inner')

# Group by and summarize
FY_PM_Detail2 = FY_PM_Detail2.groupby(['YTD', 'variablenames'])['spend'].sum().reset_index()

# Pivot wider
FY_PM_Detail2 = FY_PM_Detail2.pivot(index='variablenames', columns='YTD', values='spend').reset_index()

# Select columns with specific prefixes and suffix
FY_Detail2 = rawdata.filter(regex='^EMAIL|^SMS|^PUSH|^ORG|^NATUR|^PR_|^PRINT|_ID$')

# Pivot longer
FY_Detail2 = FY_Detail2.melt(id_vars='Q_DT_ID', var_name='variablenames', value_name='spend')

# Inner join with 'fymap'
FY_Detail2 = pd.merge(FY_Detail2, fymap, left_on='Q_DT_ID', right_on='ORDER_DT', how='inner')

# Group by and summarize
FY_Detail2 = FY_Detail2.groupby(['YTD', 'variablenames'])['spend'].sum().reset_index()

# Pivot wider
FY_Detail2 = FY_Detail2.pivot(index='variablenames', columns='YTD', values='spend').reset_index()

#--------------------------------------------------------------------------#
# ---------------- COMBINED SUMMARIES FOR THE REPORT ----------------------#
#--------------------------------------------------------------------------#
# Assuming FY_Summary, FY_PM_Summary, FY_PM_Summary2, FY_PM_Detail, FY_PM_Detail2, FY_I_Detail, FY_I_Detail2 DataFrames are already defined

# FY_PM_Summary3
FY_PM_Summary3 = pd.concat([FY_Summary, FY_PM_Summary]).merge(FY_PM_Summary2, on="variablenames", how="left")

# FY_PM_Detail3
FY_PM_Detail3 = FY_PM_Detail.merge(FY_PM_Detail2, on="variablenames", how="inner")

# FY_Detail3
FY_Detail3 = FY_Detail.merge(FY_Detail2, on="variablenames", how="inner")

# FY_Decomp
FY_Decomp = pd.concat([FY_PM_Summary3, FY_PM_Detail3, FY_Detail3])

desired_columns = [0, 2, 4, 5, 7, 9, 10, 1, 3, 6, 8]
FYTD_Decomp = FY_Decomp.iloc[:, desired_columns]

# Write to CSV
FYTD_Decomp.to_csv("YTDSummaryDecomp_r11.csv", index=False)


# In[42]:


FYTD_Decomp


# ## UO

# ### MMM Session Report (Paid Media MMM vs MTA ROAS)

# ### MTA FY

# In[43]:


# Define a dictionary to map brands to their SQL queries
brand_queries = {
    'UO': """
        WITH UO_MTA_DATA AS
        (SELECT 
        'FY'||' '||(YEAR_VALUE + 1) AS FY,'UO' AS BRAND, 
        r.BRAND_MARKETING_CHANNEL,
        r.BRAND_SUBMARKETING_CHANNEL,
        SUM(ATTRIBUTED_DEMAND_AMT) AS demand
        FROM  EDW_PROD.ADVA.ATT_MTA_ROLLUP_V r
        JOIN EDW_PROD.URBN.DW_CALENDAR_HIERARCHY c
        ON r.Q_DT_ID = c.Q_DT_ID
        WHERE BRAND_CD = 'UO'
        AND r.Q_DT_ID>= '2021-02-01' AND r.Q_DT_ID<='2023-12-31'
        AND r.BRAND_MARKETING_CHANNEL LIKE ANY ('%PAID SOCIAL%', '%PAID SEARCH%','%AFFILIATES%','%DISPLAY%') AND r.BRAND_PARENT_MARKETING_CHANNEL = 'PAID' AND BRAND_SUBMARKETING_CHANNEL NOT LIKE '%LOCALISED%'
        GROUP BY 1,3,4
        ORDER BY 3,4,1)
        
        SELECT BRAND_MARKETING_CHANNEL, BRAND_SUBMARKETING_CHANNEL, FY,SUM(DEMAND) AS MTA_DEMAND
        FROM UO_MTA_DATA
        GROUP BY 1,2,3
        ORDER BY 1,2,3
    """,
    'FP': """
        WITH FP_MTA_DATA AS
        (SELECT 
        'FY'||' '||(YEAR_VALUE + 1) AS FY, 'FP' AS BRAND, 
        r.BRAND_MARKETING_CHANNEL,
        r.BRAND_SUBMARKETING_CHANNEL,
        r.MEDIUM,
        r.CAMPAIGN,
        r.SOURCE,
         SUM(ATTRIBUTED_DEMAND_AMT) AS demand
         FROM  EDW_PROD.ADVA.ATT_MTA_ROLLUP_V r
         JOIN EDW_PROD.URBN.DW_CALENDAR_HIERARCHY c
         ON r.Q_DT_ID = c.Q_DT_ID
         WHERE BRAND_CD = 'FP'
         AND r.Q_DT_ID>= '2021-02-01' AND r.Q_DT_ID<='2023-09-30'
        AND r.BRAND_MARKETING_CHANNEL LIKE ANY ('%PAID SOCIAL%', '%PAID SEARCH%','%AFFILIATES%','%DISPLAY%') AND r.BRAND_PARENT_MARKETING_CHANNEL = 'PAID'
        AND SITE_ID = 'US'
        GROUP BY 1,2,3,4,5,6,7
        ORDER BY 1,2,3,4,5,6,7)

         SELECT BRAND_MARKETING_CHANNEL, BRAND_SUBMARKETING_CHANNEL, MEDIUM, CAMPAIGN, SOURCE, FY, SUM(DEMAND) AS MTA_DEMAND
         FROM FP_MTA_DATA
         GROUP BY 1,2,3,4,5,6
         ORDER BY 1,2,3,4,5,6
        ;
    """,
    'AN': """
        WITH AN_MTA_DATA AS
        ((SELECT 
        'FY'||' '||(YEAR_VALUE + 1) AS FY,'AN' AS BRAND, 
        r.BRAND_MARKETING_CHANNEL,
        r.BRAND_SUBMARKETING_CHANNEL,
         SUM(ATTRIBUTED_DEMAND_AMT) AS demand
         FROM  EDW_PROD.ADVA.ATT_MTA_ROLLUP_V r
         JOIN EDW_PROD.URBN.DW_CALENDAR_HIERARCHY c
         ON r.Q_DT_ID = c.Q_DT_ID
         WHERE BRAND_CD = 'AN'
         AND r.Q_DT_ID>= '2021-02-01' AND r.Q_DT_ID<='2022-10-09'
        AND r.BRAND_MARKETING_CHANNEL LIKE ANY ('%PAID SOCIAL%', '%PAID SEARCH%','%AFFILIATES%','%DISPLAY%') AND r.BRAND_PARENT_MARKETING_CHANNEL = 'PAID'
        GROUP BY 1,3,4
        ORDER BY 3,4,1)

        UNION ALL

        (SELECT 
        'FY'||' '||(YEAR_VALUE + 1) AS FY, 'AN' AS BRAND,
        r.BRAND_MARKETING_CHANNEL,
        r.BRAND_SUBMARKETING_CHANNEL,
         SUM(ATTRIBUTED_DEMAND_AMT) AS demand
         FROM  EDW_PROD.ADVA.ATT_MTA_ROLLUP_V r
         JOIN EDW_PROD.URBN.DW_CALENDAR_HIERARCHY c
         ON r.Q_DT_ID = c.Q_DT_ID
         WHERE BRAND_CD IN ('AN','BH')
         AND r.Q_DT_ID>= '2022-10-10' AND r.Q_DT_ID<='2023-07-31'
        AND r.BRAND_MARKETING_CHANNEL LIKE ANY ('%PAID SOCIAL%', '%PAID SEARCH%','%AFFILIATES%','%DISPLAY%') AND r.BRAND_PARENT_MARKETING_CHANNEL = 'PAID'
        GROUP BY 1,3,4
        ORDER BY 3,4,1
         ))

         SELECT BRAND_MARKETING_CHANNEL, BRAND_SUBMARKETING_CHANNEL, FY, SUM(DEMAND) AS MTA_DEMAND
         FROM AN_MTA_DATA
         GROUP BY 1,2,3
         ORDER BY 1,2,3
        ;
    """
    # Add more brands and their associated queries as needed
}

# Get user input for the brand
brand = input("Enter the brand (e.g., UO, AN, FP): ")

# Check if the entered brand exists in the dictionary
if brand in brand_queries:
    sql = brand_queries[brand]
else:
    print("Brand not found.")
    exit()

# Execute the SQL query
start = time.time()
cs = ctx.cursor()
cs.execute(sql)
df_raw = pd.DataFrame(cs.fetchall(), columns=[x[0] for x in cs.description])
end = time.time()

# Print the total time taken for execution
print('Total time: {}'.format(end - start))


# In[44]:


df_raw


# In[45]:


def map_brand_marketing_channel(channel):
    if 'AFFILIATES' in channel:
        return 'AFFILIATES'
    elif 'DISPLAY' in channel:
        return 'DISPLAY'
    elif 'PAID SEARCH' in channel:
        return 'PAID SEARCH'
    elif 'PAID SOCIAL' in channel:
        return 'PAID SOCIAL MEDIA'
    else:
        return 'Other' 

# Assuming df_raw is defined earlier
if brand == 'AN':
    df_raw['BRAND_MARKETING_CHANNEL'] = df_raw['BRAND_SUBMARKETING_CHANNEL'].apply(map_brand_marketing_channel)
    pivot_table = pd.pivot_table(df_raw, 
                                  values='MTA_DEMAND', 
                                  index='BRAND_MARKETING_CHANNEL', 
                                  columns='FY', 
                                  aggfunc='sum')

    filtered_data = df_raw
    grouped_data = filtered_data.groupby(['BRAND_SUBMARKETING_CHANNEL', 'FY'])['MTA_DEMAND'].sum().reset_index()
    pivot_grouped_data = grouped_data.pivot(index='BRAND_SUBMARKETING_CHANNEL', columns='FY', values='MTA_DEMAND').fillna(0).reset_index()
else:
    pivot_table = pd.pivot_table(df_raw, 
                                  values='MTA_DEMAND', 
                                  index='BRAND_SUBMARKETING_CHANNEL', 
                                  columns='FY', 
                                  aggfunc='sum')

    filtered_data = df_raw
    grouped_data = filtered_data.groupby(['BRAND_SUBMARKETING_CHANNEL', 'FY'])['MTA_DEMAND'].sum().reset_index()
    pivot_grouped_data = grouped_data.pivot(index='BRAND_SUBMARKETING_CHANNEL', columns='FY', values='MTA_DEMAND').fillna(0).reset_index()

pivot_grouped_data


# In[46]:


pivot_table_brand = pd.pivot_table(df_raw, 
                                   values='MTA_DEMAND', 
                                   index='BRAND_MARKETING_CHANNEL', 
                                   columns='FY', 
                                   aggfunc='sum' 
                                   #fill_value=0
                                  )

pivot_table_brand = pivot_table_brand.reset_index()
pivot_table_brand


# ### MTA YTD

# In[47]:


# Define a dictionary to map brands to their SQL queries
brand_queries = {
    'UO': """
        WITH UO_MTA_DATA AS
        (SELECT 
        'UO' AS BRAND, 
        r.BRAND_MARKETING_CHANNEL,
        r.BRAND_SUBMARKETING_CHANNEL,
         CASE WHEN  r.Q_DT_ID >='2021-02-01' AND r.Q_DT_ID <='2021-12-31' THEN 'FY22 YTD'
         WHEN r.Q_DT_ID >='2022-02-01' AND r.Q_DT_ID <='2022-12-31' THEN 'FY23 YTD'
         WHEN r.Q_DT_ID >='2023-02-01' AND r.Q_DT_ID <='2023-12-31' THEN 'FY24 YTD'
         ELSE 'REM' END AS YTD,
         SUM(ATTRIBUTED_DEMAND_AMT) AS demand
         FROM  EDW_PROD.ADVA.ATT_MTA_ROLLUP_V r
         JOIN EDW_PROD.URBN.DW_CALENDAR_HIERARCHY c
         ON r.Q_DT_ID = c.Q_DT_ID
         WHERE BRAND_CD = 'UO'
         AND r.Q_DT_ID>= '2021-02-01' AND r.Q_DT_ID<='2023-12-31'
        AND r.BRAND_MARKETING_CHANNEL LIKE ANY ('%PAID SOCIAL%', '%PAID SEARCH%','%AFFILIATES%','%DISPLAY%') AND r.BRAND_PARENT_MARKETING_CHANNEL = 'PAID' AND BRAND_SUBMARKETING_CHANNEL NOT LIKE '%LOCALISED%'
        GROUP BY 4,2,3,1
        ORDER BY 2,3,4,1)


         SELECT BRAND_MARKETING_CHANNEL, BRAND_SUBMARKETING_CHANNEL, YTD, SUM(DEMAND) AS MTA_DEMAND
         FROM UO_MTA_DATA
         GROUP BY 1,2,3
         ORDER BY 1,2,3
;
    """,
    'FP': """
        WITH FP_MTA_DATA AS
        (SELECT 
        'FP' AS BRAND, 
        r.BRAND_MARKETING_CHANNEL,
        r.BRAND_SUBMARKETING_CHANNEL,
        CASE WHEN  r.Q_DT_ID >='2021-02-01' AND r.Q_DT_ID <='2021-09-30' THEN 'FY22 YTD'
         WHEN r.Q_DT_ID >='2022-02-01' AND r.Q_DT_ID <='2022-09-30' THEN 'FY23 YTD'
         WHEN r.Q_DT_ID >='2023-02-01' AND r.Q_DT_ID <='2023-09-30' THEN 'FY24 YTD'
         ELSE 'REM' END AS YTD,
         SUM(ATTRIBUTED_DEMAND_AMT) AS demand
         FROM  EDW_PROD.ADVA.ATT_MTA_ROLLUP_V r
         JOIN EDW_PROD.URBN.DW_CALENDAR_HIERARCHY c
         ON r.Q_DT_ID = c.Q_DT_ID
         WHERE BRAND_CD = 'FP'
         AND r.Q_DT_ID>= '2021-02-01' AND r.Q_DT_ID<='2023-09-30'
        AND r.BRAND_MARKETING_CHANNEL LIKE ANY ('%PAID SOCIAL%', '%PAID SEARCH%','%AFFILIATES%','%DISPLAY%') AND r.BRAND_PARENT_MARKETING_CHANNEL = 'PAID'
        AND SITE_ID = 'US'
        GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)

         SELECT BRAND_MARKETING_CHANNEL, BRAND_SUBMARKETING_CHANNEL, YTD, SUM(DEMAND) AS MTA_DEMAND
         FROM FP_MTA_DATA
         GROUP BY 1,2,3
         ORDER BY 1,2,3
        ;
    """,
    'AN': """
        WITH AN_MTA_DATA AS
        ((SELECT 
        'AN' AS BRAND, 
        r.BRAND_MARKETING_CHANNEL,
        r.BRAND_SUBMARKETING_CHANNEL,
        CASE WHEN  r.Q_DT_ID >='2021-02-01' AND r.Q_DT_ID <='2021-10-09' THEN 'FY22 YTD'
         WHEN r.Q_DT_ID >='2022-02-01' AND r.Q_DT_ID <='2022-10-09' THEN 'FY23 YTD'
         WHEN r.Q_DT_ID >='2023-02-01' AND r.Q_DT_ID <='2023-10-09' THEN 'FY24 YTD'
         ELSE 'REM' END AS YTD,
         SUM(ATTRIBUTED_DEMAND_AMT) AS demand
         FROM  EDW_PROD.ADVA.ATT_MTA_ROLLUP_V r
         JOIN EDW_PROD.URBN.DW_CALENDAR_HIERARCHY c
         ON r.Q_DT_ID = c.Q_DT_ID
         WHERE BRAND_CD = 'AN'
         AND r.Q_DT_ID>= '2021-02-01' AND r.Q_DT_ID<='2022-10-09'
        AND r.BRAND_MARKETING_CHANNEL LIKE ANY ('%PAID SOCIAL%', '%PAID SEARCH%','%AFFILIATES%','%DISPLAY%') AND r.BRAND_PARENT_MARKETING_CHANNEL = 'PAID'
        GROUP BY 2,3,4
        ORDER BY 3,4,2)

        UNION ALL

        (SELECT 
        'AN' AS BRAND,
        r.BRAND_MARKETING_CHANNEL,
        r.BRAND_SUBMARKETING_CHANNEL,
        CASE WHEN  r.Q_DT_ID >='2021-02-01' AND r.Q_DT_ID <='2021-07-31' THEN 'FY22 YTD'
         WHEN r.Q_DT_ID >='2022-02-01' AND r.Q_DT_ID <='2022-07-31' THEN 'FY23 YTD'
         WHEN r.Q_DT_ID >='2023-02-01' AND r.Q_DT_ID <='2023-07-31' THEN 'FY24 YTD'
         ELSE 'REM' END AS YTD,
         SUM(ATTRIBUTED_DEMAND_AMT) AS demand
         FROM  EDW_PROD.ADVA.ATT_MTA_ROLLUP_V r
         JOIN EDW_PROD.URBN.DW_CALENDAR_HIERARCHY c
         ON r.Q_DT_ID = c.Q_DT_ID
         WHERE BRAND_CD IN ('AN','BH')
         AND r.Q_DT_ID>= '2022-10-10' AND r.Q_DT_ID<='2023-07-31'
        AND r.BRAND_MARKETING_CHANNEL LIKE ANY ('%PAID SOCIAL%', '%PAID SEARCH%','%AFFILIATES%','%DISPLAY%') AND r.BRAND_PARENT_MARKETING_CHANNEL = 'PAID'
        GROUP BY 2,3,4
        ORDER BY 3,4,2
         ))

         SELECT BRAND_MARKETING_CHANNEL, BRAND_SUBMARKETING_CHANNEL, YTD, SUM(DEMAND) AS MTA_DEMAND
         FROM AN_MTA_DATA
         GROUP BY 1,2,3
         ORDER BY 1,2,3
        
    """
    # Add more brands and their associated queries as needed
}

# Get user input for the brand
brand = input("Enter the brand (e.g., UO, AN, FP): ")

# Check if the entered brand exists in the dictionary
if brand in brand_queries:
    sql = brand_queries[brand]
else:
    print("Brand not found.")
    exit()

# Execute the SQL query
start = time.time()
cs = ctx.cursor()
cs.execute(sql)
df_raw_ytd = pd.DataFrame(cs.fetchall(), columns=[x[0] for x in cs.description])
end = time.time()

# Print the total time taken for execution
print('Total time: {}'.format(end - start))


# In[48]:


df_raw_ytd


# In[49]:


# Assuming df_raw is defined earlier
if brand == 'AN':
    df_raw_ytd['BRAND_MARKETING_CHANNEL'] = df_raw_ytd['BRAND_SUBMARKETING_CHANNEL'].apply(map_brand_marketing_channel)
    pivot_table_ytd = pd.pivot_table(df_raw_ytd, 
                             values='MTA_DEMAND', 
                             index='BRAND_SUBMARKETING_CHANNEL', 
                             columns='YTD', 
                             aggfunc='sum')

    filtered_data_ytd = df_raw_ytd

    # Group by BRAND_SUBMARKETING_CHANNEL and sum MTA_DEMAND for each FY
    grouped_data_ytd = filtered_data_ytd.groupby(['BRAND_SUBMARKETING_CHANNEL', 'YTD'])['MTA_DEMAND'].sum().reset_index()

    # Pivot the grouped data to get FY values as new columns holding sum(MTA_DEMAND)
    pivot_grouped_data_ytd = grouped_data_ytd.pivot(index='BRAND_SUBMARKETING_CHANNEL', columns='YTD', values='MTA_DEMAND').fillna(0).reset_index()
else:
    pivot_table_ytd = pd.pivot_table(df_raw_ytd, 
                             values='MTA_DEMAND', 
                             index='BRAND_SUBMARKETING_CHANNEL', 
                             columns='YTD', 
                             aggfunc='sum')

    filtered_data_ytd = df_raw_ytd

    # Group by BRAND_SUBMARKETING_CHANNEL and sum MTA_DEMAND for each FY
    grouped_data_ytd = filtered_data_ytd.groupby(['BRAND_SUBMARKETING_CHANNEL', 'YTD'])['MTA_DEMAND'].sum().reset_index()

    # Pivot the grouped data to get FY values as new columns holding sum(MTA_DEMAND)
    pivot_grouped_data_ytd = grouped_data_ytd.pivot(index='BRAND_SUBMARKETING_CHANNEL', columns='YTD', values='MTA_DEMAND').fillna(0).reset_index()

pivot_grouped_data_ytd


# In[50]:


pivot_table_brand_ytd = pd.pivot_table(df_raw_ytd, 
                                   values='MTA_DEMAND', 
                                   index='BRAND_MARKETING_CHANNEL', 
                                   columns='YTD', 
                                   aggfunc='sum' 
                                   #fill_value=0
                                  )

pivot_table_brand_ytd = pivot_table_brand_ytd.reset_index()
pivot_table_brand_ytd


# In[51]:


def process_data_by_brand(brand, pivot_table_brand, pivot_grouped_data, pivot_table_brand_ytd, pivot_grouped_data_ytd):
    # Read the original CSV file
    df = pd.read_csv('FYSummaryDecomp_r11.csv')

    # Read the new CSV file
    new_df = pd.read_csv('YTDSummaryDecomp_r11.csv')

    # Concatenate the columns from the new file to the original DataFrame
    df = pd.concat([df, new_df.iloc[:, 1:]], axis=1)

    # Replace 'NA' values with 0
    df = df.fillna(0)

    # Filter rows that end with '_S'
    filtered_rows = df[df.iloc[:, 0].str.endswith('_S')]

    # Additional rows to add at the beginning
    additional_rows = df[df['variablenames'].isin(['AFFILIATES', 'DISPLAY', 'PAIDSEARCH', 'PAIDSOCIAL'])]

    # Reorder the DataFrame
    df1 = pd.concat([additional_rows, filtered_rows], ignore_index=True)

    desired_columns = [0, 1, 2, 8, 9, 4, 5, 11, 12]
    df_final = df1.iloc[:, desired_columns]
    
#     return pivot_table_brand
    # Default parameter values
    if pivot_table_brand is None:
        pivot_table_brand = pivot_table_brand
    if pivot_grouped_data is None:
        pivot_grouped_data = pivot_grouped_data
    if pivot_table_brand_ytd is None:
        pivot_table_brand_ytd = pivot_table_brand_ytd
    if pivot_grouped_data_ytd is None:
        pivot_grouped_data_ytd = pivot_grouped_data_ytd
    
    # Your common code that is shared between both brand cases
    df_final2 = df_final.copy()
    
##################
    df_final3 = df_final2.copy()

    df_final3.insert(3, 'New_Column_1', (df_final2.iloc[:, 2] - df_final2.iloc[:, 1]) / df_final2.iloc[:, 1])
    df_final3.insert(6, 'New_Column_2', (df_final2.iloc[:, 4] - df_final2.iloc[:, 3]) / df_final2.iloc[:, 3])
    df_final3.insert(9, 'New_Column_3', (df_final2.iloc[:, 6] - df_final2.iloc[:, 5]) / df_final2.iloc[:, 5])
    df_final3.insert(12, 'New_Column_4', (df_final2.iloc[:, 8] - df_final2.iloc[:, 7]) / df_final2.iloc[:, 7])
    df_final3.insert(13, 'New_Column_5', (df_final2.iloc[:, 1]) / df_final2.iloc[:, 5])
    df_final3.insert(14, 'New_Column_6', (df_final2.iloc[:, 2]) / df_final2.iloc[:, 6])
    df_final3.insert(15, 'New_Column_7', (df_final2.iloc[:, 3]) / df_final2.iloc[:, 7])
    df_final3.insert(16, 'New_Column_8', (df_final2.iloc[:, 4]) / df_final2.iloc[:, 8])
    
    # Create df_final3 by copying df_final2
    df_final15 = df_final3.copy()
    new_column_names = ['New_Column_9', 'New_Column_10', 'New_Column_11', 
                       'New_Column_12', 'New_Column_13', 'New_Column_14']

    # Insert new columns at specified indices
    for i, col_name in enumerate(new_column_names, 1):
        df_final15.insert(6 + i, col_name, 0)

    #df_final5 = df_final15.copy()
    df_final15.iloc[0, 7] = pivot_table_brand.iloc[0,1]
    df_final15.iloc[0, 8] = pivot_table_brand.iloc[0,2]
    df_final15.iloc[1, 7] = pivot_table_brand.iloc[1,1]
    df_final15.iloc[1, 8] = pivot_table_brand.iloc[1,2]
    df_final15.iloc[2, 7] = pivot_table_brand.iloc[2,1]
    df_final15.iloc[2, 8] = pivot_table_brand.iloc[2,2]
    df_final15.iloc[3, 7] = pivot_table_brand.iloc[3,1]
    df_final15.iloc[3, 8] = pivot_table_brand.iloc[3,2]

    # Code specific to each brand
    if brand == 'UO':
        df_final15.iloc[7, 7] = pivot_grouped_data.iloc[9,1]
        df_final15.iloc[7, 8] = pivot_grouped_data.iloc[9,2]
        df_final15.iloc[5, 7] = pivot_grouped_data.iloc[3,1]
        df_final15.iloc[5, 8] = pivot_grouped_data.iloc[3,2]
        df_final15.iloc[4, 7] = pivot_grouped_data.iloc[1,1]
        df_final15.iloc[4, 8] = pivot_grouped_data.iloc[1,2]
        df_final15.iloc[6, 7] = pivot_grouped_data.iloc[4,1]
        df_final15.iloc[6, 8] = pivot_grouped_data.iloc[4,2]
        df_final15.iloc[8, 7] = pivot_grouped_data.iloc[19,1]
        df_final15.iloc[8, 8] = pivot_grouped_data.iloc[19,2]
        df_final15.iloc[10, 7] = pivot_grouped_data.iloc[16,1]
        df_final15.iloc[10, 8] = pivot_grouped_data.iloc[16,2]
        df_final15.iloc[11, 7] = pivot_grouped_data.iloc[17,1]
        df_final15.iloc[11, 8] = pivot_grouped_data.iloc[17,2]
        df_final15.iloc[20, 7] = pivot_grouped_data.iloc[13,1]
        df_final15.iloc[20, 8] = pivot_grouped_data.iloc[13,2]
        df_final15.iloc[21, 7] = pivot_grouped_data.iloc[14,1]
        df_final15.iloc[21, 8] = pivot_grouped_data.iloc[14,2]
        df_final15.iloc[13, 7] = pivot_grouped_data.iloc[7,1]
        df_final15.iloc[13, 8] = pivot_grouped_data.iloc[7,2]
        df_final15.iloc[12, 7] = pivot_grouped_data.iloc[8,1]
        df_final15.iloc[12, 8] = pivot_grouped_data.iloc[8,2]
        df_final15.iloc[14, 7] = pivot_grouped_data.iloc[7,1]
        df_final15.iloc[14, 8] = pivot_grouped_data.iloc[7,2]
        df_final15.iloc[16, 7] = pivot_grouped_data.iloc[2,1]
        df_final15.iloc[16, 8] = pivot_grouped_data.iloc[2,2]
        df_final15.iloc[17, 7] = pivot_grouped_data.iloc[10,1]
        df_final15.iloc[17, 8] = pivot_grouped_data.iloc[10,2]
        df_final15.iloc[18, 7] = pivot_grouped_data.iloc[15,1]
        df_final15.iloc[18, 8] = pivot_grouped_data.iloc[15,2]
        df_final15.iloc[19, 7] = pivot_grouped_data.iloc[12,1]
        df_final15.iloc[19, 8] = pivot_grouped_data.iloc[12,2]
        df_final15.iloc[22, 7] = pivot_grouped_data.iloc[18,1]
        df_final15.iloc[22, 8] = pivot_grouped_data.iloc[18,2]
        df_final15.iloc[23, 7] = pivot_grouped_data.iloc[20,1]
        df_final15.iloc[23, 8] = pivot_grouped_data.iloc[20,2]
        df_final15.iloc[25, 7] = pivot_grouped_data.iloc[21,1]
        df_final15.iloc[25, 8] = pivot_grouped_data.iloc[21,2]
    
    elif brand == 'FP':
        df_final15.iloc[4, 7] = pivot_grouped_data.iloc[0,1]
        df_final15.iloc[4, 8] = pivot_grouped_data.iloc[0,2]
        df_final15.iloc[5, 7] = pivot_grouped_data.iloc[2,1]
        df_final15.iloc[5, 8] = pivot_grouped_data.iloc[2,2]
        df_final15.iloc[6, 7] = pivot_grouped_data.iloc[3,1]
        df_final15.iloc[6, 8] = pivot_grouped_data.iloc[3,2]
        df_final15.iloc[7, 7] = pivot_grouped_data.iloc[5,1]
        df_final15.iloc[7, 8] = pivot_grouped_data.iloc[5,2]
        df_final15.iloc[8, 7] = pivot_grouped_data.iloc[1,1]
        df_final15.iloc[8, 8] = pivot_grouped_data.iloc[1,2]
        df_final15.iloc[11, 7] = pivot_grouped_data.iloc[6,1]
        df_final15.iloc[11, 8] = pivot_grouped_data.iloc[6,2]
        df_final15.iloc[12, 7] = pivot_grouped_data.iloc[7,1]
        df_final15.iloc[12, 8] = pivot_grouped_data.iloc[7,2]
        df_final15.iloc[16, 7] = pivot_grouped_data.iloc[10,1]
        df_final15.iloc[16, 8] = pivot_grouped_data.iloc[10,2]
        df_final15.iloc[17, 7] = pivot_grouped_data.iloc[9,1]
        df_final15.iloc[17, 8] = pivot_grouped_data.iloc[9,2]
        df_final15.iloc[18, 7] = pivot_grouped_data.iloc[14,1]
        df_final15.iloc[18, 8] = pivot_grouped_data.iloc[14,2]
      
    elif brand == 'AN':
        df_final15.iloc[7, 7] = pivot_grouped_data.iloc[5,1]
        df_final15.iloc[7, 8] = pivot_grouped_data.iloc[5,2]
        df_final15.iloc[5, 7] = pivot_grouped_data.iloc[2,1]
        df_final15.iloc[5, 8] = pivot_grouped_data.iloc[2,2]
        df_final15.iloc[4, 7] = pivot_grouped_data.iloc[0,1]
        df_final15.iloc[4, 8] = pivot_grouped_data.iloc[0,2]
        df_final15.iloc[6, 7] = pivot_grouped_data.iloc[3,1]
        df_final15.iloc[6, 8] = pivot_grouped_data.iloc[3,2]
        df_final15.iloc[22, 7] = pivot_grouped_data.iloc[15,1]
        df_final15.iloc[22, 8] = pivot_grouped_data.iloc[15,2]
        df_final15.iloc[23, 7] = pivot_grouped_data.iloc[20,1]
        df_final15.iloc[23, 8] = pivot_grouped_data.iloc[20,2]
        df_final15.iloc[11, 7] = pivot_grouped_data.iloc[7,1]
        df_final15.iloc[11, 8] = pivot_grouped_data.iloc[7,2]
        df_final15.iloc[12, 7] = pivot_grouped_data.iloc[8,1]
        df_final15.iloc[12, 8] = pivot_grouped_data.iloc[8,2]
        df_final15.iloc[17, 7] = pivot_grouped_data.iloc[9,1]
        df_final15.iloc[17, 8] = pivot_grouped_data.iloc[9,2]
        df_final15.iloc[19, 7] = pivot_grouped_data.iloc[10,1]
        df_final15.iloc[19, 8] = pivot_grouped_data.iloc[10,2]
        df_final15.iloc[24, 7] = pivot_grouped_data.iloc[16,1]
        df_final15.iloc[24, 8] = pivot_grouped_data.iloc[16,2]
        
    #df_final15.iloc[0:, 9] = (df_final15.iloc[0:, 8] - df_final15.iloc[0:, 7])/df_final15.iloc[0:, 7]
    # Avoiding division by zero
    denominator = df_final15.iloc[:, 7]
    numerator = df_final15.iloc[:, 8] - df_final15.iloc[:, 7]

    # Avoiding division by zero, set division result to zero where denominator is zero
    mask = denominator != 0
    division_result = np.zeros_like(denominator)
    division_result[mask] = numerator[mask] / denominator[mask]

    df_final15.iloc[:, 9] = division_result
#     return df_final15
    
    df_final15.iloc[0, 10] = pivot_table_brand_ytd.iloc[0,2]
    df_final15.iloc[0, 11] = pivot_table_brand_ytd.iloc[0,3]
    df_final15.iloc[1, 10] = pivot_table_brand_ytd.iloc[1,2]
    df_final15.iloc[1, 11] = pivot_table_brand_ytd.iloc[1,3]
    df_final15.iloc[2, 10] = pivot_table_brand_ytd.iloc[2,2]
    df_final15.iloc[2, 11] = pivot_table_brand_ytd.iloc[2,3]
    df_final15.iloc[3, 10] = pivot_table_brand_ytd.iloc[3,2]
    df_final15.iloc[3, 11] = pivot_table_brand_ytd.iloc[3,3]
    
    if brand == 'UO':
        df_final15.iloc[7, 10] = pivot_grouped_data_ytd.iloc[9,2]
        df_final15.iloc[7, 11] = pivot_grouped_data_ytd.iloc[9,3]
        df_final15.iloc[5, 10] = pivot_grouped_data_ytd.iloc[3,2]
        df_final15.iloc[5, 11] = pivot_grouped_data_ytd.iloc[3,3]
        df_final15.iloc[4, 10] = pivot_grouped_data_ytd.iloc[1,2]
        df_final15.iloc[4, 11] = pivot_grouped_data_ytd.iloc[1,3]
        df_final15.iloc[6, 10] = pivot_grouped_data_ytd.iloc[4,2]
        df_final15.iloc[6, 11] = pivot_grouped_data_ytd.iloc[4,3]
        df_final15.iloc[8, 10] = pivot_grouped_data_ytd.iloc[19,2]
        df_final15.iloc[8, 11] = pivot_grouped_data_ytd.iloc[19,3]
        df_final15.iloc[10, 10] = pivot_grouped_data_ytd.iloc[16,2]
        df_final15.iloc[10, 11] = pivot_grouped_data_ytd.iloc[16,3]
        df_final15.iloc[11, 10] = pivot_grouped_data_ytd.iloc[17,2]
        df_final15.iloc[11, 11] = pivot_grouped_data_ytd.iloc[17,3]
        df_final15.iloc[20, 10] = pivot_grouped_data_ytd.iloc[13,2]
        df_final15.iloc[20, 11] = pivot_grouped_data_ytd.iloc[13,3]
        df_final15.iloc[21, 10] = pivot_grouped_data_ytd.iloc[14,2]
        df_final15.iloc[21, 11] = pivot_grouped_data_ytd.iloc[14,3]
        df_final15.iloc[13, 10] = pivot_grouped_data_ytd.iloc[7,2]
        df_final15.iloc[13, 11] = pivot_grouped_data_ytd.iloc[7,3]
        df_final15.iloc[12, 10] = pivot_grouped_data_ytd.iloc[8,2]
        df_final15.iloc[12, 11] = pivot_grouped_data_ytd.iloc[8,3]
        df_final15.iloc[14, 10] = pivot_grouped_data_ytd.iloc[7,2]
        df_final15.iloc[14, 11] = pivot_grouped_data_ytd.iloc[7,3]
        df_final15.iloc[16, 10] = pivot_grouped_data_ytd.iloc[2,2]
        df_final15.iloc[16, 11] = pivot_grouped_data_ytd.iloc[2,3]
        df_final15.iloc[17, 10] = pivot_grouped_data_ytd.iloc[10,2]
        df_final15.iloc[17, 11] = pivot_grouped_data_ytd.iloc[10,3]
        df_final15.iloc[18, 10] = pivot_grouped_data_ytd.iloc[15,2]
        df_final15.iloc[18, 11] = pivot_grouped_data_ytd.iloc[15,3]
        df_final15.iloc[19, 10] = pivot_grouped_data_ytd.iloc[12,2]
        df_final15.iloc[19, 11] = pivot_grouped_data_ytd.iloc[12,3]
        df_final15.iloc[22, 10] = pivot_grouped_data_ytd.iloc[18,2]
        df_final15.iloc[22, 11] = pivot_grouped_data_ytd.iloc[18,3]
        df_final15.iloc[23, 10] = pivot_grouped_data_ytd.iloc[20,2]
        df_final15.iloc[23, 11] = pivot_grouped_data_ytd.iloc[20,3]
        df_final15.iloc[25, 10] = pivot_grouped_data_ytd.iloc[21,2]
        df_final15.iloc[25, 11] = pivot_grouped_data_ytd.iloc[21,3]
    
    elif brand == 'FP':
        df_final15.iloc[4, 10] = pivot_grouped_data_ytd.iloc[0,2]
        df_final15.iloc[4, 11] = pivot_grouped_data_ytd.iloc[0,3]
        df_final15.iloc[5, 10] = pivot_grouped_data_ytd.iloc[2,2]
        df_final15.iloc[5, 11] = pivot_grouped_data_ytd.iloc[2,3]
        df_final15.iloc[6, 10] = pivot_grouped_data_ytd.iloc[3,2]
        df_final15.iloc[6, 11] = pivot_grouped_data_ytd.iloc[3,3]
        df_final15.iloc[7, 10] = pivot_grouped_data_ytd.iloc[5,2]
        df_final15.iloc[7, 11] = pivot_grouped_data_ytd.iloc[5,3]
        df_final15.iloc[8, 10] = pivot_grouped_data_ytd.iloc[1,2]
        df_final15.iloc[8, 11] = pivot_grouped_data_ytd.iloc[1,3]
        df_final15.iloc[11, 10] = pivot_grouped_data_ytd.iloc[6,2]
        df_final15.iloc[11, 11] = pivot_grouped_data_ytd.iloc[6,3]
        df_final15.iloc[12, 10] = pivot_grouped_data_ytd.iloc[7,2]
        df_final15.iloc[12, 11] = pivot_grouped_data_ytd.iloc[7,3]
        df_final15.iloc[16, 10] = pivot_grouped_data_ytd.iloc[10,2]
        df_final15.iloc[16, 11] = pivot_grouped_data_ytd.iloc[10,3]
        df_final15.iloc[17, 10] = pivot_grouped_data_ytd.iloc[9,2]
        df_final15.iloc[17, 11] = pivot_grouped_data_ytd.iloc[9,3]
        df_final15.iloc[18, 10] = pivot_grouped_data_ytd.iloc[14,2]
        df_final15.iloc[18, 11] = pivot_grouped_data_ytd.iloc[14,3]
    
    elif brand == 'AN':
        df_final15.iloc[7, 10] = pivot_grouped_data.iloc[5,2]
        df_final15.iloc[7, 11] = pivot_grouped_data.iloc[5,3]
        df_final15.iloc[5, 10] = pivot_grouped_data.iloc[2,2]
        df_final15.iloc[5, 11] = pivot_grouped_data.iloc[2,3]
        df_final15.iloc[4, 10] = pivot_grouped_data.iloc[0,2]
        df_final15.iloc[4, 11] = pivot_grouped_data.iloc[0,3]
        df_final15.iloc[6, 10] = pivot_grouped_data.iloc[3,2]
        df_final15.iloc[6, 11] = pivot_grouped_data.iloc[3,3]
        df_final15.iloc[22, 10] = pivot_grouped_data.iloc[15,2]
        df_final15.iloc[22, 11] = pivot_grouped_data.iloc[15,3]
        df_final15.iloc[23, 10] = pivot_grouped_data.iloc[20,2]
        df_final15.iloc[23, 11] = pivot_grouped_data.iloc[20,3]
        df_final15.iloc[11, 11] = pivot_grouped_data.iloc[7,2]
        df_final15.iloc[11, 12] = pivot_grouped_data.iloc[7,3]
        df_final15.iloc[12, 10] = pivot_grouped_data.iloc[8,2]
        df_final15.iloc[12, 11] = pivot_grouped_data.iloc[8,3]
        df_final15.iloc[17, 10] = pivot_grouped_data.iloc[9,2]
        df_final15.iloc[17, 11] = pivot_grouped_data.iloc[9,3]
        df_final15.iloc[19, 10] = pivot_grouped_data.iloc[10,2]
        df_final15.iloc[19, 11] = pivot_grouped_data.iloc[10,3]
        df_final15.iloc[24, 10] = pivot_grouped_data.iloc[16,2]
        df_final15.iloc[24, 10] = pivot_grouped_data.iloc[16,3]
    
    #df_final15.iloc[0:, 12] = (df_final15.iloc[0:, 11] - df_final15.iloc[0:, 10])/df_final15.iloc[0:, 10]
    
    # Avoiding division by zero
    denominator = df_final15.iloc[:, 10]
    numerator = df_final15.iloc[:, 11] - df_final15.iloc[:, 10]

    # Avoiding division by zero, set division result to zero where denominator is zero
    mask = denominator != 0
    division_result = np.zeros_like(denominator)
    division_result[mask] = numerator[mask] / denominator[mask]

    df_final15.iloc[:, 12] = division_result
    
    # Write to Excel
    df_final15.to_csv(f'{brand}_Paid_Media_MMM_vs_MTA_ROAS.csv', index=False)
    
    return df_final15

# Call the function with the desired brand
brand = input("Enter the brand (e.g., UO, AN, FP): ")
df_final15 = process_data_by_brand(brand, pivot_table_brand, pivot_grouped_data, pivot_table_brand_ytd, pivot_grouped_data_ytd)


# In[52]:


df_final15


# ### MMM Session Report (Paid Media Demand %)

# In[53]:


def process_data_by_brand2(brand, df_final15):
    # Read the original CSV file
    df = pd.read_csv('FYSummaryDecomp_r11.csv')

    # Read the new CSV file
    new_df = pd.read_csv('YTDSummaryDecomp_r11.csv')

    # Concatenate the columns from the new file to the original DataFrame
    df = pd.concat([df, new_df.iloc[:, 1:]], axis=1)

    # Replace 'NA' values with 0
    df = df.fillna(0)

    # Additional rows to add at the beginning
    additional_rows = df[df['variablenames'].isin(['BASELINE', 'TOTALINCREMENTAL', 'MKTGOUTBOUND', 'PROMOS', 'UNPAID',
                          'PAIDMEDIA', 'AFFILIATES','DISPLAY', 'PAIDSEARCH', 'PAIDSOCIAL'])]

    # Filter rows that end with '_S'
    filtered_rows = df[~df['variablenames'].isin(additional_rows['variablenames']) & df['variablenames'].str.endswith('_S')]

    # Create custom categorical order
    category_order = ['BASELINE', 'TOTALINCREMENTAL', 'MKTGOUTBOUND', 'PROMOS', 'UNPAID', 'PAIDMEDIA', 'AFFILIATES', 'DISPLAY', 'PAIDSEARCH', 'PAIDSOCIAL']

    # Set custom categorical order
    additional_rows['variablenames'] = pd.Categorical(additional_rows['variablenames'], categories=category_order, ordered=True)

    # Reorder the DataFrame
    df1 = pd.concat([additional_rows.sort_values('variablenames'), filtered_rows], ignore_index=True)

    desired_columns = [0, 1, 2, 8, 9, 4, 5, 11, 12]
    df_final = df1.iloc[:, desired_columns]

    # Create df_final2 by copying df_final
    df_final2 = df_final.copy()

    # Calculate and add new columns
    df_final2.insert(3, 'New_Column_1', (df_final.iloc[:, 2] - df_final.iloc[:, 1]) / df_final.iloc[:, 1])
    df_final2.insert(6, 'New_Column_2', (df_final.iloc[:, 4] - df_final.iloc[:, 3]) / df_final.iloc[:, 3])
    df_final2.insert(9, 'New_Column_3', (df_final.iloc[:, 6] - df_final.iloc[:, 5]) / df_final.iloc[:, 5])
    df_final2.insert(12, 'New_Column_4', (df_final.iloc[:, 8] - df_final.iloc[:, 7]) / df_final.iloc[:, 7])

    # Create df_final3 by copying df_final2
    df_final3 = df_final2.copy()

    # Calculate the sums for the denominator
    denominator_1 = df_final2.iloc[:2, 1].sum()
    denominator_2 = df_final2.iloc[:2, 2].sum()
    denominator_3 = df_final2.iloc[:2, 4].sum()
    denominator_4 = df_final2.iloc[:2, 5].sum()

    # Insert new columns after column index 6 of df_final2
    df_final3.insert(7, 'New_Column_5', df_final2.iloc[:, 1] / denominator_1)
    df_final3.insert(8, 'New_Column_6', df_final2.iloc[:, 2] / denominator_2)
    df_final3.insert(9, 'New_Column_7', df_final2.iloc[:, 4] / denominator_3)
    df_final3.insert(10, 'New_Column_8', df_final2.iloc[:, 5] / denominator_4)

    # Create df_final4 by copying df_final3
    df_final4 = df_final3.copy()

    # Create a new row with label 'TOTAL'
    total_row = pd.DataFrame({'variablenames': ['TOTAL']})

    # Concatenate the new row to the beginning of df_final4
    df_final4 = pd.concat([total_row, df_final4], ignore_index=True)

    # Calculate the sum for column index 1 (excluding the TOTAL row)
    total_column_1_sum = df_final4.iloc[1:3, 1].sum()
    total_column_2_sum = df_final4.iloc[1:3, 2].sum()
    total_column_4_sum = df_final4.iloc[1:3, 4].sum()
    total_column_5_sum = df_final4.iloc[1:3, 5].sum()

    # Update the values for the TOTAL row
    df_final4.iloc[0, 1] = total_column_1_sum
    df_final4.iloc[0, 2] = total_column_2_sum
    df_final4.iloc[0, 4] = total_column_4_sum
    df_final4.iloc[0, 5] = total_column_5_sum

    df_final4.iloc[0, 3] = (total_column_2_sum - total_column_1_sum)/total_column_1_sum
    df_final4.iloc[0, 6] = (total_column_5_sum - total_column_4_sum)/total_column_4_sum
    df_final4.iloc[0, 7] = total_column_1_sum/total_column_1_sum
    df_final4.iloc[0, 8] = total_column_2_sum/total_column_2_sum
    df_final4.iloc[0, 9] = total_column_4_sum/total_column_4_sum
    df_final4.iloc[0, 10] = total_column_5_sum/total_column_5_sum

    df_final4.iloc[6, 11] = df_final4.iloc[7:11, 11].sum()
    df_final4.iloc[6, 12] = df_final4.iloc[7:11, 12].sum()
    df_final4.iloc[6, 13] = (df_final4.iloc[7:11, 12].sum() - df_final4.iloc[7:11, 11].sum())/df_final4.iloc[7:11, 11].sum()
    df_final4.iloc[6, 14] = df_final4.iloc[7:11, 14].sum()
    df_final4.iloc[6, 15] = df_final4.iloc[7:11, 15].sum()
    df_final4.iloc[6, 16] = (df_final4.iloc[7:11, 15].sum() - df_final4.iloc[7:11, 14].sum())/df_final4.iloc[7:11, 14].sum()

    df_final5 = df_final4.copy()

    new_column_values = df_final4.iloc[6:, 11] / df_final4.iloc[6, 11]
    new_column_values1 = df_final4.iloc[6:, 12] / df_final4.iloc[6, 12]
    new_column_values2 = df_final4.iloc[6:, 14] / df_final4.iloc[6, 14]
    new_column_values3 = df_final4.iloc[6:, 15] / df_final4.iloc[6, 15]

    # # Insert the new column after column index 10
    df_final5.insert(11, 'New_Column_10', new_column_values)
    df_final5.insert(12, 'New_Column_11', new_column_values1)
    df_final5.insert(13, 'New_Column_12', new_column_values2)
    df_final5.insert(14, 'New_Column_13', new_column_values3)

    df_final6 = df_final5.copy()

    new_column_names_1 = ['New_Column_14', 'New_Column_15', 'New_Column_16', 
                        'New_Column_17']

    # Insert new columns at specified indices
    for i, col_name in enumerate(new_column_names_1, 1):
        df_final6.insert(10 + i, col_name, 0)
    
    # Convert decimal.Decimal objects to float
    df_final15_values = df_final15.iloc[0:, 7].astype(float)
    df_final5_value = float(df_final5.iloc[0,1])
    
    # Perform division operation
    df_final6.iloc[7:, 11] = df_final15_values / df_final5_value
    
    # Convert decimal.Decimal objects to float for other divisions
    df_final15_values_8 = df_final15.iloc[0:, 8].astype(float)
    df_final5_value_2 = float(df_final5.iloc[0, 2])

    # Perform division operation
    df_final6.iloc[7:, 12] = df_final15_values_8 / df_final5_value_2

    # Convert decimal.Decimal objects to float for the next division
    df_final15_values_10 = df_final15.iloc[0:, 10].astype(float)
    df_final5_value_4 = float(df_final5.iloc[0, 4])

    # Perform division operation
    df_final6.iloc[7:, 13] = df_final15_values_10 / df_final5_value_4

    # Convert decimal.Decimal objects to float for the next division
    df_final15_values_11 = df_final15.iloc[0:, 11].astype(float)
    df_final5_value_5 = float(df_final5.iloc[0, 5])

    # Perform division operation
    df_final6.iloc[7:, 14] = df_final15_values_11 / df_final5_value_5
 
    df_final6.iloc[6, 11] = df_final6.iloc[7:11, 11].sum()
    df_final6.iloc[6, 12] = df_final6.iloc[7:11, 12].sum()
    df_final6.iloc[6, 13] = df_final6.iloc[7:11, 13].sum()
    df_final6.iloc[6, 14] = df_final6.iloc[7:11, 14].sum()


    # Display the new DataFrame
    return df_final6

    # Write to Excel
    df_final6.to_csv(f'{brand}_Paid_Media_MMM%.csv', index=False)
    
# Call the function with the desired brand
brand = input("Enter the brand (e.g., UO, AN, FP): ")
df_final6 = process_data_by_brand2(brand, df_final15)


# In[54]:


df_final6


# ### MMM Session Report (Mktg Outbound + Unpaid Results)

# In[55]:


def process_data_by_brand3(brand, df_final6):
    # Read the original CSV file
    df = pd.read_csv('FYSummaryDecomp_r11.csv')

    # Read the new CSV file
    new_df = pd.read_csv('YTDSummaryDecomp_r11.csv')

    # Concatenate the columns from the new file to the original DataFrame
    df = pd.concat([df, new_df.iloc[:, 1:]], axis=1)

    # Replace 'NA' values with 0
    df = df.fillna(0)

    filtered_rows = df[df.iloc[:, 0].str.endswith('_I')]

    # Reorder the DataFrame
    df1 = pd.concat([filtered_rows], ignore_index=True)

    desired_columns = [0, 1, 2, 8, 9, 4, 5, 11, 12]
    df_final = df1.iloc[:, desired_columns]

    # Create df_final2 by copying df_final
    df_final2 = df_final.copy()

    df_final2.insert(3, 'New_Column_1', (df_final.iloc[:, 2] - df_final.iloc[:, 1]) / df_final.iloc[:, 1])
    df_final2.insert(6, 'New_Column_2', (df_final.iloc[:, 4] - df_final.iloc[:, 3]) / df_final.iloc[:, 3])
    df_final2.insert(9, 'New_Column_3', (df_final.iloc[:, 6] - df_final.iloc[:, 5]) / df_final.iloc[:, 5])
    df_final2.insert(12, 'New_Column_4', (df_final.iloc[:, 8] - df_final.iloc[:, 7]) / df_final.iloc[:, 7])

    df_final3 = df_final2.copy()

    # Insert new columns after column index 6 of df_final2
    df_final3.insert(7, 'New_Column_5', df_final3.iloc[:, 1] / df_final6.iloc[0, 1])
    df_final3.insert(8, 'New_Column_6', df_final3.iloc[:, 2] / df_final6.iloc[0, 2])
    df_final3.insert(9, 'New_Column_7', df_final3.iloc[:, 4] / df_final6.iloc[0, 4])
    df_final3.insert(10, 'New_Column_8', df_final3.iloc[:, 5] / df_final6.iloc[0, 5])

    # Create a new row with label 'TOTAL'
    total_row = pd.DataFrame({'variablenames': ['TOTAL']})

    # Concatenate the new row to the beginning of df_final4
    df_final3 = pd.concat([total_row, df_final3], ignore_index=True)

    df_final3.iloc[0, 1] = df_final6.iloc[0, 1]
    df_final3.iloc[0, 2] = df_final6.iloc[0, 2]
    df_final3.iloc[0, 3] = df_final6.iloc[0, 3]
    df_final3.iloc[0, 4] = df_final6.iloc[0, 4]
    df_final3.iloc[0, 5] = df_final6.iloc[0, 5]
    df_final3.iloc[0, 6] = df_final6.iloc[0, 6]
    df_final3.iloc[0, 7] = df_final6.iloc[0, 7]
    df_final3.iloc[0, 8] = df_final6.iloc[0, 8]
    df_final3.iloc[0, 9] = df_final6.iloc[0, 9]
    df_final3.iloc[0, 10] = df_final6.iloc[0, 10]


    df_final4 = df_final3.copy()

    # df_final4.iloc[0, 11] = df_final4.iloc[1:10, 11].sum()
    df_final4.iloc[0, 11] = df_final4.iloc[1:len(df_final4), 11].sum()
    df_final4.iloc[0, 12] = df_final4.iloc[1:len(df_final4), 12].sum()
    df_final4.iloc[0, 13] = (df_final4.iloc[1:len(df_final4), 12].sum() - df_final4.iloc[1:len(df_final4), 11].sum())/df_final4.iloc[1:len(df_final4), 11].sum()
    df_final4.iloc[0, 14] = df_final4.iloc[1:len(df_final4), 14].sum()
    df_final4.iloc[0, 15] = df_final4.iloc[1:len(df_final4), 15].sum()
    df_final4.iloc[0, 16] = (df_final4.iloc[1:len(df_final4), 15].sum() - df_final4.iloc[1:len(df_final4), 14].sum())/df_final4.iloc[1:len(df_final4), 14].sum()
    
    return df_final4

    # Write to Excel
    df_final4.to_csv(f'{brand}_Mktg_Outbound_+_Unpaid.csv', index=False)
    
# Call the function with the desired brand
brand = input("Enter the brand (e.g., UO, AN, FP): ")
df_final4 = process_data_by_brand3(brand, df_final6)


# In[56]:


df_final4


# In[ ]:




