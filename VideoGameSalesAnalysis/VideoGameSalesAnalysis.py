#%%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pandas import Series, DataFrame
import seaborn as sns
sns.set_style('whitegrid')

# Video Game Sales DataFrame
vgs_df = pd.read_csv('Raw Data.csv')
gp_df = pd.read_csv('game_prices.csv')



# change title of game column in gp_df to 'name'
gp_df['Name'] = gp_df['game']

# print(vgs_df.head())
vgs_gp_df = pd.merge(vgs_df, gp_df, on='Name')
# print(vgs_gp_df.info)
# See Columns
#print(vgs_gp_df.columns)
#print(vgs_gp_df.head())
# Check Video Games Sales
#vgs_df.info()
#vgs_df.head()

# Remove unnecessary columns
vgs_gp_df = vgs_gp_df.drop(['game', 'console', 'date(D/M/Y)'], axis=1)


print(vgs_gp_df.columns)

vgs_gp_df = DataFrame(vgs_gp_df)
# Check for null values

#Note: Null values found only in Year and Publisher

# See datatypes of DataFrame
#print(vgs_df.dtypes)

# Find Columns with missing values
#missing_vals = vgs_df.isnull().sum()

#vgs_df.head(10)
'''
# Iterate through each column in DataFrame
for column in vgs_df.columns:
    # If the column has missing_values
    if missing_vals[column] > 0:
        # Depending on dtype there will be changes on how null values are filled
        if vgs_df[column].dtype == int:
            # Fill null values with 0
            vgs_df[column] = vgs_df[column].fillna(0)
        elif vgs_df[column].dtype == float:
            vgs_df[column] = vgs_df[column].fillna(0)
        elif vgs_df[column].dtype == object:
            # Only for Publisher (fill Unknown)
            vgs_df[column] = vgs_df[column].fillna('Unkown')

# Check to see if there are anymore null values
#vgs_df[vgs_df.isnull().any(axis=1)]

vgs_df['Year'] = vgs_df['Year'].astype('int64')

print(vgs_df.dtypes)

vgs_df.to_csv('Video Games Sales Cleaned.csv', index=False)

vgs_df = pd.read_csv('Video Games Sales Cleaned.csv')

vgs_df.head()

year2016 = vgs_df[vgs_df['Game Title'].str.contains('Pok')]
year2016
'''
# %%
missing_vals = vgs_gp_df.isnull().sum()
print(missing_vals)
# %%
# For now too many missing values in 