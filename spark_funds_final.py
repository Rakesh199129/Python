
# coding: utf-8

# # Checkpoint 1
# ## Getting and Cleaning data
# ### Here we load companies and rounds2 data using essential encoding, do the necessary cleaning, and merge them together to form master_frame which we use for furthur analysis. 

# In[89]:


# importing numpy and pandas

import numpy as np
import pandas as pd


# In[90]:


# loading companies data

companies = pd.read_csv("companies.txt", sep="\t", encoding = "latin1")
companies.head()


# In[91]:


# loading rounds2 data

rounds2 = pd.read_csv("rounds2.csv", encoding = "ISO-8859-1")
rounds2.head()


# In[92]:


# converting company_permalink of rounds2 entries into lower case
rounds2['company_permalink'] = rounds2.company_permalink.astype(str).str.lower()
rounds2.head()


# In[93]:


# converting permalink collumn of companies data into lower case
companies['permalink'] = companies.permalink.astype(str).str.lower()
companies.head()


# In[94]:


# removing special characters from permalink column of the companies datframe 

companies["permalink"] = companies["permalink"].str.encode("latin1").str.decode('ascii', 'ignore')


# In[95]:


# removing special characters from company_permalink column of rounds2 dataframe

rounds2["company_permalink"] = rounds2["company_permalink"].str.encode("ISO-8859-1").str.decode('ascii', 'ignore')


# In[96]:


# checking the number of unique values in permalink column of companies dataframe

companies[["permalink"]].nunique()


# In[97]:


# checking the number of unique values in company_permalink column of rounds2 dataframe

rounds2[["company_permalink"]].nunique()


# In[98]:


# checking an example row from rounds2 datframe whether special characters are removed

rounds2.loc[[113839]]


# In[99]:


# renaming rounds2 column from company_permalink to permalink

rounds2.rename(columns={'company_permalink':'permalink'}, inplace=True)
rounds2.head()


# In[100]:


rounds2.loc[~rounds2['permalink'].isin(companies['permalink']), :]


# In[101]:


# merging the two dataframes

master_frame = pd.merge(rounds2, companies, how='inner', on='permalink')
master_frame.head()


# In[102]:


# check how many rows have all missing values
master_frame.isnull().all(axis=1).sum()


# In[103]:


# checking for columns with null values

master_frame.isnull().sum()


# In[104]:


# summing up the missing values (columnwise)

round(100*(master_frame.isnull().sum()/len(master_frame.index)), 2)


# In[105]:


# dropping the column with highest percentage of missing values

master_frame = master_frame.drop('funding_round_code', axis=1)
master_frame.shape


# In[106]:


# dropping the column founded_at as part of data cleaning

master_frame = master_frame.drop('founded_at', axis=1)
master_frame.shape


# In[107]:


# removing the rows whose raised_amount_usd column has null values, using dropna

master_frame = master_frame.dropna(subset=['raised_amount_usd'])
master_frame.shape


# In[108]:


master_frame.head()


# ### There are 66368 unique companies in companies database and 66368 companies in rounds2 database. Here we have used the "permalink" column as unique key for each company in companies database. The merged dataframe,after deleting rows with null values, has 94959 observations totally.

# # Checkpoint 2
# ## Investment type analysis
# ### Here we try to figure out which type of investment is best for spark funds among the investment types venture, seed, angel and private_equity, where the funding amount lies between 5 to 15 million USD

# In[109]:


# forming group by for funding round type

fund_type = master_frame.groupby(['funding_round_type'])
fund_type.mean().sort_values(by = 'raised_amount_usd', ascending = False)


# In[110]:


# df for funding amount for venture type
# using dropna to remove the rows containing null values to estimate the average

fund_type_venture = master_frame.groupby(['funding_round_type']).get_group('venture')
fund_type_venture.dropna(axis = 0, subset = ['raised_amount_usd'])


# In[111]:


# calculating the average raised_amount_usd for venture type funding

fund_type_venture["raised_amount_usd"].mean()


# In[112]:


# df for funding amount for private equity type
# using dropna to remove the rows containing null values to estimate the average

fund_type_pvt = master_frame.groupby(['funding_round_type']).get_group('private_equity')
fund_type_pvt.dropna(axis = 0, subset = ['raised_amount_usd'])


# In[113]:


# calculating the average raised_amount_usd for private_equity type of investment

fund_type_pvt["raised_amount_usd"].mean()


# In[114]:


# df for funding amount for seed type
# using dropna to remove the rows containing null values to estimate the average

fund_type_seed = master_frame.groupby(['funding_round_type']).get_group('seed')
fund_type_seed.dropna(axis = 0, subset = ['raised_amount_usd'])


# In[115]:


# calculating the average raised_amount_usd for seed type of investment

fund_type_seed["raised_amount_usd"].mean()


# In[116]:


# df for funding amount for angel type
# using dropna to remove the rows containing null values to estimate the average

fund_type_angel = master_frame.groupby(['funding_round_type']).get_group('angel')
fund_type_pvt.dropna(axis = 0, subset = ['raised_amount_usd'])


# In[117]:


# calculating the average raised_amount_usd for angel type investment

fund_type_angel["raised_amount_usd"].mean()


# In[118]:


# exporting venture type as csv file for plotting
fund_type_venture.to_csv("fund_type_venture.csv")


# In[119]:


# exporting seed and pvtequity types as csv file for plotting

fund_type_seed.to_csv("fund_type_seed.csv")
fund_type_pvt.to_csv("fund-type_pvtequity.csv")


# ### So here we have observed that among funding_round_types Venture, seed, angel and private_equity, "venture" has the funding 11,748,949.13, that lies in the range 5 to 15 million USD. So the spark funds should choose "venture" type of funding.

# # Checkpoint 3 
# ## Country analysis
# ### Here we try to filter out the top 9 countries which have highest amount of funding for the "venture" type of investing.

# In[120]:


# getting the highest values from raised_amount_usd based on values from country_code column

fv = fund_type_venture.groupby('country_code')['raised_amount_usd'].sum().sort_values(ascending = False)
fv


# In[121]:


# selecting the top9 countries from the above df ftv and storing in df named top9

top9 = fv.head(9)
top9


# In[122]:


top9.to_csv("top9.csv")


# ### Here we have observed that top9 countries for the "venture" type of investment based on the maximum funding received, are of country code USA,CHN,GBR,IND,CAN,FRA,ISR,DEU,JPN
# ### So, clearly, the top 3 English speaking countries are of codes USA, GBR and IND 

# # Checkpoint 4
# ## Sector Analysis 1 

# ### So here we have obtained the dataframes for primary sector and main sector and merged them together to get the df having primary sectors as category_list, main sectors as sector, and value associated

# In[123]:


# loading mapping data

mappings = pd.read_csv("mapping.csv")
mappings


# In[124]:


# replacing the '0' from the entries by 'na' in the mappings data

mappings["category_list"] = mappings["category_list"].str.replace('0','na')
mappings


# In[125]:


# using melt function to create mappings2 containing sectors as a column

mappings1 = pd.melt(mappings, id_vars = ["category_list"], var_name = ["main_sectors"])
mappings1


# In[126]:


# Extracting main sectors for the given category_list primary sectors and remove blank space and blank column

mappings2 = mappings1.loc[mappings1['value'] ==1]
mappings2 = mappings2.drop('Blanks',axis=1)
mappings2 = mappings2.dropna(subset=['category_list'])
mappings2


# In[127]:


# extracting the primary sector from the category_list of the master frame, placed in a new column

master_frame1 = master_frame['category_list'].astype(str).apply(lambda x: x.split("|")[0])
master_frame1


# In[128]:


# concatenating the series master_frame1 with the master_frame which adds a new column primary_sector

master_frame = pd.concat([master_frame, master_frame1.rename('primary_sector')], axis=1)
master_frame


# In[129]:


master_frame.shape


# In[130]:


# merging mappings and master-frame 

masterframe_maps = pd.merge(master_frame, mappings2, how = 'inner', left_on = 'primary_sector', right_on = 'category_list')
masterframe_maps


# In[131]:


# dropping unnecessary column

masterframe_maps.drop('category_list_y', axis = 1, inplace = True)
masterframe_maps.head()


# In[132]:


# renaming category_list_x as category_list

masterframe_maps.rename(columns = {'category_list_x':'category_list'}, inplace = True)
masterframe_maps


# In[133]:


masterframe_maps.shape


# In[134]:


#exporting masterframe_maps as a csv file for plotting

masterframe_maps.to_csv("masterframe_maps.csv")


# # Checkpoint 5
# ## Sector Analysis 2
# 

# ### Analysis for USA

# In[135]:


# selecting venture type of investments for the country USA from the masterfrmae_maps

mf_usa = masterframe_maps.loc[(masterframe_maps['country_code'] == 'USA') & (masterframe_maps['funding_round_type'] == 'venture')]
mf_usa


# In[136]:


# extracting total investments for each main sector

mf_usa1 = mf_usa.groupby('main_sectors').size().sort_values(ascending = False).reset_index(name = 'count_invst')
mf_usa1


# In[137]:


# total number of investments(count)

total_invest = mf_usa1['count_invst'].sum()
print(total_invest)




# In[138]:


# extracting total amount invested in each main sector

mf_usa2 = mf_usa.groupby('main_sectors')['raised_amount_usd'].sum().sort_values(ascending = False)
mf_usa2


# In[139]:


# merging masterframe maps to mf_usa1

df_usa1 = pd.merge(masterframe_maps, mf_usa1, how = 'inner', on = 'main_sectors')
df_usa1


# In[140]:


# converting series mf_usa2 to dataframe and renaming column as total amount invst

df_usa2 = pd.Series.to_frame(mf_usa2)
df_usa3 = df_usa2.rename(columns={'raised_amount_usd':'total_amount_invst'})
df_usa3


# In[1]:


# calculating the total amount investments(USD)

df_usa3['total_amount_invst'].sum()

#calculating the highest investment company


# In[142]:


# creating D1 dataframe

D1 = pd.merge(df_usa1,df_usa3, how = 'inner', on = 'main_sectors')
D1


# ### Analysis for GBR

# In[143]:


# selecting venture type of investments for the country GBR from the masterfrmae_maps

mf_gbr = masterframe_maps.loc[(masterframe_maps['country_code'] == 'GBR') & (masterframe_maps['funding_round_type'] == 'venture')]
mf_gbr


# In[144]:


# extracting total investments for each main sector

mf_gbr1 = mf_gbr.groupby('main_sectors').size().sort_values(ascending = False).reset_index(name = 'count_invst')
mf_gbr1


# In[145]:


# extracting total amount invested in each main sector

mf_gbr2 = mf_gbr.groupby('main_sectors')['raised_amount_usd'].sum().sort_values(ascending = False)
mf_gbr2


# In[146]:


# total number of investments(count)

total_invest = mf_gbr1['count_invst'].sum()
print(total_invest)


# In[147]:


# merging masterframe_maps to mf_gbr1

df_gbr1 = pd.merge(masterframe_maps, mf_gbr1, how = 'inner', on = 'main_sectors')
df_gbr1


# In[148]:


# converting series mf_gbr2 to a dataframe and renaming column to total amount invst

df_gbr2 = pd.Series.to_frame(mf_gbr2)
df_gbr3 = df_gbr2.rename(columns={'raised_amount_usd':'total_amount_invst'})
df_gbr3


# In[149]:


# calculating total amount of investments(USD)

df_gbr3['total_amount_invst'].sum()


# In[150]:


# creating a dataframe D2

D2 = pd.merge(df_gbr1,df_gbr3, how = 'inner', on = 'main_sectors')
D2


# ### Analysis for IND

# In[151]:


# selecting venture type of investments for the country GBR from the masterfrmae_maps

mf_ind = masterframe_maps.loc[(masterframe_maps['country_code'] == 'IND') & (masterframe_maps['funding_round_type'] == 'venture')]
mf_ind


# In[152]:


# extracting total investments for each main sector

mf_ind1 = mf_ind.groupby('main_sectors').size().sort_values(ascending = False).reset_index(name = 'count_invst')
mf_ind1


# In[153]:


# extracting total amount invested in each main sector

mf_ind2 = mf_ind.groupby('main_sectors')['raised_amount_usd'].sum().sort_values(ascending = False)
mf_ind2


# In[154]:


# total number of investments(count)

total_invest = mf_ind1['count_invst'].sum()
print(total_invest)


# In[155]:


# merging masterframe maps to mf_ind1

df_ind1 = pd.merge(masterframe_maps, mf_ind1, how = 'inner', on = 'main_sectors')
df_ind1


# In[156]:


# converting series mf_ind2 as dataframe and renaming the column as total amount invst

df_ind2 = pd.Series.to_frame(mf_ind2)
df_ind3 = df_ind2.rename(columns={'raised_amount_usd':'total_amount_invst'})
df_ind3


# In[157]:


# finding the total amount of investment

total_amount_usd = df_ind3['total_amount_invst'].sum()
print(total_amount_usd)


# In[158]:


# creating D3 dataframe

D3 = pd.merge(df_ind1,df_ind3, how = 'inner', on = 'main_sectors')
D3


# ## End of Case Study
