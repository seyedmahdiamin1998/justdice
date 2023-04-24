from data_warehouse import DataWarehouse
import pandas as pd
#####################################################################
# Data Warehouse

DW = DataWarehouse()
DW.create_database()
DW.connection()
adspend_DW = DW.adspend(create=True)
installs_DW = DW.installs(create=True)
payouts_DW = DW.payouts(create=True)
revenue_DW = DW.revenue(create=True)

#####################################################################
# adspend

# df_adspend = pd.read_csv('../../data/adspend.csv')
# df_adspend['country_id'] = df_adspend.client_id.astype('string')
# df_adspend['network_id'] = df_adspend.client_id.astype('string')
# df_adspend['client_id'] = df_adspend.client_id.astype('string')

# for i in range(len(df_adspend)):
#     row = df_adspend.iloc[i,:]
#     DW.store_adspend(table=adspend_DW, item=row)

# del df_adspend
# print('adspend.csv stored')
#####################################################################
# installs
# df_installs = pd.read_csv('../../data/installs.csv')

# df_installs['country_id'] = df_installs.country_id.astype('string')
# df_installs['app_id'] = df_installs.app_id.astype('string')
# df_installs['network_id'] = df_installs.network_id.astype('string')
# df_installs['device_os_version'] = df_installs.device_os_version.astype('string')
# df_installs.drop_duplicates(inplace=True)

# for i in range(len(df_installs)):
#     row = df_installs.iloc[i,:]
#     DW.store_installs(table=installs_DW, item=row)

# del df_installs
# print('installs.csv stored')
#####################################################################
# payouts
# df_payouts = pd.read_csv('../../data/payouts.csv')

# for i in range(len(df_payouts)):
#     row = df_payouts.iloc[i,:]
#     DW.store_payouts(table=payouts_DW, item=row)

# del df_payouts
# print('payouts.csv stored')
#####################################################################
# revenue
df_revenue = pd.read_csv('../../data/revenue.csv')

for i in range(len(df_revenue)):
    row = df_revenue.iloc[i,:]
    DW.store_revenue(table=revenue_DW, item=row)

del df_revenue
print('revenue.csv stored')

