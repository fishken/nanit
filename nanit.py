import pandas as pd
from sqlalchemy import create_engine, insert, MetaData, Table, String, Column, Text, DateTime, Boolean, Integer, BigInteger, Float, ForeignKey

raw_data = pd.read_json (r'C:\Users\Home\Desktop\Nanit_BI_Test\shippingdata.json')
raw_data_df = pd.json_normalize(raw_data['Order'], max_level=0)
raw_data_df['OrderId'] = raw_data_df['OrderId'].astype('int64')
raw_data_df['ShippingAddressId']=None

##json parsing, df creation
shipping_addresses = pd.DataFrame()
for index, order in raw_data_df.iterrows():
    shipping_address = order['ShippingAddress']
    shipping_address['id'] = index
    raw_data_df.at[index, 'ShippingAddressId'] = shipping_address['id']
    shipping_address_df = pd.json_normalize(shipping_address,errors = 'ignore')
    shipping_addresses = pd.concat([shipping_addresses, shipping_address_df])

orders = raw_data_df.drop(['ShippingAddress','Dispatches', 'OrderLines'], axis=1)

dispatches_raw = pd.json_normalize(raw_data['Order'],record_path = 'Dispatches', meta = ['OrderId'],errors = 'ignore')
dispatches_raw.reset_index(inplace=True)
dispatches_raw.rename(columns={'index':'DispatchId'}, inplace=True)
dispatches = dispatches_raw.drop('DispatchedLines', axis=1)

order_lines = pd.json_normalize(raw_data['Order'],record_path = 'OrderLines', meta = ['OrderId'],errors = 'ignore')
order_lines.reset_index(inplace=True)
order_lines.rename(columns={'index':'OrderLineId'}, inplace=True)
order_lines['Quantity'] = order_lines['Quantity'].astype('int64')

dispatch_lines = pd.DataFrame()
for index, dispatch in dispatches_raw.iterrows():
    dispatch_line = pd.json_normalize(dispatch['DispatchedLines'], errors='ignore')
    dispatch_line['DispatchId'] = None
    dispatch_line['DispatchId'] = dispatch['DispatchId']
    dispatch_lines = pd.concat([dispatch_lines, dispatch_line])

dispatch_lines = dispatch_lines.explode('SerialNumbers')
dispatch_lines['Quantity'] = dispatch_lines['Quantity'].astype('int64')

##upload to mysql

sqlEngine = create_engine(
    'mysql+pymysql://nanit1111:nanit1111@nanit.chhrtv1dhb8f.us-east-2.rds.amazonaws.com:3306/orders')
sqlEngine.connect()

tables_list = ['dispatch_lines', 'dispatches', 'shipping_addresses', 'order_lines', 'orders']
for table in tables_list:
    drop_table = "DROP TABLE IF EXISTS {};".format(table)
    sqlEngine.execute(drop_table)

metadata = MetaData()
sqlEngine.execute('drop table if exists dispatches')
sqlEngine.execute('drop table if exists orders')
sqlEngine.execute('drop table if exists shipping_addresses')

orders_table = Table('orders', metadata,
                     Column('OrderId', BigInteger(), primary_key=True),
                     Column('CurrencyCode', String(4), nullable=False),
                     Column('OrderDate', DateTime(), nullable=False),
                     Column('OrderNumber', String(30), nullable=False),
                     Column('OrderSource', String(20)),
                     Column('Total', Float(7, 2)),
                     Column('TotalTax', Float(7, 2)),
                     Column('ShippingAddressId', Integer())
                     )

dispatches_table = Table('dispatches', metadata,
                         #   Column('DispatchId', BigInteger(), primary_key=True),
                         Column('DispatchId', Integer()),
                         Column('DispatchReference', String(20)),
                         Column('Carrier', String(5)),
                         Column('DispatchDate', DateTime(), nullable=False),
                         Column('TrackingNumber', BigInteger()),
                         Column('TrackingURL', String(30)),
                         Column('OrderId', ForeignKey('orders.OrderId'))
                         )

shipping_addresses_table = Table('shipping_addresses', metadata,
                                 # Column('id', Integer(), primary_key=True),
                                 Column('id', Integer()),
                                 Column('AddressLine1', String(30)),
                                 Column('AddressLine2', String(30)),
                                 Column('CountryCode', String(4)),
                                 Column('FirstName', String(20)),
                                 Column('LastName', String(20)),
                                 Column('Postcode', String(20)),
                                 Column('Town', String(20)),
                                 )

order_lines_table = Table('order_lines', metadata,
                          # Column('OrderLineId', Integer(), primary_key=True),
                          Column('OrderLineId', Integer()),
                          Column('ProductCode', String(6)),
                          Column('ProductDescription', String(30)),
                          Column('Quantity', Integer()),
                          Column('UnitCost', Float(7, 2)),
                          Column('OrderId', ForeignKey('orders.OrderId'))
                          )

dispatch_lines_table = Table('dispatch_lines', metadata,
                             # Column('SerialNumbers', String(20), primary_key=True),
                             Column('SerialNumbers', String(20)),
                             Column('ProductCode', String(6)),
                             Column('ProductDescription', String(30)),
                             Column('Quantity', Integer()),
                             # Column('DispatchId', ForeignKey('dispatches.DispatchId')
                             Column('DispatchId', Integer())

                             )

metadata.create_all(sqlEngine)

sqlEngine.execute(insert(orders_table), orders.to_dict(orient='records'))
sqlEngine.execute(insert(dispatches_table), dispatches.to_dict(orient='records'))
sqlEngine.execute(insert(shipping_addresses_table), shipping_addresses.to_dict(orient='records'))
sqlEngine.execute(insert(order_lines_table), order_lines.to_dict(orient='records'))
sqlEngine.execute(insert(dispatch_lines_table), dispatch_lines.to_dict(orient='records'))

