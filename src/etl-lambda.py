import json
import boto3 
import os
import csv

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket = 'de-x3-lle-mars'
    key = '2022/3/16/chesterfield_16-03-2022_09-00-00.csv'
    filename = s3.get_object(Bucket=bucket, Key =key)
    
    lines = filename['Body'].read().decode('utf-8').splitlines(True)
    reader = csv.reader(lines)
    
    data_list = []

    for row in reader:
            
        row_dict = {
                    'date_time':row[0],
                    'branch_name': row[1],
                    'customer_name': row[2],
                    'basket_items': row[3],
                    'total_amount': float(row[4]),
                    'payment_method': row[5],
                    'card_number': row[6]
                }
  
        data_list.append(row_dict)
        
    clean_data_list = clean_and_transform(data_list)
    
    for item in clean_data_list:
        print(item)


def clean_and_transform(raw_data_list):
       
    clean_data_list = []

    for raw_data in raw_data_list:

        date , time = (raw_data.get('date_time')).split(' ')
        clean_time = f'{time}:00'
        day , month , year = date.split('/')
        clean_date = f'{year}-{month}-{day}'

        clean_data_item= {
            'date':clean_date,
            'time' : clean_time,
            'branch_name': raw_data['branch_name'],
          # 'customer_name': raw_data['customer_name'],
            'basket_items':get_my_basket(raw_data) ,
            'total_amount':float(raw_data['total_amount']),
            'payment_method':raw_data['payment_method'],
          # 'card_number': raw_data['card_number']
        }
        
        
        clean_data_list.append(clean_data_item)
    
    return clean_data_list
  

def get_my_basket(raw_data):
    
    
    basket_list = []   
    orders = (raw_data['basket_items']).strip('"').split(',')
    
    for order in orders:

        items_list= order.split(' - ')
        item_dict = {}
        item_dict['item_price'] = float(items_list[-1].strip()) 
        item_dict['item_flavour'] = 'Standard'

        if items_list[0].strip().startswith('Regular'):
            if 'Flavoured' in items_list[0]:
                item_dict['item_size'] = 'Regular'
                item_dict['item_flavour'] = (items_list[1]).title().strip()
                item_dict['item_name'] = ((items_list[0].split('Flavoured'))[1]).title().strip()

            else:
                item_dict['item_size'] ='Regular'
                item_dict['item_name'] = (items_list[0][8:]).title().strip()

        elif items_list[0].strip().startswith('Large'):
            if 'Flavoured' in (items_list[0]):
                item_dict['item_size'] ='Large'
                item_dict['item_flavour'] = (items_list[1]).title().strip()
                item_dict['item_name'] = ((str(items_list[0]).split('Flavoured'))[1]).title().strip()
            else:
                item_dict['item_size'] ='Large'
                item_dict['item_name'] = (items_list[0][6:]).title().strip()
                
        basket_list.append(item_dict)
        
    return basket_list