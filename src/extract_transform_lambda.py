import boto3 
import os
import csv
import urllib
import json
from datetime import datetime
import logging
import sys




sqs = boto3.client("sqs", endpoint_url="https://sqs.eu-west-1.amazonaws.com")

url = "https://sqs.eu-west-1.amazonaws.com/980326165877/Mars-Queue-2.fifo"

s3 = boto3.client('s3')

def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
    
    
        response = s3.get_object(Bucket = bucket, Key = key )
        lines = response['Body'].read().decode('utf-8').splitlines(True)
        reader = csv.reader(lines)
        raw_data_list = reading_raw_data(reader)
        clean_data_list = clean_and_transform(raw_data_list)
        chunks = split_data(clean_data_list , 20)
        
        counter = 0
        
        for chunk in chunks:
            
            print('About to send msg to SQS')
            sqs.send_message(QueueUrl = url, MessageBody = json.dumps(chunk), MessageGroupId=f'message{counter}')
            counter+=1
            print(f'SQS msg number {counter} has been sent ')

    except Exception as ex:
        
        print(f"Exception raised in lambda_handler. Exception details: {ex}")
        raise ex  
    

def split_data(transaction_list, max_length):

    return [ transaction_list[i:i+max_length] for i in range(0, len(transaction_list), max_length)]
    
    

def reading_raw_data(file_content):
    
    data_list = []
    
    for row in file_content:

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
    return data_list
    

 
def clean_and_transform(raw_data_list):
       
    clean_data_list = []

    order_number = 1

    for raw_data in raw_data_list:
        
        date , time = (raw_data.get('date_time')).split(' ')
        clean_time = f'{time}:00'
        day , month , year = date.split('/')
        clean_date = f'{year}-{month}-{day}'
        time_for_id = clean_time.replace(':','-')
        transaction_id =  raw_data['branch_name'] + clean_date + time_for_id + str(order_number)
        
        clean_data_item= {
            'transaction_id': transaction_id ,
            'date':clean_date,
            'time' : clean_time,
            'branch_name': raw_data['branch_name'],
         
            'basket_items':get_my_basket(raw_data) ,
            'total_amount':float(raw_data['total_amount']),
            'payment_method':raw_data['payment_method'],
         
        }
        order_number = order_number + 1
        clean_data_list.append(clean_data_item)

    return clean_data_list 



def get_my_basket(raw_data):
    
    
    basket_list = []   
    orders = (raw_data['basket_items']).strip('"').split(',')
    print("Starting Loop7: for order in orders:")
    
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
    print("Ending Loop7: for order in orders:")    
    return basket_list
        
  
    
