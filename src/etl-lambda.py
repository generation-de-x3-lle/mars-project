import psycopg2
import boto3 
import os
import csv

s3 = boto3.client('s3')

def lambda_handler(event, context):
    
    for record in event['Records']:
        
        bucket = record['s3']['bucket']['name']
        s3_key = record['s3']['object']['key']
        filename = s3.get_object(Bucket=bucket, Key = s3_key)

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
        
        clean_data_item = clean_and_transform(data_list)
    
        connection = connect_to_db()
        cursor = connection.cursor()
    
        connection.autocommit = True
    
        for transaction in clean_data_item:
        
            ins_sql = ''' Insert into Transactions (transaction_date, transaction_time, branch_name,total_amount,payment_method) 
            values (%s, %s, %s, %s,%s);'''

            sel_sql = '''SELECT transaction_id FROM transactions WHERE transaction_date = %s AND transaction_time = %s  and
            branch_name = %s and total_amount = %s and payment_method = %s ORDER BY transaction_id DESC LIMIT 1;'''

            cursor.execute(ins_sql,(transaction.get('date'),transaction.get('time'), transaction['branch_name'],transaction['total_amount'],
            transaction.get('payment_method')  ))
            cursor.execute(sel_sql,(transaction.get('date'), transaction.get('time'), transaction['branch_name'] ,transaction['total_amount'],
            transaction.get('payment_method')))
            transaction_id = cursor.fetchone()[0]
            connection.commit()

            insert_products_and_baskets(transaction_id,transaction['basket_items'])



def connect_to_db():
    
    host = os.getenv('host')
    user = os.getenv('user')
    password = os.getenv('password')
    database = os.getenv('database')
    port = os.getenv('port')

    
    connection = psycopg2.connect(
    host= host,
    user = user,
    port = port,
    password = password,
    database= database)
    
    return connection   


def insert_products_and_baskets(transaction_id , basket_items_list):

    for basket in basket_items_list:
    
        connection = connect_to_db()
        cursor = connection.cursor()
        fetch_query = f''' SELECT * FROM products where item_size ='{basket['item_size']}' and item_name = '{basket['item_name']}' and item_flavour = '{basket['item_flavour']}' 
        and item_price = {basket['item_price']} '''
        cursor.execute(fetch_query)
        items = cursor.fetchall()
        connection.close()

        for item in items:
            item_id = item[0]
            break

        if not(len(items)):
            
            insert_query = ''' insert into products (item_size, item_name, item_flavour, item_price) values (%s,%s,%s,%s )'''
            connection = connect_to_db()
            connection.autocommit = True
            cursor = connection.cursor()

            sel_sql = '''SELECT product_id FROM products WHERE item_size = %s AND item_name = %s  and
            item_flavour = %s and item_price = %s ORDER BY product_id DESC LIMIT 1;'''

            cursor.execute(insert_query,(basket['item_size'],basket['item_name'],basket['item_flavour'],basket['item_price'] ))
            cursor.execute(sel_sql,((basket['item_size'],basket['item_name'],basket['item_flavour'],basket['item_price'] )))
            product_id = cursor.fetchone()[0]
            connection.commit()

            insert_basket_query = f''' insert into baskets (transaction_id, product_id) 
                values ({transaction_id},{product_id})  ''' 
            commit_query(insert_basket_query)
        else:

            insert_basket_query = f''' insert into baskets (transaction_id, product_id) values ({transaction_id},{item_id})  ''' 
            commit_query(insert_basket_query)


def commit_query(query):

    connection = connect_to_db()
    cursor = connection.cursor()
    connection.autocommit = True
    cursor.execute(query)
    connection.commit()
    connection.close()
 

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



