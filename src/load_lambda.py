import json
import psycopg2
import os


def lambda_handler(event, context):
        
    connection = connect_to_db()
    connection.autocommit = True
    
    print(f'This is an event: {event}')
    for record in event['Records']:
        print(f'This is a single record: {record}')
        data = json.loads(record["body"])
        print(f' The following data is about to be loaded: {data}')
        load_data_into_transactions(data , connection)
 

def load_data_into_transactions(data, connection):
    counter = 0
    
    with connection.cursor() as cursor:
    
        for transaction in data:
            print('Transaction is about to be loaded')
            select_query = f''' SELECT * FROM Transactions where transaction_id ='{transaction['transaction_id']}'  '''
            cursor.execute(select_query)
            transactions = cursor.fetchall()
            
            if (len(transactions)):
                continue
            
            else:
           
                ins_sql = ''' Insert into Transactions (transaction_id, transaction_date, transaction_time, branch_name,total_amount,payment_method) 
                values (%s, %s, %s, %s, %s,%s);'''
                cursor.execute(ins_sql,(transaction.get('transaction_id'), transaction.get('date'),transaction.get('time'), transaction['branch_name'],transaction['total_amount'],
                transaction.get('payment_method')  ))
                connection.commit()
                counter += 1
                insert_products_and_baskets(transaction.get('transaction_id'),transaction['basket_items'], connection, counter)
                print("Transaction has been loaded to the redshift")



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
    
    
def insert_products_and_baskets(transaction_id , basket_items_list , connection , counter):
    
    print(f"Basket of transaction number: {counter} is about to be loading")
  
    with connection.cursor() as cursor:
    
        for basket in basket_items_list:
  
            fetch_query = f''' SELECT * FROM products where item_size ='{basket['item_size']}' and item_name = '{basket['item_name']}' and item_flavour = '{basket['item_flavour']}' 
            and item_price = {basket['item_price']} '''
            cursor.execute(fetch_query)
            items = cursor.fetchall()
            
            for item in items:
                item_id = item[0]
                break
            
            if not(len(items)):
          
                insert_query = ''' insert into products (item_size, item_name, item_flavour, item_price) values (%s,%s,%s,%s )'''
                sel_sql = '''SELECT product_id FROM products WHERE item_size = %s AND item_name = %s  and
                item_flavour = %s and item_price = %s ORDER BY product_id DESC LIMIT 1;'''
                cursor.execute(insert_query,(basket['item_size'],basket['item_name'],basket['item_flavour'],basket['item_price'] ))
                cursor.execute(sel_sql,((basket['item_size'],basket['item_name'],basket['item_flavour'],basket['item_price'] )))
                product_id = cursor.fetchone()[0]
                connection.commit()
                insert_basket_query = f''' insert into baskets (transaction_id, product_id) 
                values ('{transaction_id}',{product_id})  '''
                cursor.execute(insert_query)
                connection.commit()
                
            else:
                insert_basket_query = f''' insert into baskets (transaction_id, product_id) values ('{transaction_id}',{item_id})  ''' 
                cursor.execute(insert_basket_query)
                connection.commit()
        print(f"Basket of transaction number: {counter} has been loaded to the redshift")
  
