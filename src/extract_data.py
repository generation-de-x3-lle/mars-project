import csv
from create_table import connect_to_db
import psycopg2
import os
from dotenv import load_dotenv




def extract_data(filename):
    data_list = []

    try:
        with open(filename, 'r', newline='') as file:

            source_file = csv.reader(file)

            for row in source_file:
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

    except Exception as error:
        print("An error occurred: " + str(error))

    finally:

        return data_list




def clean_data(raw_data_list):

    for raw_data in raw_data_list:

        date , time = (raw_data.get('date_time')).split(' ')
        clean_time = f'{time}:00'
        day , month , year = date.split('/')
        clean_date = f'{year}-{month}-{day}'

        clean_data_item= {
            'date':clean_date,
            'time' : clean_time,
            'branch_name': raw_data['branch_name'],
            'customer_name': raw_data['customer_name'],
            'basket_items': raw_data['basket_items'],
            'total_amount':raw_data['total_amount'],
            'payment_method': raw_data['payment_method'],
            'card_number': raw_data['card_number']
        }

        query = f''' Insert into transactions ( transaction_date, transaction_time, branch_name,total_amount,payment_method) 
        values ('{clean_data_item.get('date')}', '{clean_data_item.get('time')}', '{clean_data_item['branch_name']}',
        {clean_data_item['total_amount']}, '{clean_data_item.get('payment_method')}') returning transaction_id'''
        transaction_connection = connect_to_db()
        cursor = transaction_connection.cursor()
        cursor.execute(query)
        transaction_id = cursor.fetchone()[0]
        transaction_connection.commit()
        transaction_connection.close()

        orders = (clean_data_item['basket_items']).strip('"').split(',')
        transform_to_basket( transaction_id, orders)
        
       


def transform_to_basket(transaction_id, orders):

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

        connection = connect_to_db()
        cursor = connection.cursor()

        fetch_query = f''' SELECT * FROM products where item_size ='{item_dict['item_size']}' and item_name = '{item_dict['item_name']}' and item_flavour = '{item_dict['item_flavour']}' and item_price = {item_dict['item_price']} '''
        cursor.execute(fetch_query)
        items = cursor.fetchall()
        connection.close()

        for item in items:
            item_id = item[0]
            break

        if not(len(items)):
            insert_query = f''' insert into products (item_size, item_name, item_flavour, item_price)
                values ('{item_dict['item_size']}','{item_dict['item_name']}','{item_dict['item_flavour']}',
                {item_dict['item_price']} ) returning product_id   '''

            connection = connect_to_db()
            connection.autocommit = True
            cursor = connection.cursor()
            cursor.execute(insert_query)
            product_id = cursor.fetchone()[0]
            connection.commit()
            connection.close()

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
 



list_data = extract_data('chesterfield_25-08-2021_09-00-00.csv')
clean_data(list_data)




