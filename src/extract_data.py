import csv
from sqlite3 import connect
import psycopg2
import os
from dotenv import load_dotenv





def connect_to_db():
# Load environment variables from .env file
    load_dotenv()
    host = os.environ.get("POSTGRES_HOST")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    database = os.environ.get("POSTGRES_DB")

# Establish a database connection

    connection = psycopg2.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

    return connection



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

    clean_data_list = []
    baskets_list = []
    
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

        query = f''' Insert into Transactions ( transaction_date, transaction_time, branch_name,total_amount,payment_method) 
        values ('{clean_data_item.get('date')}', '{clean_data_item.get('time')}', '{clean_data_item['branch_name']}',
        {clean_data_item['total_amount']}, '{clean_data_item.get('payment_method')}') '''

        transaction_id = int(commit_query(query))

        
        orders = (clean_data_item['basket_items']).strip('"').split(',')
        #i = 0
        for order in orders:

            items_list= order.split(' - ')

            item_dict = {'transaction_id': transaction_id,
                        'item_price': float(items_list[-1]) }

            if items_list[0].strip().startswith('Regular'):
                if 'Flavoured' in items_list[0]:
                        
                    item_dict['item_size'] ='Regular'
                    item_dict['item_flavour'] = items_list[1]
                    item_dict['item_name'] = (items_list[0].split(' Flavoured '))[1]

                else:
                    item_dict['item_size'] ='Regular'
                    item_dict['item_name'] = str(items_list[0])[8:]

            elif items_list[0].strip().startswith('Large'):
                if 'Flavoured' in items_list[0]:

                    item_dict['item_size'] ='Large'
                    item_dict['item_flavour'] = items_list[1]
                    item_dict['item_name'] = (str(items_list[0]).split(' Flavoured '))[1]
                else:
                    item_dict['item_size'] ='Large'
                    item_dict['item_name'] = items_list[0][6:]

            connection = connect_to_db()
            cursor = connection.cursor()
            fetch_query = f''' SELECT * FROM products '''
            cursor.execute(fetch_query)
            items = cursor.fetchall()


            for item in items:

                if (item_dict['item_name'] == item[2]) and (item_dict['item_size'] == item[1]) and (item_dict['item_flavour'] == item [3]) and (item_dict['item_price'] == item[4]):
                    item_id = int(item[0])
                    insert_to_basket_query = f''' insert into baskets (transaction_id, product_id) 
                    values ({transaction_id},{item_id})  '''
                    commit_query(insert_to_basket_query) 

                else:
                    isert_query = f''' insert into products (item_size, item_name, item_flavour, item_price)
                    values ('{item_dict['item_size']}','{item_dict['item_name']}','{item_dict['item_flavour']}',{item_dict['item_price']})            
                    '''
                    product_id = int(commit_query(isert_query))

                    insert_basket_query = f''' insert into baskets (transaction_id, product_id) 
                    values ({transaction_id},{product_id})  ''' 

                    commit_query(insert_basket_query)


    #         baskets_list.append(item_dict)

    #     clean_data_list.append(clean_data_item)

    # return [clean_data_list , baskets_list]




def commit_query(query):

    connection = connect_to_db()
    cursor = connection.cursor()
    connection.autocommit = True
    cursor.execute(query)
    connection.commit()
    last_row_id = cursor.lastrowid
    cursor.close()
    connection.close()
    return last_row_id




list_data = extract_data('chesterfield_25-08-2021_09-00-00.csv')
clean_data(list_data)
#clean_data_list , baskets = clean_data(list_data)
#baskets =get_baskets_list(clean_data_list)


# for basket in baskets:
#     print(basket)


# connection = connect_to_db()
# connection.autocommit = True
# cursor = connection.cursor()

# query =  '''Drop TABLE employees; '''

# cursor.execute(query)
# connection.commit()
# connection.close()





