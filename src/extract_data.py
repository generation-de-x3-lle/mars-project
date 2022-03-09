import csv
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



def extract_data():
    data_list = []

    try:
        with open('chesterfield_25-08-2021_09-00-00.csv', 'r', newline='') as file:

            source_file = csv.reader(file)

            for row in source_file:
                row_dict = {
                    'date_time':row[0],
                    'branch_location': row[1],
                    'customer_name': row[2],
                    'basket_items': row[3],
                    'paying_amount': float(row[4]),
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
    order_number = 1
    
    for raw_data in raw_data_list:

        date , time = (raw_data.get('date_time')).split(' ')
        clean_time = f'{time}:00'
        day , month , year = date.split('/')
        clean_date = f'{year}-{month}-{day}'
        transaction_id = '{}-{}-{}-{}'.format(date, time, raw_data.get('branch_location'),order_number )
        
        clean_data_item= {
            'transaction_id':transaction_id,
             'date':clean_date,
            'time' : clean_time,
             'branch_location': raw_data['branch_location'],
             'customer_name': raw_data['customer_name'],
              'basket_items': raw_data['basket_items'],
              'paying_amount':raw_data['paying_amount'],
               'payment_method': raw_data['payment_method'],
              'card_number': raw_data['card_number']
        }
   #     print(clean_data_item)
        clean_data_list.append(clean_data_item)
        order_number += 1


    return clean_data_list




def get_baskets_list(clean_list):

    baskets_list = []

    for data in clean_list:

        basket= (data['basket_items']).split(',')

        for items in basket:
            item_dict = {}
            items_list= items.split('-')

            if (str(items_list[0]).strip()).startswith('Regular'):
                if 'Flavoured' in str(items_list[0]):
                        
                    item_dict['item_size'] ='Regular'
                    item_dict['item_flavour'] = items_list[1]
                    item_dict['item_name'] = (str(items_list[0])[18:])


                else:
                    item_dict['item_size'] ='Regular'
                    item_dict['item_name'] = str(items_list[0])[7:]

            elif (str(items_list[0]).strip()).startswith('Large'):
                if 'Flavoured' in str(items_list[0]):

                    item_dict['item_size'] ='Large'
                    item_dict['item_flavour'] = items_list[1]
                    item_dict['item_name'] = str(items_list[0])[16:]


                else:

                    item_dict['item_size'] ='Large'
                    item_dict['item_name'] = str(items_list[0])[6:]


                item_dict ['item_price'] = float(items_list[-1])
                item_dict['transaction_id']= data.get('transaction_id')
                baskets_list.append(item_dict)
 
   # print(baskets_list)

    return baskets_list



list_data = extract_data()
clean_data(list_data)






