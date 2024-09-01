print('Hello World!')

from faker import Faker
from tabulate import tabulate
import random, uuid, string, pandas as pd, mysql.connector as mysql, my_secrets


#print(secrets())
grc = lambda pool: random.choice(pool)

def get_db(username):
    try:
        return mysql.connect(
            host='localhost',
            user=username,
            password=my_secrets.get_db_password(username),
            database='abhi_db'
        )
    except Exception as e:
        print(f'error while obtaining DB connection object==> {e}')

query_type = ['insert_ignore_duplicates', 'insert_update_duplicate', 'gather_duplicates']

def exceute_query(conn, sql, query_type, data):
    try:
        if query_type == 'insert_ignore_duplicates':
            print(f'inside insert_ignore_duplicate block')
            conn.autocommit = False
            cursor = conn.cursor()
            cursor.executemany(sql, data)
            conn.commit()
        elif query_type == 'insert_update_duplicates':
            print(f'inside insert_update_duplicate block')
            conn.autocommit = False
            cursor = conn.cursor()
            cursor.executemany(sql, data)
            conn.commit()
        elif query_type == 'gather_duplicates':
            print(f'inside gather_duplicates block')
    # try:
    #     sql = 'select * from offer_grid limit 10'
    #     cursor = conn.cursor()
    #     cursor.execute(sql)
    #     results = cursor.fetchall()
    #     for i in results:
    #         print(i)
    #     print(f'At worlds end')
    except Exception as e:
        if conn.is_connected():
            conn.rollback()
            print(f'Transaction rolled back successfully')
        print(f'Some error occured while executing the query {e}')
    finally:
        cursor.close()
        conn.close()

def get_random_string(length,pool) -> string:
    temp = ''
    i = 0
    while i<length:
        temp =temp+grc(pool)
        i+=1
    return temp

def get_pan() -> string:
    fourth_letter_character_set = 'TFHPC'
    return f'{get_random_string(3,string.ascii_uppercase)}{random.choice(fourth_letter_character_set)}{random.randint(1000, 9999)}{get_random_string(1,string.ascii_uppercase)}'

def get_dataset(size) -> list:
    my_data_list = []
    for _ in range(10):
        my_custom_record = dict()
        my_custom_record['customer_identifier_no'] = 'ABHI' + str(uuid.uuid4())
        my_custom_record['pan'] = get_pan()
        my_custom_record['bureau_scrub_date'] = '2024-09-01'
        my_custom_record['offer_validity_date'] = '2024-09-30'
        my_custom_record['loan_amount'] = random.choice([100000, 150000, 200000, 350000, 400000])
        my_custom_record['pf'] = '2.36'
        my_data_list.append(my_custom_record)
        #df = pd.DataFrame(my_data_list)
    print(tabulate(my_data_list))
    #print(my_data_list)
    return my_data_list

def main():
    try:
        # print(my_secrets.get_db_password('abhijit'))
        df = pd.DataFrame(get_dataset(10))
        #df.to_csv('input.csv', index=False)
        conn = get_db('abhijit')
        df2 = pd.read_csv('./input.csv')
        print(f'This is df2 ===> {df2}')
        data = [tuple(row) for row in df2.itertuples(index=False)]
        print(f'This is data ===> {data}')
        conn = get_db('abhijit')
        #sql = f'insert ignore into offer_grid (customer_identifier_no, pan, bureau_scrub_date, offer_validity_date, loan_amount, pf) values (%s, %s, %s, %s, %s, %s)'
        sql = f'''insert into offer_grid (customer_identifier_no, pan, bureau_scrub_date, offer_validity_date, loan_amount, pf) values (%s, %s, %s, %s, %s, %s) 
        on duplicate key update customer_identifier_no = values(customer_identifier_no), pan = values(pan), bureau_scrub_date = values(bureau_scrub_date), 
        offer_validity_date=values(offer_validity_date), offer_validity_date = values(offer_validity_date), loan_amount=values(loan_amount), pf=values(pf) '''
        exceute_query(conn, sql, 'insert_update_duplicates', data)
    except Exception as e:
        print(f'Some error occured ==> {e}')
if __name__ == "__main__":
    main()