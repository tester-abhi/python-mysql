import string
my_secrets = list()

my_secrets.append({'username': 'abhijit', 'pwd': 'mindanaO@10031945'})

def get_db_password(username) -> string:
    for i in my_secrets:
        return i['pwd'] if i['username'] == username else None