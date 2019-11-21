import json
import glob
import pymssql
import collections

from sql_objects import SqlLogin
from db_provider import ConnectionFactory

login = "ansible"
password = "q1w2e3r4t5"
host = "192.168.62.66"
port = "1433"
sources = ["test_obj.json"]

login_querystring = host
if port != "1433":
    login_querystring = "%s:%s" % (host, port)

factory = ConnectionFactory(login_querystring, login, password)

sql_logins = {}


for path in sources:
    for file_path in glob.glob(path):
        try:
            with open(file_path, "r") as read_file:
                data = json.load(read_file)
                for item in SqlLogin.parse(data):
                    sql_logins[item.login] = item

        except Exception as e:
            print(str(e))

changed = False

sql_logins_changes = []

for sql_login in sql_logins.values():
    try:
        if True:
            result = sql_login.get_changes(factory)
        else:
            result = sql_login.apply(factory)

        sql_login_changed = result[0]

        if sql_login_changed:
            sql_logins_changes[sql_login.login] = result[1]
            changed = True

    except Exception as e:
        print(str(e))

print(sql_logins_changes)
