
class SqlLogin(object):

    def __init__(self, login, sid = None, password = None, default_database = None, default_language = None, enabled = True, state = "present"):
            """Constructor"""

            self.login = login
            self.sid = sid
            self.password = password
            self.default_database = default_database
            self.default_language = default_language
            self.enabled = enabled
            self.state = state

def parse_sql_users(dict):

    sql_users = []

    for key, value in dict.items():
        login = key
        sid = None
        password = None
        default_database = None
        default_language = None
        enabled = True
        state = "present"

        if 'sid' in value:
            sid = value['sid']

        if 'password' in value:
            password = value['password']

        if 'default_database' in value:
            default_database = value['default_database']

        if 'default_language' in value:
            default_language = value['default_language']

        if 'sid' in value:
            sid = value['sid']

        if 'state' in value:
            state = value['state']

        sql_login = SqlLogin(login, sid, password, default_database, default_language, enabled, state)

        sql_users.append(sql_login)

    return sql_users
