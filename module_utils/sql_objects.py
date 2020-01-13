class SqlDatabase(object):

    def __init__(self, name, state="present", roles=[]):
        """Constructor
         :type roles: str
         """
        self.name = name
        self.state = state
        self.roles = roles

    @staticmethod
    def parse(json_databases):
        sql_databases = []

        for key, value in json_databases.items():
            name = key.strip()
            roles = []
            state = "present"

            if 'roles' in value:
                roles = value['roles']

            if 'state' in value:
                state = value['state']
                if not state or state.lower() not in ['present', 'absent']:
                    raise Exception('db: {0} state: "{1}" parsing error, availables state: present or absent'.format(name, state))
                state = state.lower()

            sql_databases.append(SqlDatabase(name, state, roles))

        return sql_databases


class SqlUser(object):
    def __init__(self, name, databases=[]):
        """Constructor
        :type databases: SqlDatabase
        """
        self.databases = databases
        self.name = name

    @staticmethod
    def parse(json_user):

        sql_users = []

        for key, value in json_user.items():
            user_name = key.strip()
            databases = []
            state = "present"

            if 'databases' in value:
                databases = SqlDatabase.parse(value['databases'])

            if 'state' in value:
                state = value['state']
                if not state or state.lower() not in ['present', 'absent']:
                    raise Exception('user: {0} state: "{1}" parsing error, availables state: present or absent'.format(user_name, state))
                state = state.lower()

            if state == "absent":
                for database in databases:
                    database.state = state

            sql_user = SqlUser(user_name, databases)

            sql_users.append(sql_user)

        return sql_users


class SqlLogin(object):

    def __init__(self, login, sid=None, password=None, default_database=None, default_language=None, enabled=True,
                 state="present", users=[]):
        """Constructor"""

        self.login = login
        self.sid = sid
        self.password = password
        self.default_database = default_database
        self.default_language = default_language
        self.enabled = enabled
        self.state = state
        self.users = users

    @staticmethod
    def parse(json_logins):
        sql_logins = []

        for key, value in json_logins.items():
            login = key.strip()
            sid = None
            password = None
            default_database = None
            default_language = None
            enabled = True
            state = "present"
            users = []

            if 'sid' in value:
                sid = value['sid']

            if 'password' in value:
                password = value['password']

            if 'default_database' in value:
                default_database = value['default_database']

            if 'default_language' in value:
                default_language = value['default_language']

            if 'state' in value:
                state = value['state']
                if not state or state.lower() not in ['present', 'absent']:
                    raise Exception('login: {0} state: "{1}" parsing error, availables state: present or absent'.format(login, state))
                state = state.lower()

            if 'enabled' in value:
                enabled = bool(value['enabled'])

            if 'users' in value:
                users = SqlUser.parse(value['users'])

            if state == "absent":
                for user in users:
                    for database in user.databases:
                        database.state = state

            sql_login = SqlLogin(login, sid, password, default_database, default_language, enabled, state, users)

            sql_logins.append(sql_login)

        return sql_logins
