import ansible.module_utils.sql_utils as sql_utils


class SqlDatabase(object):

    def __init__(self, name, roles=[]):
        """Constructor
         :type roles: str
         """
        self.name = name
        self.roles = roles

    @staticmethod
    def parse(json_databases):
        sql_databases = []

        for key, value in json_databases.items():
            name = key
            roles = []

            if 'roles' in value:
                roles = value['roles']

            sql_databases.append(SqlDatabase(name, roles))

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
            user_name = key
            databases = []

            if 'databases' in value:
                databases = SqlDatabase.parse(value['databases'])

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

    def apply(self, connectionFactory):

        changes = []

        login_exists = sql_utils.login_exists(connectionFactory, self.login)

        if self.state == "present":

            if login_exists:
                if sql_utils.change_default_database(connectionFactory, self.login, self.default_database):
                    changes.append("[default database changed]")

                if sql_utils.change_default_language(connectionFactory, self.login, self.default_language):
                    changes.append("[default language changed]")

                if sql_utils.change_password(connectionFactory, self.login, self.password):
                    changes.append("[password changed]")
            else:
                sql_utils.create_login(connectionFactory, self.login, self.password, self.sid, self.default_database,
                                       self.default_language)
                changes.append("[created]")

            if self.enabled:
                if sql_utils.disable_or_enable_login(connectionFactory, self.login, self.enabled):
                    changes.append("[enabled]")
            else:
                if sql_utils.disable_or_enable_login(connectionFactory, self.login, self.enabled):
                    changes.append("[disabled]")

        if self.state == "absent" and login_exists and sql_utils.drop_login(connectionFactory, self.login):
            changes.append("[dropped]")

        return changes

    def get_changes(self, connectionFactory):

        changes = []

        login_exists = sql_utils.login_exists(connectionFactory, self.login)

        if self.state == "absent" and login_exists:
            changes.append("[dropped]")

        if self.state == "present":

            if login_exists:

                has_change_default_database = sql_utils.has_change_default_database(connectionFactory, self.login,
                                                                                    self.default_database)
                has_change_default_language = sql_utils.has_change_default_language(connectionFactory, self.login,
                                                                                    self.default_language)
                has_change_password = sql_utils.has_change_password(connectionFactory, self.login, self.password)
                is_enabled_login = sql_utils.is_enabled_login(connectionFactory, self.login)

                if has_change_default_database:
                    changes.append("[default database changed]")

                if has_change_default_language:
                    changes.append("[default language changed]")

                if has_change_password:
                    changes.append("[password changed]")

                if self.enabled and not is_enabled_login:
                    changes.append("[enabled]")

                if not self.enabled and is_enabled_login:
                    changes.append("[disabled]")
            else:
                changes.append("[created]")

        return changes

    @staticmethod
    def parse(json_logins):
        sql_logins = []

        for key, value in json_logins.items():
            login = key
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

            if 'sid' in value:
                sid = value['sid']

            if 'state' in value:
                state = value['state']

            if 'enabled' in value:
                enabled = bool(value['enabled'])

            if 'users' in value:
                users = SqlUser.parse(value['users'])

            sql_login = SqlLogin(login, sid, password, default_database, default_language, enabled, state, users)

            sql_logins.append(sql_login)

        return sql_logins