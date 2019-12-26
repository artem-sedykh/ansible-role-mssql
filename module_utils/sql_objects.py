import ansible.module_utils.sql_utils as sql_utils
import ansible.module_utils.sql_utils_users as sql_utils_users


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
            name = key
            roles = []
            state = "present"

            if 'roles' in value:
                roles = value['roles']

            if 'state' in value:
                state = value['state']

            sql_databases.append(SqlDatabase(name, state, roles))

        return sql_databases


class SqlUser(object):
    def __init__(self, name, databases=[]):
        """Constructor
        :type databases: SqlDatabase
        """
        self.databases = databases
        self.name = name

    def get_changes(self, connection_factory, login, sql_server_version):

        changed = False

        major_sql_server_version = int(sql_server_version.split('.')[0])

        changes = {}

        for database in self.databases:
            database_name = database.name
            roles = database.roles
            database_state = database.state
            user_name = self.name
            database_changes = {}
            database_changed = False

            if not sql_utils_users.is_database_available(connection_factory, database_name):
                database_changes["database_unavailable"] = True
                changes[database_name] = database_changes
                changed = True
                continue

            if not sql_utils_users.is_primary_hadr_replica(connection_factory, database_name):
                continue

            if database_state == "absent":
                if sql_utils_users.has_drop_user(connection_factory, user_name, database_name):
                    database_changes["drop_user"] = True
                    database_changed = True

            if database_state == "present":

                if sql_utils_users.has_create_user(connection_factory, user_name, login, database_name):
                    database_changes["create_user"] = True
                    database_changed = True

                if sql_utils_users.has_sync_user_roles(connection_factory, user_name, roles, database_name):
                    database_changes["sync_user_roles"] = True
                    database_changed = True

            if database_changed:
                changes[database_name] = database_changes

            changed = changed or database_changed

        return changed, changes

    def apply(self, connection_factory, login, sql_server_version):

        changed = False

        major_sql_server_version = int(sql_server_version.split('.')[0])

        changes = {}

        for database in self.databases:
            database_name = database.name
            roles = database.roles
            database_state = database.state
            user_name = self.name
            database_changes = {}
            database_changed = False

            if not sql_utils_users.is_database_available(connection_factory, database_name):
                database_changes["database_unavailable"] = True
                changes[database_name] = database_changes
                changed = True
                continue

            if not sql_utils_users.is_primary_hadr_replica(connection_factory, database_name):
                continue

            if database_state == "absent":
                if sql_utils_users.drop_user(connection_factory, user_name, database_name):
                    database_changes["drop_user"] = True
                    database_changed = True

            if database_state == "present":

                if sql_utils_users.create_user(connection_factory, user_name, login, database_name):
                    database_changes["create_user"] = True
                    database_changed = True

                if sql_utils_users.sync_user_roles(connection_factory, user_name, roles, database_name, major_sql_server_version):
                    database_changes["sync_user_roles"] = True
                    database_changed = True

            if database_changed:
                changes[database_name] = database_changes

            changed = changed or database_changed

        return changed, changes

    @staticmethod
    def parse(json_user):

        sql_users = []

        for key, value in json_user.items():
            user_name = key
            databases = []
            state = "present"

            if 'databases' in value:
                databases = SqlDatabase.parse(value['databases'])

            if 'state' in value:
                state = value['state']

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

    def apply(self, connection_factory, sql_server_version):

        login_exists = sql_utils.login_exists(connection_factory, self.login)

        changes = {}
        users_changes = {}
        changed = False

        if self.state == "present":

            if login_exists:
                if sql_utils.change_default_database(connection_factory, self.login, self.default_database):
                    changes["changed_default_database"] = True
                    changed = True

                if sql_utils.change_default_language(connection_factory, self.login, self.default_language):
                    changes["changed_default_language"] = True
                    changed = True

                if sql_utils.change_password(connection_factory, self.login, self.password):
                    changes["changed_password"] = True
                    changed = True
            else:
                sql_utils.create_login(connection_factory, self.login, self.password, self.sid, self.default_database,
                                       self.default_language)
                changes["created"] = True
                changed = True

            if self.enabled:
                if sql_utils.disable_or_enable_login(connection_factory, self.login, self.enabled):
                    changes["enabled"] = True
                    changed = True
            else:
                if sql_utils.disable_or_enable_login(connection_factory, self.login, self.enabled):
                    changes["disabled"] = True
                    changed = True

        if self.state == "absent" and login_exists and sql_utils.drop_login(connection_factory, self.login):
            changes["dropped"] = True
            changed = True

        for user in self.users:
            user_result = user.apply(connection_factory, self.login, sql_server_version)

            if user_result[0]:
                users_changes[user.name] = {"databases": user_result[1]}

        if users_changes:
            changes["users"] = users_changes
            changed = True

        return changed, changes, self.login

    def get_changes(self, connection_factory, sql_server_version):

        changes = {}
        users_changes = {}
        changed = False

        login_exists = sql_utils.login_exists(connection_factory, self.login)

        if self.state == "absent" and login_exists:
            changes["dropped"] = True
            changed = True

        if self.state == "present":

            if login_exists:

                has_change_default_database = sql_utils.has_change_default_database(connection_factory, self.login,
                                                                                    self.default_database)
                has_change_default_language = sql_utils.has_change_default_language(connection_factory, self.login,
                                                                                    self.default_language)
                has_change_password = sql_utils.has_change_password(connection_factory, self.login, self.password)
                is_enabled_login = sql_utils.is_enabled_login(connection_factory, self.login)

                if has_change_default_database:
                    changes["changed_default_database"] = True
                    changed = True

                if has_change_default_language:
                    changes["changed_default_language"] = True
                    changed = True

                if has_change_password:
                    changes["changed_password"] = True
                    changed = True

                if self.enabled and not is_enabled_login:
                    changes["enabled"] = True
                    changed = True

                if not self.enabled and is_enabled_login:
                    changes["disabled"] = True
                    changed = True
            else:
                changes["created"] = True
                changed = True

        for user in self.users:
            user_result = user.get_changes(connection_factory, self.login, sql_server_version)

            if user_result[0]:
                users_changes[user.name] = {"databases": user_result[1]}

        if users_changes:
            changes["users"] = users_changes
            changed = True

        return changed, changes, self.login

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

            if state == "absent":
                for user in users:
                    for database in user.databases:
                        database.state = state

            sql_login = SqlLogin(login, sid, password, default_database, default_language, enabled, state, users)

            sql_logins.append(sql_login)

        return sql_logins
