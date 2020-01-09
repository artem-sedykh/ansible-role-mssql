import hashlib
import ansible.module_utils.sql_utils as sql_utils
import multiprocessing
from joblib import Parallel, delayed

def apply_sql_logins(connection_factory, sql_logins, sql_server_version, check_mode):

    major_sql_server_version = int(sql_server_version.split('.')[0])
    default_roles = sql_utils.get_available_roles(connection_factory)
    exists_logins = list(map(str.upper, sql_utils.logins_exists(connection_factory, list(o.login for o in sql_logins))))

    changes = {}
    warnings = {}
    errors = {}

    num_cores = multiprocessing.cpu_count()

    def _apply(sql_login):

        exist = sql_login.login.upper() in exists_logins

        if check_mode:
            result = __get_sql_login_changes(connection_factory, sql_login, exist, default_roles, major_sql_server_version)
        else:
            result = __apply_sql_login(connection_factory, sql_login, exist, default_roles, major_sql_server_version)

        return sql_login.login, result


    # return changes, warnings, errors, True
    items = Parallel(n_jobs=num_cores)(delayed(_apply)(sql_login) for sql_login in sql_logins)

    for item in items:
        key = item[0]
        login_changes = item[1]
        changes[key] = {'changes': login_changes[0], 'success': login_changes[3]}

        if login_changes[1]:
            warnings[key] = login_changes[1]
            changes[key]['warnings'] = login_changes[1]

        if login_changes[2]:
            errors[key] = login_changes[2]
            changes[key]['errors'] = login_changes[2]

    return changes, warnings, errors


def __apply(connection_factory, check_mode, sql_login, default_roles, exist, major_sql_server_version):

    if check_mode:
        result = __get_sql_login_changes(connection_factory, sql_login, exist, default_roles, major_sql_server_version)
    else:
        result = __apply_sql_login(connection_factory, sql_login, exist, default_roles, major_sql_server_version)

    return sql_login.login, result


def __apply_sql_login(connection_factory, sql_login, exist, default_roles, sql_server_version):
    login = sql_login.login

    changes = []
    warnings = []
    errors = []

    if sql_login.state == "present":

        if exist:

            try:
                if sql_utils.change_default_database(connection_factory, sql_login.login, sql_login.default_database):
                    changes.append('default database changed to: {0}'.format(sql_login.default_database))
            except Exception as e:
                errors.append('error occurred while changing the default database: ' + str(e))

            try:
                if sql_utils.change_default_language(connection_factory, sql_login.login, sql_login.default_language):
                    changes.append('default language changed to: {0}'.format(sql_login.default_language))
            except Exception as e:
                errors.append('error occurred while changing the default language: ' + str(e))

            try:
                if sql_utils.change_password(connection_factory, sql_login.login, sql_login.password):
                    changes.append('password changed to: *****'.format(sql_login.default_language))
            except Exception as e:
                errors.append('error occurred while changing password: ' + str(e))

        else:

            try:
                if sql_utils.create_login(connection_factory, sql_login.login, sql_login.password, sql_login.sid,
                                          sql_login.default_database, sql_login.default_language):

                    message = 'login {0} created'

                    from_windows = "\\" in login

                    if from_windows:
                        message += ' from windows'

                    options = []

                    if not from_windows:

                        if sql_login.sid:
                            options.append("sid: {0}".format(sql_login.sid))
                        else:
                            sid = "0x" + hashlib.md5(login.upper().encode('utf-8')).hexdigest()
                            options.append("sid: {0}".format(sid))

                        if sql_login.password:
                            options.append("password: *****")

                    if sql_login.default_language:
                        options.append("default_language: {0}".format(sql_login.default_language))

                    if sql_login.default_database:
                        options.append("default_database: {0}".format(sql_login.default_database))

                    if options:
                        message += " with " + ", ".join(options)

                    changes.append(message)
            except Exception as e:
                errors.append('error occurred while creating login: ' + str(e))
                return changes, warnings, errors, False

        if sql_login.enabled:

            try:
                if sql_utils.disable_or_enable_login(connection_factory, sql_login.login, sql_login.enabled):
                    changes.append('login {0} enabled'.format(sql_login.login))
            except Exception as e:
                errors.append('error occurred while enabled login: {0} '.format(sql_login.login) + str(e))

        else:
            try:
                if sql_utils.disable_or_enable_login(connection_factory, sql_login.login, sql_login.enabled):
                    changes.append('login {0} disabled'.format(sql_login.login))
            except Exception as e:
                errors.append('error occurred while disabled login: {0} '.format(sql_login.login) + str(e))

    if sql_login.state == "absent":

        if exist:

            try:
                if sql_utils.drop_login(connection_factory, sql_login.login):
                    changes.append('login {0} dropped'.format(sql_login.login))
            except Exception as e:
                errors.append('error occurred while drop login {0}: '.format(sql_login.login) + str(e))
                return changes, warnings, errors, False

    for user in sql_login.users:

        for database in user.databases:
            database_name = database.name
            roles = []
            database_state = database.state
            user_name = user.name

            try:
                if not sql_utils.is_database_available(connection_factory, database_name):
                    warnings.append('database: {0} unavailable'.format(database_name))
                    continue
            except Exception as e:
                errors.append('error occurred while check database availability: {0} '.format(database_name) + str(e))
                continue

            try:
                if sql_server_version >= 12 and not sql_utils.is_primary_hadr_replica(connection_factory,
                                                                                      database_name):
                    warnings.append('database: {0} is not primary hadr replica'.format(database_name))
                    continue
            except Exception as e:
                errors.append('error occurred while check database: {0} on sys.fn_hadr_is_primary_replica: '.format(
                    database_name) + str(e))
                continue

            if database_state == 'absent':
                try:
                    if sql_utils.drop_user(connection_factory, user_name, database_name):
                        changes.append('database: {1}, user {0} dropped'.format(user_name, database_name))
                except Exception as e:
                    errors.append(
                        'error occurred while drop user: {0}, database: {1}; '.format(user_name, database_name) + str(
                            e))
                    continue

            if database_state == 'present':
                try:
                    if sql_utils.create_user(connection_factory, user_name, login, database_name):
                        changes.append('database {1}, user {0} created'.format(user_name, database_name))
                except Exception as e:
                    errors.append(
                        'error occurred while create user: {0}, database: {1}; '.format(user_name, database_name) + str(
                            e))
                    continue

                for role in database.roles:
                    if role.upper() in map(str.upper, default_roles):
                        roles.append(role)
                    else:
                        warnings.append(
                            'database {1}, user {0} sql role: {2} unavailable, available roles: {3}'.format(user_name,
                                                                                                            database_name,
                                                                                                            role,
                                                                                                            ", ".join(
                                                                                                                default_roles)))

                try:
                    current_user_roles = sql_utils.get_user_roles(connection_factory, user_name, database_name)

                    deleted = set(current_user_roles) - set(roles)
                    add = set(roles) - set(current_user_roles)

                    for role in deleted:
                        try:
                            if sql_utils.remove_user_role(connection_factory, user_name, role, database_name,
                                                          sql_server_version):
                                changes.append(
                                    'database {1}, user {0}, role: {2} removed'.format(user_name, database_name, role))
                        except Exception as e:
                            errors.append(
                                'error occurred while remove role: {2} user: {0}, database: {1}; '.format(user_name,
                                                                                                          database_name,
                                                                                                          role) + str(
                                    e))

                    for role in add:
                        try:
                            if sql_utils.add_user_role(connection_factory, user_name, role, database_name,
                                                       sql_server_version):
                                changes.append(
                                    'database {1}, user {0}, role: {2} added'.format(user_name, database_name, role))
                        except Exception as e:
                            errors.append(
                                'error occurred while add role: {2} to user: {0}, database: {1}; '.format(user_name,
                                                                                                          database_name,
                                                                                                          role) + str(
                                    e))

                except Exception as e:
                    errors.append('error occurred while get user: {0} roles in database: {1}; '.format(user_name,
                                                                                                       database_name) + str(
                        e))

    return changes, warnings, errors, True


def __get_sql_login_changes(connection_factory, sql_login, exist, default_roles, sql_server_version):
    login = sql_login.login

    changes = []
    warnings = []
    errors = []

    if sql_login.state == "present":

        if exist:

            try:
                if sql_utils.has_change_default_database(connection_factory, sql_login.login, sql_login.default_database):
                    changes.append('default database changed to: {0}'.format(sql_login.default_database))
            except Exception as e:
                errors.append('error occurred while changing the default database: ' + str(e))

            try:
                if sql_utils.has_change_default_language(connection_factory, sql_login.login, sql_login.default_language):
                    changes.append('default language changed to: {0}'.format(sql_login.default_language))
            except Exception as e:
                errors.append('error occurred while changing the default language: ' + str(e))

            try:
                if sql_utils.has_change_password(connection_factory, sql_login.login, sql_login.password):
                    changes.append('password changed to: *****'.format(sql_login.default_language))
            except Exception as e:
                errors.append('error occurred while changing password: ' + str(e))

            try:
                is_enabled_login = sql_utils.is_enabled_login(connection_factory, sql_login.login)

                if sql_login.enabled and not is_enabled_login:
                    changes.append('login {0} enabled'.format(sql_login.login))

                if not sql_login.enabled and is_enabled_login:
                    changes.append('login {0} disabled'.format(sql_login.login))
            except Exception as e:
                errors.append('error occurred while disabled/enabled login: {0} '.format(sql_login.login) + str(e))

        else:

            try:
                message = 'login {0} created'

                from_windows = "\\" in login

                if from_windows:
                    message += ' from windows'

                options = []

                if not from_windows:

                    if sql_login.sid:
                        options.append("sid: {0}".format(sql_login.sid))
                    else:
                        sid = "0x" + hashlib.md5(login.upper().encode('utf-8')).hexdigest()
                        options.append("sid: {0}".format(sid))

                    if sql_login.password:
                        options.append("password: *****")

                if sql_login.default_language:
                    options.append("default_language: {0}".format(sql_login.default_language))

                if sql_login.default_database:
                    options.append("default_database: {0}".format(sql_login.default_database))

                if options:
                    message += " with " + ", ".join(options)

                changes.append(message)
            except Exception as e:
                errors.append('error occurred while creating login: ' + str(e))
                return changes, warnings, errors, False

    if sql_login.state == "absent" and exist:
        changes.append('login {0} dropped'.format(sql_login.login))

    for user in sql_login.users:

        for database in user.databases:
            database_name = database.name
            roles = []
            database_state = database.state
            user_name = user.name

            try:
                if not sql_utils.is_database_available(connection_factory, database_name):
                    warnings.append('database: {0} unavailable'.format(database_name))
                    continue
            except Exception as e:
                errors.append('error occurred while check database availability: {0} '.format(database_name) + str(e))
                continue

            try:
                if sql_server_version >= 12 and not sql_utils.is_primary_hadr_replica(connection_factory, database_name):
                    warnings.append('database: {0} is not primary hadr replica'.format(database_name))
                    continue
            except Exception as e:
                errors.append('error occurred while check database: {0} on sys.fn_hadr_is_primary_replica: '.format(
                    database_name) + str(e))
                continue

            if database_state == 'absent':
                try:
                    if sql_utils.has_drop_user(connection_factory, user_name, database_name):
                        changes.append('database: {1}, user {0} dropped'.format(user_name, database_name))
                except Exception as e:
                    errors.append(
                        'error occurred while drop user: {0}, database: {1}; '.format(user_name, database_name) + str(
                            e))
                    continue

            if database_state == 'present':
                try:
                    if sql_utils.has_create_user(connection_factory, user_name, login, database_name):
                        changes.append('database {1}, user {0} created'.format(user_name, database_name))
                except Exception as e:
                    errors.append('error occurred while create user: {0}, database: {1}; '.format(user_name, database_name) + str(e))
                    continue

                for role in database.roles:
                    if role.upper() in map(str.upper, default_roles):
                        roles.append(role)
                    else:
                        warnings.append('database {1}, user {0} sql role: {2} unavailable, available roles: {3}'.format(user_name, database_name, role, ", ".join(default_roles)))

                try:
                    current_user_roles = sql_utils.get_user_roles(connection_factory, user_name, database_name)

                    deleted = set(current_user_roles) - set(roles)
                    add = set(roles) - set(current_user_roles)

                    for role in deleted:
                        changes.append('database {1}, user {0}, role: {2} removed'.format(user_name, database_name, role))

                    for role in add:
                        changes.append('database {1}, user {0}, role: {2} added'.format(user_name, database_name, role))

                except Exception as e:
                    errors.append('error occurred while get user: {0} roles in database: {1}; '.format(user_name, database_name) + str(e))

    return changes, warnings, errors, True
