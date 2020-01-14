import hashlib
import ansible.module_utils.sql_utils as sql_utils
import multiprocessing
from joblib import Parallel, delayed

def apply_sql_logins(connection_factory, sql_logins, sql_server_version, check_mode):

    major_sql_server_version = int(sql_server_version.split('.')[0])
    exists_logins = list(map(str.upper, sql_utils.logins_exists(connection_factory, list(o.login for o in sql_logins))))

    changes = {}
    warnings = {}
    errors = {}

    num_cores = multiprocessing.cpu_count()

    def _apply(sql_login):

        exist = sql_login.login.upper() in exists_logins

        if check_mode:
            result = __get_sql_login_changes(connection_factory, sql_login, exist, major_sql_server_version)
        else:
            result = __apply_sql_login(connection_factory, sql_login, exist, major_sql_server_version)

        return sql_login.login, result


    # return changes, warnings, errors, True
    items = Parallel(n_jobs=num_cores)(delayed(_apply)(sql_login) for sql_login in sql_logins)
    changed = False

    for item in items:
        key = item[0]
        login_changes = item[1]
        changes[key] = { 'success': login_changes[3], 'changed': False }

        if login_changes[0]:
            changes[key]['changes'] = login_changes[0]
            changes[key]['changed'] = True
            changed = True

        if login_changes[1]:
            warnings[key] = login_changes[1]
            changes[key]['warnings'] = login_changes[1]

        if login_changes[2]:
            errors[key] = login_changes[2]
            changes[key]['errors'] = login_changes[2]

    return changes, warnings, errors, changed


def __apply_sql_login(connection_factory, sql_login, exist, sql_server_version):
    login = sql_login.login

    changes = []
    warnings = []
    errors = []

    if sql_login.state == "present":

        if exist:
            options = []

            try:
                if sql_utils.change_default_database(connection_factory, sql_login.login, sql_login.default_database):
                    options.append('DEFAULT_DATABASE: {0}'.format(sql_login.default_database))
            except Exception as e:
                errors.append('[LOGIN: {0}] ERROR OCCIRRED WHILE CHANGING DEFAULT_DATABASE: {1}: {2}'.format(sql_login.login, sql_login.default_database, str(e)))

            try:
                if sql_utils.change_default_language(connection_factory, sql_login.login, sql_login.default_language):
                    options.append('DEFAULT_LANGUAGE: {0}'.format(sql_login.default_language))
            except Exception as e:
                errors.append('[LOGIN: {0}] ERROR OCCIRRED WHILE CHANGING DEFAULT_LANGUAGE: {1}: {2}'.format(sql_login.login, sql_login.default_language, str(e)))

            try:
                if sql_utils.change_password(connection_factory, sql_login.login, sql_login.password):
                    options.append('PASSWORD: *****')
            except Exception as e:
                errors.append('[LOGIN: {0}] ERROR OCCIRRED WHILE CHANGING PASSWORD: {1}'.format(sql_login.login, str(e)))

            if options:
                changes.append('[LOGIN: {0}; {1}] - CHANGED'.format(sql_login.login, "; ".join(options)))

        else:

            try:
                if sql_utils.create_login(connection_factory, sql_login.login, sql_login.password, sql_login.sid, sql_login.default_database, sql_login.default_language):

                    options = []
                    from_windows = "\\" in login

                    options.append('LOGIN: {0}'.format(sql_login.login))

                    if from_windows:
                        options.append('TYPE: WINDOWS')

                    if not from_windows:

                        if sql_login.sid:
                            options.append("SID: {0}".format(sql_login.sid))
                        else:
                            sid = "0x" + hashlib.md5(login.upper().encode('utf-8')).hexdigest()
                            options.append("SID: {0}".format(sid))

                        if sql_login.password:
                            options.append("PASSWORD: *****")

                    if sql_login.default_language:
                        options.append("DEFAULT_LANGUAGE: {0}".format(sql_login.default_language))

                    if sql_login.default_database:
                        options.append("DEFAULT_DATABASE: {0}".format(sql_login.default_database))

                    changes.append('[{0}] - [CREATED]'.format("; ".join(options)))

            except Exception as e:
                errors.append('[LOGIN: {0}] ERROR OCCIRRED WHILE CREATING: {1}'.format(sql_login.login, str(e)))
                return changes, warnings, errors, False

        if sql_login.enabled:

            try:
                if sql_utils.disable_or_enable_login(connection_factory, sql_login.login, sql_login.enabled):
                    changes.append('[LOGIN: {0}] - [ENABLED]'.format(sql_login.login))
            except Exception as e:
                errors.append('[LOGIN: {0}] ERROR OCCIRRED WHILE ENABLED: {1}'.format(sql_login.login, str(e)))

        else:
            try:
                if sql_utils.disable_or_enable_login(connection_factory, sql_login.login, sql_login.enabled):
                    changes.append('[LOGIN: {0}] - [DISABLED]'.format(sql_login.login))
            except Exception as e:
                errors.append('[LOGIN: {0}] ERROR OCCIRRED WHILE DISABLED: {1}'.format(sql_login.login, str(e)))

    if sql_login.state == "absent":

        if exist:

            try:
                if sql_utils.drop_login(connection_factory, sql_login.login):
                    changes.append('[LOGIN: {0}] - [DROPPED]'.format(sql_login.login))
            except Exception as e:
                errors.append('[LOGIN: {0}] ERROR OCCIRRED WHILE DROP: {1}'.format(sql_login.login, str(e)))
                return changes, warnings, errors, False

    for user in sql_login.users:

        for database in user.databases:
            database_name = database.name
            roles = []
            database_state = database.state
            user_name = user.name
            default_roles = []

            try:
                if sql_server_version == 10 and sql_utils.is_mirror_database(connection_factory, database_name):
                    warnings.append('[DB: {0}] - IS MIRROR DATABASE'.format(database_name))
                    continue
            except Exception as e:
                errors.append('[DB: {0}] ERROR OCCIRRED WHILE CHECK MIRRORING: {1}'.format(database_name, str(e)))
                continue

            try:
                if not sql_utils.is_database_available(connection_factory, database_name):
                    warnings.append('[DB: {0}] - UNAVAILABLE'.format(database_name))
                    continue
            except Exception as e:
                errors.append('[DB: {0}] ERROR OCCIRRED WHILE CHECK DATABASE AVAILABILITY: {1}'.format(database_name, str(e)))
                continue

            try:
                if sql_server_version >= 12 and not sql_utils.is_primary_hadr_replica(connection_factory, database_name):
                    warnings.append('[DB: {0}] - IS NOT PRIMARY HADR REPLICA'.format(database_name))
                    continue
            except Exception as e:
                errors.append('[DB: {0}] ERROR OCCIRRED WHILE CHECK PRIMARY HADR REPLICA(sys.fn_hadr_is_primary_replica): {1}'.format(database_name, str(e)))
                continue

            if database_state == 'absent':
                try:
                    if sql_utils.drop_user(connection_factory, user_name, database_name):
                        changes.append('[DB: {1}] USER: [{0}] - [DROPPED]'.format(user_name, database_name))
                except Exception as e:
                    errors.append('[DB: {1}]: ERROR OCCURRED WHILE DROP USER: [{0}]; {2}'.format(user_name, database_name, str(e)))
                    continue

            try:
                default_roles = sql_utils.get_available_roles(connection_factory, database_name)
            except Exception as e:
                errors.append('[DB: {0}] ERROR OCCIRRED WHILE GET AVAILABLE ROLES: {1}'.format(database_name, str(e)))
                continue

            if 'db_executor' in database.roles and 'db_executor' not in default_roles:
                try:
                    if sql_utils.create_db_executor_role(connection_factory, database_name):
                        changes.append('[DB: {0}] CREATE ROLE db_executor'.format(database_name))

                    default_roles.append('db_executor')
                except Exception as e:
                    errors.append('[DB: {0}]: create role db_executor and grant execute to db_executor exception: {1}'.format(database_name, str(e)))
                    continue

            if database_state == 'present':
                try:
                    if sql_utils.create_user(connection_factory, user_name, login, database_name):
                        changes.append('[DB: {1}] USER: [{0}] - [CREATED]'.format(user_name, database_name))
                except Exception as e:
                    errors.append('[DB: {1}]: ERROR OCCURRED WHILE CREATE USER: [{0}]; {2}'.format(user_name, database_name, str(e)))
                    continue

                for role in database.roles:
                    if role.upper() in map(str.upper, default_roles):
                        roles.append(role)
                    else:
                        warnings.append('[DB: {1}; USER: {0}]: SQL ROLE: [{2}] - UNAVAILABLE, AVAILABLE ROLES - [{3}]'.format(user_name, database_name, role, ", ".join(default_roles)))

                try:
                    current_user_roles = sql_utils.get_user_roles(connection_factory, user_name, database_name)

                    deleted = set(current_user_roles) - set(roles)
                    add = set(roles) - set(current_user_roles)
                    added_roles = []
                    removed_roles = []

                    for role in deleted:
                        try:
                            if sql_utils.remove_user_role(connection_factory, user_name, role, database_name, sql_server_version):
                                removed_roles.append(role)
                        except Exception as e:
                            errors.append('[DB: {1}; USER: {0}]: ERROR OCCURRED WHILE REMOVE ROLE: {2} - {3}'.format(user_name, database_name, role, str(e)))

                    for role in add:
                        try:
                            if sql_utils.add_user_role(connection_factory, user_name, role, database_name, sql_server_version):
                                added_roles.append(role)
                        except Exception as e:
                            errors.append('[DB: {1}; USER: {0}]: ERROR OCCURRED WHILE ADD ROLE: {2} - {3}'.format(user_name, database_name, role, str(e)))
                    
                    if removed_roles:
                        changes.append('[DB: {1}; USER: {0}]: REMOVED ROLES - [{2}]'.format(user_name, database_name, ", ".join(removed_roles)))

                    if added_roles:
                        changes.append('[DB: {1}; USER: {0}]: ADDED ROLES - [{2}]'.format(user_name, database_name, ", ".join(added_roles)))

                except Exception as e:
                    errors.append('[DB: {1}; USER: {0}]: ERROR OCCURRED WHILE GET USER ROLES: - {2}'.format(user_name, database_name, str(e)))


    return changes, warnings, errors, True


def __get_sql_login_changes(connection_factory, sql_login, exist, sql_server_version):
    login = sql_login.login

    changes = []
    warnings = []
    errors = []

    if sql_login.state == "present":

        if exist:

            options = []

            try:
                if sql_utils.has_change_default_database(connection_factory, sql_login.login, sql_login.default_database):
                    options.append('DEFAULT_DATABASE: {0}'.format(sql_login.default_database))
            except Exception as e:
                errors.append('[LOGIN: {0}] ERROR OCCIRRED WHILE CHANGING DEFAULT_DATABASE: {1}: {2}'.format(sql_login.login, sql_login.default_database, str(e)))

            try:
                if sql_utils.has_change_default_language(connection_factory, sql_login.login, sql_login.default_language):
                    options.append('DEFAULT_LANGUAGE: {0}'.format(sql_login.default_language))
            except Exception as e:
                errors.append('[LOGIN: {0}] ERROR OCCIRRED WHILE CHANGING DEFAULT_LANGUAGE: {1}: {2}'.format(sql_login.login, sql_login.default_language, str(e)))

            try:
                if sql_utils.has_change_password(connection_factory, sql_login.login, sql_login.password):
                    options.append('PASSWORD: *****')
            except Exception as e:
                errors.append('[LOGIN: {0}] ERROR OCCIRRED WHILE CHANGING PASSWORD: {1}'.format(sql_login.login, str(e)))

            try:
                is_enabled_login = sql_utils.is_enabled_login(connection_factory, sql_login.login)

                if sql_login.enabled and not is_enabled_login:
                    options.append('STATE: ENABLED')

                if not sql_login.enabled and is_enabled_login:
                    options.append('STATE: DISABLED')

            except Exception as e:
                errors.append('[LOGIN: {0}] ERROR OCCIRRED WHILE DISABLE/ENABLE: {1}'.format(sql_login.login, str(e)))
            
            if options:
                changes.append('[LOGIN: {0}; {1}] - CHANGED'.format(sql_login.login, "; ".join(options)))

        else:

            try:
                options = []
                from_windows = "\\" in login

                options.append('LOGIN: {0}'.format(sql_login.login))

                if from_windows:
                    options.append('TYPE: WINDOWS')

                if not from_windows:

                    if sql_login.sid:
                        options.append("SID: {0}".format(sql_login.sid))
                    else:
                        sid = "0x" + hashlib.md5(login.upper().encode('utf-8')).hexdigest()
                        options.append("SID: {0}".format(sid))

                    if sql_login.password:
                        options.append("PASSWORD: *****")

                if sql_login.default_language:
                    options.append("DEFAULT_LANGUAGE: {0}".format(sql_login.default_language))

                if sql_login.default_database:
                    options.append("DEFAULT_DATABASE: {0}".format(sql_login.default_database))

                changes.append('[{0}] - [CREATED]'.format("; ".join(options)))
            except Exception as e:
                errors.append('[LOGIN: {0}] ERROR OCCIRRED WHILE CREATING: {1}'.format(sql_login.login, str(e)))
                return changes, warnings, errors, False

    if sql_login.state == "absent" and exist:
        changes.append('[LOGIN: {0}] - [DROPPED]'.format(sql_login.login))

    for user in sql_login.users:

        for database in user.databases:
            database_name = database.name
            roles = []
            database_state = database.state
            user_name = user.name
            default_roles = [] 

            try:
                if sql_server_version == 10 and sql_utils.is_mirror_database(connection_factory, database_name):
                    warnings.append('[DB: {0}] - IS MIRROR DATABASE'.format(database_name))
                    continue
            except Exception as e:
                errors.append('[DB: {0}] ERROR OCCIRRED WHILE CHECK MIRRORING: {1}'.format(database_name, str(e)))
                continue

            try:
                if not sql_utils.is_database_available(connection_factory, database_name):
                    warnings.append('[DB: {0}] - UNAVAILABLE'.format(database_name))
                    continue
            except Exception as e:
                errors.append('[DB: {0}] ERROR OCCIRRED WHILE CHECK DATABASE AVAILABILITY: {1}'.format(database_name, str(e)))
                continue

            try:
                if sql_server_version >= 12 and not sql_utils.is_primary_hadr_replica(connection_factory, database_name):
                    warnings.append('[DB: {0}] - IS NOT PRIMARY HADR REPLICA'.format(database_name))
                    continue
            except Exception as e:
                errors.append('[DB: {0}] ERROR OCCIRRED WHILE CHECK PRIMARY HADR REPLICA(sys.fn_hadr_is_primary_replica): {1}'.format(database_name, str(e)))
                continue

            if database_state == 'absent':
                try:
                    if sql_utils.has_drop_user(connection_factory, user_name, database_name):
                        changes.append('[DB: {1}] USER: [{0}] - [DROPPED]'.format(user_name, database_name))
                except Exception as e:
                    errors.append('[DB: {1}]: ERROR OCCURRED WHILE DROP USER: [{0}]; {2}'.format(user_name, database_name, str(e)))
                    continue

            try:
                default_roles = sql_utils.get_available_roles(connection_factory, database_name)
            except Exception as e:
                errors.append('[DB: {0}] ERROR OCCIRRED WHILE GET AVAILABLE ROLES: {1}'.format(database_name, str(e)))
                continue

            if 'db_executor' in database.roles and 'db_executor' not in default_roles:
                default_roles.append('db_executor')
                changes.append('[DB: {0}] CREATE ROLE db_executor'.format(database_name))

            if database_state == 'present':
                try:
                    if sql_utils.has_create_user(connection_factory, user_name, login, database_name):
                        changes.append('[DB: {1}] USER: [{0}] - [CREATED]'.format(user_name, database_name))
                except Exception as e:
                    errors.append('[DB: {1}]: ERROR OCCURRED WHILE CREATE USER: [{0}]; {2}'.format(user_name, database_name, str(e)))
                    continue

                for role in database.roles:
                    if role.upper() in map(str.upper, default_roles):
                        roles.append(role)
                    else:
                        warnings.append('[DB: {1}; USER: {0}]: SQL ROLE: [{2}] - UNAVAILABLE, AVAILABLE ROLES - [{3}]'.format(user_name, database_name, role, ", ".join(default_roles)))

                try:
                    current_user_roles = sql_utils.get_user_roles(connection_factory, user_name, database_name)

                    deleted = set(current_user_roles) - set(roles)
                    add = set(roles) - set(current_user_roles)

                    if deleted:
                        changes.append('[DB: {1}; USER: {0}]: REMOVED ROLES - [{2}]'.format(user_name, database_name, ", ".join(deleted)))

                    if add:
                        changes.append('[DB: {1}; USER: {0}]: ADDED ROLES - [{2}]'.format(user_name, database_name, ", ".join(add)))

                except Exception as e:
                    errors.append('[DB: {1}; USER: {0}]: ERROR OCCURRED WHILE GET USER ROLES; {2}'.format(user_name, database_name, str(e)))

    return changes, warnings, errors, True
