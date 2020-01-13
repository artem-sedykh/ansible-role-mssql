import hashlib


# region logins

def login_exists(connection_factory, login):
    """Метод проверять существует ли логин.
    Args:
        connection_factory (connection_factory): Коннект к базе данных
        login (str): логин пользователя

    Returns:
        bool: Метод возвращает True если такой логин уже существует, в противном случае False.
    """

    if not login:
        raise ValueError("login cannot be empty")

    with connection_factory.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT NULL FROM master.sys.syslogins WHERE name = %(login)s", dict(login=login))
            return bool(cursor.rowcount)


def logins_exists(connection_factory, logins):
    if not logins:
        return None

    with connection_factory.connect() as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.execute("SELECT name FROM master.sys.syslogins WHERE name in %(logins)s", dict(logins=logins))
            exists_logins = []

            for row in cursor:
                login = row["name"]
                exists_logins.append(login)

            return exists_logins


def create_login(connection_factory, login, password=None, sid=None, default_database=None, default_language=None):
    """Метод создает логин.
    Args:
        connection_factory (connection_factory): Коннект к базе данных
        login (str): логин пользователя
        password (str): пароль
        sid (str): использвется не в доменной авторизации
        default_database (str): база данных по умолчанию
        default_language (str): язык по умолчанию
    Returns:
        None
    """
    if not login:
        raise ValueError("login cannot be empty")

    sql = "create login [{0}]".format(login)

    from_windows = "\\" in login

    if from_windows:
        sql += " from windows"

    options = []

    if not from_windows:

        if password:
            options.append("password = N'{0}'".format(password))

        if sid:
            options.append("sid = {0}".format(sid))
        else:
            sid = "0x" + hashlib.md5(login.upper().encode('utf-8')).hexdigest()
            options.append("sid = {0}".format(sid))

    if default_language:
        options.append("default_language = [{0}]".format(default_language))

    if default_database:
        options.append("default_database = [{0}]".format(default_database))

    if options:
        sql += " with " + ", ".join(options)

    with connection_factory.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            conn.commit()
            return True


def has_change_default_database(connection_factory, login, default_database):
    """Метод проверяет нужно ли менять базу данных по умолчанию у существующего логина
    Args:
        connection_factory (connection_factory): Коннект к базе данных
        login (str): логин пользователя
        default_database (str): база данных по умолчанию

    Returns:
        bool: Метод возвращает True если база данных по умолчанию будет изменена, в противном случае False
    """
    has_change_default_database_command = '''
                                IF %(default_database)s is not null and not exists(select name from sys.server_principals where name = %(login)s and default_database_name = %(default_database)s)
                                    begin 
                                        select 1
                                    end
                                else
                                    begin
                                        select 0
                                    end'''
    changed = False

    if default_database:
        with connection_factory.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(has_change_default_database_command.format(login, default_database),
                               dict(login=login, default_database=default_database))
                row = cursor.fetchone()
                changed = bool(row[0])

    return changed


def has_change_default_language(connection_factory, login, default_language):
    """Метод проверяет нужно ли менять язык по умолчанию у существующего логина
    Args:
        connection_factory (connection_factory): Коннект к базе данных
        login (str): логин пользователя
        default_language (str): язык по умолчанию

    Returns:
        bool: Метод возвращает True если язык по умолчанию будет изменен, в противном случае False
    """

    has_change_default_language_command = '''
                                if %(default_language)s is not null and not exists(select name from sys.server_principals where name = %(login)s and default_language_name = %(default_language)s)
                                    begin
                                        select 1
                                    end
                                else
                                    begin
                                        select 0
                                    end
                                '''

    changed = False

    if default_language:
        with connection_factory.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(has_change_default_language_command.format(login, default_language),
                               dict(login=login, default_language=default_language))
                row = cursor.fetchone()
                changed = bool(row[0])

    return changed


def has_change_password(connection_factory, login, password):
    """Метод проверяет нужно ли менять базу данных по умолчанию у существующего логиа
    Args:
        connection_factory (connection_factory): Коннект к базе данных
        login (str): логин пользователя
        password (str): пароль

    Returns:
        bool: Метод возвращает True если пароль будет изменен, в противном случае False
    """
    has_change_password_command = '''
                                    if not exists(select * from sys.server_principals sp inner join sys.sql_logins sl on sp.principal_id = sl.principal_id and sp.name = %(login)s and pwdcompare(N'{1}', sl.password_hash) = 1)
                                        begin
                                            if charindex('\', '{0}') = 0
                                                begin
                                                    select 1
                                                end
                                            else
                                                begin
                                                    select 0
                                                end
                                        end
                                    else
                                        begin
                                            select 0
                                        end'''

    changed = False

    with connection_factory.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(has_change_password_command.format(login, password), dict(login=login, password=password))
            row = cursor.fetchone()
            changed = bool(row[0])

    return changed


def is_enabled_login(connection_factory, login):
    """Метод проверяет включен ли логин
    Args:
        connection_factory (connection_factory): Коннект к базе данных
        login (str): логин пользователя

    Returns:
        bool: True если логин включен, в противном случае False.
    """

    is_enabled_login_command = "select ~is_disabled from sys.server_principals where name = %(login)s"

    with connection_factory.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(is_enabled_login_command, dict(login=login))
            row = cursor.fetchone()
            is_enabled = bool(row[0])
            return is_enabled


def change_default_database(connection_factory, login, default_database):
    """Метод изменяет базу данных логин по умолчанию
    Args:
        connection_factory (connection_factory): Коннект к базе данных
        login (str): логин пользователя
        default_database (str): база данных по умолчанию

    Returns:
        bool: True если была изменена база данный по умолчанию
    """
    _alter_default_database_sql = '''
                                if %(default_database)s is not null and not exists(select name from sys.server_principals where name = %(login)s and default_database_name = %(default_database)s)
                                    begin
                                        alter login [{0}] with default_database = [{1}]
                                        select 1;
                                    end
                                else
                                    begin
                                        select 0
                                    end'''

    if not default_database:
        return False

    with connection_factory.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(_alter_default_database_sql.format(login, default_database),
                           dict(login=login, default_database=default_database))
            row = cursor.fetchone()
            conn.commit()
            return bool(row[0])


def change_default_language(connection_factory, login, default_language):
    """Метод изменяет язык логина по умолчанию
    Args:
        connection_factory (connection_factory): Коннект к базе данных
        login (str): логин пользователя
        default_language (str): язык по умолчанию

    Returns:
        bool: True если был изменен язык по умолчанию
    """
    _alter_default_language_sql = '''
                                if %(default_language)s is not null and not exists(select name from sys.server_principals where name = %(login)s and default_language_name = %(default_language)s)
                                    begin
                                        alter login [{0}] with default_language = [{1}]
                                        select 1
                                    end
                                else
                                    begin
                                        select 0
                                    end
                                '''

    if not default_language:
        return False

    with connection_factory.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(_alter_default_language_sql.format(login, default_language),
                           dict(login=login, default_language=default_language))
            row = cursor.fetchone()
            conn.commit()
            return bool(row[0])


def change_password(connection_factory, login, password):
    """Метод изменяет пароль логина
    Args:
        connection_factory (connection_factory): Коннект к базе данных
        login (str): логин пользователя
        password (str): пароль

    Returns:
        bool: True если был изменен пароль, в противном случае False
    """
    _alter_password_sql = '''
                                        if not exists(select * from sys.server_principals sp inner join sys.sql_logins sl on sp.principal_id = sl.principal_id and sp.name = %(login)s and pwdcompare(N'{1}', sl.password_hash) = 1)
                                            begin
                                                if charindex('\', '{0}') = 0
                                                    begin
                                                        alter login [{0}] with password = N'{1}';
                                                        select 1
                                                    end
                                                else
                                                    begin
                                                        select 0
                                                    end
                                            end
                                        else
                                            begin
                                                select 0
                                            end
                                            '''

    if not password:
        return False

    with connection_factory.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(_alter_password_sql.format(login, password), dict(login=login, password=password))
            row = cursor.fetchone()
            conn.commit()
            return bool(row[0])


def disable_or_enable_login(connection_factory, login, enabled=True):
    """Метод изменяет пароль логина
    Args:
        connection_factory (connection_factory): Коннект к базе данных
        login (str): логин пользователя
        enabled (bool): включена ли учетная запись

    Returns:
        bool: True если была включена или выключена учетная запись, в противном случае False
    """

    _disable_or_enable_login_sql = '''
                                if %(disabled)d is not null and not exists(select name from sys.server_principals where name = %(login)s and is_disabled = %(disabled)d)
                                    begin
                                        if %(disabled)d = 1
                                            begin
                                                alter login [{0}] disable;
                                                select 1
                                            end
                                        else
                                            begin
                                                alter login [{0}] enable;
                                                select 1
                                            end
                                    end
                                else
                                    begin
                                        select 0
                                    end'''

    with connection_factory.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(_disable_or_enable_login_sql.format(login), dict(login=login, disabled=not enabled))
            row = cursor.fetchone()
            conn.commit()
            return bool(row[0])


def drop_login(connection_factory, login):
    """Метод удаляет логин
    Args:
        connection_factory (connection_factory): Коннект к базе данных
        login (str): логин пользователя

    Returns:
        int: Метод возвращает количество изменений.
    """

    _drop_login_sql = '''
                    if exists(select name from sys.server_principals where name = %(login)s) 
                        begin
                            drop login [{0}];
                            select 1; 
                        end
                    else
                        begin
                            select 0;
                        end'''

    with connection_factory.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(_drop_login_sql.format(login), dict(login=login))
            row = cursor.fetchone()
            conn.commit()
            return bool(row[0])


# endregion

# region users

def is_database_available(connection_factory, database):
    with connection_factory.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute("select database_id from sys.databases where name = %(database)s and is_read_only = 0 and "
                           "[state] = 0", dict(database=database))
            return cursor.rowcount != 0


def has_drop_user(connection_factory, user_name, database):
    _sql_command = '''
    if exists(select name from sys.database_principals where name = %(user_name)s) 
        begin 
            select 1;
        end
    else
        begin
            select 0;
        end
    '''

    with connection_factory.connect(database=database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(_sql_command.format(user_name), dict(user_name=user_name))
            row = cursor.fetchone()
            return bool(row[0])


def is_primary_hadr_replica(connection_factory, database):
    _sql_command = '''select coalesce(sys.fn_hadr_is_primary_replica(%(database_name)s), 1)'''

    with connection_factory.connect(database="master") as conn:
        with conn.cursor() as cursor:
            cursor.execute(_sql_command, dict(database_name=database))
            row = cursor.fetchone()
            return bool(row[0])


def is_mirror_database(connection_factory, database):
    _sql_command = '''
    if exists(SELECT null FROM sys.database_mirroring AS sd WHERE mirroring_guid IS NOT null and db_name(sd.[database_id]) = %(database_name)s and sd.mirroring_role=2)
        select 1;
    else
        select 0;
    '''

    with connection_factory.connect(database="master") as conn:
        with conn.cursor() as cursor:
            cursor.execute(_sql_command, dict(database_name=database))
            row = cursor.fetchone()
            return bool(row[0])


def drop_user(connection_factory, user_name, database):
    _sql_command = '''
    if exists(select name from sys.database_principals where name = %(user_name)s) 
        begin
            drop user [{0}]; 
            select 1;
        end
    else
        begin
            select 0;
        end
    '''

    with connection_factory.connect(database=database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(_sql_command.format(user_name), dict(user_name=user_name))
            row = cursor.fetchone()
            conn.commit()
            return bool(row[0])


def has_create_user(connection_factory, user_name, login, database):
    _sql_command = '''
                        if not exists(select name from sys.database_principals where name = %(user_name)s) 
                            begin 
                                select 1;
                            end
                        else
                            begin
                                if not exists (select 1 from sys.database_principals sdp inner join sys.server_principals ssp on ssp.sid = sdp.sid and ssp.name = %(user_name)s and sdp.name = %(user_name)s)
                                    begin
                                        select 1
                                    end
                                else
                                    begin
                                        select 0;
                                    end
                        end'''

    with connection_factory.connect(database=database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(_sql_command.format(user_name, login), dict(user_name=user_name))
            row = cursor.fetchone()
            conn.commit()
            return bool(row[0])


def create_user(connection_factory, user_name, login, database):
    _sql_command = '''
                        if not exists(select name from sys.database_principals where name = %(user_name)s) 
                            begin 
                                create user [{0}] for login [{1}];
                                select 1;
                            end
                        else
                            begin
                                if not exists (select 1 from sys.database_principals sdp inner join sys.server_principals ssp on ssp.sid = sdp.sid and ssp.name = %(user_name)s and sdp.name = %(user_name)s)
                                    begin
                                        alter user [{0}] with login = [{1}];
                                        select 1
                                    end
                                else
                                    begin
                                        select 0;
                                    end
                        end'''

    with connection_factory.connect(database=database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(_sql_command.format(user_name, login), dict(user_name=user_name))
            row = cursor.fetchone()
            conn.commit()
            return bool(row[0])


def get_available_roles(connection_factory, database='master'):
    _sql_command = '''
    SELECT rl.name AS [database_role] FROM sys.database_principals AS rl WHERE (rl.type = 'R') ORDER BY [database_role] ASC
    '''
    with connection_factory.connect(database=database) as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.execute(_sql_command)
            roles = []
            for row in cursor:
                role = row["database_role"]
                roles.append(role)

            return roles


def get_user_roles(connection_factory, user_name, database):
    _sql_command = '''
    select rp.name as database_role from sys.database_role_members drm
        inner join sys.database_principals rp on (drm.role_principal_id = rp.principal_id)
        inner join sys.database_principals mp on (drm.member_principal_id = mp.principal_id)
    where mp.name = %(user_name)s
    '''
    with connection_factory.connect(database=database) as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.execute(_sql_command, dict(user_name=user_name))
            roles = []
            for row in cursor:
                role = row["database_role"]
                roles.append(role)

            return roles


def add_user_role(connection_factory, user_name, role_name, database, sql_server_version=12):
    if sql_server_version in [12, 14]:
        _sql_command = "alter role [{0}] add member [{1}]"
    else:
        _sql_command = "exec sp_addrolemember %(role_name)s, %(user_name)s"

    with connection_factory.connect(database=database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(_sql_command.format(role_name, user_name), dict(role_name=role_name, user_name=user_name))
            conn.commit()
            return True


def remove_user_role(connection_factory, user_name, role_name, database, sql_server_version=12):
    if sql_server_version in [12, 14]:
        _sql_command = "alter role [{0}] drop member [{1}]"
    else:
        _sql_command = "exec sp_droprolemember %(role_name)s, %(user_name)s"

    with connection_factory.connect(database=database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(_sql_command.format(role_name, user_name), dict(role_name=role_name, user_name=user_name))
            conn.commit()
            return True


def has_sync_user_roles(connection_factory, user_name, roles, database):
    current_user_roles = get_user_roles(connection_factory, user_name, database)
    deleted = set(current_user_roles) - set(roles)
    add = set(roles) - set(current_user_roles)

    return len(deleted) > 0 or len(add)


def sync_user_roles(connection_factory, user_name, roles, database, sql_server_version=10):
    current_user_roles = get_user_roles(connection_factory, user_name, database)
    deleted = set(current_user_roles) - set(roles)
    add = set(roles) - set(current_user_roles)
    changed = False

    for role in deleted:
        if remove_user_role(connection_factory, user_name, role, database, sql_server_version):
            changed = True

    for role in add:
        if add_user_role(connection_factory, user_name, role, database, sql_server_version):
            changed = True

    return changed

# endregion
