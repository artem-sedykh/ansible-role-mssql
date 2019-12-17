import pymssql
import hashlib


def is_sql_server_available(connectionFactory):
    """Метод поверяет можно ли подключиться к базе данных
    Args:
        connectionFactory (ConnectionFactory): коннект к базе данных

    Returns:
        bool: Метод возвращает True к базе данных можно подключиться, в противном случае False.
    """
    try:
        with connectionFactory.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT database_id FROM sys.databases WHERE name = %s", "tempdb")
                return True
    except Exception:
        return False


def login_exists(connectionFactory, login):
    """Метод проверять существует ли логин.
    Args:
        connectionFactory (ConnectionFactory): Коннект к базе данных
        login (str): логин пользователя

    Returns:
        bool: Метод возвращает True если такой логин уже существует, в противном случае False.
    """

    if not login:
        raise ValueError("login cannot be empty")

    with connectionFactory.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT NULL FROM master.sys.syslogins WHERE name = %(login)s", dict(login=login))
            return bool(cursor.rowcount)


def create_login(connectionFactory, login, password=None, sid=None, default_database=None, default_language=None):
    """Метод создает логин.
    Args:
        connectionFactory (ConnectionFactory): Коннект к базе данных
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

    with connectionFactory.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            conn.commit()


def has_change_default_database(connectionFactory, login, default_database):
    """Метод проверяет нужно ли менять базу данных по умолчанию у существующего логина
    Args:
        connectionFactory (ConnectionFactory): Коннект к базе данных
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
        with connectionFactory.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(has_change_default_database_command.format(login, default_database),
                               dict(login=login, default_database=default_database))
                row = cursor.fetchone()
                changed = bool(row[0])

    return changed


def has_change_default_language(connectionFactory, login, default_language):
    """Метод проверяет нужно ли менять язык по умолчанию у существующего логина
    Args:
        connectionFactory (ConnectionFactory): Коннект к базе данных
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
        with connectionFactory.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(has_change_default_language_command.format(login, default_language),
                               dict(login=login, default_language=default_language))
                row = cursor.fetchone()
                changed = bool(row[0])

    return changed


def has_change_password(connectionFactory, login, password):
    """Метод проверяет нужно ли менять базу данных по умолчанию у существующего логиа
    Args:
        connectionFactory (ConnectionFactory): Коннект к базе данных
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

    with connectionFactory.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(has_change_password_command.format(login, password), dict(login=login, password=password))
            row = cursor.fetchone()
            changed = bool(row[0])

    return changed


def is_enabled_login(connectionFactory, login):
    """Метод проверяет включен ли логин
    Args:
        connectionFactory (ConnectionFactory): Коннект к базе данных
        login (str): логин пользователя

    Returns:
        bool: True если логин включен, в противном случае False.
    """

    is_enabled_login_command = "select ~is_disabled from sys.server_principals where name = %(login)s"

    with connectionFactory.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(is_enabled_login_command, dict(login=login))
            row = cursor.fetchone()
            is_enabled = bool(row[0])
            return is_enabled


def change_default_database(connectionFactory, login, default_database):
    """Метод изменяет базу данных логин по умолчанию
    Args:
        connectionFactory (ConnectionFactory): Коннект к базе данных
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

    with connectionFactory.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(_alter_default_database_sql.format(login, default_database),
                           dict(login=login, default_database=default_database))
            row = cursor.fetchone()
            conn.commit()
            return bool(row[0])


def change_default_language(connectionFactory, login, default_language):
    """Метод изменяет язык логина по умолчанию
    Args:
        connectionFactory (ConnectionFactory): Коннект к базе данных
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

    with connectionFactory.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(_alter_default_language_sql.format(login, default_language),
                           dict(login=login, default_language=default_language))
            row = cursor.fetchone()
            conn.commit()
            return bool(row[0])


def change_password(connectionFactory, login, password):
    """Метод изменяет пароль логина
    Args:
        connectionFactory (ConnectionFactory): Коннект к базе данных
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

    with connectionFactory.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(_alter_password_sql.format(login, password), dict(login=login, password=password))
            row = cursor.fetchone()
            conn.commit()
            return bool(row[0])


def disable_or_enable_login(connectionFactory, login, enabled=True):
    """Метод изменяет пароль логина
    Args:
        connectionFactory (ConnectionFactory): Коннект к базе данных
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

    with connectionFactory.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(_disable_or_enable_login_sql.format(login), dict(login=login, disabled=not enabled))
            row = cursor.fetchone()
            conn.commit()
            return bool(row[0])


def drop_login(connectionFactory, login):
    """Метод удаляет логин
    Args:
        connectionFactory (ConnectionFactory): Коннект к базе данных
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

    with connectionFactory.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(_drop_login_sql.format(login), dict(login=login))
            row = cursor.fetchone()
            conn.commit()
            return bool(row[0])