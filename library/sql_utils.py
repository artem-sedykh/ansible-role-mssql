import pymssql
import hashlib
import db_provider

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
            cursor.execute("SELECT NULL FROM master.sys.syslogins WHERE name = %s", login)
            return bool(cursor.rowcount)

def create_login(connectionFactory, login, password = None, sid = None, default_database = None, default_language = None):
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

    fromWindows = "\\" in login

    if fromWindows:
        sql += " from windows"

    options = []

    if not fromWindows:

        if password:
            options.append("password = N'{0}'".format(password))

        if sid:
            options.append("sid = {0}".format(sid))
        else:
            sid = "0x"+hashlib.md5(login.upper().encode('utf-8')).hexdigest()
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

def alter_login(connectionFactory, login, password, enabled=True, default_database = None, default_language = None):
    """Метод изменяет существующий логин
    Args:
        connectionFactory (ConnectionFactory): Коннект к базе данных
        login (str): логин пользователя
        password (str): пароль
        enabled (bool): включена ли учетная запись
        default_database (str): база данных по умолчанию
        default_language (str): язык по умолчанию

    Returns:
        int: Метод возвращает количество изменений.
    """

    alter_default_database = '''
                                if %(default_database)s is not null and not exists(select name from sys.server_principals where name = %(login)s and default_database_name = %(default_database)s)
                                    begin
                                        alter login [{0}] with default_database = [{1}]
                                        select 1;
                                    end
                                else
                                    begin
                                        select 0
                                    end'''

    alter_default_language = '''
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

    alter_login_sql_command='''
                                declare @applied int
                                set @applied = 0;

                                if not exists(select * from sys.server_principals sp inner join sys.sql_logins sl on sp.principal_id = sl.principal_id and sp.name = %(login)s and pwdcompare(N'{1}', sl.password_hash) = 1)
                                begin
                                  if charindex('\', '{0}') = 0
                                  begin
                                    alter login [{0}] with password = N'{1}';
                                    set @applied = @applied + 1;
                                  end
                                end

                                if %(disabled)d is not null and not exists(select name from sys.server_principals where name = %(login)s and is_disabled = %(disabled)d)
                                begin
                                  if %(disabled)d = 1
                                  begin
                                    alter login [{0}] disable;
                                    set @applied = @applied + 1;
                                  end
                                  else
                                  begin
                                    alter login [{0}] enable;
                                    set @applied = @applied + 1;
                                  end
                                end

                              select @applied'''

    result = 0

    with connectionFactory.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(alter_login_sql_command.format(login, password), dict(login = login, disabled = not enabled))
            row = cursor.fetchone()
            result += int(row[0])

            if default_database:
                cursor.execute(alter_default_database.format(login, default_database), dict(login = login, default_database = default_database))
                row = cursor.fetchone()
                result += int(row[0])

            if default_language:
                cursor.execute(alter_default_language.format(login, default_language), dict(login = login, default_language = default_language))
                row = cursor.fetchone()
                result += int(row[0])

    return result

def drop_login(connectionFactory, login):
    """Метод удаляет логин
    Args:
        connectionFactory (ConnectionFactory): Коннект к базе данных
        login (str): логин пользователя

    Returns:
        int: Метод возвращает количество изменений.
    """

    drop_login_sql_command='''
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
            cursor.execute(drop_login_sql_command.format(login), dict(login = login))
            row = cursor.fetchone()
            return int(row[0])