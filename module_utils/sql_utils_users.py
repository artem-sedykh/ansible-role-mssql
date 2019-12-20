import pymssql


def is_database_available(connectionFactory, database):
    with connectionFactory.connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute("select database_id from sys.databases where name = %(database)s and is_read_only = 0 and "
                           "[state] = 0 and collation_name is not null", dict(database=database))
            return cursor.rowcount != 0


def has_drop_user(connectionFactory, user_name, database):
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

    with connectionFactory.connect(database=database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(_sql_command.format(user_name), dict(user_name=user_name))
            row = cursor.fetchone()
            return bool(row[0])


def drop_user(connectionFactory, user_name, database):
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

    with connectionFactory.connect(database=database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(_sql_command.format(user_name), dict(user_name=user_name))
            row = cursor.fetchone()
            conn.commit()
            return bool(row[0])


def has_create_user(connectionFactory, user_name, login, database):
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

    with connectionFactory.connect(database=database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(_sql_command.format(user_name, login), dict(user_name=user_name))
            row = cursor.fetchone()
            conn.commit()
            return bool(row[0])


def create_user(connectionFactory, user_name, login, database):
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

    with connectionFactory.connect(database=database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(_sql_command.format(user_name, login), dict(user_name=user_name))
            row = cursor.fetchone()
            conn.commit()
            return bool(row[0])


def get_user_roles(connectionFactory, user_name, database):
    _sql_command = '''
    select rp.name as database_role from sys.database_role_members drm
        inner join sys.database_principals rp on (drm.role_principal_id = rp.principal_id)
        inner join sys.database_principals mp on (drm.member_principal_id = mp.principal_id)
    where mp.name = %(user_name)s
    '''
    with connectionFactory.connect(database=database) as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.execute(_sql_command, dict(user_name=user_name))
            roles = []
            for row in cursor:
                role = row["database_role"]
                roles.append(role)

            return roles


def add_user_role(connectionFactory, user_name, role_name, database, sql_server_version=12):
    if sql_server_version == 12:
        _sql_command = "alter role [{0}] add member [{1}]"
    else:
        _sql_command = "exec sp_addrolemember %(role_name)s, %(user_name)s"

    with connectionFactory.connect(database=database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(_sql_command.format(role_name, user_name), dict(role_name=role_name, user_name=user_name))
            conn.commit()
            return True


def remove_user_role(connectionFactory, user_name, role_name, database, sql_server_version=12):
    if sql_server_version == 12:
        _sql_command = "alter role [{0}] drop member [{1}]"
    else:
        _sql_command = "exec sp_droprolemember %(role_name)s, %(user_name)s"

    with connectionFactory.connect(database=database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(_sql_command.format(role_name, user_name), dict(role_name=role_name, user_name=user_name))
            conn.commit()
            return True


def has_sync_user_roles(connectionFactory, user_name, roles, database, sql_server_version=10):
    current_user_roles = get_user_roles(connectionFactory, user_name, database)
    deleted = set(current_user_roles) - set(roles)
    add = set(roles) - set(current_user_roles)

    return len(deleted) > 0 or len(add)


def sync_user_roles(connectionFactory, user_name, roles, database, sql_server_version=10):
    current_user_roles = get_user_roles(connectionFactory, user_name, database)
    deleted = set(current_user_roles) - set(roles)
    add = set(roles) - set(current_user_roles)
    changed = False

    for role in deleted:
        if remove_user_role(connectionFactory, user_name, role, database, sql_server_version):
            changed = True

    for role in add:
        if add_user_role(connectionFactory, user_name, role, database, sql_server_version):
            changed = True

    return changed
