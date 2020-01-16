#!/usr/bin/python3
# -*- coding: utf-8 -*-

# (c) 2019, Artem Sedykh <artem.sedykh@anywayanyday.com>
# Outline and parts are reused from mssql_db
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type

import time
import os
import traceback
import json
import glob

PYMSSQL_IMP_ERR = None
JSON_IMP_ERR = None

try:
    import pymssql
except ImportError:
    PYMSSQL_IMP_ERR = traceback.format_exc()
    mssql_found = False
else:
    mssql_found = True

from ansible.module_utils.basic import AnsibleModule

def main():

    connection_spec=dict(
        type='dict',
        required=True,
        login=dict(type='str', required=True),
        password=dict(type='str', no_log=True, required=True),
        host=dict(type='str', required=True),
        port=dict(type='int', default=1433, required=False)
    )

    database_spec=dict(
        name=dict(type='str', required=True),
        state=dict(choices=['present', 'absent'], default='present', required=True),
        roles=dict(type='list', elements='str')
    )

    user_spec=dict(
        name=dict(type='str', required=True),
        state=dict(choices=['present', 'absent'], default='present', required=True),
        databases=dict(type='list', elements='dict', options=database_spec)
    )

    sql_login_spec=dict(
        type='dict',
        login=dict(type='str', required=True),
        enabled=dict(type='bool',default=True, required=True),
        sid=dict(type='str', required=False),
        state=dict(choices=['present', 'absent'], default='present', required=True),
        password=dict(type='str', no_log=True, required=False),
        default_database=dict(type='str', required=False),
        default_language=dict(type='str', required=False),
        users=dict(type='list', elements='dict', options=user_spec)
    )

    module_args=dict(connection=connection_spec, sql_login=sql_login_spec)

    module = AnsibleModule(argument_spec=module_args, supports_check_mode=True)

    if not mssql_found:
        module.fail_json(msg='required pymssql module', exception=PYMSSQL_IMP_ERR)

    from ansible.module_utils.sql_objects import SqlLogin
    from ansible.module_utils.db_provider import ConnectionFactory
    import ansible.module_utils.sql_processor as SqlProcessor

    connection_settings = module.params['connection']
    login = connection_settings['login']
    password = connection_settings['password']
    host = connection_settings['host']
    port = connection_settings['port']

    sql_item = SqlLogin.from_json(module.params['sql_login'])

    login_querystring = host
    if port != "1433":
        login_querystring = "%s:%s" % (host, port)

    if login != "" and password == "":
        module.fail_json(msg="when supplying login arguments password must be provided")

    start_time = time.time()
    connection_factory = ConnectionFactory(login_querystring, login, password)

    try:
        sql_server_version = connection_factory.get_sql_server_version()
    except Exception as e:
        if "Unknown database" in str(e):
            errno, errstr = e.args
            module.fail_json(msg="ERROR: %s %s" % (errno, errstr))
        else:
            module.fail_json(msg="unable to connect to {0}, check login and password are correct".format(host))

    major_sql_server_version = int(sql_server_version.split('.')[0])

    if major_sql_server_version not in [10, 12, 14]:
        module.fail_json(msg="sql server version {0} not supported".format(sql_server_version))

    try:
        result = SqlProcessor.apply_sql_login(connection_factory, sql_item, major_sql_server_version, module.check_mode)
    except Exception as e:
        module.fail_json(msg="{0}".format(str(e)))

    end_time = time.time()

    execution_time = end_time - start_time

    if result[3]:
        module.fail_json(msg="; ".join(result[3]))

    module.exit_json(changed=result[4], sql_server_version=sql_server_version, execution_time=execution_time, changes=result[0], sql_info=result[1], sql_warnings=result[2], sql_errors=result[3])

if __name__ == '__main__':
    main()

