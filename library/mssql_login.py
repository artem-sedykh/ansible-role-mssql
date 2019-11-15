#!/usr/bin/python
# -*- coding: utf-8 -*-

# (c) 2019, Artem Sedykh <artem.sedykh@anywayanyday.com>
# Outline and parts are reused from mssql_db
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function
__metaclass__ = type


ANSIBLE_METADATA = {'metadata_version': '1.1',
                    'status': ['preview'],
                    'supported_by': 'activated'}


DOCUMENTATION = '''
---
module: mssql_login
short_description: Add or remove MSSQL users and logins from a remote host.
description:
   - Add or remove MSSQL users and logins from a remote host.
version_added: "2.8"
options:
  files:
    description:
      - json files contains informations of the login
    required: true
  login_user:
    description:
      - The username used to authenticate with
  login_password:
    description:
      - The password used to authenticate with
  login_host:
    description:
      - Host running the database
  login_port:
    description:
      - Port of the MSSQL server. Requires login_host be defined as other then localhost if login_port is used
    default: 1433
notes:
   - Requires the pymssql Python package on the remote host. For Ubuntu, this
     is as easy as pip install pymssql (See M(pip).)
requirements:
   - python >= 2.7
   - pymssql
   - json
author:Artem Sedykh <artem.sedykh@anywayanyday.com>
'''

EXAMPLES = '''
# Create a new login
- mssql_db:
    files: [ 'logins.json' ]
'''

RETURN = '''
#
'''

import os
import traceback

PYMSSQL_IMP_ERR = None
JSON_IMP_ERR = None

try:
    import pymssql
except ImportError:
    PYMSSQL_IMP_ERR = traceback.format_exc()
    mssql_found = False
else:
    mssql_found = True

try:
    import json
except ImportError:
    JSON_IMP_ERR = traceback.format_exc()
    json_found = False
else:
    json_found = True

from ansible.module_utils.basic
import AnsibleModule, missing_required_lib, json, db_provider, module_objects, sql_utils 

def main():
    module = AnsibleModule(
        argument_spec=dict(
            files=dict(required=True),
            login_user=dict(default=''),
            login_password=dict(default='', no_log=True),
            login_host=dict(required=True),
            login_port=dict(default='1433')
        )
    )

    if not mssql_found:
        module.fail_json(msg=missing_required_lib('pymssql'), exception=PYMSSQL_IMP_ERR)

    if not json_found:
        module.fail_json(msg=missing_required_lib('json'), exception=JSON_IMP_ERR)

    files = module.params['files']

    login_user = module.params['login_user']
    login_password = module.params['login_password']
    login_host = module.params['login_host']
    login_port = module.params['login_port']
    files = module.params['files']

    login_querystring = login_host
    if login_port != "1433":
        login_querystring = "%s:%s" % (login_host, login_port)

    if login_user != "" and login_password == "":
        module.fail_json(msg="when supplying login_user arguments login_password must be provided")

    factory = db_provider.ConnectionFactory(login_querystring, login_user, login_password)

    # проверка соединения
    try:
        conn = factory.connect()
        cursor = conn.cursor()
    except Exception as e:
        if "Unknown database" in str(e):
            errno, errstr = e.args
            module.fail_json(msg="ERROR: %s %s" % (errno, errstr))
        else:
            module.fail_json(msg="unable to connect, check login_user and login_password are correct, or alternatively check your "
                                 "@sysconfdir@/freetds.conf / ${HOME}/.freetds.conf")

    sql_logins = []

    try:
        for file in files:
            with open(file, "r") as read_file:
                data = json.load(read_file)
                items = module_objects.parse_sql_users(data)
                sql_logins.append(items)
    except Exception as e:
         module.fail_json(msg="parse sql logins exception: %s" % (str(e)))

    changed = False

    for sql_login in sql_logins:
        try:
            changed = changed or apply_to_sql_login(connectionFactory, sql_login)
        except Exception as e:
             module.fail_json(msg="error deleting login: " + str(e))

    module.exit_json(changed = changed, logins = sql_logins)

def apply_to_sql_login(connectionFactory, sql_login):
    if sql_login.state == "present":
        changes = 0

        exists = sql_utils.login_exists(connectionFactory, sql_login.login)

        if not exists:
            sql_utils.create_login(connectionFactory, login=sql_login.login, password = sql_login.password, sid = sql_login.sid, default_database = sql_login.default_database, default_language=sql_login.default_language)
            changes += 1

        changes += sql_utils.alter_login(connectionFactory, login = sql_login.login, sql_login = sql_login.password, enabled = sql_login.enabled, default_database = sql_login.default_database, default_language=sql_login.default_language)

    if sql_login.state == "absent"
        changes += sql_utils.drop_login(connectionFactory, login = sql_login.login)

    return return changes > 0


if __name__ == '__main__':
    main()