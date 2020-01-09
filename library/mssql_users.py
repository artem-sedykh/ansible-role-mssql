#!/usr/bin/python3
# -*- coding: utf-8 -*-

# (c) 2019, Artem Sedykh <artem.sedykh@anywayanyday.com>
# Outline and parts are reused from mssql_db
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type

import time

ANSIBLE_METADATA = {'metadata_version': '1.1',
                    'status': ['preview'],
                    'supported_by': 'artem.sedykh'}

DOCUMENTATION = '''
---
module: mssql_users
short_description: Add or remove MSSQL users and logins from a remote host.
description:
   - Add or remove MSSQL users and logins from a remote host.
version_added: "2.8"
options:
  sources:
    description:
      - json files contains informations of the users
    required: true
  login:
    description:
      - The username used to authenticate with
  password:
    description:
      - The password used to authenticate with
  host:
    description:
      - Host running the database
  port:
    description:
      - Port of the MSSQL server. Requires login_host be defined as other then localhost if login_port is used
    default: 1433
notes:
   - Requires the pymssql Python package on the remote host. For Ubuntu, this
     is as easy as pip install pymssql (See M(pip).)
requirements:
   - python >= 2.7
   - pymssql
author:Artem Sedykh <artem.sedykh@anywayanyday.com>
'''

EXAMPLES = '''
# Create a new login
- mssql_users:
    host: '{{ sql_host }}'
    login: '{{ sql_user }}'
    password: '{{ sql_password }}'
    sources:
      - information.json
'''

RETURN = '''
#
'''

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

try:
    import multiprocessing
    from joblib import Parallel, delayed
except ImportError:
    JOBLIB_IMP_ERR = traceback.format_exc()
    joblib_found = False
else:
    joblib_found = True

from ansible.module_utils.basic import AnsibleModule


def main():
    module_args = dict(
        sources=dict(required=True, type='list', elements='path'),
        login=dict(default=''),
        password=dict(default='', no_log=True),
        host=dict(required=True),
        port=dict(default='1433'))

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    if not mssql_found:
        module.fail_json(msg='required pymssql module', exception=PYMSSQL_IMP_ERR)

    if not joblib_found:
        module.fail_json(msg='required joblib module', exception=JOBLIB_IMP_ERR)

    from ansible.module_utils.sql_objects import SqlLogin
    from ansible.module_utils.db_provider import ConnectionFactory
    from ansible.module_utils.sql_processor import SqlProcessor

    login = module.params['login']
    password = module.params['password']
    host = module.params['host']
    port = module.params['port']
    sources = module.params['sources']

    login_querystring = host
    if port != "1433":
        login_querystring = "%s:%s" % (host, port)

    if login != "" and password == "":
        module.fail_json(msg="when supplying login arguments password must be provided")

    start_time = time.time()
    connection_factory = ConnectionFactory(login_querystring, login, password)
    warnings = []

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

    sql_logins = {}

    for path in sources:
        file_paths = glob.glob(path)

        if not file_paths:
            msg = "no files found for source: {0}".format(path)
            module.log(msg)
            warnings.append(msg)
            continue

        for file_path in file_paths:
            try:
                with open(file_path, "r") as read_file:
                    data = json.load(read_file)
                    for item in SqlLogin.parse(data):
                        if item.login in sql_logins:
                            msg = "Login duplication [{0}]".format(item.login)
                            module.log(msg)
                            warnings.append(msg)
                        sql_logins[item.login] = item
            except Exception as e:
                module.fail_json(msg="file: %s, parse sql logins exception: %s" % (file_path, str(e)))

    changed = False

    num_cores = multiprocessing.cpu_count()

    module_info = {'sql_server_version': sql_server_version, 'cpu_count': num_cores}

    results = SqlProcessor.apply_sql_logins(sql_logins, connection_factory, sql_server_version, module.check_mode)

    end_time = time.time()

    module_info['execution_time'] = end_time - start_time
    module_info['sql_logins_count'] = len(sql_logins)

    module_info['changes'] = results[0]
    module_info['warnings'] = results[1]
    module_info['errors'] = results[2]

    module.exit_json(changed=changed, changes=module_info)


if __name__ == '__main__':
    main()
