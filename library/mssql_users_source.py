#!/usr/bin/python3
# -*- coding: utf-8 -*-

# (c) 2019, Artem Sedykh <artem.sedykh@anywayanyday.com>
# Outline and parts are reused from mssql_db
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type


import os
import traceback
import json
import glob
from ansible.module_utils.basic import AnsibleModule


def main():
    module_args = dict(
        sources=dict(required=True, type='list', elements='path'))

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    from ansible.module_utils.sql_objects import SqlLogin
    sources = module.params['sources']

    sql_logins = {}
    files_info = {}

    for path in sources:
        file_paths = glob.glob(path)

        files_info[path] = { 'files':file_paths }
        
        if not file_paths:
            msg = "no files found for source: {0}".format(path)
            module.log(msg)
            files_info[path]['warnings'].append(msg)
            continue

        for file_path in file_paths:
            try:
                with open(file_path, "r") as read_file:
                    data = json.load(read_file)
                    for item in SqlLogin.parse(data):
                        if item.login in sql_logins:
                            module.fail_json(msg="DUPLICATE LOGIN: [{0}], FILE: [{1}]".format(item.login,file_path))
                        sql_logins[item.login] = item
            except Exception as e:
                module.fail_json(msg="FILE: %s, PARSE SQL LOGIN EXCEPTION: %s" % (file_path, str(e)))

    ansible_facts  = { 'sql_logins':json.loads(json.dumps(sql_logins, default=lambda o: o.__dict__, sort_keys=True)), 'files_info': files_info }

    module.exit_json(changed=False, ansible_facts = ansible_facts)

if __name__ == '__main__':
    main()

