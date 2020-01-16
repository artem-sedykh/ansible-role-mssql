Role Name
=========

A brief description of the role goes here.

Requirements
------------

Any pre-requisites that may not be covered by Ansible itself or the role should be mentioned here. For instance, if the role uses the EC2 module, it may be a good idea to mention in this section that the boto package is required.

Role Variables
--------------

A description of the settable variables for this role should go here, including any variables that are in defaults/main.yml, vars/main.yml, and any variables that can/should be set via parameters to the role. Any variables that are read from other roles and/or the global scope (ie. hostvars, group vars, etc.) should be mentioned here as well.

Dependencies
------------

A list of other roles hosted on Galaxy should go here, plus any details in regards to parameters that may need to be set for other roles, or variables that are used from other roles.

Example Playbook
----------------

Including an example of how to use your role (for instance, with variables passed in as parameters) is always nice for users too:

```yaml
- hosts: sql_servers
  gather_facts: no
  tasks:
    - import_role:
       name: artem_sedykh.mssql_security
      vars:
        sources:
          - users_1.json
      register: out
      delegate_to: localhost

    - debug: var=out
```

## Example roles source:

You can define database roles with following syntax (will add mssql_internal_user with access to testdb, master; add domain\\DomainUser with access to testdb, master, remove access for domain\\DomainUser2 to testdb, master):

```json
{
    "mssql_internal_user": {
        "state": "present",
        "enabled": "true",
        "default_language": "English",
        "password": "12asdasSSSdd33",
        "sid": "0xE06B7BAED3369C869B20CAC9421E7D51",
        "users": {
            "mssql_internal_user": {
                "databases": {
                    "testdb": { "roles": ["db_datareader"] },
                    "master": { "roles": ["db_datareader", "db_datawriter"], "state": "present" } }
                }
            }
        }
    },
    "domain\\DomainUser": {
        "state": "present",
        "enabled": "true",
        "default_language": "English",
        "users": {
            "domain\\DomainUser": {
                "state": "present",
                "databases": {
                    "testdb": { "roles": ["db_datareader", "db_datawriter"], "state": "present" },
                    "master": { "roles": ["db_datareader"]}
                }
            }
        }
    },
    "domain\\DomainUser2": {
        "state": "absent",
        "users": {
            "domain\\DomainUser2": {
                "databases": {
                    "testdb": { "roles": ["db_datareader", "db_datawriter"], "state": "absent" },
                    "master": { "roles": ["db_datareader"] }
                }
            }
        }
    }
}
```

##### Parameters description:

| parameter        | possible values                                              | description                                                  |
| :--------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| username         | "mssql_user", "domain\user"                                  | can be domain user or internal mssql user                    |
| enabled          | "true", "false" (default: true)                              | user login enabled or disabled                               |
| default_database | database, None (default: None)                               | sets default database for user                               |
| default_language | "English", "Russian", ..., None (default: None)              | sets default language for user                               |
| state            | "absent", "present"                                          | add or remove this user access rights                        |
| password         | any string                                                   | user password (suitable only for internal mssql user) and not suitable for domain user. |
| sid              | 0xANY_HEX_NUMBER                                             | hex identifier to identify internal mssql user (not suitable for domain user) |
| roles            | "db_datareader","db_datawriter", "db_ddladmin","db_owner", etc... | array of database roles                                      |

 



License
-------

GNU General Public License v3.0

Author Information
------------------

An optional section for the role authors to include contact information, or a website (HTML is not allowed).
