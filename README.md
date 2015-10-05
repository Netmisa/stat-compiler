README
======

What is STAT-COMPILER ?
------------------

- [Git](https://github.com/CanalTP/stat-compiler)


Requirements
-------------

- Git (https://git-scm.com/downloads)
- Composer (https://getcomposer.org/download/)


Installation
-------------

### 1. Clone the repository

> git clone git@github.com:CanalTP/stat-compiler.git

### 2. Initializing with Composer

> composer.phar install

### 3. Create in conf/ folder your own config files based on *.dist ones:

> parameters.xml


How to update database
-----------------------

The following command is used to update the database:

> bin/stat_compiler updatedb


Example use cases:

1/ I want to update the data on a specific period. Example: from 2015-10-01 to 2015-11-01.

> bin/stat_compiler updatedb 2015-10-01 2015-11-01

- start_date: Consolidation start date (YYYY-MM-DD). Defaults to yesterday.
- end_date: Consolidation end date (YYYY-MM-DD). Defaults to yesterday.

2/ Based on the above example, I want to update only the table requests_calls.

> bin/stat_compiler updatedb 2015-10-01 2015-11-01 --only-update=resquests_calls

For more information about the arguments and options, execute command:

> bin/stat_compiler updatedb --help


Contributing
-------------

1. Vincent Lepot - vincent.lepot@canaltp.fr