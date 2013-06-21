
# pg on Debian/GNU Linux

changed /etc/postgresql/8.4/main/pg_hba.conf to include:

```
# "local" is for Unix domain socket connections only
local   all         all                               trust
# IPv4 local connections:
host    all         all         127.0.0.1/32          trust
# IPv6 local connections:
host    all         all         ::1/128               trust
```

then

```
$ sudo su - postgres
# psql
psql> create database ruote_test;
psql> create user jmettraux;
psql> grant all privileges on ruote_test to jmettraux;
```

