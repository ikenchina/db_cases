# 案例脚本

案例：https://ikenchina.github.io/2021/09/10/postgresql_sql_case/#more

## setup.sql

用来创建表和生成数据


## cost_index.py

估算此案例的start cost 和 run cost。

一般情况下是正确的。


**获取表的统计信息**
```
select * from pg_stats where tablename='users';
```

**获取主键信息**
```
SELECT relpages, reltuples FROM pg_class WHERE relname = 'users_pkey';
```

**获取主表信息**
```
SELECT relpages, reltuples FROM pg_class WHERE relname = 'users';
```
