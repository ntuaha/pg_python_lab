# 使用 Python 與 postgresql 做資料處理最佳化手段研究

最快的速度與最少的記憶體資源，一直都是資料科學家最重要的基礎工作，這裡是要針對常見的操作手法，做一系列的基礎研究並找出最佳解決方案，做為未來研究與業務上最佳好用的參考文件。
## 驗證手法

```shell
python3 query.py
```


## 建立資料庫
```sql
CREATE DATABASE "aha" ENCODING 'UTF8';
CREATE DATABASE "rawdb" ENCODING 'UTF8';
CREATE DATABASE "features" ENCODING 'UTF8';
-- create user aha with encrypted password '<password>';
alter user aha with encrypted password '<password>';
grant all privileges on database aha to aha;
grant all privileges on database rawdb to aha;
grant all privileges on database features to aha;

SHOW hba_files;
 -- /var/lib/pgsql/13/data/pg_hba.conf
```



## 建立資料表 @ rawdb
```sql
create schema lab;
create table if not exists lab.t1(
    a1 real,
    a2 real,
    a3 real,
    a4 real,
    a5 real,
    a6 real,
    a7 real,
    a8 real
);
```
## 建立資料表 @ features
```sql
create schema lab;
create UNLOGGED table if not exists lab.t1(
    a1 real,
    a2 real,
    a3 real,
    a4 real,
    a5 real,
    a6 real,
    a7 real,
    a8 real
);
```

## 放入資料

```shell
python3 init_db.py
```
