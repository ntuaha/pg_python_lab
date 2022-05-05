## 從 SQL 讀取資料到 Python 

讀取資料有以下幾種方式，依照最終格式的不同，可以有以下幾個操作：


### 實驗數據

| 測試案例 | 讀取策略 | 從 SQL 讀取 | 讀取類型 | 本機佔存方法 | 本機轉換資料方法 | 資料筆數 | 總時間 |格式轉換時間|記憶體用量|平均每秒處理數量|
| -------- | -------- | -------- | -------- | -------- | -------- | -------- | -------- | -------- | -------- | -------- | 
|lab.t1||fetchone|csv|      |      |      |      |      |      |      |      |
|lab.t1||fetchmany|csv|      |      |      |      |      |      |      |      |
|lab.t1||copy_expert|csv|tempfile|pd.read_csv (default)|(10000000,8)|12.36|4.593|1220|
|lab.t1||copy_expert|csv|tempfile|pd.read_csv (pyarrow)|(10000000,8)|10.01|2.238|1781|1M| 
|lab.t1||copy_expert|csv|tempfile|pyarrow.csv.read_csv.to_pandas|(10000000,8)|10.01 |2.177|1685|1M| 