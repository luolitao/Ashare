import sys
import os
import json
import time
import pandas as pd
from sqlalchemy import text

import datetime

# 1. 路径修复：确保能引用到 ashare 包
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ashare.core.db import DatabaseConfig, MySQLWriter

def execute_sql(sql_raw: str, limit: int = 50):
    """
    智能 SQL 执行器：
    1. 支持 'desc <table>' 快捷指令
    2. 支持 ';' 分隔的多语句执行
    3. 自动为 SELECT 注入 LIMIT
    4. 返回详细的元数据 (Total Rows, Time, Truncated)
    """
    t0 = time.perf_counter()
    try:
        db = MySQLWriter(DatabaseConfig.from_env())
        
        # 处理 desc 快捷指令
        if sql_raw.lower().startswith("desc "):
            table_name = sql_raw.split()[1].strip(";")
            sql_raw = f"SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_DEFAULT FROM information_schema.COLUMNS WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = DATABASE();"

        # 分离多条 SQL
        statements = [s.strip() for s in sql_raw.split(";") if s.strip()]
        if not statements:
            return {"status": "error", "message": "Empty SQL statement"}

        final_results = []
        
        with db.engine.begin() as conn:
            for sql in statements:
                sql_lower = sql.lower()
                is_read = sql_lower.startswith(("select", "show", "describe", "explain"))
                
                # 自动注入 LIMIT
                if sql_lower.startswith("select") and "limit" not in sql_lower and "count" not in sql_lower:
                    sql += f" LIMIT {limit}"
                
                if is_read:
                    df = pd.read_sql(text(sql), conn)
                    # 转换结果
                    rows = df.to_dict(orient="records")
                    # 日期序列化
                    for row in rows:
                        for k, v in row.items():
                            if isinstance(v, (pd.Timestamp, pd.DatetimeIndex)):
                                row[k] = v.isoformat()
                            elif isinstance(v, (datetime.date, datetime.datetime)):
                                row[k] = v.isoformat()
                    
                    final_results.append({
                        "statement": sql[:100] + "..." if len(sql) > 100 else sql,
                        "type": "read",
                        "count": len(df),
                        "data": rows
                    })
                else:
                    res = conn.execute(text(sql))
                    final_results.append({
                        "statement": sql[:100] + "..." if len(sql) > 100 else sql,
                        "type": "write",
                        "rows_affected": res.rowcount
                    })

        duration = round(time.perf_counter() - t0, 3)
        return {
            "status": "ok",
            "execution_time_sec": duration,
            "results": final_results
        }
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"status": "error", "message": "Usage: python db_query.py \"SQL\" or \"desc table\""}))
        sys.exit(1)
        
    query = sys.argv[1]
    result = execute_sql(query)
    print(json.dumps(result, ensure_ascii=False, indent=2))