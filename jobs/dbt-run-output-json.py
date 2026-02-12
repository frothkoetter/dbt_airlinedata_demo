import json
import os
import cml.data_v1 as cmldata

# Configuration
LOG_FILE_PATH = os.path.expandvars("$HOME//logs/dbt_airlinedata_demo_run.log")
DATABASE = 'dbt_airlinedata_demo'
CONNECTION_NAME = "cdw-aw-se-hive"

def load_logs_with_double_quote_saver():
    if not os.path.exists(LOG_FILE_PATH):
        print(f"File {LOG_FILE_PATH} not found.")
        return

    value_rows = []
    
    with open(LOG_FILE_PATH, 'r') as f:
        for line in f:
            try:
                log = json.loads(line)
                info = log.get('info', {})
                data = log.get('data', {})
                node_info = data.get('node_info', {})

                # FIXED: Alternating quote types to avoid SyntaxError
                def q(val):
                    if val is None: return "NULL"
                    clean_val = str(val).replace('"', '""').replace('\n', ' ')
                    return f'"{clean_val}"'

                # Map to exactly 17 columns to match Hive DDL
                row = (
                    q(info.get('invocation_id')),                   # 1
                    q(info.get('ts', '').replace('T', ' ').replace('Z', '')), # 2
                    q(info.get('code')),                            # 3
                    q(info.get('level')),                           # 4
                    q(info.get('name')),                            # 5
                    q(node_info.get('node_name') or data.get('node_name')), # 6
                    q(node_info.get('unique_id')),                  # 7
                    q(node_info.get('node_status')),                # 8
                    q(node_info.get('materialized')),               # 9
                    q(node_info.get('node_path')),                  # 10
                    q(node_info.get('node_checksum')),              # 11
                    str(float(data.get('execution_time', 0))),      # 12
                    q(data.get('target_name')),                     # 13
                    str(data.get('num_threads') or "NULL"),         # 14
                    q(data.get('stat_line')),                       # 15
                    q(data.get('msg') or info.get('msg')),          # 16
                    q(info.get('thread'))                           # 17
                )
                value_rows.append(f"({', '.join(row)})")
            except (json.JSONDecodeError, KeyError):
                continue

    if not value_rows:
        print("No valid logs found to load.")
        return

    # Bulk insert into the specific database
    bulk_sql = f"INSERT INTO {DATABASE}.dbt_structured_logs_full VALUES {', '.join(value_rows)}"

    conn = cmldata.get_connection(CONNECTION_NAME)
    try:
        cursor = conn.get_cursor()
        cursor.execute(bulk_sql)
        print(f"Successfully loaded {len(value_rows)} lines into {DATABASE}.")
    except Exception as e:
        print(f"SQL Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    load_logs_with_double_quote_saver()