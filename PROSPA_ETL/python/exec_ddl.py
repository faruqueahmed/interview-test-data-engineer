import os
import pymysql
import json

def parse_sql(filename):
    data = open(filename, 'r').readlines()
    stmts = []
    DELIMITER = ';'
    stmt = ""

    for lineno, line in enumerate(data):
        if not line.strip():
            continue

        if line.startswith('--'):
            continue

        if 'DELIMITER' in line:
            DELIMITER = line.split()[1]
            continue

        if (DELIMITER not in line):
            stmt += line.replace(DELIMITER, ';')
            continue

        if stmt:
            stmt += line
            stmts.append(stmt.strip())
            stmt = ''
        else:
            stmts.append(line.strip())
    return stmts

def create_tables():
    with open(os.getcwd()+'/config/config.json') as config_file:
        params = json.load(config_file)
        ddl_file=os.getcwd()+params['ddl_file']      
        
        # Open database connection
        conn = pymysql.connect(params['hostname'],params['db_user'],params['dbpassword'],"sys" )
        stmts = parse_sql(ddl_file)
        try:
            # Execute the SQL command
            with conn.cursor() as cursor:
                for stmt in stmts:
                    cursor.execute(stmt)
                    conn.commit()
        except:
            print ("Error: Database Error from DDL Scripts.")
        # disconnect from server
        conn.close()
        print("Tables created successfully")


create_tables()
