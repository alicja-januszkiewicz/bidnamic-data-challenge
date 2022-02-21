def ingest(conn, path: str, table_name: str):
    """Upsert from csv in path into table_name"""
    try:
        with conn.cursor() as cur:
            # Create a temporary staging table
            cur.execute(f"""CREATE TEMPORARY TABLE {table_name}_STAGING ( LIKE {table_name} )
                        ON COMMIT DROP""")

            # Populate the staging table directly from the csv file
            with open(path) as data:
                all_csv_columns = data.readline()
                cur.copy_expert(f"""COPY {table_name}_STAGING ( {all_csv_columns} )
                                FROM STDIN WITH CSV""", data)

            # Fetch primary key column names to figure out upsert on conflict and dedupe strategy
            # Credit: https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
            cur.execute(f"""SELECT a.attname
            FROM   pg_index i
            JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                AND a.attnum = ANY(i.indkey)
            WHERE  i.indrelid = '{table_name}'::regclass
            AND    i.indisprimary;""")
            private_key_columns = ','.join([x[0] for x in cur.fetchall()])

            # Drop duplicates in the staging table to avoid cardinality violation errors. See
            # https://wiki.postgresql.org/wiki/UPSERT#.22Cardinality_violation.22_errors_in_detail
            # for details.
            and_conditions = [f'AND a.{col} = b.{col}' for col in private_key_columns.split(',')]
            and_conditions_sql = '\n'.join(and_conditions)

            cur.execute(f"""DELETE FROM {table_name}_STAGING a USING (
            SELECT MIN(ctid) as ctid, {private_key_columns}
                FROM {table_name}_STAGING
                GROUP BY {private_key_columns} HAVING COUNT(*) > 1
            ) b
            WHERE a.ctid <> b.ctid {and_conditions_sql}""")

            # Upsert from the staging table and update on error
            non_primary_key_columns = list(set(all_csv_columns.split(',')) 
                                            - set(private_key_columns.split(',')))
            set_statements = [f'{col} = EXCLUDED.{col}' for col in non_primary_key_columns]
            set_statements_sql = ','.join(set_statements)
            cur.execute(f"""INSERT INTO {table_name} ( {all_csv_columns} )
                        SELECT {all_csv_columns}
                        FROM {table_name}_STAGING
                        ON CONFLICT ( {private_key_columns} )
                        DO UPDATE SET {set_statements_sql}""")

    except:
        conn.rollback()
        raise
    else:
        conn.commit()

# with conn.cursor() as cur:
#     cur.execute(f"""SELECT * FROM {table_name}""")
#     print(cur.fetchone())