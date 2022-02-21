import psycopg2
from typing import List

conn_options = {
    'dbname': 'bidnamic',
    'user': 'dawid',
    'password': 'som3longp@sswordIwontforget',
    'host': 'localhost',
}

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

            # Drop exact duplicates in the staging table to avoid cardinality violation error. See
            # https://wiki.postgresql.org/wiki/UPSERT#.22Cardinality_violation.22_errors_in_detail
            # for details.
            and_conditions = [f'AND a.{col} = b.{col}' for col in all_csv_columns.split(',')]
            and_conditions_sql = '\n'.join(and_conditions)

            cur.execute(f"""DELETE FROM {table_name}_STAGING a USING (
            SELECT MIN(ctid) as ctid, {all_csv_columns}
                FROM {table_name}_STAGING
                GROUP BY {all_csv_columns} HAVING COUNT(*) > 1
            ) b
            WHERE a.ctid <> b.ctid {and_conditions_sql}""")

            # Upsert from the staging table ignoring duplicates.
            # See alternative_ingest.py for an update on error strategy.
            cur.execute(f"""INSERT INTO {table_name} ( {all_csv_columns} )
                        SELECT {all_csv_columns}
                        FROM {table_name}_STAGING
                        ON CONFLICT
                        DO NOTHING""")
    except:
        conn.rollback()
        raise
    else:
        conn.commit()

def aggregate_roas(cur, by: List[str]):
    """Aggregate ROAS by columns supplied in b"""
    # Validate input param
    aggregate_columns = by
    valid_aggregate_columns = {'country', 'priority'}
    invalid_aggregate_columns = set(aggregate_columns) - valid_aggregate_columns
    if invalid_aggregate_columns:
        raise ValueError(f'invalid aggregate columns supplied: {invalid_aggregate_columns}')

    # names and positions of each adgroup alias field
    alias_field_to_position_mapper = {
        'country': 3,
        'priority': 5,
    }

    split_part_of_alias_on_agg_cols = [f"split_part(adgroups.alias, ' - ', {pos}) as {field}"
                                    for field, pos in alias_field_to_position_mapper.items()
                                    if field in aggregate_columns]
    
    split_part_of_alias_on_agg_cols_sql = ','.join(split_part_of_alias_on_agg_cols)

    cur.execute(f"""SELECT {split_part_of_alias_on_agg_cols_sql},
                SUM(search_terms.conversion_value / NULLIF(search_terms.cost, 0)) as ROAS
                FROM search_terms
                INNER JOIN adgroups ON (search_terms.ad_group_id = adgroups.ad_group_id)
                GROUP BY {','.join(aggregate_columns)}
                ORDER BY ROAS ASC
                """)

# from alternative_ingest import ingest

def main():
    """Load files into the database and run the aggregate queries."""
    with psycopg2.connect(**conn_options) as conn:
        ingest(conn, 'campaigns.csv', 'campaigns')
        ingest(conn, 'adgroups.csv', 'adgroups')
        ingest(conn, 'search_terms.csv', 'search_terms')

        with conn.cursor() as cur:
            aggregate_roas(cur, by=['country', 'priority'])
            print(cur.fetchall())

            aggregate_roas(cur, by=['country'])
            print(cur.fetchall())

            aggregate_roas(cur, by=['priority'])
            print(cur.fetchall())

if __name__ == '__main__':
    main()
