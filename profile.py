import timeit
import psycopg2
from main import conn_options, ingest, aggregate_roas

def ingest_all(conn):
    ingest(conn, 'campaigns.csv', 'campaigns')
    ingest(conn, 'adgroups.csv', 'adgroups')
    ingest(conn, 'search_terms.csv', 'search_terms')

def profile():
    with psycopg2.connect(**conn_options) as conn:
        number_of_calls = 100
        time_to_execute = timeit.timeit(lambda: ingest_all(conn), number=number_of_calls)
        print("Average time taken to ingest()", time_to_execute / number_of_calls, " seconds.")

        with conn.cursor() as cur:
            number_of_calls = 100
            time_to_execute = timeit.timeit(lambda: aggregate_roas(cur, by=['country', 'priority']), number=number_of_calls)
            print("Average time taken to aggregate_roas():", time_to_execute / number_of_calls, " seconds.")

if __name__ == '__main__':
    profile()