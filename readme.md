## Requirements:
- `psycopg2`

## How to run:
1. Set up a database to hold the data. See `prepare_db.py` for an example query to create the required tables on a newly set up database.
2. Fill out `conn_options` in main.py as required.
3. Run `main.py` as a python script to ingest and aggregate the data.

## Performance
Data ingestion takes around 2.5s as measured with `timeit()` on a Ryzen 1700X. `aggregate_roas()` takes on the order of x10^-8 seconds.
The `profile.py` module facilitates measuring these values.

## Ingest upsert strategy
As it stands the `main.py` script will simply ignore rows violating any unique or primary key constraints. There is an alternative approach implemented in `alternative_ingest.py` which instead will update the rows with new values. To use it, uncomment line 79 (`# from alternative_ingest import ingest`) in `main.py`.

## Notes on extending the functionality
### `status` columns
The status columns seem to only ever contain two values, `ENABLED` and `REMOVED`. One could implement these sql columns to be of boolean type to save memory. However, this would require editing the `ingest()` function to map these values from strings to booleans. The speed-memory tradeoff might not be worth it.
### `aggregate_roas()`
The `aggregate_roas()` function can be easily extended to allow aggregating by more fields of the alias column; One only needs to append the field name to `valid_aggregate_columns` and the name with its position to `alias_field_to_position_mapper`. For example adding in `'structure_value'` and `{'structure_value': 4}` respectively would allow the following usage: `aggregate_roas(cursor, ['country', 'structure_value'])`.

## ER Diagram
<img src="ER Diagram.png">
