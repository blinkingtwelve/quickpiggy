# QuickPiggy 

## Launch an impromptu PostgreSQL server, hassle free.

Prerequisites:

- postgresql-server (tested with v9.0 - v13.2), providing `postgres`, `initdb` and `createdb` on your `$PATH`
- postgresql libraries and clients (tested with v9.0 - v13.2), providing `psql` on your `$PATH`

This is mainly a library module, but you can take it for a for a demo spin by running `quickpiggy.py` 
as a program (`python quickpiggy.py`).

When used as a library, an ephemeral PostgresSQL instance can be obtained quite easily:

```python
pig = quickpiggy.Piggy(volatile=True, create_db='somedb')
conn = psycopg2.connect(pig.dsnstring())
```

Many use cases can be accommodated for by supplying appropriate parameters to the constructor of Piggy.

This version works with Python 2.7 and 3.1+.
