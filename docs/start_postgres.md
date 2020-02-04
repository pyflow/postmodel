
```
docker run -d --name my_postgres -v my_pgdata:/var/lib/postgresql/data -p 54320:5432 postgres -c log_statement=all
```

```
docker start my_postgres
```