
Donwload the [DVD Rental Database](https://www.postgresqltutorial.com/postgresql-getting-started/postgresql-sample-database/)

```bash
mkdir ~/.dbdata/postgres
docker run --name pgdb -e POSTGRES_PASSWORD=S3cur3DB! \
    -e PGDATA=/opt/db_data \
    -v ~/.dbdata/postgres:/opt/db_data \
    -v ./mount/:/mnt/ext \
    -v ./postgres.docker-entrypoint-initdb.d/:/docker-entrypoint-initdb.d \
    -v "$PWD/my-postgres.conf":/etc/postgresql/postgresql.conf \
    -p 5432:5432 \
    -d postgres -c 'config_file=/etc/postgresql/postgresql.conf'
```
Connect to the database:
```bash
docker exec -it local-postgres psql -U postgres
```
Cretae a database:
```psql
CREATE DATABASE dvdrental;
```
Use pg restore to load the database:
```bash
docker exec -it local-postgres pg_restore -U postgres -d dvdrental /docker-entrypoint-initdb.d
```
Check the results:
```bash
docker exec -it local-postgres psql -U postgres
```
List tables:
```psql
\c dvdrental
\dt
select * from actor;
```
Create a configuration for postgres:
```bash
docker run -i --rm postgres cat /usr/share/postgresql/postgresql.conf.sample > my-postgres.conf
```

These are the 15 tables in the database:
    actor – stores actor data including first name and last name.
    film – stores film data such as title, release year, length, rating, etc.
    film_actor – stores the relationships between films and actors.
    category – stores film’s categories data.
    film_category- stores the relationships between films and categories.
    store – contains the store data including manager staff and address.
    inventory – stores inventory data.
    rental – stores rental data.
    payment – stores customer’s payments.
    staff – stores staff data.
    customer – stores customer data.
    address – stores address data for staff and customers
    city – stores city names.
    country – stores country names.

Connect to the database:
```bash
pip install "psycopg[binary,pool]"
pip install pgcli
pgcli -h localhost -U postgres -d dvdrental -W
```
# Installing Quadrant
A good guide is [here](https://qdrant.tech/documentation/getting-started/) and [here](https://github.com/qdrant/qdrant-python) and [here](https://gorannikolovski.com/blog/qdrant-simplified-setting-up-and-using-a-vector-database)

```bash
docker pull qdrant/qdrant
mkdir ~/.dbdata/qdrant
mkdir -p ~/.qdrant/configs
lvim ~/.qdrant/configs/config.yaml
docker run --name vectordb -p 6333:6333 \
    -v ~/.qdrant/config/config.yaml:/config/config.yaml \
    -v ~/.dbdata/qdrant/:/qdrant/storage \
    -d qdrant/qdrant
```

Access local dashboard: http://localhost:6333/dashboard