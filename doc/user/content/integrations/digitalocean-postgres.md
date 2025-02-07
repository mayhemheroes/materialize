---
title: "DigitalOcean Managed PostgreSQL"
description: "How to connect DigitalOcean Managed PostgreSQL as a source to Materialize."
---

Materialize can read data from DigitalOcean Managed PostgreSQL via the direct [Postgres Source](/sql/create-source/postgres/).

1. Materialize will need access to connect to the upstream database. This is usually controlled by IP address. If you are hosting your own installation of Materialize, in the DigitalOcean console, add your Materialize instance's IP address to the [Trusted Source list](https://docs.digitalocean.com/products/databases/postgresql/how-to/secure/#firewalls) for your Managed PostgreSQL Cluster.

1. Connect to your PostgreSQL cluster as the `doadmin` user and create a [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html) with the tables you want to replicate:

    ```sql
    CREATE PUBLICATION mz_source FOR TABLE table1, table2;
    ```

    **Note:** Because the `doadmin` user is not a superuser, you will not be able to create a publication for _all_ tables.

    The `mz_source` publication will contain the set of change events generated from the specified tables, and will later be used to ingest the replication stream.

For more information, see the [Managed PostgreSQL](https://docs.digitalocean.com/products/databases/postgresql/) documentation.

## Related pages

- [`CREATE SOURCE`: PostgreSQL](/sql/create-source/postgres/)
- [PostgreSQL CDC guide](../cdc-postgres/)
