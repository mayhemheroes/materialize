---
title: "Connect to Materialize via a SQL CLI"
description: "How to connect to Materialize using PostgreSQL-compatible tools"
aliases:
  - /connect/
  - /connect/cli/
  - /integrations/psql/
menu:
  main:
    parent: "integrations"
    weight: 5
    name: "SQL clients"
---

You can connect to Materialize using a PostgreSQL-compatible client, like `psql`. For an overview of compatible SQL clients and their current level of support, check out [Tools and Integrations](/integrations/#sql-clients).

### Connection details

Detail       | Info
-------------|------
**Host**     | `<host>`
**Port**     | `6875`
**User**     | `<user>`
**Database** | `materialize`

[//]: # "Add details about managing users once RBAC lands"

### Supported tools

Tool     | Description                   | Install
---------|-------------------------------|-----------------------------------
`psql`   | Vanilla PostgreSQL CLI        | `postgresql` or `postgresql-client`
 DBeaver | Open source universal SQL CLI | [Download DBeaver](https://dbeaver.io/download/)

If there's a tool that you'd like to use with Materialize but is not listed here or in [Tools and Integrations](/integrations/#sql-clients), let us know by submitting a [feature request](https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml)!

## Installation instructions for `psql`

Start by checking if you have `psql` installed:

```shell
psql --version
```

### macOS installation

Assuming you’ve installed [Homebrew](https://brew.sh/):

```shell
brew install libpq
```

Then symlink the `psql` binary to your `/usr/local/bin` directory:

```shell
brew link --force libpq
```

### Linux installation

For Debian-based distributions:

```shell
sudo apt-get update
sudo apt-get install postgresql-client
```

The `postgresql-client` package includes only the client binaries, not the PostgreSQL server.

For other Linux distributions, check out the [PostgreSQL documentation](https://www.postgresql.org/download/linux/).

### Windows installation

For Windows, it is best to use the [PostgreSQL installer](https://www.postgresql.org/download/windows/) certified by EDB.

## Examples

### Connecting with `psql`

{{< warning >}}
Not all features of `psql` are supported by Materialize yet, including some backslash meta-commands {{% gh 9721 %}}.
{{< /warning >}}

You can use any of the following connection string formats to connect to Materialize with `psql`:

```shell
psql "postgres://<user>@<host>:6875/materialize"
psql -U <user> -h <host> -p 6875 materialize
psql -U <user> -h <host> -p 6875 -d materialize
```
