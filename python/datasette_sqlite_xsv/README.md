# The `datasette-sqlite-xsv` Datasette Plugin

`datasette-sqlite-xsv` is a [Datasette plugin](https://docs.datasette.io/en/stable/plugins.html) that loads the [`sqlite-xsv`](https://github.com/asg017/sqlite-xsv) extension in Datasette instances, allowing you to generate and work with [xsvs](https://github.com/xsv/spec) in SQL.

```
datasette install datasette-sqlite-xsv
```

See [`docs.md`](../../docs.md) for a full API reference for the xsv SQL functions.

Alternatively, when publishing Datasette instances, you can use the `--install` option to install the plugin.

```
datasette publish cloudrun data.db --service=my-service --install=datasette-sqlite-xsv

```
