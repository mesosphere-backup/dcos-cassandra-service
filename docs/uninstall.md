---
post_title: Uninstall
menu_order: 30
feature_maturity: preview
enterprise: 'no'
---







Uninstalling a cluster is straightforward. Replace `app-id` with the name of the Cassandra instance to be uninstalled. If you have only one instance and are using the default package name (`cassandra`), you can omit `--app-id=<app-id>` in the command below.

```
dcos package uninstall --app-id=<app-id> cassandra
```

Then, use the [framework cleaner script](https://docs.mesosphere.com/1.9/deploying-services/uninstall/#framework-cleaner) to remove your Cassandra instance from Zookeeper and destroy all data associated with it. The script requires several arguments. The default values are:

- `framework_role` is `cassandra-role`.
- `framework_principal` is `cassandra-principal`.
- `zk_path` is `dcos-service-<service-name>`.

These values may vary if you customized them during installation.
