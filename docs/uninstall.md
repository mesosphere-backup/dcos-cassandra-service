---
post_title: Uninstall
menu_order: 40
---

Uninstalling a cluster is straightforward. Replace `cassandra` with the name of the Cassandra instance to be uninstalled.

```
$ dcos package uninstall --app-id=cassandra
```

Then, use the [framework cleaner script](/1.8/usage/managing-services/uninstall/) to remove your Cassandra instance from Zookeeper and destroy all data associated with it. The script requires several arguments, the default values to be used are:

- `framework_role` is `cassandra-role`.
- `framework_principal` is `cassandra-principal`.
- `zk_path` is `dcos-service-<service-name>`.

These values may vary if you customized them during installation.