---
post_title: Quick Start
menu_order: 10
---

* Step 1. Install a Cassandra cluster using DC/OS CLI:

**Note:** Your cluster must have at least 3 private nodes.

```
$ dcos package install cassandra
```

* Step 2. Once the cluster is installed, retrieve connection information by running the `connection` command:

```
$ dcos cassandra connection
{
    "address": [
        "10.0.2.136:9042",
        "10.0.2.138:9042",
        "10.0.2.137:9042"
    ],
    "dns": [
         "node-0.cassandra.mesos:9042",
         "node-1.cassandra.mesos:9042",
         "node-2.cassandra.mesos:9042"
    ]

}
```

* Step 3. [SSH into a DC/OS node](https://docs.mesosphere.com/administration/access-node/sshcluster/):

```
$ dcos node ssh --master-proxy --leader
core@ip-10-0-6-153 ~ $
```

Now that you are inside your DC/OS cluster, you can connect to your Cassandra cluster directly.

* Step 4. Launch a docker container containing `cqlsh` to connect to your cassandra cluster. Use one of the nodes you retrieved from the `connection` command:

```
core@ip-10-0-6-153 ~ $ docker run -ti cassandra:3.0.7 cqlsh --cqlversion="3.4.0" 10.0.2.136
cqlsh>
```

* Step 5. You are now connected to your Cassandra cluster. Create a sample keyspace called `demo`:

```
cqlsh> CREATE KEYSPACE demo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
```

* Step 6. Create a sample table called `map` in our `demo` keyspace:

```
cqlsh> USE demo;CREATE TABLE map (key varchar, value varchar, PRIMARY KEY(key));
```

* Step 7. Insert some data in the table:

```
cqlsh> INSERT INTO demo.map(key, value) VALUES('Cassandra', 'Rocks!');
cqlsh> INSERT INTO demo.map(key, value) VALUES('StaticInfrastructure', 'BeGone!');
cqlsh> INSERT INTO demo.map(key, value) VALUES('Buzz', 'DC/OS is the new black!');
```

* Step 8. Query the data back to make sure it persisted correctly:

```
cqlsh> SELECT * FROM demo.map;
```