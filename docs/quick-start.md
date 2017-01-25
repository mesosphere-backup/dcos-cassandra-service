---
post_title: Quick Start
menu_order: 0
feature_maturity: preview
enterprise: 'no'
---


1. Install a Cassandra cluster using DC/OS CLI:

    **Note:** Your cluster must have at least 3 private nodes.

        $ dcos package install cassandra

1. Once the cluster is installed, retrieve connection information by running the `connection` command:
        
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

1. [SSH into a DC/OS node][2]:

        $ dcos node ssh --master-proxy --leader
        core@ip-10-0-6-153 ~ $

    Now that you are inside your DC/OS cluster, you can connect to your Cassandra cluster directly.

1. Launch a docker container containing `cqlsh` to connect to your cassandra cluster:

        core@ip-10-0-6-153 ~ $ docker run -ti cassandra:3.0.7 cqlsh --cqlversion="3.4.0" node-0.cassandra.mesos
        cqlsh>

1. You are now connected to your Cassandra cluster. Create a sample keyspace called `demo`:

        cqlsh> CREATE KEYSPACE demo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

1. Create a sample table called `map` in our `demo` keyspace:
        
        cqlsh> USE demo;CREATE TABLE map (key varchar, value varchar, PRIMARY KEY(key));

1. Insert some data in the table:

        cqlsh> INSERT INTO demo.map(key, value) VALUES('Cassandra', 'Rocks!');
        cqlsh> INSERT INTO demo.map(key, value) VALUES('StaticInfrastructure', 'BeGone!');
        cqlsh> INSERT INTO demo.map(key, value) VALUES('Buzz', 'DC/OS is the new black!');

1. Query the data back to make sure it persisted correctly:

        cqlsh> SELECT * FROM demo.map;

 [2]: https://docs.mesosphere.com/1.9/administration/access-node/sshcluster/
