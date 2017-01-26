---
post_title: Connecting Clients
menu_order: 60
feature_maturity: preview
enterprise: 'no'
---

The only supported client for the DC/OS Cassandra Service is the Datastax Java CQL Driver. Note that this means that Thrift RPC-based clients are not supported for use with this service and any legacy applications that use this communication mechanism are run at the user's risk.

# Connection Info Using the DC/OS CLI

The following command can be executed from the CLI to retrieve a set of nodes to connect to.

```
dcos cassandra --name=<service-name> connection
```

# Connection Info Response

The response is as below.

```
{
    "address": [
        "10.0.0.47:9042",
        "10.0.0.50:9042",
        "10.0.0.49:9042"
    ],
    "dns": [
         "node-0.cassandra.mesos:9042",
         "node-1.cassandra.mesos:9042",
         "node-2.cassandra.mesos:9042"
    ]

}
```

This address JSON array contains a list of valid nodes addresses for nodes in the cluster.
The dns JSON array contains valid MesosDNS names for the same nodes. For availability
reasons, it is best to specify multiple nodes in the configuration of the CQL Driver used
by the application.

If IP addresses are used, and a Cassandra node is moved to a different IP
address, the address in the list passed to the cluster configuration of the application
should be changed. Once the application is connected to the cluster, moving a
node to a new IP address will not result in a loss of connectivity. The CQL Driver is
capable of dealing with topology changes. However, the application's
configuration should be pointed to the new address the next time the application is
restarted.

If DNS names are used, the DNS name will always resolve to correct IP address of the node.
This is true, even if the node is moved to a new IP address. However, it is important to
understand the DNS caching behavior of your application. For a Java application using
the CQL driver, if a SecurityManager is installed the default behavior is to cache a
successful DNS lookup forever. Therefore, if a node moves, your application will always
maintain the original address. If no security manager is installed, the default cache
behavior falls back to an implementation defined timeout. If a node moves in this case,
the behavior is generally undefined. If you choose to use DNS to resolve entry points to
the cluster, the safest method is to set networkaddress.cache.ttl to a reasonable value.
As with the IP address method, the CQL driver still detect topology changes and reamin
connected even if a node moves.

## Configuring the CQL Driver
### Adding the Driver to Your Application

```
<dependency>
  <groupId>com.datastax.cassandra</groupId>
  <artifactId>cassandra-driver-core</artifactId>
  <version>3.0.0</version>
</dependency>
```

The snippet above is the correct dependency for CQL driver to use with the DC/OS Apache Cassandra service. After adding this dependency to your project, you should have access to the correct binary dependencies to interface with the Cassandra cluster.

## Connecting the CQL Driver.
The code snippet below demonstrates how to connect the CQL driver to the cluster and perform a simple query. Run this script from anywhere where the private IP addresses of your nodes are reachable. Find the IP addresses of your nodes by running the `dcos cassandra connection` command from the DC/OS CLI.

```
Cluster cluster = null;
try {

   List<InetSocketAddress> addresses = Arrays.asList(
       new InetSocketAddress("10.0.0.47", 9042),
       new InetSocketAddress("10.0.0.48", 9042),
       new InetSocketAddress("10.0.0.45", 9042));

    cluster = Cluster.builder()
            .addContactPointsWithPorts(addresses)
            .build();
    Session session = cluster.connect();

    ResultSet rs = session.execute("select release_version from system.local");
    Row row = rs.one();
    System.out.println(row.getString("release_version"));
} finally {
    if (cluster != null) cluster.close();
}
```
