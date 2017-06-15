---
post_title: Cassandra Quickstart
menu_order: 0
feature_maturity: preview
enterprise: 'no'
---

This tutorial will get you up and running in minutes with Cassandra. You will install the DC/OS Cassandra, create a key space, and insert data.

**Prerequisites:**

-  [DC/OS and DC/OS CLI installed](https://docs.mesosphere.com/1.9/installing/) with a minimum of three agent nodes with eight GB of memory and ten GB of disk available on each agent.
-  Depending on your [security mode](https://docs.mesosphere.com/1.9/overview/security/security-modes/), Kafka requires a service authentication for access to DC/OS. For more information, see [Configuring DC/OS Access for Kafka](https://docs.mesosphere.com/service-docs/kafka/kafka-auth/).

   | Security mode | Service Account |
   |---------------|-----------------------|
   | Disabled      | Not available   |
   | Permissive    | Optional   |
   | Strict        | Required |

1.  Install the Cassandra package. This may take a few minutes.

    ```bash
    dcos package install cassandra
    ```
   
    **Tip:** Type `dcos cassandra` to view the Cassandra CLI options.

1.  View the connection information.
        
    ```bash
    dcos cassandra connection 
    ```
     
    The output should resemble:
     
    ```bash
    {
      "address": [
        "10.0.3.71:9042",
        "10.0.1.147:9042",
        "10.0.1.208:9042"
      ],
      "dns": [
        "node-0.cassandra.mesos:9042",
        "node-1.cassandra.mesos:9042",
        "node-2.cassandra.mesos:9042"
      ],
      "vip": "node.cassandra.l4lb.thisdcos.directory:9042"
    }
    ```

1.  Create a keyspace.

    1.  [SSH](https://docs.mesosphere.com/1.9/administering-clusters/sshcluster/) to the leading master node.

        ```bash
        dcos node ssh --master-proxy --leader
        ```

        You are now connected to your Cassandra cluster.

    1.  Pull the Cassandra Docker container down to your node and start an interactive psuedo-TTY session. The CQL utility (cqlsh) is included in this container. Use one of the nodes you retrieved from the `connection` command.

        ```bash
        docker run -ti cassandra:3.0.7 cqlsh --cqlversion="3.4.0" 10.0.3.71
        ```
        
        The output should resemble:
                
        ```bash
        Unable to find image 'cassandra:3.0.7' locally
        3.0.7: Pulling from library/cassandra
        5c90d4a2d1a8: Pull complete 
        fdf442b3a2aa: Pull complete 
        3f338921a7f4: Pull complete 
        46699e0990a4: Pull complete 
        0ea0efbc9d29: Pull complete 
        b6d79161856d: Pull complete 
        bd7fded3c991: Pull complete 
        a9c9234d3f54: Pull complete 
        73113260ebde: Pull complete 
        6037ba8a05d9: Pull complete 
        Digest: sha256:a74197281dbcc83974aa3abd82729a929f2183e1bb1ea0868b341ca8582fe63e
        Status: Downloaded newer image for cassandra:3.0.7
        Connected to cassandra at 10.0.3.71:9042.
        [cqlsh 5.0.1 | Cassandra 3.0.10 | CQL spec 3.4.0 | Native protocol v4]
        Use HELP for help.
        ```
        
1.  Create a sample keyspace called `demo`.

    ```bash
    CREATE KEYSPACE demo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
    ```

1.  Create a sample table called `map` in our `demo` keyspace.
        
    ```
    USE demo;CREATE TABLE map (key varchar, value varchar, PRIMARY KEY(key));
    ```

1.  Insert some data in the table.

    ```
    INSERT INTO demo.map(key, value) VALUES('Cassandra', 'Rocks!');
    INSERT INTO demo.map(key, value) VALUES('StaticInfrastructure', 'BeGone!');
    INSERT INTO demo.map(key, value) VALUES('Buzz', 'DC/OS is the new black!');
    ```

1.  Query the data.

    ```bash
    SELECT * FROM demo.map;
    ```
    
    The output should resemble:
    
    ```   
     key                  | value
    ----------------------+-------------------------
                Cassandra |                  Rocks!
     StaticInfrastructure |                 BeGone!
                     Buzz | DC/OS is the new black!
    
    (3 rows)
    ```

 [2]: https://docs.mesosphere.com/1.8/administration/access-node/sshcluster/