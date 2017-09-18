package com.mesosphere.dcos.cassandra.scheduler.mds.resources;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.dcos.Capabilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.mesosphere.dcos.cassandra.common.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.scheduler.resources.ConnectionResource;

@Path("/v1/itest")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MdsItestManageResource {
	private static final Logger LOGGER = LoggerFactory.getLogger(MdsItestManageResource.class);
    private final ConfigurationManager configurationManager;
    private final Capabilities capabilities;
    private final CassandraState state;
    
    @Inject
    public MdsItestManageResource(Capabilities capabilities, CassandraState state,
                    ConfigurationManager configurationManager) {
        this.configurationManager = configurationManager;
        this.capabilities = capabilities;
        this.state = state;
    }


    @PUT
    @Path("/keyspace/{keyspace}/{replicationFactor}")
    public Response alterKeyspace(@PathParam("keyspace") final String keyspace,@PathParam("replicationFactor") final String replicationFactor,
    		CassandraAuth cassandraAuth) throws ConfigStoreException {
      

        try (Session session = MdsCassandraUtills.getSession(cassandraAuth, capabilities, state, configurationManager)) {
            // session = getSession(alterSysteAuthRequest.getCassandraAuth());
            
            String query = "ALTER KEYSPACE "+keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': "+replicationFactor+"};";
            LOGGER.info("Alter keyspace query:" + query);

            session.execute(query);
        } catch (NoHostAvailableException e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(e.getMessage()).build();
        }catch(QueryExecutionException  e){
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }catch(QueryValidationException e){
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        }catch(Exception e){
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }

        return Response.status(Response.Status.OK).entity("Successfull").build();
    }
}
