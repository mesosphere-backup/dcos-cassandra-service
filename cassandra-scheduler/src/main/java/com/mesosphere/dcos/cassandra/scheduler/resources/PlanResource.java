package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.scheduler.plan.CassandraPlanManager;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/v1/plan")
@Produces(MediaType.APPLICATION_JSON)
public class PlanResource {

    private final CassandraPlanManager manager;

    private enum COMMAND{
        INTERRUPT,
        PROCEED
    }

    private static COMMAND getCommand(String id){
        switch(id.trim().toLowerCase()){
            case "interrupt": return COMMAND.INTERRUPT;
            case "proceed" : return COMMAND.PROCEED;
            default : throw new BadRequestException("Illegal command " + id);
        }
    }

    @Inject
    public PlanResource( final CassandraPlanManager manager){
        this.manager = manager;
    }

    @GET
    public PlanInfo getPlan(){
        return PlanInfo.forPlan(manager.getPlan());
    }

    @GET
    @Path("/phases")
    public List<PhaseInfo> getPhases(){

        return  getPlan().getPhases();
    }

    @GET
    @Path("/summary")
    public PlanSummary getSummary(){
        return PlanSummary.forPlan(manager);
    }

    @PUT
    @Path("/command")
    public void executeCommand(@QueryParam("id") final String id){
        switch(getCommand(id)){
            case INTERRUPT: manager.interrupt(); break;
            case PROCEED: manager.proceed(); break;
        }

    }
}
