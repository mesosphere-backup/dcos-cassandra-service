package com.mesosphere.dcos.cassandra.scheduler.mds.resources;

public class MatrixStatus {

    private String hostName;
    private String hostAddress;
    private String load;
    private Float ownsPercentage;

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getLoad() {
        return load;
    }

    public void setLoad(String load) {
        this.load = load;
    }

    public Float getOwnsPercentage() {
        return ownsPercentage;
    }

    public void setOwnsPercentage(Float ownsPercentage) {
        this.ownsPercentage = ownsPercentage;
    }

    public String getHostAddress() {
        return hostAddress;
    }

    public void setHostAddress(String hostAddress) {
        this.hostAddress = hostAddress;
    }

    @Override
    public String toString() {
        return "MatrixStatus [hostName=" + hostName + ", hostAddress=" + hostAddress + ", load=" + load
                        + ", ownsPercentage=" + ownsPercentage + "]";
    }
    
    
}
