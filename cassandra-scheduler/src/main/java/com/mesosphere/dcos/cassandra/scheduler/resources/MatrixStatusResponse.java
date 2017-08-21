package com.mesosphere.dcos.cassandra.scheduler.resources;

import java.util.ArrayList;
import java.util.List;

public class MatrixStatusResponse {

    private List<MatrixStatus> matrixStatusList ;

    public MatrixStatusResponse() {
        matrixStatusList = new ArrayList<>();
    }

    public List<MatrixStatus> getMatrixStatusList() {
        return matrixStatusList;
    }

    public void setMatrixStatusList(List<MatrixStatus> matrixStatusList) {
        this.matrixStatusList = matrixStatusList;
    }

    @Override
    public String toString() {
        return "MatrixStatusResponse [matrixStatusList=" + matrixStatusList + "]";
    }
    
}
