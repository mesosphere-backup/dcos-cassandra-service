package com.mesosphere.dcos.cassandra.scheduler.mds.resources;

public class RoleRequest {

    private String password;
    private boolean isSuperuser;
    private boolean isLogin;
    private boolean grantAllPermissions;
    private CassandraAuth cassandraAuth;

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isSuperuser() {
        return isSuperuser;
    }

    public void setSuperuser(boolean isSuperuser) {
        this.isSuperuser = isSuperuser;
    }

    public boolean isLogin() {
        return isLogin;
    }

    public void setLogin(boolean isLogin) {
        this.isLogin = isLogin;
    }

    public CassandraAuth getCassandraAuth() {
        return cassandraAuth;
    }

    public void setCassandraAuth(CassandraAuth cassandraAuth) {
        this.cassandraAuth = cassandraAuth;
    }

    public boolean isGrantAllPermissions() {
        return grantAllPermissions;
    }

    public void setGrantAllPermissions(boolean grantAllPermissions) {
        this.grantAllPermissions = grantAllPermissions;
    }

    @Override
    public String toString() {
        return "RoleRequest [isSuperuser=" + isSuperuser + ", isLogin=" + isLogin
                        + ", grantAllPermissions=" + grantAllPermissions + ", cassandraAuth=" + cassandraAuth + "]";
    }

}
