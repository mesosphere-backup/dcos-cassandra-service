package com.mesosphere.dcos.cassandra.common.config;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.ByteString;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.mesosphere.dcos.cassandra.common.util.JsonUtils.MAPPER;
import static com.mesosphere.dcos.cassandra.common.util.JsonUtils.YAML_MAPPER;

/**
 * Contains DSE specific configuration
 */
public class DseConfig {
  public static final String AUTHENTICATOR_CLASS = "com.datastax.bdp.cassandra.auth.DseAuthenticator";
  public static final String AUTHORIZOR_CLASS = "com.datastax.bdp.cassandra.auth.DseAuthorizer";
  public static final String ROLE_MANAGER_CLASS = "com.datastax.bdp.cassandra.auth.DseRoleManager";


  public static class AuthenticationOptions {

    @JsonCreator
    public static AuthenticationOptions create(
      @JsonProperty("enabled")
      final boolean enabled,
      @JsonProperty("default_scheme")
      final String defaultScheme,
      @JsonProperty("other_schemes")
      final List<String> otherSchemes,
      @JsonProperty("scheme_permissions")
      final boolean schemePermissions,
      @JsonProperty("allow_digest_with_kerberos")
      final boolean allowDigestWithKerberos,
      @JsonProperty("plain_text_without_ssl")
      final String plainTextWithoutSsl,
      @JsonProperty("transitional_mode")
      final String transitionalMode) {

      return new AuthenticationOptions(
        enabled,
        defaultScheme,
        otherSchemes,
        schemePermissions,
        allowDigestWithKerberos,
        plainTextWithoutSsl,
        transitionalMode
      );
    }

    @JsonIgnore
    public static AuthenticationOptions DEFAULT =
      AuthenticationOptions.create(
        false, "kerberos", Arrays.asList("internal"), true, true, "warn", "disabled");

    @JsonIgnore
    private final boolean enabled;
    @JsonIgnore
    private final String defaultScheme;
    @JsonIgnore
    private final List<String> otherSchemes;
    @JsonIgnore
    private final boolean schemePermissions;
    @JsonIgnore
    private final boolean allowDigestWithKerberos;
    @JsonIgnore
    private final String plainTextWithoutSsl;
    @JsonIgnore
    private final String transitionalMode;

    private AuthenticationOptions(boolean enabled,
                                  String defaultScheme,
                                  List<String> otherSchemes,
                                  boolean schemePermissions,
                                  boolean allowDigestWithKerberos,
                                  String plainTextWithoutSsl,
                                  String transitionalMode) {
      this.enabled = enabled;
      this.defaultScheme = defaultScheme;
      this.otherSchemes = otherSchemes;
      this.schemePermissions = schemePermissions;
      this.allowDigestWithKerberos = allowDigestWithKerberos;
      this.plainTextWithoutSsl = plainTextWithoutSsl;
      this.transitionalMode = transitionalMode;
    }

    @JsonProperty("enabled")
    public boolean isEnabled() {
      return enabled;
    }

    @JsonProperty("default_scheme")
    public String getDefaultScheme() {
      return defaultScheme;
    }

    @JsonProperty("other_schemes")
    public List<String> getOtherSchemes() {
      return otherSchemes;
    }

    @JsonProperty("scheme_permissions")
    public boolean isSchemePermissions() {
      return schemePermissions;
    }

    @JsonProperty("allow_digest_with_kerberos")
    public boolean isAllowDigestWithKerberos() {
      return allowDigestWithKerberos;
    }

    @JsonProperty("plain_text_without_ssl")
    public String getPlainTextWithoutSsl() {
      return plainTextWithoutSsl;
    }

    @JsonProperty("transitional_mode")
    public String getTransitionalMode() {
      return transitionalMode;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AuthenticationOptions that = (AuthenticationOptions) o;
      return enabled == that.enabled &&
        schemePermissions == that.schemePermissions &&
        allowDigestWithKerberos == that.allowDigestWithKerberos &&
        Objects.equals(defaultScheme, that.defaultScheme) &&
        Objects.equals(otherSchemes, that.otherSchemes) &&
        Objects.equals(plainTextWithoutSsl, that.plainTextWithoutSsl) &&
        Objects.equals(transitionalMode, that.transitionalMode);
    }

    @Override
    public int hashCode() {
      return Objects.hash(enabled, defaultScheme, otherSchemes, schemePermissions, allowDigestWithKerberos, plainTextWithoutSsl, transitionalMode);
    }

    @Override
    public String toString() {
      return JsonUtils.toJsonString(this);
    }
  }

  public static class RoleManagementOptions {

    @JsonIgnore
    public static RoleManagementOptions DEFAULT =
      RoleManagementOptions.create("internal",false);

    @JsonCreator
    public static RoleManagementOptions create(
      @JsonProperty("mode")
      final String mode,
      @JsonProperty("enabled")
      final boolean enabled) {
      return new RoleManagementOptions(mode,enabled);
    }

    @JsonIgnore
    private final String mode;

    @JsonIgnore
    private final boolean enabled;

    private RoleManagementOptions(final String mode, boolean enabled) {
      this.mode = mode;
      this.enabled = enabled;
    }

    @JsonProperty("mode")
    public String getMode() {
      return mode;
    }

    @JsonProperty("enabled")
    public boolean isEnabled() {
      return enabled;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RoleManagementOptions that = (RoleManagementOptions) o;
      return Objects.equals(mode, that.mode) &&
        Objects.equals(enabled, that.enabled);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mode, enabled);
    }

    @Override
    public String toString() {
      return JsonUtils.toJsonString(this);
    }
  }

  private static final class RoleManagementFileOptions {

    @JsonCreator
    public static RoleManagementFileOptions create(
      @JsonProperty("mode")
      final String mode) {
      return new RoleManagementFileOptions(mode);
    }

    @JsonIgnore
    private final String mode;

    private RoleManagementFileOptions(final String mode) {
      this.mode = mode;
    }

    @JsonProperty("mode")
    public String getMode() {
      return mode;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RoleManagementFileOptions that = (RoleManagementFileOptions) o;
      return Objects.equals(mode, that.mode);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mode);
    }

    @Override
    public String toString() {
      return JsonUtils.toJsonString(this);
    }

  }

  public static final class AuthorizationOptions {

    @JsonIgnore
    public static AuthorizationOptions DEFAULT =
      AuthorizationOptions.create(false, "disabled");

    @JsonCreator
    public static AuthorizationOptions create(
      @JsonProperty("enabled") boolean enabled,
      @JsonProperty("transitional_mode") String transitionalMode) {
      return new AuthorizationOptions(enabled, transitionalMode);
    }

    @JsonIgnore
    private final boolean enabled;
    @JsonIgnore
    private final String transitionalMode;

    public AuthorizationOptions(
      boolean enabled,
      String transitionalMode) {
      this.enabled = enabled;
      this.transitionalMode = transitionalMode;
    }

    @JsonProperty("enabled")
    public boolean isEnabled() {
      return enabled;
    }

    @JsonProperty("transitional_mode")
    public String getTransitionalMode() {
      return transitionalMode;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AuthorizationOptions that = (AuthorizationOptions) o;
      return enabled == that.enabled &&
        Objects.equals(transitionalMode, that.transitionalMode);
    }

    @Override
    public int hashCode() {
      return Objects.hash(enabled, transitionalMode);
    }

    @Override
    public String toString() {
      return JsonUtils.toJsonString(this);
    }
  }

  public static class KerberosOptions {

    @JsonIgnore
    public static KerberosOptions DEFAULT =
      KerberosOptions.create(
        "dse.keytab",
        "dse/_HOST@REALM",
        "HTTP/_HOST@REALM",
        "auth");

    @JsonCreator
    public static KerberosOptions create(
      @JsonProperty("keytab")
      final String keytab,
      @JsonProperty("service_principal")
      final String servicePrincipal,
      @JsonProperty("http_principal")
      final String httpPrincipal,
      @JsonProperty("qop")
      final String qop) {
      return new KerberosOptions(
        keytab,
        servicePrincipal,
        httpPrincipal,
        qop);
    }

    @JsonIgnore
    private final String keytab;
    @JsonIgnore
    private final String servicePrincipal;
    @JsonIgnore
    private final String httpPrincipal;
    @JsonIgnore
    private final String qop;

    private KerberosOptions(
      final String keytab,
      final String servicePrincipal,
      final String httpPrincipal,
      final String qop) {
      this.keytab = keytab;
      this.servicePrincipal = servicePrincipal;
      this.httpPrincipal = httpPrincipal;
      this.qop = qop;
    }

    @JsonProperty("keytab")
    public String getKeytab() {
      return keytab;
    }

    @JsonProperty("service_principal")
    public String getServicePrincipal() {
      return servicePrincipal;
    }

    @JsonProperty("http_principal")
    public String getHttpPrincipal() {
      return httpPrincipal;
    }

    @JsonProperty("qop")
    public String getQop() {
      return qop;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      KerberosOptions that = (KerberosOptions) o;
      return Objects.equals(keytab, that.keytab) &&
        Objects.equals(servicePrincipal, that.servicePrincipal) &&
        Objects.equals(httpPrincipal, that.httpPrincipal) &&
        Objects.equals(qop, that.qop);
    }

    @Override
    public int hashCode() {
      return Objects.hash(keytab, servicePrincipal, httpPrincipal, qop);
    }

    @Override
    public String toString() {
      return JsonUtils.toJsonString(this);
    }
  }

  public static class NodeHealthOptions {

    @JsonIgnore
    public static final NodeHealthOptions DEFAULT =
      NodeHealthOptions.create(60000, 84000, 30);

    @JsonCreator
    public static NodeHealthOptions create(
      @JsonProperty("refresh_rate_ms")
      final int refreshRateMs,
      @JsonProperty("uptime_ramp_up_period_seconds")
      final int updteRampUpSeconds,
      @JsonProperty("dropped_mutation_window_minutes")
      final int getDroppedMutationWindowMin) {
      return new NodeHealthOptions(refreshRateMs,
        updteRampUpSeconds,
        getDroppedMutationWindowMin);
    }

    @JsonIgnore
    private final int refreshRateMs;
    @JsonIgnore
    private final int updteRampUpSeconds;
    @JsonIgnore
    private final int droppedMutationWindowMin;

    private NodeHealthOptions(
      int refreshRateMs,
      int updteRampUpSeconds,
      int droppedMutationWindowMin) {
      this.refreshRateMs = refreshRateMs;
      this.updteRampUpSeconds = updteRampUpSeconds;
      this.droppedMutationWindowMin = droppedMutationWindowMin;
    }

    @JsonProperty("refresh_rate_ms")
    public int getRefreshRateMs() {
      return refreshRateMs;
    }

    @JsonProperty("uptime_ramp_up_period_seconds")
    public int getUpdteRampUpSeconds() {
      return updteRampUpSeconds;
    }

    @JsonProperty("dropped_mutation_window_minutes")
    public int getDroppedMutationWindowMin() {
      return droppedMutationWindowMin;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      NodeHealthOptions that = (NodeHealthOptions) o;
      return refreshRateMs == that.refreshRateMs &&
        updteRampUpSeconds == that.updteRampUpSeconds &&
        droppedMutationWindowMin == that.droppedMutationWindowMin;
    }

    @Override
    public int hashCode() {
      return Objects.hash(refreshRateMs, updteRampUpSeconds, droppedMutationWindowMin);
    }

    @Override
    public String toString() {
      return JsonUtils.toJsonString(this);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class LeaseMetricOptions {

    @JsonIgnore
    public static final LeaseMetricOptions DEFAULT =
      LeaseMetricOptions.create(false, 604800);

    @JsonCreator
    public static LeaseMetricOptions create(
      @JsonProperty("enabled")
      final boolean enabled,
      @JsonProperty("ttl_seconds")
      final int ttlSeconds) {
      return new LeaseMetricOptions(enabled, ttlSeconds);
    }

    @JsonIgnore
    private final boolean enabled;
    @JsonIgnore
    private final int ttlSeconds;

    private LeaseMetricOptions(boolean enabled, int ttlSeconds) {
      this.enabled = enabled;
      this.ttlSeconds = ttlSeconds;
    }

    @JsonProperty("enabled")
    public boolean isEnabled() {
      return enabled;
    }

    @JsonProperty("ttl_seconds")
    public int getTtlSeconds() {
      return ttlSeconds;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LeaseMetricOptions that = (LeaseMetricOptions) o;
      return enabled == that.enabled &&
        ttlSeconds == that.ttlSeconds;
    }

    @Override
    public int hashCode() {
      return Objects.hash(enabled, ttlSeconds);
    }

    @Override
    public String toString() {
      return JsonUtils.toJsonString(this);
    }
  }

  public static final class AuditWriterOptions {

    @JsonIgnore
    public static final AuditWriterOptions DEFAULT =
      AuditWriterOptions.create("sync",
        50,
        500,
        10,
        10000,
        "QUORUM");

    @JsonCreator
    public static AuditWriterOptions create(
      @JsonProperty("mode")
      final String mode,
      @JsonProperty("batch_size")
      final int batchSize,
      @JsonProperty("flush_time")
      final int flushTime,
      @JsonProperty("num_writers")
      final int numWriters,
      @JsonProperty("queue_size")
      final int queueSize,
      @JsonProperty("write_consistency")
      final String writeConsistency) {

      return new AuditWriterOptions(
        mode,
        batchSize,
        flushTime,
        numWriters,
        queueSize,
        writeConsistency);
    }

    @JsonIgnore
    private final String mode;
    @JsonIgnore
    private final int batchSize;
    @JsonIgnore
    private final int flushTime;
    @JsonIgnore
    private final int numWriters;
    @JsonIgnore
    private final int queueSize;
    @JsonIgnore
    private final String writeConsistency;

    private AuditWriterOptions(
      String mode,
      int batchSize,
      int flushTime,
      int numWriters,
      int queueSize,
      String writeConsistency) {
      this.mode = mode;
      this.batchSize = batchSize;
      this.flushTime = flushTime;
      this.numWriters = numWriters;
      this.queueSize = queueSize;
      this.writeConsistency = writeConsistency;
    }

    @JsonProperty("mode")
    public String getMode() {
      return mode;
    }

    @JsonProperty("batch_size")
    public int getBatchSize() {
      return batchSize;
    }

    @JsonProperty("flush_time")
    public int getFlushTime() {
      return flushTime;
    }

    @JsonProperty("num_writers")
    public int getNumWriters() {
      return numWriters;
    }

    @JsonProperty("queue_size")
    public int getQueueSize() {
      return queueSize;
    }

    @JsonProperty("write_consistency")
    public String getWriteConsistency() {
      return writeConsistency;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AuditWriterOptions that = (AuditWriterOptions) o;
      return batchSize == that.batchSize &&
        flushTime == that.flushTime &&
        numWriters == that.numWriters &&
        queueSize == that.queueSize &&
        Objects.equals(mode, that.mode) &&
        Objects.equals(writeConsistency, that.writeConsistency);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mode,
        batchSize,
        flushTime,
        numWriters,
        queueSize,
        writeConsistency);
    }

    @Override
    public String toString() {
      return JsonUtils.toJsonString(this);
    }
  }

  public static class AuditLoggingOptions {

    public static AuditLoggingOptions DEFAULT =
      AuditLoggingOptions.create(
        false,
        "SLF4JAuditWriter",
        0,
        AuditWriterOptions.DEFAULT
      );

    @JsonCreator
    public static AuditLoggingOptions create(
      @JsonProperty("enabled")
      final boolean enabled,
      @JsonProperty("logger")
      final String logger,
      @JsonProperty("retention_time")
      final int retentionTime,
      @JsonProperty("cassandra_audit_writer_options")
      final AuditWriterOptions writerOptions) {
      return new AuditLoggingOptions(
        enabled,
        logger,
        retentionTime,
        writerOptions
      );
    }
    @JsonIgnore
    private final boolean enabled;
    @JsonIgnore
    private final String logger;
    @JsonIgnore
    private final int retentionTime;
    @JsonIgnore
    private final AuditWriterOptions writerOptions;

    public AuditLoggingOptions(
      boolean enabled,
      String logger,
      int retentionTime,
      AuditWriterOptions writerOptions) {
      this.enabled = enabled;
      this.logger = logger;
      this.retentionTime = retentionTime;
      this.writerOptions = writerOptions;
    }

    @JsonProperty("enabled")
    public boolean isEnabled() {
      return enabled;
    }

    @JsonProperty("logger")
    public String getLogger() {
      return logger;
    }

    @JsonProperty("retention_time")
    public int getRetentionTime() {
      return retentionTime;
    }

    @JsonProperty("cassandra_audit_writer_options")
    public AuditWriterOptions getWriterOptions() {
      return writerOptions;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AuditLoggingOptions that = (AuditLoggingOptions) o;
      return enabled == that.enabled &&
        retentionTime == that.retentionTime &&
        Objects.equals(logger, that.logger) &&
        Objects.equals(writerOptions, that.writerOptions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(enabled, logger, retentionTime, writerOptions);
    }

    @Override
    public String toString() {
      return JsonUtils.toJsonString(this);
    }
  }

  public static class MessagingOptions{
    @JsonIgnore
    private final int port;
    @JsonIgnore
    public static MessagingOptions DEFAULT = MessagingOptions.create(8609);

    @JsonCreator
    public static final MessagingOptions create(
      @JsonProperty("port") int port) {
      return new MessagingOptions(port);
    }
    private MessagingOptions(int port) {
      this.port = port;
    }

    @JsonProperty("port")
    public int getPort(){
      return port;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      MessagingOptions that = (MessagingOptions) o;
      return port == that.port;
    }

    @Override
    public int hashCode() {
      return Objects.hash(port);
    }

    @Override
    public String toString() {
      return JsonUtils.toJsonString(this);
    }

  }

  private static class DseConfigFile {
    @JsonIgnore
    private final AuthenticationOptions authenticationOptions;
    @JsonIgnore
    private final RoleManagementFileOptions roleManagementOptions;
    @JsonIgnore
    private final AuthorizationOptions authorizationOptions;
    @JsonIgnore
    private final KerberosOptions kerberosOptions;
    @JsonIgnore
    private final NodeHealthOptions nodeHealthOptions;
    @JsonIgnore
    private final LeaseMetricOptions leaseMetricOptions;
    @JsonIgnore
    private final AuditLoggingOptions loggingOptions;
    @JsonIgnore
    private final MessagingOptions messagingOptions;

    @JsonCreator
    public static DseConfigFile create(
      @JsonProperty("authentication_options")
      final AuthenticationOptions authenticationOptions,
      @JsonProperty("role_management_options")
      final RoleManagementFileOptions roleManagementOptions,
      @JsonProperty("authorization_options")
      final AuthorizationOptions authorizationOptions,
      @JsonProperty("kerberos_options")
      final KerberosOptions kerberosOptions,
      @JsonProperty("node_health_options")
      final NodeHealthOptions nodeHealthOptions,
      @JsonProperty("lease_metrics_options")
      final LeaseMetricOptions leaseMetricOptions,
      @JsonProperty("audit_logging_options")
      final AuditLoggingOptions loggingOptions,
      @JsonProperty("internode_messaging_options")
      final MessagingOptions messagingOptions
    ) {
      return new DseConfigFile(
        authenticationOptions,
        roleManagementOptions,
        authorizationOptions,
        kerberosOptions,
        nodeHealthOptions,
        leaseMetricOptions,
        loggingOptions,
        messagingOptions);
    }

    @JsonIgnore
    public static DseConfigFile create (final DseConfig config) {
      return create(config.getAuthenticationOptions(),
        RoleManagementFileOptions.create(
          config.getRoleManagementOptions().getMode()),
        config.getAuthorizationOptions(),
        config.getKerberosOptions(),
        config.getNodeHealthOptions(),
        config.getLeaseMetricOptions(),
        config.getLoggingOptions(),
        config.getMessagingOptions()
        );
    }

    private DseConfigFile(
      final AuthenticationOptions authenticationOptions,
      final RoleManagementFileOptions roleManagementOptions,
      final AuthorizationOptions authorizationOptions,
      final KerberosOptions kerberosOptions,
      final NodeHealthOptions nodeHealthOptions,
      final LeaseMetricOptions leaseMetricOptions,
      final AuditLoggingOptions loggingOptions,
      final MessagingOptions messagingOptions) {
      this.authenticationOptions = authenticationOptions;
      this.roleManagementOptions = roleManagementOptions;
      this.authorizationOptions = authorizationOptions;
      this.kerberosOptions = kerberosOptions;
      this.nodeHealthOptions = nodeHealthOptions;
      this.leaseMetricOptions = leaseMetricOptions;
      this.loggingOptions = loggingOptions;
      this.messagingOptions = messagingOptions;
    }

    @JsonProperty("role_management_options")
    public RoleManagementFileOptions getRoleManagementOptions() {
      return roleManagementOptions;
    }

    @JsonProperty("authentication_options")
    public AuthenticationOptions getAuthenticationOptions() {
      return authenticationOptions;
    }

    @JsonProperty("authorization_options")
    public AuthorizationOptions getAuthorizationOptions() {
      return authorizationOptions;
    }

    @JsonProperty("kerberos_options")
    public KerberosOptions getKerberosOptions() {
      return kerberosOptions;
    }

    @JsonProperty("node_health_options")
    final NodeHealthOptions getNodeHealthOptions() {
      return nodeHealthOptions;
    }

    @JsonProperty("lease_metrics_options")
    final LeaseMetricOptions getLeaseMetricOptions() {
      return leaseMetricOptions;
    }

    @JsonProperty("audit_logging_options")
    final AuditLoggingOptions loggingOptions() {
      return loggingOptions;
    }

    @JsonProperty("internode_messaging_options")
    final MessagingOptions getMessagingOptions(){
      return messagingOptions;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DseConfigFile dseConfig = (DseConfigFile) o;
      return Objects.equals(authenticationOptions, dseConfig.authenticationOptions) &&
        Objects.equals(roleManagementOptions, dseConfig.roleManagementOptions) &&
        Objects.equals(authorizationOptions, dseConfig.authorizationOptions) &&
        Objects.equals(kerberosOptions, dseConfig.kerberosOptions) &&
        Objects.equals(nodeHealthOptions, dseConfig.nodeHealthOptions) &&
        Objects.equals(loggingOptions, dseConfig.loggingOptions) &&
        Objects.equals(messagingOptions,dseConfig.messagingOptions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
        authenticationOptions,
        roleManagementOptions,
        authorizationOptions,
        kerberosOptions,
        nodeHealthOptions,
        loggingOptions,
        messagingOptions);
    }
  }

  @JsonIgnore
  public static final DseConfig DEFAULT =
    DseConfig.create(AuthenticationOptions.DEFAULT,
      RoleManagementOptions.DEFAULT,
      AuthorizationOptions.DEFAULT,
      KerberosOptions.DEFAULT,
      NodeHealthOptions.DEFAULT,
      LeaseMetricOptions.DEFAULT,
      AuditLoggingOptions.DEFAULT,
      MessagingOptions.DEFAULT);


  @JsonCreator
  public static DseConfig create(
    @JsonProperty("authentication_options")
    final AuthenticationOptions authenticationOptions,
    @JsonProperty("role_management_options")
    final RoleManagementOptions roleManagementOptions,
    @JsonProperty("authorization_options")
    final AuthorizationOptions authorizationOptions,
    @JsonProperty("kerberos_options")
    final KerberosOptions kerberosOptions,
    @JsonProperty("node_health_options")
    final NodeHealthOptions nodeHealthOptions,
    @JsonProperty("lease_metrics_options")
    final LeaseMetricOptions leaseMetricOptions,
    @JsonProperty("audit_logging_options")
    final AuditLoggingOptions loggingOptions,
    @JsonProperty("internode_messaging_options")
    final MessagingOptions messagingOptions
  ) {
    return new DseConfig(
      authenticationOptions,
      roleManagementOptions,
      authorizationOptions,
      kerberosOptions,
      nodeHealthOptions,
      leaseMetricOptions,
      loggingOptions,
    messagingOptions);
  }

  @JsonIgnore
  public static DseConfig parse(byte[] bytes)
    throws IOException {
    return MAPPER.readValue(bytes, DseConfig.class);
  }

  @JsonIgnore
  public static DseConfig parse(ByteString bytes)
    throws IOException {
    return parse(bytes.toByteArray());
  }

  @JsonIgnore
  public void writeYaml(final Path path) throws IOException {
    YAML_MAPPER.writeValue(path.toFile(), DseConfigFile.create(this));
  }

  @JsonIgnore
  public byte[] toByteArray() {
    try {
      return MAPPER.writeValueAsBytes(this);
    } catch (JsonProcessingException ex) {
      throw new IllegalStateException("Failed to produce value JSON " +
        "from application configuration", ex);
    }

  }

  @JsonIgnore
  public ByteString toByteString() {
    return ByteString.copyFrom(toByteArray());
  }

  @JsonIgnore
  private final AuthenticationOptions authenticationOptions;
  @JsonIgnore
  private final RoleManagementOptions roleManagementOptions;
  @JsonIgnore
  private final AuthorizationOptions authorizationOptions;
  @JsonIgnore
  private final KerberosOptions kerberosOptions;
  @JsonIgnore
  private final NodeHealthOptions nodeHealthOptions;
  @JsonIgnore
  private final LeaseMetricOptions leaseMetricOptions;
  @JsonIgnore
  private final AuditLoggingOptions loggingOptions;
  @JsonIgnore
  private final MessagingOptions messagingOptions;

  public DseConfig(
    final AuthenticationOptions authenticationOptions,
    final RoleManagementOptions roleManagementOptions,
    final AuthorizationOptions authorizationOptions,
    final KerberosOptions kerberosOptions,
    final NodeHealthOptions nodeHealthOptions,
    final LeaseMetricOptions leaseMetricOptions,
    final AuditLoggingOptions loggingOptions,
    final MessagingOptions messagingOptions) {
    this.authenticationOptions = authenticationOptions;
    this.roleManagementOptions = roleManagementOptions;
    this.authorizationOptions = authorizationOptions;
    this.kerberosOptions = kerberosOptions;
    this.nodeHealthOptions = nodeHealthOptions;
    this.leaseMetricOptions = leaseMetricOptions;
    this.loggingOptions = loggingOptions;
    this.messagingOptions = messagingOptions;
  }

  @JsonProperty("role_management_options")
  public RoleManagementOptions getRoleManagementOptions() {
    return roleManagementOptions;
  }

  @JsonProperty("authentication_options")
  public AuthenticationOptions getAuthenticationOptions() {
    return authenticationOptions;
  }

  @JsonProperty("authorization_options")
  public AuthorizationOptions getAuthorizationOptions() {
    return authorizationOptions;
  }

  @JsonProperty("kerberos_options")
  public KerberosOptions getKerberosOptions() {
    return kerberosOptions;
  }

  @JsonProperty("node_health_options")
  final NodeHealthOptions getNodeHealthOptions() {
    return nodeHealthOptions;
  }

  @JsonProperty("lease_metrics_options")
  final LeaseMetricOptions getLeaseMetricOptions() {
    return leaseMetricOptions;
  }

  @JsonProperty("audit_logging_options")
  final AuditLoggingOptions getLoggingOptions() {
    return loggingOptions;
  }

  @JsonProperty("internode_messaging_options")
  final MessagingOptions getMessagingOptions(){
    return messagingOptions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DseConfig dseConfig = (DseConfig) o;
    return Objects.equals(authenticationOptions, dseConfig.authenticationOptions) &&
      Objects.equals(roleManagementOptions, dseConfig.roleManagementOptions) &&
      Objects.equals(authorizationOptions, dseConfig.authorizationOptions) &&
      Objects.equals(kerberosOptions, dseConfig.kerberosOptions) &&
      Objects.equals(nodeHealthOptions, dseConfig.nodeHealthOptions) &&
      Objects.equals(loggingOptions, dseConfig.loggingOptions) &&
      Objects.equals(messagingOptions,dseConfig.messagingOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
      authenticationOptions,
      roleManagementOptions,
      authorizationOptions,
      kerberosOptions,
      nodeHealthOptions,
      loggingOptions,
      messagingOptions);
  }

  @JsonIgnore
  public CassandraApplicationConfig update(
    final CassandraApplicationConfig config) {
    CassandraApplicationConfig.Builder builder = config.toBuilder();
    if (authenticationOptions.isEnabled()) {
      builder = builder.setAuthenticator(AUTHENTICATOR_CLASS);
    }
    if (authorizationOptions.isEnabled()) {
      builder = builder.setAuthorizer(AUTHORIZOR_CLASS);
    }
    if (roleManagementOptions.isEnabled()) {
      builder = builder.setRoleManager(ROLE_MANAGER_CLASS);
    }
    return builder.build();
  }
  @Override
  public String toString() {
    return JsonUtils.toJsonString(this);
  }
}
