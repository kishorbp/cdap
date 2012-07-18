package com.continuuity.common.conf;

/**
 * Constants used by different systems are all defined here.
 */
public class Constants {

  /**
   * Common across components.
   */
  public static final String CFG_ZOOKEEPER_ENSEMBLE = "zookeeper.quorum";

  /**
   * Default constants for common
   */
  public static final String DEFAULT_ZOOKEEPER_ENSEMBLE = "localhost:2181";

  /**
   * Configuration key names used by resource manager.
   */
  public static final String CFG_RESOURCE_MANAGER_REMOTE_DIR = "resource.manager.remote.dir";
  public static final String CFG_RESOURCE_MANAGER_LOCAL_DIR = "resource.manager.local.dir";
  public static final String CFG_RESOURCE_MANAGER_STORAGE_MODE = "resource.manager.storage.mode";
  public static final String CFG_RESOURCE_MANAGER_SERVER_PORT = "resource.manager.server.port";
  public static final String CFG_RESOURCE_MANAGER_SERVER_THREADS = "resource.manager.server.threads";
  public static final String CFG_RESOURCE_MANAGER_SERVER_ADDRESS = "resource.manager.server.address";

  /**
   * Defaults for resource manager.
   */
  public static final String DEFAULT_RESOURCE_MANAGER_LOCAL_DIR = "build/continuuity/flow/manager/local";
  public static final String DEFAULT_RESOURCE_MANAGER_SERVER_ADDRESS = "0.0.0.0";

  /**
   * Default constants defined for resource manager
   */
  public static final String DEFAULT_RESOURCE_MANAGER_SERVER_PORT = "45000";
  public static final String DEFAULT_RESOURCE_MANAGER_SERVER_THREADS = "2";

  /**
   * Configuration key names used by flow manager
   */
  public static final String CFG_FLOW_MANAGER_SERVER_PORT = "flow.manager.server.port";
  public static final String CFG_FLOW_MANAGER_SERVER_THREADS = "flow.manager.server.threads";
  public static final String CFG_FLOW_MANAGER_SERVER_ADDRESS = "flow.manager.server.address";

  /**
   * Default constants defined for flow manager.
   */
  public static final String DEFAULT_FLOW_MANAGER_SERVER_PORT = "45001";
  public static final String DEFAULT_FLOW_MANAGER_SERVER_THREADS = "2";
  public static final String DEFAULT_FLOW_MANAGER_SERVER_ADDRESS = "0.0.0.0";

  /**
   * Configuration key names used flow monitor.
   */
  public static final String CFG_FLOW_MONITOR_SERVER_PORT = "flow.monitor.server.port";
  public static final String CFG_FLOW_MONITOR_SERVER_THREADS = "flow.monitor.server.threads";
  public static final String CFG_FLOW_MONITOR_SERVER_ADDRESS = "flow.monitor.server.address";

  /**
   * Default constants defined for flow monitor
   */
  public static final String DEFAULT_FLOW_MONITOR_SERVER_PORT = "45002";
  public static final String DEFAULT_FLOW_MONITOR_SERVER_THREADS = "2";
  public static final String DEFAULT_FLOW_MONITOR_SERVER_ADDRESS = "0.0.0.0";

  /**
   * Configuration for Cloud FAR Service.
   */
  public static final String CFG_RESOURCE_MANAGER_CLOUD_HOST = "resource.manager.cloud.hostname";
  public static final String CFG_RESOURCE_MANAGER_CLOUD_PORT = "resource.manager.cloud.port";
  public static final String DEFAULT_RESOURCE_MANAGER_CLOUD_HOST = "localhost";
  public static final String DEFAULT_RESOURCE_MANAGER_CLOUD_PORT = DEFAULT_RESOURCE_MANAGER_SERVER_PORT;
}
