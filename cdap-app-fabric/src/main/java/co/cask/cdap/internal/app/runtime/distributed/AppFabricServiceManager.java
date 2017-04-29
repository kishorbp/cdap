/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.twill.MasterServiceManager;
import co.cask.cdap.common.zookeeper.election.LeaderElectionInfoService;
import co.cask.cdap.proto.Containers;
import co.cask.cdap.proto.SystemServiceLiveInfo;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.api.logging.LogEntry;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * App Fabric Service Management in Distributed Mode.
 */
public class AppFabricServiceManager implements MasterServiceManager {

  private static final long ELECTION_PARTICIPANTS_TIMEOUT_MS = 2000L;

  private final InetAddress hostname;
  private LeaderElectionInfoService electionInfoService;

  @Inject
  public AppFabricServiceManager(@Named(Constants.Service.MASTER_SERVICES_BIND_ADDRESS) InetAddress hostname) {
    this.hostname = hostname;
  }

  @Inject(optional = true)
  public void setLeaderElectionInfoService(LeaderElectionInfoService electionInfoService) {
    // Use optional Guice injection to make setup easier.
    // Currently all tools uses AppFabricServiceRuntimeModule().getDistributedModules() (which is bad),
    // which pull in this class.
    this.electionInfoService = electionInfoService;
  }

  @Override
  public String getDescription() {
    return Constants.AppFabric.SERVICE_DESCRIPTION;
  }

  @Override
  public int getMaxInstances() {
    return 1;
  }

  @Override
  public SystemServiceLiveInfo getLiveInfo() {
    SystemServiceLiveInfo.Builder builder = SystemServiceLiveInfo.builder();

    try {
      SortedMap<Integer, LeaderElectionInfoService.Participant> participants =
        electionInfoService.getParticipants(ELECTION_PARTICIPANTS_TIMEOUT_MS, TimeUnit.MILLISECONDS);

      for (LeaderElectionInfoService.Participant participant : participants.values()) {
        builder.addContainer(new Containers.ContainerInfo(Containers.ContainerType.SYSTEM_SERVICE,
                                                          Constants.Service.APP_FABRIC_HTTP, null, null,
                                                          participant.getHostname(), null, null, null));
      }
    } catch (Exception e) {
      // If failed to get the leader election information, just return a static one with the current process
      builder.addContainer(new Containers.ContainerInfo(Containers.ContainerType.SYSTEM_SERVICE,
                                                        Constants.Service.APP_FABRIC_HTTP, null, null,
                                                        hostname.getHostName(), null, null, null));
    }

    return builder.build();
  }

  @Override
  public int getInstances() {
    return 1;
  }

  @Override
  public boolean setInstances(int instanceCount) {
    return false;
  }

  @Override
  public int getMinInstances() {
    return 1;
  }

  @Override
  public boolean isLogAvailable() {
    return true;
  }

  @Override
  public boolean canCheckStatus() {
    return true;
  }

  @Override
  public boolean isServiceAvailable() {
    return true;
  }

  @Override
  public boolean isServiceEnabled() {
    return true;
  }

  @Override
  public void restartAllInstances() {
    // no-op
  }

  @Override
  public void restartInstances(int instanceId, int... moreInstanceIds) {
    // no-op
  }

  @Override
  public void updateServiceLogLevels(Map<String, LogEntry.Level> logLevels) {
    // no-op
  }

  @Override
  public void resetServiceLogLevels(Set<String> loggerNames) {
    // no-op
  }
}
