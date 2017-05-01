/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.AppWithMultipleWorkflows;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.runtime.schedule.constraint.Constraint;
import co.cask.cdap.internal.app.runtime.schedule.trigger.PartitionTrigger;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class NotificationSubscriberServiceTest {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationSubscriberServiceTest.class);

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Gson GSON = new Gson();

  protected static Injector injector;
  protected static CConfiguration cConf;
  protected static MessagingService messagingService;
  private static Store store;

  private static final Supplier<File> TEMP_FOLDER_SUPPLIER = new Supplier<File>() {
    @Override
    public File get() {
      try {
        return TEMP_FOLDER.newFolder();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  };

  private static final TopicId T1 = NamespaceId.DEFAULT.topic("topic1");
  private static final TopicId T2 = NamespaceId.DEFAULT.topic("topic2");
  private static final ApplicationId APP_ID = NamespaceId.DEFAULT.app("AppWithMultipleWorkflows");
  private static final int GENERATION = 1;
  private static final Map<String, String> DEFAULT_PROPERTY = ImmutableMap.of(TopicMetadata.TTL_KEY,
                                                                              Integer.toString(10000),
                                                                              TopicMetadata.GENERATION_KEY,
                                                                              Integer.toString(GENERATION));

  @BeforeClass
  public static void init() throws Exception {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.AppFabric.SCHEDULER_MESSAGING_TOPICS, "topic1,topic2");
    injector = AppFabricTestHelper.getInjector(cConf);
    store = injector.getInstance(Store.class);
    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }
    messagingService.createTopic(new TopicMetadata(T1, DEFAULT_PROPERTY));
    messagingService.createTopic(new TopicMetadata(T2, DEFAULT_PROPERTY));
  }

  @AfterClass
  public static void stop() throws Exception {
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
    messagingService.deleteTopic(T1);
    messagingService.deleteTopic(T2);
  }

  @Test
  public void testRunWorkflow() throws Exception {
    for (int i = 0; i < 5; i++) {
      testNewPartition();
    }
  }

  private void testNewPartition() throws Exception {
    AppFabricTestHelper.deployApplicationWithManager(AppWithMultipleWorkflows.class, TEMP_FOLDER_SUPPLIER);
    NotificationSubscriberService subscriberService = injector.getInstance(NotificationSubscriberService.class);
    Map<String, List<ProgramSchedule>> scheduleMap = new ConcurrentHashMap<>();
    subscriberService.setScheduleMap(scheduleMap);
    subscriberService.startAndWait();

    final ProgramId workflow1 = APP_ID.program(ProgramType.WORKFLOW, AppWithMultipleWorkflows.SomeWorkflow.NAME);
    final ProgramId workflow2 = APP_ID.program(ProgramType.WORKFLOW, AppWithMultipleWorkflows.AnotherWorkflow.NAME);
    Assert.assertEquals(0, store.getRuns(workflow1, ProgramRunStatus.ALL, 0, Long.MAX_VALUE, Integer.MAX_VALUE).size());
    Assert.assertEquals(0, store.getRuns(workflow2, ProgramRunStatus.ALL, 0, Long.MAX_VALUE, Integer.MAX_VALUE).size());
    publishNotification(T1, workflow1, scheduleMap);
    publishNotification(T2, workflow2, scheduleMap);

    try {
      waitForCompleteRuns(1, workflow1);
      waitForCompleteRuns(1, workflow2);
    } finally {
      subscriberService.stopAndWait();
      LOG.info("workflow1 runRecords: {}",
               store.getRuns(workflow1, ProgramRunStatus.ALL, 0, Long.MAX_VALUE, Integer.MAX_VALUE));
      LOG.info("workflow2 runRecords: {}",
               store.getRuns(workflow2, ProgramRunStatus.ALL, 0, Long.MAX_VALUE, Integer.MAX_VALUE));
    }
    store.removeAll(NamespaceId.DEFAULT);
  }

  private void waitForCompleteRuns(int numRuns, final ProgramId program)
    throws InterruptedException, ExecutionException, TimeoutException {

    Tasks.waitFor(numRuns, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return store.getRuns(program, ProgramRunStatus.COMPLETED, 0, Long.MAX_VALUE, Integer.MAX_VALUE).size();
      }
    }, 5, TimeUnit.SECONDS);
  }

  private void publishNotification(TopicId topicId, ProgramId programId,
                                   Map<String, List<ProgramSchedule>> scheduleMap)
    throws TopicNotFoundException, IOException {

    String name = topicId.getTopic() + "-" + programId.getProgram();
    Notification notification =
      new Notification(Notification.Type.PARTITION, ImmutableMap.of("datasetId", name));
    scheduleMap.put(notification.getNotificationKey(),
                    ImmutableList.of(
                      new ProgramSchedule(name, "",
                                          programId, ImmutableMap.<String, String>of(),
                                          new PartitionTrigger(programId.getNamespaceId().dataset(name), 1),
                                          ImmutableList.<Constraint>of())));
    messagingService.publish(StoreRequestBuilder.of(topicId).addPayloads(GSON.toJson(notification)).build());
  }
}
