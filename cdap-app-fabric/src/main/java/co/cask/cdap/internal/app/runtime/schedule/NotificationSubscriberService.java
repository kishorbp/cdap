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

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.messaging.MultiThreadMessagingContext;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.internal.app.services.PropertiesResolver;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Subscribe to notification TMS topic and update schedules in schedule store and job queue
 */
public class NotificationSubscriberService extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationSubscriberService.class);
  // Sampling log only log once per 10000
  private static final Logger SAMPLING_LOG = Loggers.sampling(LOG, LogSamplers.limitRate(10000));


  private static final Gson GSON = new Gson();

  private final MessagingService messagingService;
  private final String[] topics;
  private final List<NotificationSubscriberThread> subscriberThreads;
  /** Temporarily unused **/
  private final Transactional transactional;
  private final MultiThreadMessagingContext messagingContext;
  private final Store store;
  private final ProgramLifecycleService lifecycleService;
  private final PropertiesResolver propertiesResolver;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final CConfiguration cConf;
  private ScheduleTaskRunner taskRunner;
  private Map<String, List<ProgramSchedule>> scheduleMap;
  private ListeningExecutorService taskExecutorService;

  @Inject
  NotificationSubscriberService(MessagingService messagingService,
                                Store store,
                                ProgramLifecycleService lifecycleService, PropertiesResolver propertiesResolver,
                                NamespaceQueryAdmin namespaceQueryAdmin,
                                CConfiguration cConf,
                                DatasetFramework datasetFramework,
                                TransactionSystemClient txClient) {
    this.store = store;
    this.lifecycleService = lifecycleService;
    this.propertiesResolver = propertiesResolver;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.cConf = cConf;
    this.messagingService = messagingService;
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), txClient,
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null, this.messagingContext)),
      RetryStrategies.retryOnConflict(20, 100)
    );
    this.topics = cConf.getTrimmedStrings(Constants.AppFabric.SCHEDULER_MESSAGING_TOPICS);
    this.subscriberThreads = new ArrayList<>();
  }

  @Override
  protected void startUp() throws Exception {
    taskExecutorService = MoreExecutors.listeningDecorator(
      Executors.newCachedThreadPool(Threads.createDaemonThreadFactory("notification-subscriber-task")));
    taskRunner = new ScheduleTaskRunner(store, lifecycleService, propertiesResolver,
                                        taskExecutorService, namespaceQueryAdmin, cConf);
  }

  /**
   * Temporary workaround when Schedule store is not ready.
   *
   * @param scheduleMap Map storing keys constructed from {@link Notification}
   *                    and corresponding {@link ProgramSchedule} as values
   */
  public void setScheduleMap(Map<String, List<ProgramSchedule>> scheduleMap) {
    this.scheduleMap = scheduleMap;
  }

  @Override
  protected void run() {
    LOG.info("Start running NotificationSubscriberService");
    if (!isRunning()) {
      return;
    }

    for (String topic : topics) {
      subscriberThreads.add(new NotificationSubscriberThread(topic, null));
    }

    for (NotificationSubscriberThread thread : subscriberThreads) {
      thread.start();
    }

    for (NotificationSubscriberThread thread : subscriberThreads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        LOG.info("Thread {} is being terminated while waiting for it to finish.", thread.getName());
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  protected void triggerShutdown() {
    LOG.info("Stopping NotificationSubscriberService.");
    for (NotificationSubscriberThread thread : subscriberThreads) {
      thread.interrupt();
    }

    LOG.info("NotificationSubscriberService stopped.");
  }

  @Override
  protected void shutDown() throws Exception {
    if (taskExecutorService != null) {
      taskExecutorService.shutdownNow();
    }
  }

  private class NotificationSubscriberThread extends Thread {
    private final String topic;
    private final RetryStrategy scheduleStrategy;
    private final Deque<Job> readyJobs;
    private int emptyFetchCount;
    private int failureCount;
    private String messageId;


    NotificationSubscriberThread(String topic, @Nullable String messageId) {
      super(String.format("NotificationSubscriberThread-%s", topic));
      this.topic = topic;
      this.messageId = messageId;
      // TODO: [CDAP-11370] Need to be configured in cdap-default.xml. Retry with delay ranging from 0.1s to 30s
      scheduleStrategy =
        co.cask.cdap.common.service.RetryStrategies.exponentialDelay(100, 30000, TimeUnit.MILLISECONDS);
      this.readyJobs = new ArrayDeque<>();
    }

    @Override
    public void run() {
      while (isRunning()) {
        try {
          long sleepTime = processNotifications();
          // Don't sleep if sleepTime returned is 0
          if (sleepTime > 0) {
            TimeUnit.MILLISECONDS.sleep(sleepTime);
          }
        } catch (InterruptedException e) {
          // sleep is interrupted, just exit without doing anything
        }
      }
    }

    /**
     * Fetch new notifications and update job queue
     *
     * @return sleep time in milliseconds before next fetch
     */
    private long processNotifications() {
      try {
        final MessageFetcher fetcher = messagingContext.getMessageFetcher();
        emptyFetchCount++;
        transactional.execute(new TxRunnable() {
          @Override
          public void run(DatasetContext context) throws TopicNotFoundException, IOException {
            fetchNotifications(fetcher);
          }
        });
      } catch (Exception e) {
        LOG.warn("Failed to get notification. Will retry in next run", e);
        failureCount++;
        // Exponential strategy doesn't use the time component, so doesn't matter what we passed in as startTime
        return scheduleStrategy.nextRetry(failureCount, 0);
      }
      failureCount = 0;

        try {
          runReadyJobs();
        } catch (Exception e) {
          LOG.error("Failed to run scheduled programs", e);
        }
      // Back-off if it was empty fetch.
      if (emptyFetchCount > 0) {
        return 2000L;
      }
      return 0L; // No sleep if the fetch is non-empty
    }

    private void fetchNotifications(MessageFetcher fetcher) throws TopicNotFoundException, IOException {
      try (CloseableIterator<Message> iterator = fetcher.fetch(NamespaceId.SYSTEM.getNamespace(),
                                                               topic, 100, messageId)) {
        LOG.trace("Fetch with messageId = {}", messageId);
        String currentMessageId = null;
        while (iterator.hasNext() && isRunning()) {
          emptyFetchCount = 0;
          Message message = iterator.next();
          Notification notification = null;
          try {
            notification = GSON.fromJson(new String(message.getPayload(), StandardCharsets.UTF_8),
                                         Notification.class);
          } catch (JsonSyntaxException e) {
            LOG.warn("Failed to decode message with id {}. Skipped. ", message.getId(), e);
          }
          if (notification != null) {
            processNotification(notification);
          }
          // Record current message's Id no matter decode is successful or not,
          // so that this message will be skipped in next fetch
          currentMessageId = message.getId();
        }
        if (currentMessageId != null) {
          // Update messageId with the last message's Id
          messageId = currentMessageId;
        }
      } catch (ServiceUnavailableException | TopicNotFoundException e) {
        SAMPLING_LOG.info("Failed to fetch from TMS. Will retry later.", e);
        failureCount++;
      }
    }

    private void runReadyJobs() {
      Iterator<Job> jobIterator = readyJobs.iterator();
      while (jobIterator.hasNext()) {
        Job job = jobIterator.next();
        ProgramSchedule schedule = job.getSchedule();
        try {
          // TODO: Temporarily execute scheduled program without any checks. Need to check appSpec and scheduleSpec
          taskRunner.execute(schedule.getProgramId(), ImmutableMap.<String, String>of(),
                             ImmutableMap.<String, String>of());
          LOG.debug("Run program {} in schedule", schedule.getProgramId(), schedule.getName());
        } catch (Exception e) {
          LOG.warn("Failed to run program {} in schedule {}. Skip running this program.",
                   schedule.getProgramId(), schedule.getName(), e);
        }
        jobIterator.remove();
      }
    }

    /**
     * Update PendingJobs with schedules and remove Jobs ready to run from PendingJobs to ReadyJobs
     *
     * @return A list of {@link ProgramSchedule} ready to run
     */
    private List<Job> updateJobs(List<ProgramSchedule> triggeredSchedules) {
      List<Job> newReadyJobs = new ArrayList<>();
      for (ProgramSchedule schedule : triggeredSchedules) {
        newReadyJobs.add(new Job(schedule));
      }
      return newReadyJobs;
    }

    private void processNotification(Notification notification) {
      String key = notification.getNotificationKey();
      if (key == null) {
        return;
      }
      List<ProgramSchedule> triggeredSchedules = getSchedules(key);
      if (triggeredSchedules == null) {
        return;
      }

      List<Job> newReadyJobs = updateJobs(triggeredSchedules);
      for (Job job : newReadyJobs) {
          readyJobs.add(job);
      }
    }
  }

  class Job {
    ProgramSchedule schedule;

    Job(ProgramSchedule schedule) {
      this.schedule = schedule;
    }

    public ProgramSchedule getSchedule() {
      return schedule;
    }
  }

  @Nullable
  private List<ProgramSchedule> getSchedules(String key) {
    return scheduleMap.get(key);
  }
}
