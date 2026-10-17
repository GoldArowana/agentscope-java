/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.agentscope.extensions.scheduler.quartz;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.extensions.scheduler.config.RuntimeAgentConfig;
import io.agentscope.extensions.scheduler.config.ScheduleConfig;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.KeyMatcher;
import org.quartz.listeners.JobListenerSupport;

/** Exercises task resolution through a real Quartz worker and job listener. */
@Timeout(15)
class AgentQuartzJobIntegrationTest {

    @Test
    void missingRegistryReportsFailedExecutionToQuartz() throws Exception {
        executeOnce(true);
    }

    @Test
    void registeredTaskStillExecutesSuccessfully() throws Exception {
        executeOnce(false);
    }

    private void executeOnce(boolean removeRegistry) throws Exception {
        String schedulerId = "job-resolution-" + UUID.randomUUID();
        Properties properties = new Properties();
        properties.setProperty("org.quartz.scheduler.instanceName", schedulerId);
        properties.setProperty("org.quartz.scheduler.makeSchedulerThreadDaemon", "true");
        properties.setProperty("org.quartz.threadPool.threadCount", "1");
        properties.setProperty("org.quartz.threadPool.makeThreadsDaemons", "true");
        properties.setProperty("org.quartz.jobStore.class", "org.quartz.simpl.RAMJobStore");
        Scheduler quartz = new StdSchedulerFactory(properties).getScheduler();
        QuartzAgentScheduler scheduler =
                QuartzAgentScheduler.builder()
                        .scheduler(quartz)
                        .schedulerId(schedulerId)
                        .autoStart(false)
                        .build();
        try {
            QuartzScheduleAgentTask task =
                    (QuartzScheduleAgentTask)
                            scheduler.schedule(
                                    RuntimeAgentConfig.builder()
                                            .name("diagnostic-task")
                                            .modelConfig(new StubModelConfig())
                                            .sysPrompt("test")
                                            .build(),
                                    ScheduleConfig.builder()
                                            .fixedRate(60_000L)
                                            .initialDelay(60_000L)
                                            .build());
            TriggerKey scheduledTrigger = quartz.getTriggersOfJob(task.getJobKey()).get(0).getKey();
            CountDownLatch completed = new CountDownLatch(1);
            AtomicReference<JobExecutionException> failure = new AtomicReference<>();
            AtomicReference<JobExecutionContext> execution = new AtomicReference<>();
            quartz.getListenerManager()
                    .addJobListener(
                            new JobListenerSupport() {
                                @Override
                                public String getName() {
                                    return "execution-observer";
                                }

                                @Override
                                public void jobWasExecuted(
                                        JobExecutionContext context,
                                        JobExecutionException jobException) {
                                    execution.set(context);
                                    failure.set(jobException);
                                    completed.countDown();
                                }
                            },
                            KeyMatcher.keyEquals(task.getJobKey()));
            if (removeRegistry) {
                // Keep the real stored JobDetail/trigger while removing process-local resolution.
                QuartzAgentSchedulerRegistry.unregister(schedulerId);
            }
            quartz.start();
            quartz.triggerJob(task.getJobKey());
            assertTrue(completed.await(5, TimeUnit.SECONDS), "Quartz must fire the job");
            if (removeRegistry) {
                JobExecutionException exception = failure.get();
                assertNotNull(
                        exception, "Quartz must not report a dropped execution as successful");
                assertTrue(exception.getMessage().contains(schedulerId));
                assertTrue(exception.getMessage().contains("diagnostic-task"));
                assertTrue(exception.getMessage().contains(task.getJobKey().toString()));
                assertTrue(
                        exception
                                .getMessage()
                                .contains(execution.get().getTrigger().getKey().toString()));
                assertFalse(exception.refireImmediately());
                assertFalse(exception.unscheduleFiringTrigger());
                assertFalse(exception.unscheduleAllTriggers());
                assertEquals(0, task.getExecutionCount());
            } else {
                assertNull(failure.get());
                assertEquals(1, task.getExecutionCount());
            }
            assertTrue(quartz.checkExists(scheduledTrigger));
            assertNotNull(quartz.getTrigger(scheduledTrigger).getNextFireTime());
        } finally {
            scheduler.shutdown();
        }
    }
}
