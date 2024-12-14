/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.node;

import com.google.common.base.Preconditions;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.CounterGroup;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PollingPropertiesFileConfigurationProvider
        extends PropertiesFileConfigurationProvider
        implements LifecycleAware {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(PollingPropertiesFileConfigurationProvider.class);

    private final EventBus eventBus;
    private final File file;
    private final int interval;
    private final CounterGroup counterGroup;
    private LifecycleState lifecycleState;

    private ScheduledExecutorService executorService;

    public PollingPropertiesFileConfigurationProvider(String agentName,
                                                      File file, EventBus eventBus, int interval) {
        super(agentName, file);
        this.eventBus = eventBus;
        //todo 配置文件
        this.file = file;

        this.interval = interval;
        counterGroup = new CounterGroup();
        lifecycleState = LifecycleState.IDLE;
    }

    @Override
    public void start() {
        LOGGER.info("Configuration provider starting");

        Preconditions.checkState(file != null,
                "The parameter file must not be null");
        //todo //定义一个单线程的任务调度池
        executorService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("conf-file-poller-%d")
                        .build());
        //todo //构建一个FileWatcherRunnable服务对象
        FileWatcherRunnable fileWatcherRunnable =
                new FileWatcherRunnable(file, counterGroup);
        //todo //以interval为时间间隔运行FileWatcherRunnable
        executorService.scheduleWithFixedDelay(fileWatcherRunnable, 0, interval,
                TimeUnit.SECONDS);
        //todo // 设置为START状态
        lifecycleState = LifecycleState.START;

        LOGGER.debug("Configuration provider started");
    }

    @Override
    public void stop() {
        LOGGER.info("Configuration provider stopping");

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                LOGGER.debug("File watcher has not terminated. Forcing shutdown of executor.");
                executorService.shutdownNow();
                while (!executorService.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                    LOGGER.debug("Waiting for file watcher to terminate");
                }
            }
        } catch (InterruptedException e) {
            LOGGER.debug("Interrupted while waiting for file watcher to terminate");
            Thread.currentThread().interrupt();
        }
        lifecycleState = LifecycleState.STOP;
        LOGGER.debug("Configuration provider stopped");
    }

    @Override
    public synchronized LifecycleState getLifecycleState() {
        return lifecycleState;
    }


    @Override
    public String toString() {
        return "{ file:" + file + " counterGroup:" + counterGroup + "  provider:"
                + getClass().getCanonicalName() + " agentName:" + getAgentName() + " }";
    }

    public class FileWatcherRunnable implements Runnable {

        private final File file;
        private final CounterGroup counterGroup;

        private long lastChange;

        public FileWatcherRunnable(File file, CounterGroup counterGroup) {
            super();
            this.file = file;
            this.counterGroup = counterGroup;
            this.lastChange = 0L;
        }

        @Override
        public void run() {
            LOGGER.debug("Checking file:{} for changes", file);

            counterGroup.incrementAndGet("file.checks");
            //todo //调用File.lastModified获取文件的上一次更新时的时间戳
            long lastModified = file.lastModified();
            //todo //初始lastChange 为0
            if (lastModified > lastChange) {
                LOGGER.info("Reloading configuration file:{}", file);

                counterGroup.incrementAndGet("file.loads");
                //todo // 设置本次的lastChange 为更新时间，如果下一次更新文件，lastModified 会大于这个lastChange
                lastChange = lastModified;

                try {
                    eventBus.post(getConfiguration());
                } catch (Exception e) {
                    LOGGER.error("Failed to load configuration data. Exception follows.",
                            e);
                } catch (NoClassDefFoundError e) {
                    LOGGER.error("Failed to start agent because dependencies were not " +
                            "found in classpath. Error follows.", e);
                } catch (Throwable t) {
                    // caught because the caller does not handle or log Throwables
                    LOGGER.error("Unhandled error", t);
                }
            }
        }
    }

}
