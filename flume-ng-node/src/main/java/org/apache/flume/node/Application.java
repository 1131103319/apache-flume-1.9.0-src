/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.node;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flume.Channel;
import org.apache.flume.Constants;
import org.apache.flume.Context;
import org.apache.flume.SinkRunner;
import org.apache.flume.SourceRunner;
import org.apache.flume.instrumentation.MonitorService;
import org.apache.flume.instrumentation.MonitoringType;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.lifecycle.LifecycleSupervisor;
import org.apache.flume.lifecycle.LifecycleSupervisor.SupervisorPolicy;
import org.apache.flume.util.SSLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

public class Application {

    private static final Logger logger = LoggerFactory
            .getLogger(Application.class);

    public static final String CONF_MONITOR_CLASS = "flume.monitoring.type";
    public static final String CONF_MONITOR_PREFIX = "flume.monitoring.";

    private final List<LifecycleAware> components;
    private final LifecycleSupervisor supervisor;
    private MaterializedConfiguration materializedConfiguration;
    private MonitorService monitorServer;
    private final ReentrantLock lifecycleLock = new ReentrantLock();

    public Application() {
        this(new ArrayList<LifecycleAware>(0));
    }

    public Application(List<LifecycleAware> components) {
        this.components = components;
        //todo 监控服务
        supervisor = new LifecycleSupervisor();
    }

    public void start() {
        lifecycleLock.lock();
        try {
            //todo //start方法会对每一个组件调用LifecycleSupervisor.supervise方法，参数为组件，
            // AlwaysRestartPolicy和START状态（即期望的状态为START）
            for (LifecycleAware component : components) {
                supervisor.supervise(component,
                        new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Subscribe
    public void handleConfigurationEvent(MaterializedConfiguration conf) {
        try {
            lifecycleLock.lockInterruptibly();
            //todo stopAllComponents方法用于关闭所有的组件，
            stopAllComponents();
            startAllComponents(conf);
        } catch (InterruptedException e) {
            logger.info("Interrupted while trying to handle configuration event");
            return;
        } finally {
            // If interrupted while trying to lock, we don't own the lock, so must not attempt to unlock
            if (lifecycleLock.isHeldByCurrentThread()) {
                lifecycleLock.unlock();
            }
        }
    }

    public void stop() {
        lifecycleLock.lock();
        stopAllComponents();
        try {
            supervisor.stop();
            if (monitorServer != null) {
                monitorServer.stop();
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    private void stopAllComponents() {
        if (this.materializedConfiguration != null) {
            logger.info("Shutting down configuration: {}", this.materializedConfiguration);
            for (Entry<String, SourceRunner> entry :
                    this.materializedConfiguration.getSourceRunners().entrySet()) {
                try {
                    logger.info("Stopping Source " + entry.getKey());
                    supervisor.unsupervise(entry.getValue());
                } catch (Exception e) {
                    logger.error("Error while stopping {}", entry.getValue(), e);
                }
            }

            for (Entry<String, SinkRunner> entry :
                    this.materializedConfiguration.getSinkRunners().entrySet()) {
                try {
                    logger.info("Stopping Sink " + entry.getKey());
                    supervisor.unsupervise(entry.getValue());
                } catch (Exception e) {
                    logger.error("Error while stopping {}", entry.getValue(), e);
                }
            }

            for (Entry<String, Channel> entry :
                    this.materializedConfiguration.getChannels().entrySet()) {
                try {
                    logger.info("Stopping Channel " + entry.getKey());
                    supervisor.unsupervise(entry.getValue());
                } catch (Exception e) {
                    logger.error("Error while stopping {}", entry.getValue(), e);
                }
            }
        }
        if (monitorServer != null) {
            monitorServer.stop();
        }
    }

    private void startAllComponents(MaterializedConfiguration materializedConfiguration) {
        logger.info("Starting new configuration:{}", materializedConfiguration);

        this.materializedConfiguration = materializedConfiguration;

        for (Entry<String, Channel> entry :
                materializedConfiguration.getChannels().entrySet()) {
            try {
                logger.info("Starting Channel " + entry.getKey());
                supervisor.supervise(entry.getValue(),
                        new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
            } catch (Exception e) {
                logger.error("Error while starting {}", entry.getValue(), e);
            }
        }

        /*
         * Wait for all channels to start.
         */
        for (Channel ch : materializedConfiguration.getChannels().values()) {
            while (ch.getLifecycleState() != LifecycleState.START
                    && !supervisor.isComponentInErrorState(ch)) {
                try {
                    logger.info("Waiting for channel: " + ch.getName() +
                            " to start. Sleeping for 500 ms");
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    logger.error("Interrupted while waiting for channel to start.", e);
                    Throwables.propagate(e);
                }
            }
        }

        for (Entry<String, SinkRunner> entry : materializedConfiguration.getSinkRunners().entrySet()) {
            try {
                logger.info("Starting Sink " + entry.getKey());
                supervisor.supervise(entry.getValue(),
                        new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
            } catch (Exception e) {
                logger.error("Error while starting {}", entry.getValue(), e);
            }
        }

        for (Entry<String, SourceRunner> entry :
                materializedConfiguration.getSourceRunners().entrySet()) {
            try {
                logger.info("Starting Source " + entry.getKey());
                supervisor.supervise(entry.getValue(),
                        new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
            } catch (Exception e) {
                logger.error("Error while starting {}", entry.getValue(), e);
            }
        }

        this.loadMonitoring();
    }

    @SuppressWarnings("unchecked")
    private void loadMonitoring() {
        Properties systemProps = System.getProperties();
        Set<String> keys = systemProps.stringPropertyNames();
        try {
            if (keys.contains(CONF_MONITOR_CLASS)) {
                String monitorType = systemProps.getProperty(CONF_MONITOR_CLASS);
                Class<? extends MonitorService> klass;
                try {
                    //Is it a known type?
                    klass = MonitoringType.valueOf(
                            monitorType.toUpperCase(Locale.ENGLISH)).getMonitorClass();
                } catch (Exception e) {
                    //Not a known type, use FQCN
                    klass = (Class<? extends MonitorService>) Class.forName(monitorType);
                }
                this.monitorServer = klass.newInstance();
                Context context = new Context();
                for (String key : keys) {
                    if (key.startsWith(CONF_MONITOR_PREFIX)) {
                        context.put(key.substring(CONF_MONITOR_PREFIX.length()),
                                systemProps.getProperty(key));
                    }
                }
                monitorServer.configure(context);
                monitorServer.start();
            }
        } catch (Exception e) {
            logger.warn("Error starting monitoring. "
                    + "Monitoring might not be available.", e);
        }

    }

    public static void main(String[] args) {

        try {
            SSLUtil.initGlobalSSLParameters();

            Options options = new Options();

            Option option = new Option("n", "name", true, "the name of this agent");
            option.setRequired(true);
            options.addOption(option);

            option = new Option("f", "conf-file", true,
                    "specify a config file (required if -z missing)");
            option.setRequired(false);
            options.addOption(option);

            option = new Option(null, "no-reload-conf", false,
                    "do not reload config file if changed");
            options.addOption(option);

            // Options for Zookeeper
            option = new Option("z", "zkConnString", true,
                    "specify the ZooKeeper connection to use (required if -f missing)");
            option.setRequired(false);
            options.addOption(option);

            option = new Option("p", "zkBasePath", true,
                    "specify the base path in ZooKeeper for agent configs");
            option.setRequired(false);
            options.addOption(option);

            option = new Option("h", "help", false, "display help text");
            options.addOption(option);

            CommandLineParser parser = new GnuParser();
            //todo
            CommandLine commandLine = parser.parse(options, args);

            if (commandLine.hasOption('h')) {
                new HelpFormatter().printHelp("flume-ng agent", options, true);
                return;
            }

            String agentName = commandLine.getOptionValue('n');
            //todo //获取是否含有no-reload-conf的设置，如果没有设置no-reload-conf则reload为true
            boolean reload = !commandLine.hasOption("no-reload-conf");

            boolean isZkConfigured = false;
            if (commandLine.hasOption('z') || commandLine.hasOption("zkConnString")) {
                isZkConfigured = true;
            }

            Application application;
            if (isZkConfigured) {
                // get options
                String zkConnectionStr = commandLine.getOptionValue('z');
                String baseZkPath = commandLine.getOptionValue('p');

                if (reload) {
                    EventBus eventBus = new EventBus(agentName + "-event-bus");
                    List<LifecycleAware> components = Lists.newArrayList();
                    PollingZooKeeperConfigurationProvider zookeeperConfigurationProvider =
                            new PollingZooKeeperConfigurationProvider(
                                    agentName, zkConnectionStr, baseZkPath, eventBus);
                    components.add(zookeeperConfigurationProvider);
                    application = new Application(components);
                    eventBus.register(application);
                } else {
                    StaticZooKeeperConfigurationProvider zookeeperConfigurationProvider =
                            new StaticZooKeeperConfigurationProvider(
                                    agentName, zkConnectionStr, baseZkPath);
                    application = new Application();
                    application.handleConfigurationEvent(zookeeperConfigurationProvider.getConfiguration());
                }
            } else {
                //todo 读取配置文件
                File configurationFile = new File(commandLine.getOptionValue('f'));

                /*
                 * The following is to ensure that by default the agent will fail on
                 * startup if the file does not exist.
                 */
                if (!configurationFile.exists()) {
                    // If command line invocation, then need to fail fast
                    if (System.getProperty(Constants.SYSPROP_CALLED_FROM_SERVICE) ==
                            null) {
                        String path = configurationFile.getPath();
                        try {
                            path = configurationFile.getCanonicalPath();
                        } catch (IOException ex) {
                            logger.error("Failed to read canonical path for file: " + path,
                                    ex);
                        }
                        throw new ParseException(
                                "The specified configuration file does not exist: " + path);
                    }
                }
                //todo //初始化一个List<LifecycleAware>对象，用来存放需要启动的组件，这个只有在支持reload的情况才会使用
                List<LifecycleAware> components = Lists.newArrayList();

                if (reload) {
                    //todo //实例化一个EventBus对象
                    EventBus eventBus = new EventBus(agentName + "-event-bus");
                    PollingPropertiesFileConfigurationProvider configurationProvider =
                            //todo 看着就是监控这个配置文件修改的
                            new PollingPropertiesFileConfigurationProvider(
                                    //todo /默认的检测文件是否更新的interval时间为30s
                                    agentName, configurationFile, eventBus, 30);
                    //todo //添加到启动列表中，在start方法中会启动PollingPropertiesFileConfigurationProvider 的计划任务线程池
                    components.add(configurationProvider);
                    application = new Application(components);
                    //todo //向EventBus中注册Application对象，让Application对象作为事件的监听者
                    eventBus.register(application);
                } else {
                    //todo //不知道reload的情况
                    //todo //实例化一个PropertiesFileConfigurationProvider 对象，参数是agent的名称和配置文件（即n和f的设置）
                    PropertiesFileConfigurationProvider configurationProvider =
                            new PropertiesFileConfigurationProvider(agentName, configurationFile);
                    application = new Application();
                    //todo //调用handleConfigurationEvent方法
                    application.handleConfigurationEvent(configurationProvider.getConfiguration());
                }
            }
            application.start();

            final Application appReference = application;
            Runtime.getRuntime().addShutdownHook(new Thread("agent-shutdown-hook") {
                @Override
                public void run() {
                    appReference.stop();
                }
            });

        } catch (Exception e) {
            logger.error("A fatal error occurred while running. Exception follows.", e);
        }
    }
}