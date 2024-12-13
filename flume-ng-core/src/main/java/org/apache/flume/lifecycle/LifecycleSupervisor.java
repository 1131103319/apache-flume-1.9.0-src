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

package org.apache.flume.lifecycle;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class LifecycleSupervisor implements LifecycleAware {

    private static final Logger logger = LoggerFactory.getLogger(LifecycleSupervisor.class);

    private Map<LifecycleAware, Supervisoree> supervisedProcesses;
    private Map<LifecycleAware, ScheduledFuture<?>> monitorFutures;

    private ScheduledThreadPoolExecutor monitorService;

    private LifecycleState lifecycleState;
    private Purger purger;
    private boolean needToPurge;

    public LifecycleSupervisor() {
        lifecycleState = LifecycleState.IDLE;
        //todo // supervisedProcesses 用于存放LifecycleAware和Supervisoree对象的键值对，代表已经管理的组件
        supervisedProcesses = new HashMap<LifecycleAware, Supervisoree>();
        //todo //monitorFutures 用于存放LifecycleAware对象和ScheduledFuture对象的键值对
        monitorFutures = new HashMap<LifecycleAware, ScheduledFuture<?>>();
        monitorService = new ScheduledThreadPoolExecutor(10,
                new ThreadFactoryBuilder().setNameFormat(
                                "lifecycleSupervisor-" + Thread.currentThread().getId() + "-%d")
                        //todo // monitorService 用于调用Purger线程，定时移除线程池中已经cancel的task
                        .build());
        monitorService.setMaximumPoolSize(20);
        monitorService.setKeepAliveTime(30, TimeUnit.SECONDS);
        purger = new Purger();
        //todo // 初始时为false，在有task cancel的时候设置为true
        needToPurge = false;
    }

    @Override
    public synchronized void start() {

        logger.info("Starting lifecycle supervisor {}", Thread.currentThread()
                .getId());
        //todo //在两小时后每隔两小时运行一次Purger，释放线程池的工作队列
        monitorService.scheduleWithFixedDelay(purger, 2, 2, TimeUnit.HOURS);
        //todo //设置状态为START
        lifecycleState = LifecycleState.START;

        logger.debug("Lifecycle supervisor started");
    }

    @Override
    public synchronized void stop() {

        logger.info("Stopping lifecycle supervisor {}", Thread.currentThread()
                .getId());

        if (monitorService != null) {
            //todo 线程池关闭
            monitorService.shutdown();
            try {
                monitorService.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for monitor service to stop");
            }
            if (!monitorService.isTerminated()) {
                monitorService.shutdownNow();
                try {
                    while (!monitorService.isTerminated()) {
                        monitorService.awaitTermination(10, TimeUnit.SECONDS);
                    }
                } catch (InterruptedException e) {
                    logger.error("Interrupted while waiting for monitor service to stop");
                }
            }
        }

        for (final Entry<LifecycleAware, Supervisoree> entry : supervisedProcesses.entrySet()) {

            if (entry.getKey().getLifecycleState().equals(LifecycleState.START)) {
                //todo //如果组件的当前状态是START，则首先设置其需要变成的状态为STOP，并调用组件的stop方法
                entry.getValue().status.desiredState = LifecycleState.STOP;
                entry.getKey().stop();
            }
        }

        /* If we've failed, preserve the error state. */
        if (lifecycleState.equals(LifecycleState.START)) {
            lifecycleState = LifecycleState.STOP;
        }
        supervisedProcesses.clear();
        monitorFutures.clear();
        logger.debug("Lifecycle supervisor stopped");
    }

    public synchronized void fail() {
        lifecycleState = LifecycleState.ERROR;
    }

    public synchronized void supervise(LifecycleAware lifecycleAware,
                                       SupervisorPolicy policy, LifecycleState desiredState) {
        if (this.monitorService.isShutdown()
                || this.monitorService.isTerminated()
                //todo //检测监控线程池是否正常
                || this.monitorService.isTerminating()) {
            throw new FlumeException("Supervise called on " + lifecycleAware + " " +
                    "after shutdown has been initiated. " + lifecycleAware + " will not" +
                    " be started");
        }
        //todo //检测是否已经管理
        Preconditions.checkState(!supervisedProcesses.containsKey(lifecycleAware),
                "Refusing to supervise " + lifecycleAware + " more than once");

        if (logger.isDebugEnabled()) {
            logger.debug("Supervising service:{} policy:{} desiredState:{}",
                    new Object[]{lifecycleAware, policy, desiredState});
        }
        //todo //初始化Supervisoree对象
        Supervisoree process = new Supervisoree();
        //todo //并实例化Supervisoree对象的Status属性
        process.status = new Status();
        //todo //设置Supervisoree的属性
        process.policy = policy;
        process.status.desiredState = desiredState;
        process.status.error = false;
        //todo //初始化一个MonitorRunnable 对象（线程），并设置对象的属性
        MonitorRunnable monitorRunnable = new MonitorRunnable();
        monitorRunnable.lifecycleAware = lifecycleAware;
        monitorRunnable.supervisoree = process;
        monitorRunnable.monitorService = monitorService;
        //todo //向supervisedProcesses中插入键值对，代表已经开始管理的组件
        supervisedProcesses.put(lifecycleAware, process);
        //todo // 设置计划任务线程池，每隔3s之后运行monitorRunnable
        ScheduledFuture<?> future = monitorService.scheduleWithFixedDelay(
                monitorRunnable, 0, 3, TimeUnit.SECONDS);
        //todo // 向monitorFutures中插入键值对
        monitorFutures.put(lifecycleAware, future);
    }

    public synchronized void unsupervise(LifecycleAware lifecycleAware) {

        Preconditions.checkState(supervisedProcesses.containsKey(lifecycleAware),
                "Unaware of " + lifecycleAware + " - can not unsupervise");

        logger.debug("Unsupervising service:{}", lifecycleAware);

        synchronized (lifecycleAware) {
            //todo //从已经管理的Supervisoree  hashmap中获取Supervisoree对象
            Supervisoree supervisoree = supervisedProcesses.get(lifecycleAware);
            //todo //设置Supervisoree对象的Status属性的discard 值为discard
            supervisoree.status.discard = true;
            //todo //调用setDesiredState方法，设置Supervisoree对象的Status属性的desiredState
            // 值为STOP（supervisoree.status.desiredState = desiredState）
            this.setDesiredState(lifecycleAware, LifecycleState.STOP);
            logger.info("Stopping component: {}", lifecycleAware);
            //todo //调用组件的stop方法
            lifecycleAware.stop();
        }
        //todo //从supervisedProcesses hashmap中移除这个组件
        supervisedProcesses.remove(lifecycleAware);
        //We need to do this because a reconfiguration simply unsupervises old
        //components and supervises new ones.
        //todo     //调用组件对应的ScheduledFuture的cancel方法取消任务
        // （A Future represents the result of an asynchronous computation.
        monitorFutures.get(lifecycleAware).cancel(false);
        //purges are expensive, so it is done only once every 2 hours.
        //todo //设置needToPurge 的属性为true，这样就可以在purge中删除已经cancel的ScheduledFuture对象
        needToPurge = true;
        monitorFutures.remove(lifecycleAware);
    }

    public synchronized void setDesiredState(LifecycleAware lifecycleAware,
                                             LifecycleState desiredState) {

        Preconditions.checkState(supervisedProcesses.containsKey(lifecycleAware),
                "Unaware of " + lifecycleAware + " - can not set desired state to "
                        + desiredState);

        logger.debug("Setting desiredState:{} on service:{}", desiredState,
                lifecycleAware);

        Supervisoree supervisoree = supervisedProcesses.get(lifecycleAware);
        supervisoree.status.desiredState = desiredState;
    }

    @Override
    public synchronized LifecycleState getLifecycleState() {
        return lifecycleState;
    }

    public synchronized boolean isComponentInErrorState(LifecycleAware component) {
        return supervisedProcesses.get(component).status.error;

    }

    //todo 1.MonitorRunnable，实现了Runnable接口的线程类
    public static class MonitorRunnable implements Runnable {

        public ScheduledExecutorService monitorService;
        public LifecycleAware lifecycleAware;
        public Supervisoree supervisoree;

        @Override
        public void run() {
            logger.debug("checking process:{} supervisoree:{}", lifecycleAware,
                    supervisoree);
            //todo //第一次开始运行时，设置firstSeen为System.currentTimeMillis()
            long now = System.currentTimeMillis();

            try {
                if (supervisoree.status.firstSeen == null) {
                    logger.debug("first time seeing {}", lifecycleAware);
                    supervisoree.status.firstSeen = now;
                }
                //todo //设置lastSeen为now
                supervisoree.status.lastSeen = now;
                synchronized (lifecycleAware) {
                    //todo //如果Status的discard或者error的值为true，会直接退出
                    if (supervisoree.status.discard) {
                        // Unsupervise has already been called on this.
                        logger.info("Component has already been stopped {}", lifecycleAware);
                        return;
                    } else if (supervisoree.status.error) {
                        logger.info("Component {} is in error state, and Flume will not"
                                + "attempt to change its state", lifecycleAware);
                        return;
                    }
                    //todo //设置lastSeenState的值
                    supervisoree.status.lastSeenState = lifecycleAware.getLifecycleState();
                    //todo //如果获取的lifecycleAware对象状态不是想设置的desiredState状态
                    if (!lifecycleAware.getLifecycleState().equals(
                            supervisoree.status.desiredState)) {

                        logger.debug("Want to transition {} from {} to {} (failures:{})",
                                new Object[]{lifecycleAware, supervisoree.status.lastSeenState,
                                        supervisoree.status.desiredState,
                                        supervisoree.status.failures});
                        //todo //根据设置的desiredState状态调用lifecycleAware的不同方法,desiredState的值只有两种START和STOP
                        switch (supervisoree.status.desiredState) {
                            case START:
                                try {
                                    //todo //状态为START时设置运行start方法
                                    lifecycleAware.start();
                                } catch (Throwable e) {
                                    logger.error("Unable to start " + lifecycleAware
                                            + " - Exception follows.", e);
                                    if (e instanceof Error) {
                                        // This component can never recover, shut it down.
                                        supervisoree.status.desiredState = LifecycleState.STOP;
                                        try {
                                            lifecycleAware.stop();
                                            logger.warn("Component {} stopped, since it could not be"
                                                            + "successfully started due to missing dependencies",
                                                    lifecycleAware);
                                        } catch (Throwable e1) {
                                            logger.error("Unsuccessful attempt to "
                                                    + "shutdown component: {} due to missing dependencies."
                                                    + " Please shutdown the agent"
                                                    + "or disable this component, or the agent will be"
                                                    + "in an undefined state.", e1);
                                            supervisoree.status.error = true;
                                            if (e1 instanceof Error) {
                                                throw (Error) e1;
                                            }
                                            // Set the state to stop, so that the conf poller can
                                            // proceed.
                                        }
                                    }
                                    //todo //start方法异常时failures的值加1
                                    supervisoree.status.failures++;
                                }
                                break;
                            case STOP:
                                try {
                                    //todo //状态为STOP时设置运行stop方法
                                    lifecycleAware.stop();
                                } catch (Throwable e) {
                                    logger.error("Unable to stop " + lifecycleAware
                                            + " - Exception follows.", e);
                                    if (e instanceof Error) {
                                        throw (Error) e;
                                    }
                                    //todo //stop方法异常时failures的值加1
                                    supervisoree.status.failures++;
                                }
                                break;
                            default:
                                logger.warn("I refuse to acknowledge {} as a desired state",
                                        supervisoree.status.desiredState);
                        }
                        //todo                //调用SupervisorPolicy的isValid方法，
                        // 比如OnceOnlyPolicy 的isValid的方法会判断Status.failures 的值，如果为0则返回true,否则返回false
                        if (!supervisoree.policy.isValid(lifecycleAware, supervisoree.status)) {
                            logger.error(
                                    "Policy {} of {} has been violated - supervisor should exit!",
                                    supervisoree.policy, lifecycleAware);
                        }
                    }
                }
            } catch (Throwable t) {
                logger.error("Unexpected error", t);
            }
            logger.debug("Status check complete");
        }
    }

    private class Purger implements Runnable {

        @Override
        public void run() {
            //todo //如果needToPurge设置为true
            if (needToPurge) {
                //todo //ScheduledThreadPoolExecutor.purge方法用于从工作队列中删除已经cancel的
                // java.util.concurrent.Future对象（释放队列空间）
                monitorService.purge();
                //todo //并设置needToPurge为false
                needToPurge = false;
            }
        }
    }

    public static class Status {
        public Long firstSeen;
        public Long lastSeen;
        public LifecycleState lastSeenState;
        public LifecycleState desiredState;
        public int failures;
        public boolean discard;
        public volatile boolean error;

        @Override
        public String toString() {
            return "{ lastSeen:" + lastSeen + " lastSeenState:" + lastSeenState
                    + " desiredState:" + desiredState + " firstSeen:" + firstSeen
                    + " failures:" + failures + " discard:" + discard + " error:" +
                    error + " }";
        }

    }

    public abstract static class SupervisorPolicy {

        abstract boolean isValid(LifecycleAware object, Status status);

        public static class AlwaysRestartPolicy extends SupervisorPolicy {

            @Override
            boolean isValid(LifecycleAware object, Status status) {
                return true;
            }
        }

        public static class OnceOnlyPolicy extends SupervisorPolicy {

            @Override
            boolean isValid(LifecycleAware object, Status status) {
                return status.failures == 0;
            }
        }

    }

    private static class Supervisoree {

        public SupervisorPolicy policy;
        public Status status;

        @Override
        public String toString() {
            return "{ status:" + status + " policy:" + policy + " }";
        }

    }

}
