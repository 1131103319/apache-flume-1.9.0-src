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
package org.apache.flume.channel;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.flume.ChannelException;
import org.apache.flume.ChannelFullException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.annotations.Recyclable;
import org.apache.flume.conf.TransactionCapacitySupported;
import org.apache.flume.instrumentation.ChannelCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * MemoryChannel is the recommended channel to use when speeds which
 * writing to disk is impractical is required or durability of data is not
 * required.
 * </p>
 * <p>
 * Additionally, MemoryChannel should be used when a channel is required for
 * unit testing purposes.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@Recyclable
public class MemoryChannel extends BasicChannelSemantics implements TransactionCapacitySupported {
    private static Logger LOGGER = LoggerFactory.getLogger(MemoryChannel.class);
    //todo   //定义队列中一次允许的事件总数
    private static final Integer defaultCapacity = 100;
    //todo   //定义一个事务中允许的事件总数
    private static final Integer defaultTransCapacity = 100;
    //todo   //将物理内存转换成槽(slot)数，默认是100
    private static final double byteCapacitySlotSize = 100;
    //todo   //定义队列中事件所使用空间的最大字节数(默认是JVM最大可用内存的0.8)
    private static final Long defaultByteCapacity = (long) (Runtime.getRuntime().maxMemory() * .80);
    //todo   //定义byteCapacity和预估Event大小之间的缓冲区百分比：
    private static final Integer defaultByteCapacityBufferPercentage = 20;
    //todo   //添加或者删除一个event的超时时间，单位秒：
    private static final Integer defaultKeepAlive = 3;

    private class MemoryTransaction extends BasicTransactionSemantics {
        //todo takeList：take事务用到的队列；阻塞双端队列，从channel中取event先放入takeList，输送到sink，
        // commit成功，从channel queue中删除；
        private LinkedBlockingDeque<Event> takeList;
        //todo putList：put事务用到的队列；从source 会先放至putList，然后commit传送到channel queue队列；
        private LinkedBlockingDeque<Event> putList;
        //todo channelCounter：channel属性；ChannelCounter类定义了监控指标数据的一些属性方法；
        private final ChannelCounter channelCounter;
        //todo putByteCounter：put字节数计数器；
        private int putByteCounter = 0;
        //todo takeByteCounter：take字节计数器；
        private int takeByteCounter = 0;

        public MemoryTransaction(int transCapacity, ChannelCounter counter) {
            putList = new LinkedBlockingDeque<Event>(transCapacity);
            takeList = new LinkedBlockingDeque<Event>(transCapacity);

            channelCounter = counter;
        }

        @Override
        protected void doPut(Event event) throws InterruptedException {
            //todo       //增加放入事件计数器
            channelCounter.incrementEventPutAttemptCount();
            //todo       //estimateEventSize计算当前Event body大小
            int eventByteSize = (int) Math.ceil(estimateEventSize(event) / byteCapacitySlotSize);
            //todo * offer若立即可行且不违反容量限制，则将指定的元素插入putList阻塞双端队列中（队尾），
            //       * 并在成功时返回，如果当前没有空间可用，则抛异常回滚事务
            if (!putList.offer(event)) {
                throw new ChannelException(
                        "Put queue for MemoryTransaction of capacity " +
                                putList.size() + " full, consider committing more frequently, " +
                                "increasing capacity or increasing thread count");
            }
            putByteCounter += eventByteSize;
        }

        @Override
        protected Event doTake() throws InterruptedException {
            //todo //将正在从channel中取出的event计数器原子的加一，即增加取出事件计数器
            channelCounter.incrementEventTakeAttemptCount();
            //todo  		 //如果takeList队列没有剩余容量，即当前事务已经消费了最大容量的Event，抛异常
            //todo //takeList队列剩余容量为0
            if (takeList.remainingCapacity() == 0) {
                throw new ChannelException("Take list for MemoryTransaction, capacity " +
                        takeList.size() + " full, consider committing more frequently, " +
                        "increasing capacity, or increasing thread count");
            }
            //todo      //尝试获取一个信号量获取许可，如果可以获取到许可的话，证明queue队列有空间，超时直接返回null
            if (!queueStored.tryAcquire(keepAlive, TimeUnit.SECONDS)) {
                return null;
            }
            Event event;
            synchronized (queueLock) {
                //todo //获取并移除MemoryChannel双端队列表示的队列的头部(也就是队列的第一个元素)，队列为空返回null，
                // 同一时间只能有一个线程访问，加锁同步
                event = queue.poll();
            }
            //todo //因为信号量的保证，Channel Queue不应该返回null，出现了就不正常了
            Preconditions.checkNotNull(event, "Queue.poll returned NULL despite semaphore " +
                    "signalling existence of entry");
            //todo //将取出的event暂存到事务的takeList队列
            takeList.put(event);
            //todo    		/* 计算event的byte大小 */
            int eventByteSize = (int) Math.ceil(estimateEventSize(event) / byteCapacitySlotSize);
            //todo 			//更新takeByteCounter大小
            takeByteCounter += eventByteSize;

            return event;
        }

        @Override
        protected void doCommit() throws InterruptedException {
            //todo //计算改变的Event数量，即取出数量-放入数量；如果放入的多，那么改变的Event数量将是负数
            //    	//如果takeList更小，说明该MemoryChannel放的数据比取的数据要多，所以需要判断该MemoryChannel是否有空间来放
            //todo //takeList.size()可以看成source，putList.size()看成sink
            int remainingChange = takeList.size() - putList.size();
            if (remainingChange < 0) {
                //todo  			//如果remainingChange小于0，则需要获取Channel Queue剩余容量的信号量
                //todo //sink的消费速度慢于source的产生速度
                // 利用bytesRemaining信号量判断是否有足够空间接收putList中的events所占的空间
                // putByteCounter是需要推到channel中的数据大小，bytesRemainingchannel是容量剩余
                // 获取putByteCounter个字节容量信号量，如果失败说明超过字节容量限制了，回滚事务
                if (!bytesRemaining.tryAcquire(putByteCounter, keepAlive, TimeUnit.SECONDS)) {
                    //todo          	//channel 数据大小容量不足，事物不能提交
                    throw new ChannelException("Cannot commit transaction. Byte capacity " +
                            "allocated to store event body " + byteCapacity * byteCapacitySlotSize +
                            "reached. Please increase heap space/byte capacity allocated to " +
                            "the channel as the sinks may not be keeping up with the sources");
                }
                //todo //获取Channel Queue的-remainingChange个信号量用于放入-remainingChange个Event，如果获取不到，则释放putByteCounter个字节容量信号量，并抛出异常回滚事务
                //        //因为source速度快于sink速度，需判断queue是否还有空间接收event
                if (!queueRemaining.tryAcquire(-remainingChange, keepAlive, TimeUnit.SECONDS)) {
                    //todo        	 //remainingChange如果是负数的话，说明source的生产速度，大于sink的消费速度，且这个速度大于channel所能承载的值
                    bytesRemaining.release(putByteCounter);
                    throw new ChannelFullException("Space for commit to queue couldn't be acquired." +
                            " Sinks are likely not keeping up with sources, or the buffer size is too tight");
                }
            }
            //todo //事务期间生产的event
            int puts = putList.size();
            //todo //事务期间等待消费的event
            int takes = takeList.size();
            //todo //如果上述两个信号量都有空间的话，那么把putList中的Event放到该MemoryChannel中的queue中。
            //     	//锁住队列开始，进行数据的流转
            //  操作Channel Queue时一定要锁定queueLock
            synchronized (queueLock) {
                if (puts > 0) {
                    //todo //如果有Event，则循环放入Channel Queue
                    while (!putList.isEmpty()) {
                        if (!queue.offer(putList.removeFirst())) {
                            //todo           		//如果放入Channel Queue失败了，说明信号量控制出问题了，这种情况不应该发生
                            throw new RuntimeException("Queue add failed, this shouldn't be able to happen");
                        }
                    }
                }
                //todo         //以上步骤执行成功，清空事务的putList和takeList
                putList.clear();
                takeList.clear();
            }
            //todo //更新queue大小控制的信号量bytesRemaining
            //		  //释放takeByteCounter个字节容量信号量
            bytesRemaining.release(takeByteCounter);
            //todo 		  //重置字节计数器
            takeByteCounter = 0;
            putByteCounter = 0;
            //todo  			//释放puts个queueStored信号量，这样doTake方法就可以获取数据了
            // 从queueStored释放puts个信号量
            queueStored.release(puts);
            //todo       //释放remainingChange个queueRemaining信号量
            if (remainingChange > 0) {
                queueRemaining.release(remainingChange);
            }
            //todo   		//ChannelCounter一些数据计数
            // 更新成功放入Channel中的events监控指标数据
            if (puts > 0) {
                channelCounter.addToEventPutSuccessCount(puts);
            }
            //todo //更新成功从Channel中取出的events的数量
            if (takes > 0) {
                channelCounter.addToEventTakeSuccessCount(takes);
            }

            channelCounter.setChannelSize(queue.size());
        }

        @Override
        protected void doRollback() {
            //todo     	//获取takeList的大小，然后bytesRemaining中释放
            int takes = takeList.size();
            //todo     	//将takeList中的Event重新放回到queue队列中。
            // 操作Channel Queue时一定锁住queueLock
            synchronized (queueLock) {
                //todo       	//前置条件判断，检查是否有足够容量回滚事务
                Preconditions.checkState(queue.remainingCapacity() >= takeList.size(),
                        "Not enough space in memory channel " +
                                "queue to rollback takes. This should never happen, please report");
                //todo     		//回滚事务的takeList队列到Channel Queue
                //todo //takeList不为空，将其events全部放回queue
                while (!takeList.isEmpty()) {
                    //todo           //removeLast()获取并移除此双端队列的最后一个元素
                    queue.addFirst(takeList.removeLast());
                }
                //todo       	//最后清空putList
                putList.clear();
            }
            //todo     //计数器重置
            putByteCounter = 0;
            takeByteCounter = 0;
            //todo //清空了putList，所以需要把putList占用的空间添加到bytesRemaining中
            //    //即，释放putByteCounter个bytesRemaining信号量
            queueStored.release(takes);
            //todo     //释放takeList队列大小个已存储事件容量
            channelCounter.setChannelSize(queue.size());
        }

    }

    // lock to guard queue, mainly needed to keep it locked down during resizes
    // it should never be held through a blocking operation
    //todo queueLock：创建一个Object当做队列锁，操作队列的时候保证数据的一致性；
    private Object queueLock = new Object();
    //todo queue：使用LinkedBlockingDeque queue维持一个队列，队列的两端分别是source和sink；
    @GuardedBy(value = "queueLock")
    private LinkedBlockingDeque<Event> queue;

    // invariant that tracks the amount of space remaining in the queue(with all uncommitted takeLists deducted)
    // we maintain the remaining permits = queue.remaining - takeList.size()
    // this allows local threads waiting for space in the queue to commit without denying access to the
    // shared lock to threads that would make more space on the queue
    //todo queueRemaining：来保存queue中当前可用的容量，即空闲的容量大小，
    // 可以用来判断当前是否有可以提交一定数量的event到queue中；
    private Semaphore queueRemaining;

    // used to make "reservations" to grab data from the queue.
    // by using this we can block for a while to get data without locking all other threads out
    // like we would if we tried to use a blocking call on queue
    //todo queueStored：来保存queue中当前的保存的event的数目，即已经存储的容量大小，
    // 后面tryAcquire方法可以判断是否可以take到一个event；
    private Semaphore queueStored;

    // maximum items in a transaction queue
    private volatile Integer transCapacity;
    private volatile int keepAlive;
    private volatile int byteCapacity;
    private volatile int lastByteCapacity;
    private volatile int byteCapacityBufferPercentage;
    //todo bytesRemaining ： 表示可以使用的内存大小。该大小就是计算后的byteCapacity值。
    private Semaphore bytesRemaining;
    //todo 其就是把channel的一些属性封装了一下，初始化了一个ChannelCounter，是一个计数器，
    // 记录如当前队列放入Event数、取出Event数、成功数等。
    private ChannelCounter channelCounter;

    public MemoryChannel() {
        super();
    }

    /**
     * Read parameters from context
     * <li>capacity = type long that defines the total number of events allowed at one time in the queue.
     * <li>transactionCapacity = type long that defines the total number of events allowed in one transaction.
     * <li>byteCapacity = type long that defines the max number of bytes used for events in the queue.
     * <li>byteCapacityBufferPercentage = type int that defines the percent of buffer between byteCapacity and the estimated event size.
     * <li>keep-alive = type int that defines the number of second to wait for a queue permit
     */
    @Override
    public void configure(Context context) {
        Integer capacity = null;
        try {
            capacity = context.getInteger("capacity", defaultCapacity);
        } catch (NumberFormatException e) {
            capacity = defaultCapacity;
            LOGGER.warn("Invalid capacity specified, initializing channel to "
                    + "default capacity of {}", defaultCapacity);
        }

        if (capacity <= 0) {
            capacity = defaultCapacity;
            LOGGER.warn("Invalid capacity specified, initializing channel to "
                    + "default capacity of {}", defaultCapacity);
        }
        try {
            transCapacity = context.getInteger("transactionCapacity", defaultTransCapacity);
        } catch (NumberFormatException e) {
            transCapacity = defaultTransCapacity;
            LOGGER.warn("Invalid transation capacity specified, initializing channel"
                    + " to default capacity of {}", defaultTransCapacity);
        }

        if (transCapacity <= 0) {
            transCapacity = defaultTransCapacity;
            LOGGER.warn("Invalid transation capacity specified, initializing channel"
                    + " to default capacity of {}", defaultTransCapacity);
        }
        Preconditions.checkState(transCapacity <= capacity,
                "Transaction Capacity of Memory Channel cannot be higher than " +
                        "the capacity.");

        try {
            byteCapacityBufferPercentage = context.getInteger("byteCapacityBufferPercentage",
                    defaultByteCapacityBufferPercentage);
        } catch (NumberFormatException e) {
            byteCapacityBufferPercentage = defaultByteCapacityBufferPercentage;
        }

        try {
            byteCapacity = (int) ((context.getLong("byteCapacity", defaultByteCapacity).longValue() *
                    (1 - byteCapacityBufferPercentage * .01)) / byteCapacitySlotSize);
            if (byteCapacity < 1) {
                byteCapacity = Integer.MAX_VALUE;
            }
        } catch (NumberFormatException e) {
            byteCapacity = (int) ((defaultByteCapacity * (1 - byteCapacityBufferPercentage * .01)) /
                    byteCapacitySlotSize);
        }

        try {
            keepAlive = context.getInteger("keep-alive", defaultKeepAlive);
        } catch (NumberFormatException e) {
            keepAlive = defaultKeepAlive;
        }

        if (queue != null) {
            try {
                resizeQueue(capacity);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            synchronized (queueLock) {
                queue = new LinkedBlockingDeque<Event>(capacity);
                queueRemaining = new Semaphore(capacity);
                queueStored = new Semaphore(0);
            }
        }

        if (bytesRemaining == null) {
            bytesRemaining = new Semaphore(byteCapacity);
            lastByteCapacity = byteCapacity;
        } else {
            if (byteCapacity > lastByteCapacity) {
                bytesRemaining.release(byteCapacity - lastByteCapacity);
                lastByteCapacity = byteCapacity;
            } else {
                try {
                    if (!bytesRemaining.tryAcquire(lastByteCapacity - byteCapacity, keepAlive,
                            TimeUnit.SECONDS)) {
                        LOGGER.warn("Couldn't acquire permits to downsize the byte capacity, resizing has been aborted");
                    } else {
                        lastByteCapacity = byteCapacity;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        if (channelCounter == null) {
            channelCounter = new ChannelCounter(getName());
        }
    }

    private void resizeQueue(int capacity) throws InterruptedException {
        int oldCapacity;
        //todo //首先计算扩容前的Channel Queue的容量
        //    //计算原本的Channel Queue的容量
        synchronized (queueLock) {
            //todo       //老的容量=队列现有余额+在事务被处理了但是是未被提交的容量
            oldCapacity = queue.size() + queue.remainingCapacity();
        }
        //todo     //新容量和老容量相等，不需要调整返回
        //  如果老容量大于新容量，缩容
        if (oldCapacity == capacity) {
            return;
        } else if (oldCapacity > capacity) {
            //todo 缩容
            // 首先要预占老容量-新容量的大小，以便缩容容量
            // 首先要预占用未被占用的容量，防止其他线程进行操作
            // 尝试占用即将缩减的空间，以防被他人占用
            if (!queueRemaining.tryAcquire(oldCapacity - capacity, keepAlive, TimeUnit.SECONDS)) {
                //todo    //如果获取失败，默认是记录日志然后忽略
                LOGGER.warn("Couldn't acquire permits to downsize the queue, resizing has been aborted");
            } else {
                //todo //直接缩容量
                //        //锁定queueLock进行缩容，先创建新capacity的双端阻塞队列，然后复制老Queue数据。线程安全
                //  //否则，直接缩容，然后复制老Queue的数据，缩容时需要锁定queueLock，因为这一系列操作要线程安全
                synchronized (queueLock) {
                    LinkedBlockingDeque<Event> newQueue = new LinkedBlockingDeque<Event>(capacity);
                    newQueue.addAll(queue);
                    queue = newQueue;
                }
            }
        } else {
            //todo //扩容，加锁，创建新newQueue，复制老queue数据
            //     //扩容
            synchronized (queueLock) {
                LinkedBlockingDeque<Event> newQueue = new LinkedBlockingDeque<Event>(capacity);
                newQueue.addAll(queue);
                queue = newQueue;
            }
            //todo //增加/减少Channel Queue的新的容量
            //      //释放capacity - oldCapacity个许可，即就是增加这么多可用许可
            queueRemaining.release(capacity - oldCapacity);
        }
    }

    @Override
    public synchronized void start() {
        channelCounter.start();
        channelCounter.setChannelSize(queue.size());
        channelCounter.setChannelCapacity(Long.valueOf(
                queue.size() + queue.remainingCapacity()));
        super.start();
    }

    @Override
    public synchronized void stop() {
        channelCounter.setChannelSize(queue.size());
        channelCounter.stop();
        super.stop();
    }

    @Override
    protected BasicTransactionSemantics createTransaction() {
        return new MemoryTransaction(transCapacity, channelCounter);
    }

    private long estimateEventSize(Event event) {
        byte[] body = event.getBody();
        if (body != null && body.length != 0) {
            return body.length;
        }
        //Each event occupies at least 1 slot, so return 1.
        return 1;
    }

    @VisibleForTesting
    int getBytesRemainingValue() {
        return bytesRemaining.availablePermits();
    }

    public long getTransactionCapacity() {
        return transCapacity;
    }
}
