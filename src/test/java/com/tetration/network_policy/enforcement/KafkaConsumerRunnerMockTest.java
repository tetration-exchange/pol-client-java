/*
 * Copyright 2017 Cisco Systems, Inc.
 * Cisco Tetration
 */

package com.tetration.network_policy.enforcement;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.tetration.tetration_network_policy.proto.TetrationNetworkPolicyProto;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.testng.Assert.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit mock test against KafkaConsumerRunner
 */
public class KafkaConsumerRunnerMockTest {
  /**
   * Enum specifies behaviour of calls to KafkaConsumer.poll()
   */
  public enum POLL_MODE {
    /* a call to poll returns all messages belonging to one snapshot */
    ONE_SNAPSHOT_PER_POLL,
    /* a call to poll returns a randomized number of messages
     * this is to verify proper detection of start and end message as well as
     * skip ahead feature, ie previous messages are ignored if there is a
     * pending snapshot
     */
    RANDOM_NUMBER_OF_MSG_PER_POLL,
    /* a call to poll returns all generated snapshots
     * in this case only the last snapshot should be delivered to callback
     * (skip ahead feature)
     */
    ALL_SNAPSHOTS_IN_ONE_POLL
  }
  protected static Logger logger = LoggerFactory.getLogger(KafkaConsumerRunnerMockTest.class);
  protected static Random random = new Random(System.currentTimeMillis());
  protected static int intentId;
  protected static int inventoryFilterId;

  /**
   * Inner helper class to gather received network policies in a list,
   * which is then used for verification
   */
  private class PollerCallback implements KafkaConsumerRunner.PollCallback {
    public List<TetrationNetworkPolicyProto.NetworkPolicy> eventList;
    public Lock lock;
    // cond var is used to signal main thread (test case) about received network policy
    public Condition condition;

    @Override
    public void notify(TetrationNetworkPolicyProto.TenantNetworkPolicy tenantNetworkPolicy) {
      lock.lock();
      // we've got always one network policy only
      eventList.add(tenantNetworkPolicy.getNetworkPolicy(0));
      this.condition.signalAll();
      lock.unlock();
    }

    public PollerCallback() {
      this.eventList = new LinkedList<>();
      this.lock = new ReentrantLock();
      this.condition = this.lock.newCondition();
    }
  }

  /**
   * Test start and stop a KafkaConsumerRunner object
   */
  @Test()
  public void testStartStop() {
    KafkaConsumerFactory kafkaConsumerFactoryMock = mock(KafkaConsumerFactory.class);
    assertNotEquals(null, kafkaConsumerFactoryMock);
    KafkaConsumerRunner.kafkaConsumerFactory = kafkaConsumerFactoryMock;

    KafkaConsumer<String, byte[]> kafkaConsumerMock = mock(KafkaConsumer.class);
    assertNotEquals(null, kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.createInstance(any(Properties.class))).thenReturn(kafkaConsumerMock);

    final String topicName = "topic-name";
    when(kafkaConsumerFactoryMock.getTopicName(any(Properties.class))).thenReturn(topicName);

    Properties configProps = new Properties();
    configProps.put("tetration.user.credential", "we're mocking");

    PartitionInfo partitionInfo = new PartitionInfo(topicName, 1, null, null, null);
    when(kafkaConsumerMock.partitionsFor(topicName)).thenReturn(Arrays.asList(partitionInfo));
    when(kafkaConsumerMock.poll(anyLong())).thenAnswer(
      new Answer() {
        public ConsumerRecords<String, byte[]> answer(InvocationOnMock invocation) {
          try {
            Thread.sleep(50);
          } catch (InterruptedException e) {
            logger.error(e.getMessage());
            System.exit(-1);
          }
          // always return empty list of messages
          Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> consumerRecordsData = new HashMap<>();
          return new ConsumerRecords<String, byte[]>(consumerRecordsData);
        }
      }
    );

    try {
      PollerCallback callback = new PollerCallback();
      KafkaConsumerRunner runner = new KafkaConsumerRunner(configProps, callback);
      assertNotEquals(null, runner);
      Thread.sleep(100);
      assertEquals(0, callback.eventList.size());
      assertEquals(runner.isConsumerThreadAlive(), true);
      runner.stopPoller();
      assertEquals(runner.isConsumerThreadAlive(), false);
    } catch (Exception e) {
      logger.error(e.getMessage());
      System.exit(-1);
    }
  }

  /**
   * Helper method to create a TenantNetworkPolicy based on protobuf
   * The returned network policy has a random number of Intents and InventoryFilters
   * @return TetrationNetworkPolicyProto.TenantNetworkPolicy
   */
  private TetrationNetworkPolicyProto.TenantNetworkPolicy createTenantNetworkPolicy() {
    TetrationNetworkPolicyProto.NetworkPolicy.Builder networkPolicyBuilder = TetrationNetworkPolicyProto.NetworkPolicy.newBuilder();
    for (int k = 0; k < random.nextInt(9); ++k) {
      TetrationNetworkPolicyProto.Intent.Builder intenBuilder = TetrationNetworkPolicyProto.Intent.newBuilder();
      intenBuilder.setId(Integer.toString(intentId));
      networkPolicyBuilder.addIntents(intenBuilder);
      ++intentId;
    }
    for (int k = 0; k < random.nextInt(7); ++k) {
      TetrationNetworkPolicyProto.InventoryGroup.Builder inventoryBuilder = TetrationNetworkPolicyProto.InventoryGroup.newBuilder();
      inventoryBuilder.setId(Integer.toString(inventoryFilterId));
      networkPolicyBuilder.addInventoryFilters(inventoryBuilder);
      ++inventoryFilterId;
    }
    TetrationNetworkPolicyProto.TenantNetworkPolicy.Builder tenantNetworkPolicyBuilder = TetrationNetworkPolicyProto.TenantNetworkPolicy.newBuilder();
    tenantNetworkPolicyBuilder.addNetworkPolicy(networkPolicyBuilder);
    return tenantNetworkPolicyBuilder.build();
  }

  /**
   * Inner helper class to mock KafkaConsumer.poll()
   */
  private class PollAnswer implements Answer<ConsumerRecords<String, byte[]>> {
    private List<TetrationNetworkPolicyProto.KafkaUpdate> kafkaUpdates;
    private Iterator<TetrationNetworkPolicyProto.KafkaUpdate> kafkaUpdateIterator;
    private int kafkaOffset;
    private String topicName;
    private int partition;
    private POLL_MODE pollMode;
    private List<Long> snapshotVersion2ReceiveList;
    private Semaphore semaSendComplete;

    /**
     * Construct a PollAnswer object with list of generated KafkaUpdate objects
     * (messages). The pollMode parameter specifies the pattern of returning list
     * of messages, see POLL_MODE for details.
     * @param kafkaUpdates
     * @param topicName
     * @param partition
     * @param pollMode
     */
    public PollAnswer(List<TetrationNetworkPolicyProto.KafkaUpdate> kafkaUpdates,
                      String topicName, int partition, POLL_MODE pollMode) {
      this.kafkaUpdates = kafkaUpdates;
      this.kafkaUpdateIterator = kafkaUpdates.iterator();
      this.kafkaOffset = 0;
      this.topicName = topicName;
      this.partition = partition;
      this.pollMode = pollMode;
      /* once all generated KafkaUpdate objects have been returned, this list
       * contains the versions of snapshots to be delivered to callback
       * note that there might be broken snapshot, ie missing end message
       */
      this.snapshotVersion2ReceiveList = new ArrayList<>();
      /* this semaphore is used to block main thread (test case) until
       * all KafkaUpdate objects have been returned to caller of poll()
       */
      this.semaSendComplete = new Semaphore(1);
      try {
        this.semaSendComplete.acquire();
      } catch (InterruptedException e) {
        logger.error(e.getMessage());
        System.exit(-1);
      }
    }

    /**
     * This is the mocked method of KafkaConsumer.poll()
     * @param invocation
     * @return ConsumerRecords<String, byte[]>
     */
    public ConsumerRecords<String, byte[]> answer(InvocationOnMock invocation) {
      Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> consumerRecordsData = new HashMap<>();
      if (this.kafkaUpdateIterator.hasNext()) {
        List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
        int maxNumOfMsg = Integer.MAX_VALUE;
        if (this.pollMode == POLL_MODE.RANDOM_NUMBER_OF_MSG_PER_POLL) {
          maxNumOfMsg = random.nextInt(this.kafkaUpdates.size() - kafkaOffset);
          if (maxNumOfMsg == 0) {
            maxNumOfMsg = 1;
          }
        }
        int msgCount = 0;
        TetrationNetworkPolicyProto.KafkaUpdate kafkaUpdate = null;
        while (msgCount < maxNumOfMsg && this.kafkaUpdateIterator.hasNext()) {
          kafkaUpdate = this.kafkaUpdateIterator.next();
          ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(this.topicName, this.partition,
              this.kafkaOffset, "somekey", kafkaUpdate.toByteArray());
          records.add(record);
          ++this.kafkaOffset;
          ++msgCount;
          if (this.pollMode == POLL_MODE.ONE_SNAPSHOT_PER_POLL &&
              kafkaUpdate.getTypeValue() == TetrationNetworkPolicyProto.KafkaUpdate.UpdateType.UPDATE_END_VALUE) {
            break;
          }
        }
        if (kafkaUpdate.getTypeValue() == TetrationNetworkPolicyProto.KafkaUpdate.UpdateType.UPDATE_END_VALUE) {
          this.snapshotVersion2ReceiveList.add(kafkaUpdate.getVersion());
        }
        TopicPartition topicPartition = new TopicPartition(this.topicName, this.partition);
        consumerRecordsData.put(topicPartition, records);
      }
      if (!consumerRecordsData.isEmpty() && this.kafkaOffset == this.kafkaUpdates.size()) {
        this.semaSendComplete.release();
      }
      return new ConsumerRecords<String, byte[]>(consumerRecordsData);
    }

  }

  /**
   * Helper method to check two network policies on equality
   * @param expectedNetworkPolicy
   * @param receivedNetworkPolicy
   */
  private void assertEqualsNetworkPolicy(TetrationNetworkPolicyProto.NetworkPolicy expectedNetworkPolicy, TetrationNetworkPolicyProto.NetworkPolicy receivedNetworkPolicy) {
    assertEquals(expectedNetworkPolicy.getIntentsCount(), receivedNetworkPolicy.getIntentsCount());
    for (int i = 0; i < expectedNetworkPolicy.getIntentsCount(); ++i) {
      TetrationNetworkPolicyProto.Intent expectedIntent = expectedNetworkPolicy.getIntents(i);
      TetrationNetworkPolicyProto.Intent receivedIntent = receivedNetworkPolicy.getIntents(i);
      assertEquals(expectedIntent.getId(), receivedIntent.getId());
    }
    assertEquals(expectedNetworkPolicy.getInventoryFiltersCount(), receivedNetworkPolicy.getInventoryFiltersCount());
    for (int i = 0; i < expectedNetworkPolicy.getInventoryFiltersCount(); ++i) {
      TetrationNetworkPolicyProto.InventoryGroup expectedInventoryFilter = expectedNetworkPolicy.getInventoryFilters(i);
      TetrationNetworkPolicyProto.InventoryGroup receivedInventoryFilter = receivedNetworkPolicy.getInventoryFilters(i);
      assertEquals(expectedInventoryFilter.getId(), receivedInventoryFilter.getId());
    }
  }

  /**
   * Generate combinations of parameters for testReceiveSnapshot
   * @return Object[][]
   */
  @DataProvider(name="receiveSnapshotParameters")
  private Object[][] generateReceiveSnapshotParameters() {
    List<Object[]> result = new ArrayList<>();
    final int numberOfSnapshots[] = new int[] {1, 10, 100};
    final int numberOfMessages[] = new int[] {2, 5, 10}; // -1 means random number
    for (int i = 0; i < numberOfSnapshots.length; ++i) {
      for (int j = 0; j < numberOfMessages.length; ++j) {
        for (POLL_MODE pollMode: POLL_MODE.values()) {
          result.add(new Object[]{
              Integer.valueOf(numberOfSnapshots[i]),
              Integer.valueOf(numberOfMessages[j]),
              pollMode,
              Boolean.FALSE
          });
        } // eof pollMode
      } // eof j
    } // eof i
    result.add(new Object[]{
        Integer.valueOf(1),
        Integer.valueOf(2),
        POLL_MODE.ONE_SNAPSHOT_PER_POLL,
        Boolean.TRUE //injectIncompleteSnapshot
    });
    return result.toArray(new Object[result.size()][]);
  }

  /**
   * Test receiving snapshots of network policies
   * @param numberOfSnapshots
   * @param numberOfMessages
   * @param pollMode
   * @param injectIncompleteSnapshot
   */
  @Test(dataProvider = "receiveSnapshotParameters")
  public void testReceiveSnapshot(int numberOfSnapshots, int numberOfMessages,
                                  POLL_MODE pollMode, boolean injectIncompleteSnapshot) {
    assertTrue(numberOfSnapshots > 0);
    if (numberOfMessages != -1) {
      assertTrue(numberOfMessages >= 2);
    }
    KafkaConsumerFactory kafkaConsumerFactoryMock = mock(KafkaConsumerFactory.class);
    assertNotEquals(null, kafkaConsumerFactoryMock);
    KafkaConsumerRunner.kafkaConsumerFactory = kafkaConsumerFactoryMock;

    KafkaConsumer<String, byte[]> kafkaConsumerMock = mock(KafkaConsumer.class);
    assertNotEquals(null, kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.createInstance(any(Properties.class))).thenReturn(kafkaConsumerMock);

    final String topicName = "topic-name";
    when(kafkaConsumerFactoryMock.getTopicName(any(Properties.class))).thenReturn(topicName);

    Properties configProps = new Properties();
    configProps.put("tetration.user.credential", "we're mocking");

    PartitionInfo partitionInfo = new PartitionInfo(topicName, 1, null, null, null);
    when(kafkaConsumerMock.partitionsFor(topicName)).thenReturn(Arrays.asList(partitionInfo));

    // generate kafka data
    List<TetrationNetworkPolicyProto.KafkaUpdate> kafkaUpdates = new ArrayList<>();
    intentId = 0;
    inventoryFilterId = 0;
    final int versionStartNr = 100;
    int versionNr = versionStartNr;
    for (int i = 0; i < numberOfSnapshots; ) {
      int seqNum;
      TetrationNetworkPolicyProto.KafkaUpdate.Builder kafkaUpdateBuilder = TetrationNetworkPolicyProto.KafkaUpdate.newBuilder();
      kafkaUpdateBuilder.setSequenceNum(0);
      kafkaUpdateBuilder.setType(TetrationNetworkPolicyProto.KafkaUpdate.UpdateType.UPDATE_START);
      kafkaUpdateBuilder.setVersion(versionNr);
      kafkaUpdateBuilder.setTenantNetworkPolicy(createTenantNetworkPolicy());
      kafkaUpdates.add(kafkaUpdateBuilder.build());
      int nomForSnapshot;
      if (numberOfMessages == -1) {
        nomForSnapshot = random.nextInt(100) + 1;
      } else {
        nomForSnapshot = numberOfMessages;
      }
      for (seqNum = 1; seqNum < nomForSnapshot - 1; ++seqNum) {
        kafkaUpdateBuilder = TetrationNetworkPolicyProto.KafkaUpdate.newBuilder();
        kafkaUpdateBuilder.setSequenceNum(seqNum);
        kafkaUpdateBuilder.setType(TetrationNetworkPolicyProto.KafkaUpdate.UpdateType.UPDATE);
        kafkaUpdateBuilder.setVersion(versionNr);
        kafkaUpdateBuilder.setTenantNetworkPolicy(createTenantNetworkPolicy());
        kafkaUpdates.add(kafkaUpdateBuilder.build());
      }
      if (injectIncompleteSnapshot && random.nextInt(13) == 1) {
        ++versionNr;
        continue;
      }
      kafkaUpdateBuilder = TetrationNetworkPolicyProto.KafkaUpdate.newBuilder();
      kafkaUpdateBuilder.setSequenceNum(seqNum);
      kafkaUpdateBuilder.setType(TetrationNetworkPolicyProto.KafkaUpdate.UpdateType.UPDATE_END);
      kafkaUpdateBuilder.setVersion(versionNr);
      kafkaUpdates.add(kafkaUpdateBuilder.build());
      ++versionNr;
      ++i;
    }

    PollAnswer pollAnswer = new PollAnswer(kafkaUpdates, topicName, partitionInfo.partition(), pollMode);
    when(kafkaConsumerMock.poll(anyLong())).thenAnswer(pollAnswer);

    try {
      // next two lines are consumer's code (to be verified)
      PollerCallback callback = new PollerCallback();
      KafkaConsumerRunner runner = new KafkaConsumerRunner(configProps, callback);
      assertNotEquals(null, runner);
      // wait for send completion of all generated KafkaUpdate objects
      pollAnswer.semaSendComplete.acquire();
      // loop waits until we receive all expected snapshots
      callback.lock.lock();
      while (callback.eventList.size() != pollAnswer.snapshotVersion2ReceiveList.size()) {
        callback.condition.await(5, TimeUnit.SECONDS);
      }
      callback.lock.unlock();
      assertEquals(pollAnswer.snapshotVersion2ReceiveList.size(), callback.eventList.size());
      runner.stopPoller();

      /* verify received data:
       * 1) iterate kafkaUpdates and gather intents and inventoryFilters into snapshots
       * 2) compare expected snapshots against received snapshots
       */
      Map<Long, TetrationNetworkPolicyProto.NetworkPolicy> networkPolicies = new HashMap<>();
      TetrationNetworkPolicyProto.NetworkPolicy.Builder networkPolicy = null;
      for (TetrationNetworkPolicyProto.KafkaUpdate kafkaUpdate: kafkaUpdates) {
        TetrationNetworkPolicyProto.NetworkPolicy srcNetworkPolicy;
        switch (kafkaUpdate.getTypeValue()) {
          case TetrationNetworkPolicyProto.KafkaUpdate.UpdateType.UPDATE_START_VALUE:
            networkPolicy = TetrationNetworkPolicyProto.NetworkPolicy.newBuilder();
          case TetrationNetworkPolicyProto.KafkaUpdate.UpdateType.UPDATE_VALUE:
            assertNotEquals(null, networkPolicy);
            srcNetworkPolicy = kafkaUpdate.getTenantNetworkPolicy().getNetworkPolicy(0);
            networkPolicy.addAllIntents(srcNetworkPolicy.getIntentsList());
            networkPolicy.addAllInventoryFilters(srcNetworkPolicy.getInventoryFiltersList());
            break;
          case TetrationNetworkPolicyProto.KafkaUpdate.UpdateType.UPDATE_END_VALUE:
            assertNotEquals(null, networkPolicy);
            networkPolicies.put(kafkaUpdate.getVersion(), networkPolicy.build());
            networkPolicy = null;
            break;
        }
      }
      assertEquals(networkPolicies.size(), numberOfSnapshots);
      Iterator<Long> versionIterator = pollAnswer.snapshotVersion2ReceiveList.iterator();
      for (TetrationNetworkPolicyProto.NetworkPolicy receivedNetworkPolicy: callback.eventList) {
        Long expectedVersionNr = versionIterator.next();
        TetrationNetworkPolicyProto.NetworkPolicy expectedNetworkPolicy = networkPolicies.get(expectedVersionNr);
        this.assertEqualsNetworkPolicy(expectedNetworkPolicy, receivedNetworkPolicy);
      }
      assertEquals(false, versionIterator.hasNext());
    } catch (Exception e) {
      logger.error(e.getMessage());
      System.exit(-1);
    }
  }

  @Test
  public void testKafkaRecoveryWithOOO() {
    KafkaConsumerFactory kafkaConsumerFactoryMock = mock(KafkaConsumerFactory.class);
    assertNotEquals(null, kafkaConsumerFactoryMock);
    KafkaConsumerRunner.kafkaConsumerFactory = kafkaConsumerFactoryMock;

    KafkaConsumer<String, byte[]> kafkaConsumerMock = mock(KafkaConsumer.class);
    assertNotEquals(null, kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.createInstance(any(Properties.class))).thenReturn(kafkaConsumerMock);

    final String topicName = "topic-name";
    when(kafkaConsumerFactoryMock.getTopicName(any(Properties.class))).thenReturn(topicName);

    Properties configProps = new Properties();
    configProps.put("tetration.user.credential", "we're mocking");

    PartitionInfo partitionInfo = new PartitionInfo(topicName, 1, null, null, null);
    when(kafkaConsumerMock.partitionsFor(topicName)).thenReturn(Arrays.asList(partitionInfo));
    when(kafkaConsumerMock.poll(anyLong())).thenThrow(new OutOfMemoryError("This should stop kafka runner"));

    PollerCallback callback = new PollerCallback();
    KafkaConsumerRunner runner = new KafkaConsumerRunner(configProps, callback);
    assertNotEquals(null, runner);

    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertEquals(runner.isConsumerThreadAlive(), false);
  }

  @Test
  public void testKafkaRecoveryWithPollExceptions() {
    KafkaConsumerFactory kafkaConsumerFactoryMock = mock(KafkaConsumerFactory.class);
    assertNotEquals(null, kafkaConsumerFactoryMock);
    KafkaConsumerRunner.kafkaConsumerFactory = kafkaConsumerFactoryMock;

    KafkaConsumer<String, byte[]> kafkaConsumerMock = mock(KafkaConsumer.class);
    assertNotEquals(null, kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.createInstance(any(Properties.class))).thenReturn(kafkaConsumerMock);

    final String topicName = "topic-name";
    when(kafkaConsumerFactoryMock.getTopicName(any(Properties.class))).thenReturn(topicName);

    Properties configProps = new Properties();
    configProps.put("tetration.user.credential", "we're mocking");

    PartitionInfo partitionInfo = new PartitionInfo(topicName, 1, null, null, null);
    when(kafkaConsumerMock.partitionsFor(topicName)).thenReturn(Arrays.asList(partitionInfo));
    when(kafkaConsumerMock.poll(anyLong())).thenThrow(new IllegalStateException("This should trigger recovery 1")).
        thenThrow(new IllegalStateException("This should trigger recovery 2")).
        thenThrow(new IllegalStateException("This should trigger recovery 3")).
        thenThrow(new IllegalStateException("This should trigger recovery 4")).
        thenThrow(new IllegalStateException("This should stop kafka runner"));

    PollerCallback callback = new PollerCallback();
    KafkaConsumerRunner runner = new KafkaConsumerRunner(configProps, callback);
    assertNotEquals(null, runner);

    try {
      Thread.sleep(6000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertEquals(runner.isConsumerThreadAlive(), false);
  }

  @Test
  public void testKafkaRecoveryWithCreateExceptions() {
    KafkaConsumerFactory kafkaConsumerFactoryMock = mock(KafkaConsumerFactory.class);
    assertNotEquals(null, kafkaConsumerFactoryMock);
    KafkaConsumerRunner.kafkaConsumerFactory = kafkaConsumerFactoryMock;

    KafkaConsumer<String, byte[]> kafkaConsumerMock = mock(KafkaConsumer.class);
    assertNotEquals(null, kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.createInstance(any(Properties.class))).
        thenReturn(kafkaConsumerMock).
        thenThrow(new IllegalStateException("This should trigger recovery 1")).
        thenThrow(new IllegalStateException("This should trigger recovery 2")).
        thenThrow(new IllegalStateException("This should trigger recovery 3")).
        thenThrow(new IllegalStateException("This should stop kafka runner"));

    final String topicName = "topic-name";
    when(kafkaConsumerFactoryMock.getTopicName(any(Properties.class))).thenReturn(topicName);

    Properties configProps = new Properties();
    configProps.put("tetration.user.credential", "we're mocking");

    PartitionInfo partitionInfo = new PartitionInfo(topicName, 1, null, null, null);
    when(kafkaConsumerMock.partitionsFor(topicName)).thenReturn(Arrays.asList(partitionInfo));
    when(kafkaConsumerMock.poll(anyLong())).thenThrow(new IllegalStateException("This should trigger recovery"));

    PollerCallback callback = new PollerCallback();
    KafkaConsumerRunner runner = new KafkaConsumerRunner(configProps, callback);
    assertNotEquals(null, runner);

    try {
      Thread.sleep(6000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertEquals(runner.isConsumerThreadAlive(), false);
  }

  @Test()
  public void testKafkaRecovery() throws InterruptedException {
    KafkaConsumerFactory kafkaConsumerFactoryMock = mock(KafkaConsumerFactory.class);
    assertNotEquals(null, kafkaConsumerFactoryMock);
    KafkaConsumerRunner.kafkaConsumerFactory = kafkaConsumerFactoryMock;

    KafkaConsumer<String, byte[]> kafkaConsumerMock = mock(KafkaConsumer.class);
    assertNotEquals(null, kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.createInstance(any(Properties.class))).thenReturn(kafkaConsumerMock);

    final String topicName = "topic-name";
    when(kafkaConsumerFactoryMock.getTopicName(any(Properties.class))).thenReturn(topicName);

    Properties configProps = new Properties();
    configProps.put("tetration.user.credential", "we're mocking");

    PartitionInfo partitionInfo = new PartitionInfo(topicName, 1, null, null, null);
    when(kafkaConsumerMock.partitionsFor(topicName)).thenReturn(Arrays.asList(partitionInfo));

    // generate kafka data
    List<TetrationNetworkPolicyProto.KafkaUpdate> kafkaUpdates = new ArrayList<>();
    intentId = 0;
    inventoryFilterId = 0;
    final int versionStartNr = 100;
    int versionNr = versionStartNr;
    for (int i = 0; i < 5; ) {
      int seqNum;
      TetrationNetworkPolicyProto.KafkaUpdate.Builder kafkaUpdateBuilder = TetrationNetworkPolicyProto.KafkaUpdate.newBuilder();
      kafkaUpdateBuilder.setSequenceNum(0);
      kafkaUpdateBuilder.setType(TetrationNetworkPolicyProto.KafkaUpdate.UpdateType.UPDATE_START);
      kafkaUpdateBuilder.setVersion(versionNr);
      kafkaUpdateBuilder.setTenantNetworkPolicy(createTenantNetworkPolicy());
      kafkaUpdates.add(kafkaUpdateBuilder.build());
      for (seqNum = 1; seqNum < 4; ++seqNum) {
        kafkaUpdateBuilder = TetrationNetworkPolicyProto.KafkaUpdate.newBuilder();
        kafkaUpdateBuilder.setSequenceNum(seqNum);
        kafkaUpdateBuilder.setType(TetrationNetworkPolicyProto.KafkaUpdate.UpdateType.UPDATE);
        kafkaUpdateBuilder.setVersion(versionNr);
        kafkaUpdateBuilder.setTenantNetworkPolicy(createTenantNetworkPolicy());
        kafkaUpdates.add(kafkaUpdateBuilder.build());
      }
      kafkaUpdateBuilder = TetrationNetworkPolicyProto.KafkaUpdate.newBuilder();
      kafkaUpdateBuilder.setSequenceNum(seqNum);
      kafkaUpdateBuilder.setType(TetrationNetworkPolicyProto.KafkaUpdate.UpdateType.UPDATE_END);
      kafkaUpdateBuilder.setVersion(versionNr);
      kafkaUpdates.add(kafkaUpdateBuilder.build());
      ++versionNr;
      ++i;
    }

    PollAnswer pollAnswer = new PollAnswer(kafkaUpdates, topicName, partitionInfo.partition(),
        POLL_MODE.ONE_SNAPSHOT_PER_POLL);
    when(kafkaConsumerMock.poll(anyLong())).
        thenThrow(new IllegalStateException("This should trigger recovery")).
        thenAnswer(pollAnswer);

    PollerCallback callback = new PollerCallback();
    KafkaConsumerRunner runner = new KafkaConsumerRunner(configProps, callback);
    assertNotEquals(null, runner);

    // wait for send completion of all generated KafkaUpdate objects
    pollAnswer.semaSendComplete.acquire();
    // loop waits until we receive all expected snapshots
    callback.lock.lock();
    while (callback.eventList.size() != pollAnswer.snapshotVersion2ReceiveList.size()) {
      callback.condition.await(5, TimeUnit.SECONDS);
    }
    callback.lock.unlock();
    assertEquals(pollAnswer.snapshotVersion2ReceiveList.size(), callback.eventList.size());
    assertEquals(runner.isConsumerThreadAlive(), true);
    runner.stopPoller();
    assertEquals(runner.isConsumerThreadAlive(), false);
  }

  @Test()
  public void testBrokenSequenceNr() throws InterruptedException {
    KafkaConsumerFactory kafkaConsumerFactoryMock = mock(KafkaConsumerFactory.class);
    assertNotEquals(null, kafkaConsumerFactoryMock);
    KafkaConsumerRunner.kafkaConsumerFactory = kafkaConsumerFactoryMock;

    KafkaConsumer<String, byte[]> kafkaConsumerMock = mock(KafkaConsumer.class);
    assertNotEquals(null, kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.createInstance(any(Properties.class))).thenReturn(kafkaConsumerMock);

    final String topicName = "topic-name";
    when(kafkaConsumerFactoryMock.getTopicName(any(Properties.class))).thenReturn(topicName);

    Properties configProps = new Properties();
    configProps.put("tetration.user.credential", "we're mocking");

    PartitionInfo partitionInfo = new PartitionInfo(topicName, 1, null, null, null);
    when(kafkaConsumerMock.partitionsFor(topicName)).thenReturn(Arrays.asList(partitionInfo));

    // generate kafka data
    List<TetrationNetworkPolicyProto.KafkaUpdate> kafkaUpdates = new ArrayList<>();
    intentId = 0;
    inventoryFilterId = 0;
    final int versionStartNr = 100;
    int versionNr = versionStartNr;
    for (int i = 0; i < 10; ) {
      int seqNum;
      TetrationNetworkPolicyProto.KafkaUpdate.Builder kafkaUpdateBuilder = TetrationNetworkPolicyProto.KafkaUpdate.newBuilder();
      if (i == 3) {
        kafkaUpdateBuilder.setSequenceNum(2); // inject wrong start seq nr
      } else {
        kafkaUpdateBuilder.setSequenceNum(0);
      }
      kafkaUpdateBuilder.setType(TetrationNetworkPolicyProto.KafkaUpdate.UpdateType.UPDATE_START);
      kafkaUpdateBuilder.setVersion(versionNr);
      kafkaUpdateBuilder.setTenantNetworkPolicy(createTenantNetworkPolicy());
      kafkaUpdates.add(kafkaUpdateBuilder.build());
      for (seqNum = 1; seqNum < 4; ++seqNum) {
        kafkaUpdateBuilder = TetrationNetworkPolicyProto.KafkaUpdate.newBuilder();
        if (i == 7 && seqNum == 2) {
          kafkaUpdateBuilder.setSequenceNum(seqNum + 1); // inject wrong seq nr in the middle
        } else {
          kafkaUpdateBuilder.setSequenceNum(seqNum);
        }
        kafkaUpdateBuilder.setType(TetrationNetworkPolicyProto.KafkaUpdate.UpdateType.UPDATE);
        kafkaUpdateBuilder.setVersion(versionNr);
        kafkaUpdateBuilder.setTenantNetworkPolicy(createTenantNetworkPolicy());
        kafkaUpdates.add(kafkaUpdateBuilder.build());
      }
      kafkaUpdateBuilder = TetrationNetworkPolicyProto.KafkaUpdate.newBuilder();
      if (i == 9) {
        kafkaUpdateBuilder.setSequenceNum(seqNum + 1); // inject wrong end seq nr
      } else {
        kafkaUpdateBuilder.setSequenceNum(seqNum);
      }
      kafkaUpdateBuilder.setType(TetrationNetworkPolicyProto.KafkaUpdate.UpdateType.UPDATE_END);
      kafkaUpdateBuilder.setVersion(versionNr);
      kafkaUpdates.add(kafkaUpdateBuilder.build());
      ++versionNr;
      ++i;
    }

    PollAnswer pollAnswer = new PollAnswer(kafkaUpdates, topicName, partitionInfo.partition(),
        POLL_MODE.ONE_SNAPSHOT_PER_POLL);
    when(kafkaConsumerMock.poll(anyLong())).
        thenAnswer(pollAnswer);

    PollerCallback callback = new PollerCallback();
    KafkaConsumerRunner runner = new KafkaConsumerRunner(configProps, callback);
    assertNotEquals(null, runner);

    // wait for send completion of all generated KafkaUpdate objects
    pollAnswer.semaSendComplete.acquire();
    // loop waits until we receive all expected snapshots
    callback.lock.lock();
    final int numOfInjects = 3;
    while (callback.eventList.size() != pollAnswer.snapshotVersion2ReceiveList.size() - numOfInjects) {
      callback.condition.await(5, TimeUnit.SECONDS);
    }
    callback.lock.unlock();
    Thread.sleep(1000);
    assertEquals(pollAnswer.snapshotVersion2ReceiveList.size() - numOfInjects, callback.eventList.size());
    assertEquals(runner.isConsumerThreadAlive(), true);
    runner.stopPoller();
    assertEquals(runner.isConsumerThreadAlive(), false);
  }
}
