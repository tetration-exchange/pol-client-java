/*
 * Copyright 2017 Cisco Systems, Inc.
 * Cisco Tetration
 */

package com.tetration.network_policy.enforcement;

import com.google.protobuf.ByteString;
import com.tetration.tetration_network_policy.proto.TetrationNetworkPolicyProto;
import com.tetration.tetration_network_policy.proto.TetrationNetworkPolicyProto.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

/**
 * Unit mock test against PolicyEnforcementClient
 * Since receiving and hanlding of kafka messages regarding network policies
 * are tested by KafkaConsumerRunnerMockTest, this UT focuses on verification
 * of delta computation of network policies only
 */
public class PolicyEnforcementClientMockTest {
  protected static Logger logger = LoggerFactory.getLogger(PolicyEnforcementClientMockTest.class);

  private static String MSG_KEY = "676767";
  private Properties configProperties;
  private String topicName;
  private Integer partitionId;

  /**
   * Helper class implements EventCallback to collect received events (notifications)
   * in a list, which then can be used for verification at the end
   */
  private class EventCallback implements PolicyEnforcementClient.EventCallback {
    public List<PolicyEnforcementClient.Event> eventList;
    /* condition var is used to signal main thread (test case) when an expected
     * number of events have been received
     */
    public Lock lock;
    public Condition condition;
    public int expectedEventCount;

    @Override
    public void notify(PolicyEnforcementClient.Event event) {
      logger.info("Received event from " + event.getClass().getName());
      lock.lock();
      eventList.add(event);
      if (eventList.size() == this.expectedEventCount) {
        this.condition.signalAll();
        logger.info("Condition var signaled for " + this.eventList.size() + " events");
      }
      lock.unlock();
    }

    public EventCallback(int anExpectedEventCount) {
      this.eventList = new LinkedList<>();
      this.lock = new ReentrantLock();
      this.condition = this.lock.newCondition();
      this.expectedEventCount = anExpectedEventCount;
    }
  }

  /**
   * Inner helper class to mock KafkaConsumer.poll()
   * This implementation returns a snapshot of network policies per one
   * call to KafkaConsumer.poll(). No need to vary this behaviour as
   * it's already tested by KafkaConsumerRunnerMockTest
   */
  private class PollAnswer implements Answer<ConsumerRecords<String, byte[]>> {
    private KafkaUpdate[] kafkaUpdates;
    private int kafkaOffset;
    private ConsumerRecords<String, byte[]> emptyConsumerRecords;

    public PollAnswer(KafkaUpdate[] kafkaUpdates) {
      this.kafkaUpdates = kafkaUpdates;
      this.kafkaOffset = 0;
      Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> consumerRecordsData = new HashMap<>();
      this.emptyConsumerRecords = new ConsumerRecords<>(consumerRecordsData);
    }

    public ConsumerRecords<String, byte[]> answer(InvocationOnMock invocation) {
      if (this.kafkaOffset == this.kafkaUpdates.length) {
        return this.emptyConsumerRecords;
      }
      Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> consumerRecordsData = new HashMap<>();
      List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
      while (this.kafkaOffset < this.kafkaUpdates.length) {
        KafkaUpdate kafkaUpdate = this.kafkaUpdates[this.kafkaOffset];
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(PolicyEnforcementClientMockTest.this.topicName,
            PolicyEnforcementClientMockTest.this.partitionId,
            this.kafkaOffset, "somekey", kafkaUpdate.toByteArray());
        records.add(record);
        ++this.kafkaOffset;
        if (kafkaUpdate.getTypeValue() == KafkaUpdate.UpdateType.UPDATE_END_VALUE) {
          break;
        }
      }
      if (!records.isEmpty()) {
        TopicPartition topicPartition = new TopicPartition(PolicyEnforcementClientMockTest.this.topicName,
            PolicyEnforcementClientMockTest.this.partitionId);
        consumerRecordsData.put(topicPartition, records);
      }
      return new ConsumerRecords<String, byte[]>(consumerRecordsData);
    }
  }

  private static InventoryItem createInventoryItem(InetAddress inetAddress, int prefixLength) {
    TetrationNetworkPolicyProto.AddressWithPrefix.Builder addressBuilder =
        TetrationNetworkPolicyProto.AddressWithPrefix.newBuilder();
    if (inetAddress instanceof Inet4Address) {
      addressBuilder.setPrefixLength(32);
      addressBuilder.setAddrFamily(TetrationNetworkPolicyProto.IPAddressFamily.IPv4);
    } else {
      addressBuilder.setPrefixLength(128);
      addressBuilder.setAddrFamily(TetrationNetworkPolicyProto.IPAddressFamily.IPv6);
    }
    addressBuilder.setIpAddr(ByteString.copyFrom(inetAddress.getAddress()));
    TetrationNetworkPolicyProto.InventoryItem.Builder inventoryItemBuilder =
        TetrationNetworkPolicyProto.InventoryItem.newBuilder();
    inventoryItemBuilder.setIpAddress(addressBuilder);
    return inventoryItemBuilder.build();
  }

  private static InventoryItem createInventoryItem(InetAddress inetAddress, InetAddress endInetAddress) {
    TetrationNetworkPolicyProto.AddressWithRange.Builder addressBuilder =
        TetrationNetworkPolicyProto.AddressWithRange.newBuilder();
    if (inetAddress instanceof Inet4Address) {
      addressBuilder.setAddrFamily(TetrationNetworkPolicyProto.IPAddressFamily.IPv4);
    } else {
      addressBuilder.setAddrFamily(TetrationNetworkPolicyProto.IPAddressFamily.IPv6);
    }
    addressBuilder.setStartIpAddr(ByteString.copyFrom(inetAddress.getAddress()));
    addressBuilder.setEndIpAddr(ByteString.copyFrom(endInetAddress.getAddress()));
    TetrationNetworkPolicyProto.InventoryItem.Builder inventoryItemBuilder =
        TetrationNetworkPolicyProto.InventoryItem.newBuilder();
    inventoryItemBuilder.setAddressRange(addressBuilder);
    return inventoryItemBuilder.build();
  }

  @BeforeTest
  public void setupBeforeClass() throws UnknownHostException {
    this.topicName = "tetra_test";
    this.partitionId = new Integer(0);
    this.configProperties = new Properties();
    this.configProperties.put("tetration.user.credential", "we're mocking");
    // this is the IP to be matched against network policies received from kafka
    this.configProperties.put("appliance.inventoryItem",
        createInventoryItem(InetAddress.getByName("192.168.1.10"), 32));
  }

  @DataProvider(name="twoSnapshotsWithUpdateParameters")
  private Object[][] generateTwoSnapshotsWithUpdateParameters() throws UnknownHostException {
    List<Object[]> result = new ArrayList<>();

    Properties configProperties = new Properties();
    configProperties.put("tetration.user.credential", "we're mocking");
    // this is the IP to be matched against network policies received from kafka
    InventoryItem inventoryItem = createInventoryItem(
        InetAddress.getByName("192.168.1.10"), 32);
    configProperties.put("appliance.inventoryItem", inventoryItem);

    Properties configPropertiesWithAddrRange = new Properties();
    configPropertiesWithAddrRange.put("tetration.user.credential", "we're mocking");
    // this is the IP to be matched against network policies received from kafka
    inventoryItem = createInventoryItem(
        InetAddress.getByName("192.168.1.8"), InetAddress.getByName("192.168.1.11"));
    configPropertiesWithAddrRange.put("appliance.inventoryItem", inventoryItem);

    KafkaUpdateCreator kafkaUpdateCreator = new KafkaUpdateCreator();
    kafkaUpdateCreator.updateType = KafkaUpdate.UpdateType.UPDATE_START;
    kafkaUpdateCreator.intentId = "123";
    kafkaUpdateCreator.flowFilterId = "1234";
    // with single IP
    kafkaUpdateCreator.consumerFilterId = "abcde";
    kafkaUpdateCreator.consumerIp = new byte[] {(byte)192, (byte)168, 1, 10};
    kafkaUpdateCreator.consumerPrefixLen = 32;
    kafkaUpdateCreator.providerFilterId = "klmno";
    kafkaUpdateCreator.providerIp = new byte[] {(byte)192, (byte)168, 1, 13};
    kafkaUpdateCreator.providerPrefixLen = 32;
    result.add(new Object[]{configProperties, kafkaUpdateCreator});

    // with subnet
    kafkaUpdateCreator = kafkaUpdateCreator.clone();
    kafkaUpdateCreator.consumerIp[3] = 0;
    kafkaUpdateCreator.consumerPrefixLen = 24;
    result.add(new Object[]{configProperties, kafkaUpdateCreator});

    // with address range
    kafkaUpdateCreator = kafkaUpdateCreator.clone();
    kafkaUpdateCreator.consumerEndIp = kafkaUpdateCreator.consumerIp.clone();
    kafkaUpdateCreator.consumerIp[3] = 4;
    kafkaUpdateCreator.consumerEndIp[3] = 10;
    result.add(new Object[]{configProperties, kafkaUpdateCreator});

    kafkaUpdateCreator = kafkaUpdateCreator.clone();
    kafkaUpdateCreator.consumerIp[3] = 10;
    kafkaUpdateCreator.consumerEndIp[3] = 15;
    result.add(new Object[]{configPropertiesWithAddrRange, kafkaUpdateCreator});

    kafkaUpdateCreator = kafkaUpdateCreator.clone();
    kafkaUpdateCreator.consumerIp[3] = 9;
    kafkaUpdateCreator.consumerEndIp[3] = 11;
    result.add(new Object[]{configPropertiesWithAddrRange, kafkaUpdateCreator});

    return result.toArray(new Object[result.size()][]);
  }

  /**
   * This test sends two snapshots to kafka via mocking. Those two snapshots
   * contain the same intent with appliance IP, while the second one has a
   * change vs the first one.
   * Expectation: Two events to be received with full snapshot and an update
   * for inventory change.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  @Test(dataProvider = "twoSnapshotsWithUpdateParameters")
  public void testTwoSnapshotsWithUpdate(Properties configProperties,
                                         KafkaUpdateCreator kafkaUpdateCreator)
      throws InterruptedException {
    KafkaConsumerFactory kafkaConsumerFactoryMock = mock(KafkaConsumerFactory.class);
    assertNotEquals(null, kafkaConsumerFactoryMock);
    KafkaConsumerRunner.kafkaConsumerFactory = kafkaConsumerFactoryMock;

    KafkaConsumer<String, byte[]> kafkaConsumerMock = mock(KafkaConsumer.class);
    assertNotEquals(null, kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.createInstance(any(Properties.class))).thenReturn(kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.getTopicName(any(Properties.class))).thenReturn(this.topicName);

    PartitionInfo partitionInfo = new PartitionInfo(this.topicName, this.partitionId, null, null, null);
    when(kafkaConsumerMock.partitionsFor(any(String.class))).thenReturn(Arrays.asList(partitionInfo));

    KafkaUpdate kafkaUpdate1 = kafkaUpdateCreator.create();

    KafkaUpdate.Builder kafkaUpdate2 = KafkaUpdate.newBuilder();
    kafkaUpdate2.setSequenceNum(1);
    kafkaUpdate2.setType(KafkaUpdate.UpdateType.UPDATE_END);
    kafkaUpdate2.setVersion(123456);
    TenantNetworkPolicy.Builder tenantNetworkPolicy = TenantNetworkPolicy.newBuilder();
    tenantNetworkPolicy.setTenantName("1");
    kafkaUpdate2.setTenantNetworkPolicy(tenantNetworkPolicy);

    kafkaUpdateCreator.providerIp[3] = 110;
    KafkaUpdate kafkaUpdate3 = kafkaUpdateCreator.create();

    KafkaUpdate[] kafkaUpdates = new KafkaUpdate[] {
        kafkaUpdate1, kafkaUpdate2.build(), kafkaUpdate3, kafkaUpdate2.build()
    };

    when(kafkaConsumerMock.poll(anyLong())).thenAnswer(new PollAnswer(kafkaUpdates));

    int expectedEventCount = 2;
    EventCallback eventCallback = new EventCallback(expectedEventCount);
    PolicyEnforcementClient client = new PolicyEnforcementClientImpl();
    // use the given config, not the one from setup!!!
    client.init(configProperties, eventCallback);

    eventCallback.lock.lock();
    while (eventCallback.eventList.size() < expectedEventCount) {
      eventCallback.condition.await(5, TimeUnit.SECONDS);
    }
    eventCallback.lock.unlock();
    assertEquals(eventCallback.eventList.size(), expectedEventCount);

    client.release();

    // verify data
    PolicyEnforcementClient.Event event0 = eventCallback.eventList.get(0);
    PolicyEnforcementClient.Event event1 = eventCallback.eventList.get(1);
    assertEquals(event0.getEventType(), PolicyEnforcementClient.EventType.FULL_SNAPSHOT);
    assertEquals(event1.getEventType(), PolicyEnforcementClient.EventType.INCREMENTAL_UPDATE);
    PolicyEnforcementClient.IntentRecord intentRecord0 = event0.getIntentRecords().get(0);
    assertEquals(intentRecord0.getRecordType(), PolicyEnforcementClient.RecordType.CREATE);
    assertEquals(intentRecord0.getIntent().getId(), kafkaUpdateCreator.intentId);
    assertEquals(event1.getIntentRecords().size(), 0);
    PolicyEnforcementClient.InventoryFilterRecord inventoryFilterRecord = event1.getInventoryFilterRecords().get(0);
    assertEquals(inventoryFilterRecord.getRecordType(), PolicyEnforcementClient.RecordType.UPDATE);
  }

  /**
   * This test sends two snapshots to kafka via mocking. Only the first snapshot
   * contains appliance IP. Second snapshot contains another intent with no IP
   * matched against appliance IP.
   * Expectation: two events to be received with full snapshot, update to delete intent
   * @throws ExecutionException
   * @throws InterruptedException
   */
  @Test
  public void testTwoSnapshotsWithOneMatch() throws InterruptedException {
    KafkaConsumerFactory kafkaConsumerFactoryMock = mock(KafkaConsumerFactory.class);
    assertNotEquals(null, kafkaConsumerFactoryMock);
    KafkaConsumerRunner.kafkaConsumerFactory = kafkaConsumerFactoryMock;

    KafkaConsumer<String, byte[]> kafkaConsumerMock = mock(KafkaConsumer.class);
    assertNotEquals(null, kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.createInstance(any(Properties.class))).thenReturn(kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.getTopicName(any(Properties.class))).thenReturn(this.topicName);

    PartitionInfo partitionInfo = new PartitionInfo(this.topicName, this.partitionId, null, null, null);
    when(kafkaConsumerMock.partitionsFor(any(String.class))).thenReturn(Arrays.asList(partitionInfo));

    final String intentId = "123";
    KafkaUpdateCreator kafkaUpdateCreator = new KafkaUpdateCreator();
    kafkaUpdateCreator.updateType = KafkaUpdate.UpdateType.UPDATE_START;
    kafkaUpdateCreator.intentId = intentId;
    kafkaUpdateCreator.flowFilterId = "1234";
    kafkaUpdateCreator.consumerFilterId = "abcde";
    kafkaUpdateCreator.consumerIp = new byte[] {(byte)192, (byte)168, 1, 10};
    kafkaUpdateCreator.consumerPrefixLen = 32;
    kafkaUpdateCreator.providerFilterId = "klmno";
    kafkaUpdateCreator.providerIp = new byte[] {(byte)192, (byte)168, 1, 11};
    kafkaUpdateCreator.providerPrefixLen = 32;
    KafkaUpdate kafkaUpdate1 = kafkaUpdateCreator.create();

    KafkaUpdate.Builder kafkaUpdate2 = KafkaUpdate.newBuilder();
    kafkaUpdate2.setSequenceNum(1);
    kafkaUpdate2.setType(KafkaUpdate.UpdateType.UPDATE_END);
    kafkaUpdate2.setVersion(123456);
    TenantNetworkPolicy.Builder tenantNetworkPolicy = TenantNetworkPolicy.newBuilder();
    tenantNetworkPolicy.setTenantName("1");
    kafkaUpdate2.setTenantNetworkPolicy(tenantNetworkPolicy);

    kafkaUpdateCreator.intentId += "2";
    kafkaUpdateCreator.flowFilterId += "2";
    kafkaUpdateCreator.consumerFilterId += "2";
    kafkaUpdateCreator.consumerIp[3] = 110;
    KafkaUpdate kafkaUpdate3 = kafkaUpdateCreator.create();

    KafkaUpdate[] kafkaUpdates = new KafkaUpdate[] {
        kafkaUpdate1, kafkaUpdate2.build(), kafkaUpdate3, kafkaUpdate2.build()
    };

    when(kafkaConsumerMock.poll(anyLong())).thenAnswer(new PollAnswer(kafkaUpdates));

    int expectedEventCount = 2;
    EventCallback eventCallback = new EventCallback(expectedEventCount);
    PolicyEnforcementClient client = new PolicyEnforcementClientImpl();
    client.init(this.configProperties, eventCallback);

    eventCallback.lock.lock();
    while (eventCallback.eventList.size() < expectedEventCount) {
      eventCallback.condition.await(5, TimeUnit.SECONDS);
    }
    eventCallback.lock.unlock();
    assertEquals(eventCallback.eventList.size(), expectedEventCount);

    client.release();

    // verify data
    PolicyEnforcementClient.Event event0 = eventCallback.eventList.get(0);
    assertEquals(event0.getEventType(), PolicyEnforcementClient.EventType.FULL_SNAPSHOT);
    PolicyEnforcementClient.IntentRecord intentRecord0 = event0.getIntentRecords().get(0);
    assertEquals(intentRecord0.getRecordType(), PolicyEnforcementClient.RecordType.CREATE);
    assertEquals(intentRecord0.getIntent().getId(), intentId);
    PolicyEnforcementClient.Event event1 = eventCallback.eventList.get(1);
    assertEquals(event1.getEventType(), PolicyEnforcementClient.EventType.INCREMENTAL_UPDATE);
    PolicyEnforcementClient.IntentRecord intentRecord1 = event1.getIntentRecords().get(0);
    assertEquals(intentRecord1.getRecordType(), PolicyEnforcementClient.RecordType.DELETE);
    assertEquals(intentRecord1.getIntent().getId(), intentId);
  }

  /**
   * This test sends two snapshots to kafka via mocking. Both snapshots contains
   * provider filter matched with appliance ip. Second snapshot changes consumer filter id.
   * Expectation: two events to be received with full snapshot, update for consumer filter
   * and intent
   * @throws ExecutionException
   * @throws InterruptedException
   */
  @Test
  public void testTwoSnapshotsWithConsumerFilterIdChange() throws InterruptedException {
    KafkaConsumerFactory kafkaConsumerFactoryMock = mock(KafkaConsumerFactory.class);
    assertNotEquals(null, kafkaConsumerFactoryMock);
    KafkaConsumerRunner.kafkaConsumerFactory = kafkaConsumerFactoryMock;

    KafkaConsumer<String, byte[]> kafkaConsumerMock = mock(KafkaConsumer.class);
    assertNotEquals(null, kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.createInstance(any(Properties.class))).thenReturn(kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.getTopicName(any(Properties.class))).thenReturn(this.topicName);

    PartitionInfo partitionInfo = new PartitionInfo(this.topicName, this.partitionId, null, null, null);
    when(kafkaConsumerMock.partitionsFor(any(String.class))).thenReturn(Arrays.asList(partitionInfo));

    final String intentId = "123";
    KafkaUpdateCreator kafkaUpdateCreator = new KafkaUpdateCreator();
    kafkaUpdateCreator.updateType = KafkaUpdate.UpdateType.UPDATE_START;
    kafkaUpdateCreator.intentId = intentId;
    kafkaUpdateCreator.flowFilterId = "1234";
    kafkaUpdateCreator.consumerFilterId = "abcde";
    kafkaUpdateCreator.consumerIp = new byte[] {(byte)192, (byte)168, 1, 111};
    kafkaUpdateCreator.consumerPrefixLen = 32;
    kafkaUpdateCreator.providerFilterId = "klmno";
    kafkaUpdateCreator.providerIp = new byte[] {(byte)192, (byte)168, 1, 10};
    kafkaUpdateCreator.providerPrefixLen = 32;
    KafkaUpdate kafkaUpdate1 = kafkaUpdateCreator.create();

    KafkaUpdate.Builder kafkaUpdate2 = KafkaUpdate.newBuilder();
    kafkaUpdate2.setSequenceNum(1);
    kafkaUpdate2.setType(KafkaUpdate.UpdateType.UPDATE_END);
    kafkaUpdate2.setVersion(123456);
    TenantNetworkPolicy.Builder tenantNetworkPolicy = TenantNetworkPolicy.newBuilder();
    tenantNetworkPolicy.setTenantName("1");
    kafkaUpdate2.setTenantNetworkPolicy(tenantNetworkPolicy);

    kafkaUpdateCreator.consumerFilterId += "2";
    kafkaUpdateCreator.consumerIp[3] = 110;
    KafkaUpdate kafkaUpdate3 = kafkaUpdateCreator.create();

    KafkaUpdate[] kafkaUpdates = new KafkaUpdate[] {
        kafkaUpdate1, kafkaUpdate2.build(), kafkaUpdate3, kafkaUpdate2.build()
    };

    when(kafkaConsumerMock.poll(anyLong())).thenAnswer(new PollAnswer(kafkaUpdates));

    int expectedEventCount = 2;
    EventCallback eventCallback = new EventCallback(expectedEventCount);
    PolicyEnforcementClient client = new PolicyEnforcementClientImpl();
    client.init(this.configProperties, eventCallback);

    eventCallback.lock.lock();
    while (eventCallback.eventList.size() < expectedEventCount) {
      eventCallback.condition.await(5, TimeUnit.SECONDS);
    }
    eventCallback.lock.unlock();
    assertEquals(eventCallback.eventList.size(), expectedEventCount);

    client.release();

    // verify data
    PolicyEnforcementClient.Event event0 = eventCallback.eventList.get(0);
    assertEquals(event0.getEventType(), PolicyEnforcementClient.EventType.FULL_SNAPSHOT);
    PolicyEnforcementClient.IntentRecord intentRecord0 = event0.getIntentRecords().get(0);
    assertEquals(intentRecord0.getRecordType(), PolicyEnforcementClient.RecordType.CREATE);
    assertEquals(intentRecord0.getIntent().getId(), intentId);
    PolicyEnforcementClient.Event event1 = eventCallback.eventList.get(1);
    assertEquals(event1.getEventType(), PolicyEnforcementClient.EventType.INCREMENTAL_UPDATE);
    PolicyEnforcementClient.InventoryFilterRecord inventoryFilterRecord = event1.getInventoryFilterRecords().get(0);
    assertEquals(inventoryFilterRecord.getRecordType(), PolicyEnforcementClient.RecordType.CREATE);
    PolicyEnforcementClient.IntentRecord intentRecord1 = event1.getIntentRecords().get(0);
    assertEquals(intentRecord1.getRecordType(), PolicyEnforcementClient.RecordType.UPDATE);
    assertEquals(intentRecord1.getIntent().getId(), intentId);
  }

  /**
   * This test sends two snapshots to kafka via mocking. Each of the snapshots
   * contains only one intent with appliance IP. However, the intents have different
   * intent ID.
   * Expectation: Two events to be received with full snapshot and an update, which
   * consists of a CREATE for second intent and a DELETE for first intent.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  @Test
  public void testTwoSnapshotsWithDelete() throws InterruptedException {
    KafkaConsumerFactory kafkaConsumerFactoryMock = mock(KafkaConsumerFactory.class);
    assertNotEquals(null, kafkaConsumerFactoryMock);
    KafkaConsumerRunner.kafkaConsumerFactory = kafkaConsumerFactoryMock;

    KafkaConsumer<String, byte[]> kafkaConsumerMock = mock(KafkaConsumer.class);
    assertNotEquals(null, kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.createInstance(any(Properties.class))).thenReturn(kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.getTopicName(any(Properties.class))).thenReturn(this.topicName);

    PartitionInfo partitionInfo = new PartitionInfo(this.topicName, this.partitionId, null, null, null);
    when(kafkaConsumerMock.partitionsFor(any(String.class))).thenReturn(Arrays.asList(partitionInfo));

    final String intentId = "123";
    KafkaUpdateCreator kafkaUpdateCreator = new KafkaUpdateCreator();
    kafkaUpdateCreator.updateType = KafkaUpdate.UpdateType.UPDATE_START;
    kafkaUpdateCreator.intentId = intentId;
    kafkaUpdateCreator.flowFilterId = "1234";
    kafkaUpdateCreator.consumerFilterId = "abcde";
    kafkaUpdateCreator.consumerIp = new byte[] {(byte)192, (byte)168, 1, 10};
    kafkaUpdateCreator.consumerPrefixLen = 32;
    kafkaUpdateCreator.providerFilterId = "klmno";
    kafkaUpdateCreator.providerIp = new byte[] {(byte)192, (byte)168, 1, 11};
    kafkaUpdateCreator.providerPrefixLen = 32;
    KafkaUpdate kafkaUpdate1 = kafkaUpdateCreator.create();

    KafkaUpdate.Builder kafkaUpdate2 = KafkaUpdate.newBuilder();
    kafkaUpdate2.setSequenceNum(1);
    kafkaUpdate2.setType(KafkaUpdate.UpdateType.UPDATE_END);
    kafkaUpdate2.setVersion(123456);
    TenantNetworkPolicy.Builder tenantNetworkPolicy = TenantNetworkPolicy.newBuilder();
    tenantNetworkPolicy.setTenantName("1");
    kafkaUpdate2.setTenantNetworkPolicy(tenantNetworkPolicy);

    kafkaUpdateCreator.intentId += "2";
    kafkaUpdateCreator.flowFilterId += "2";
    kafkaUpdateCreator.consumerFilterId += "2";
    kafkaUpdateCreator.providerFilterId += "2";
    kafkaUpdateCreator.consumerIp[3] = 110;
    kafkaUpdateCreator.providerIp[3] = 10;
    KafkaUpdate kafkaUpdate3 = kafkaUpdateCreator.create();

    KafkaUpdate[] kafkaUpdates = new KafkaUpdate[] {
        kafkaUpdate1, kafkaUpdate2.build(), kafkaUpdate3, kafkaUpdate2.build()
    };

    when(kafkaConsumerMock.poll(anyLong())).thenAnswer(new PollAnswer(kafkaUpdates));

    int expectedEventCount = 2;
    EventCallback eventCallback = new EventCallback(expectedEventCount);
    PolicyEnforcementClient client = new PolicyEnforcementClientImpl();
    client.init(this.configProperties, eventCallback);

    eventCallback.lock.lock();
    while (eventCallback.eventList.size() < expectedEventCount) {
      eventCallback.condition.await(5, TimeUnit.SECONDS);
    }
    eventCallback.lock.unlock();
    assertEquals(eventCallback.eventList.size(), expectedEventCount);

    client.release();

    // verify data
    PolicyEnforcementClient.Event event0 = eventCallback.eventList.get(0);
    PolicyEnforcementClient.Event event1 = eventCallback.eventList.get(1);
    assertEquals(event0.getEventType(), PolicyEnforcementClient.EventType.FULL_SNAPSHOT);
    assertEquals(event1.getEventType(), PolicyEnforcementClient.EventType.INCREMENTAL_UPDATE);
    PolicyEnforcementClient.IntentRecord intentRecord0 = event0.getIntentRecords().get(0);
    PolicyEnforcementClient.IntentRecord intentRecord1 = event1.getIntentRecords().get(0);
    PolicyEnforcementClient.IntentRecord intentRecord2 = event1.getIntentRecords().get(1);
    assertEquals(intentRecord0.getRecordType(), PolicyEnforcementClient.RecordType.CREATE);
    assertEquals(intentRecord2.getRecordType(), PolicyEnforcementClient.RecordType.CREATE);
    assertEquals(intentRecord1.getRecordType(), PolicyEnforcementClient.RecordType.DELETE);
    assertEquals(intentRecord0.getIntent().getId(), intentId);
    assertEquals(intentRecord2.getIntent().getId(), kafkaUpdateCreator.intentId);
    assertEquals(intentRecord1.getIntent().getId(), intentId);
  }

  /**
   * This test sends two snapshots to kafka via mocking. The generated intents
   * IP is not equal to the appliance IP.
   * Expectation: No event should be received within 5 sec.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  @Test
  public void testTwoSnapshotsWithoutMatch() throws InterruptedException {
    KafkaConsumerFactory kafkaConsumerFactoryMock = mock(KafkaConsumerFactory.class);
    assertNotEquals(null, kafkaConsumerFactoryMock);
    KafkaConsumerRunner.kafkaConsumerFactory = kafkaConsumerFactoryMock;

    KafkaConsumer<String, byte[]> kafkaConsumerMock = mock(KafkaConsumer.class);
    assertNotEquals(null, kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.createInstance(any(Properties.class))).thenReturn(kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.getTopicName(any(Properties.class))).thenReturn(this.topicName);

    PartitionInfo partitionInfo = new PartitionInfo(this.topicName, this.partitionId, null, null, null);
    when(kafkaConsumerMock.partitionsFor(any(String.class))).thenReturn(Arrays.asList(partitionInfo));

    final String intentId = "123";
    KafkaUpdateCreator kafkaUpdateCreator = new KafkaUpdateCreator();
    kafkaUpdateCreator.updateType = KafkaUpdate.UpdateType.UPDATE_START;
    kafkaUpdateCreator.intentId = intentId;
    kafkaUpdateCreator.flowFilterId = "1234";
    kafkaUpdateCreator.consumerFilterId = "abcde";
    kafkaUpdateCreator.consumerIp = new byte[] {(byte)192, (byte)168, 1, 19};
    kafkaUpdateCreator.consumerPrefixLen = 32;
    kafkaUpdateCreator.providerFilterId = "klmno";
    kafkaUpdateCreator.providerIp = new byte[] {(byte)192, (byte)168, 1, 11};
    kafkaUpdateCreator.providerPrefixLen = 32;
    KafkaUpdate kafkaUpdate1 = kafkaUpdateCreator.create();

    KafkaUpdate.Builder kafkaUpdate2 = KafkaUpdate.newBuilder();
    kafkaUpdate2.setSequenceNum(1);
    kafkaUpdate2.setType(KafkaUpdate.UpdateType.UPDATE_END);
    kafkaUpdate2.setVersion(123456);
    TenantNetworkPolicy.Builder tenantNetworkPolicy = TenantNetworkPolicy.newBuilder();
    tenantNetworkPolicy.setTenantName("1");
    kafkaUpdate2.setTenantNetworkPolicy(tenantNetworkPolicy);

    kafkaUpdateCreator.intentId += "2";
    kafkaUpdateCreator.flowFilterId += "2";
    kafkaUpdateCreator.consumerFilterId += "2";
    kafkaUpdateCreator.providerFilterId += "2";
    kafkaUpdateCreator.consumerIp[3] = 110;
    KafkaUpdate kafkaUpdate3 = kafkaUpdateCreator.create();

    KafkaUpdate[] kafkaUpdates = new KafkaUpdate[] {
        kafkaUpdate1, kafkaUpdate2.build(), kafkaUpdate3, kafkaUpdate2.build()
    };

    when(kafkaConsumerMock.poll(anyLong())).thenAnswer(new PollAnswer(kafkaUpdates));

    int maxEventCount = 2;
    EventCallback eventCallback = new EventCallback(maxEventCount);
    PolicyEnforcementClient client = new PolicyEnforcementClientImpl();
    client.init(this.configProperties, eventCallback);

    Thread.sleep(5000);
    int eventCount;
    eventCallback.lock.lock();
    eventCount = eventCallback.eventList.size();
    eventCallback.lock.unlock();
    assertEquals(0, eventCount);

    client.release();
  }

  private static class KafkaUpdateCreator {
    public KafkaUpdate.UpdateType updateType;
    public String intentId;
    public String flowFilterId;
    public String consumerFilterId;
    public byte[] consumerIp;
    public byte[] consumerEndIp;    // if set, prefixLen is ignored
    public int consumerPrefixLen;
    public String providerFilterId;
    public byte[] providerIp;
    public byte[] providerEndIp;    // if set, prefixLen is ignored
    public int providerPrefixLen;

    public KafkaUpdateCreator clone() {
      KafkaUpdateCreator result = new KafkaUpdateCreator();
      result.updateType = updateType;
      result.intentId = new String(intentId);
      result.flowFilterId = new String(flowFilterId);
      result.consumerFilterId = new String(consumerFilterId);
      result.consumerIp = consumerIp.clone();
      if (consumerEndIp != null) {
        result.consumerEndIp = consumerEndIp.clone();
      }
      result.consumerPrefixLen = consumerPrefixLen;
      result.providerFilterId = new String(providerFilterId);
      result.providerIp = providerIp.clone();
      if (providerEndIp != null) {
        result.providerEndIp = providerEndIp.clone();
      }
      result.providerPrefixLen = providerPrefixLen;
      return result;
    }

    /**
     * Helper method to create a protobuf KafkaUpdate object with an intent
     * and two inventory filters with given parameters
     * @return
     */
    public KafkaUpdate create() {
      PortRange.Builder portRange = PortRange.newBuilder();
      portRange.setStartPort(4234);
      portRange.setEndPort(4239);

      ProtocolAndPorts.Builder protocolAndPorts = ProtocolAndPorts.newBuilder();
      protocolAndPorts.setProtocolValue(IPProtocol.TCP_VALUE);
      protocolAndPorts.addPortRanges(portRange);

      FlowFilter.Builder flowFilter = FlowFilter.newBuilder();
      flowFilter.setId(flowFilterId);
      flowFilter.setConsumerFilterId(consumerFilterId);
      flowFilter.setProviderFilterId(providerFilterId);
      flowFilter.addProtocolAndPorts(protocolAndPorts);

      Intent.Builder intent = Intent.newBuilder();
      intent.setAction(Intent.Action.ALLOW);
      intent.setId(intentId);
      intent.setFlowFilter(flowFilter);
      intent.addTargetDevices(Intent.TargetDevice.MIDDLE_BOXES);

      TetrationNetworkPolicyProto.InventoryFilter.Builder inventoryFilter1 = TetrationNetworkPolicyProto.InventoryFilter.newBuilder();
      inventoryFilter1.setId(consumerFilterId);
      TetrationNetworkPolicyProto.InventoryItem.Builder inventoryItem1 = TetrationNetworkPolicyProto.InventoryItem.newBuilder();
      if (consumerEndIp != null) {
        AddressWithRange.Builder addressWithRange1 = AddressWithRange.newBuilder();
        addressWithRange1.setAddrFamily(IPAddressFamily.IPv4);
        addressWithRange1.setStartIpAddr(ByteString.copyFrom(consumerIp));
        addressWithRange1.setEndIpAddr(ByteString.copyFrom(consumerEndIp));
        inventoryItem1.setAddressRange(addressWithRange1);
      } else {
        AddressWithPrefix.Builder addressWithPrefix1 = AddressWithPrefix.newBuilder();
        addressWithPrefix1.setAddrFamily(IPAddressFamily.IPv4);
        addressWithPrefix1.setIpAddr(ByteString.copyFrom(consumerIp));
        addressWithPrefix1.setPrefixLength(consumerPrefixLen);
        inventoryItem1.setIpAddress(addressWithPrefix1);
      }
      inventoryFilter1.addInventoryItems(inventoryItem1);

      TetrationNetworkPolicyProto.InventoryFilter.Builder inventoryFilter2 = TetrationNetworkPolicyProto.InventoryFilter.newBuilder();
      inventoryFilter2.setId(providerFilterId);
      TetrationNetworkPolicyProto.InventoryItem.Builder inventoryItem2 = TetrationNetworkPolicyProto.InventoryItem.newBuilder();
      if (providerEndIp != null) {
        AddressWithRange.Builder addressWithRange2 = AddressWithRange.newBuilder();
        addressWithRange2.setAddrFamily(IPAddressFamily.IPv4);
        addressWithRange2.setStartIpAddr(ByteString.copyFrom(providerIp));
        addressWithRange2.setEndIpAddr(ByteString.copyFrom(providerEndIp));
        inventoryItem2.setAddressRange(addressWithRange2);
      } else {
        AddressWithPrefix.Builder addressWithPrefix2 = AddressWithPrefix.newBuilder();
        addressWithPrefix2.setAddrFamily(IPAddressFamily.IPv4);
        addressWithPrefix2.setIpAddr(ByteString.copyFrom(providerIp));
        addressWithPrefix2.setPrefixLength(providerPrefixLen);
        inventoryItem2.setIpAddress(addressWithPrefix2);
      }
      inventoryFilter2.addInventoryItems(inventoryItem2);

      NetworkPolicy.Builder networkPolicy = NetworkPolicy.newBuilder();
      networkPolicy.addIntents(intent);
      networkPolicy.addInventoryFilters(inventoryFilter1);
      networkPolicy.addInventoryFilters(inventoryFilter2);

      TenantNetworkPolicy.Builder tenantNetworkPolicy = TenantNetworkPolicy.newBuilder();
      tenantNetworkPolicy.setTenantName("1");
      tenantNetworkPolicy.addNetworkPolicy(networkPolicy);

      KafkaUpdate.Builder kafkaUpdate1 = KafkaUpdate.newBuilder();
      kafkaUpdate1.setSequenceNum(0);
      kafkaUpdate1.setType(updateType);
      kafkaUpdate1.setVersion(123456);
      kafkaUpdate1.setTenantNetworkPolicy(tenantNetworkPolicy);
      return kafkaUpdate1.build();
    }
  }
}
