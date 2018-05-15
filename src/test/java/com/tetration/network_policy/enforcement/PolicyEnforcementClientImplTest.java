/*
 * Copyright 2017 Cisco Systems, Inc.
 * Cisco Tetration
 */

package com.tetration.network_policy.enforcement;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.tetration.tetration_network_policy.proto.TetrationNetworkPolicyProto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

public class PolicyEnforcementClientImplTest implements PolicyEnforcementClient.EventCallback {
  protected static Logger logger = LoggerFactory.getLogger(PolicyEnforcementClientImplTest.class);

  private static int IPV4_ADDR_LEN = 32;
  private static int IPV6_ADDR_LEN = 128;
  private Properties configProperties;
  private String topicName;
  private Integer partitionId;
  private PolicyEnforcementClient.Event receivedEvent = null;
  private int notifyCount = 0;

  private static TetrationNetworkPolicyProto.InventoryItem createInventoryItem(InetAddress inetAddress, int prefixLength) {
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

  @BeforeTest
  public void setupBeforeClass() throws UnknownHostException {
    this.topicName = "tetra_test";
    this.partitionId = new Integer(0);
    this.configProperties = new Properties();
    this.configProperties.put("tetration.user.credential", "we're mocking");
    // this is the IPs to be matched against network policies received from kafka
    TetrationNetworkPolicyProto.InventoryItem applianceInventoryItems[] = new TetrationNetworkPolicyProto.InventoryItem[2];
    try {
      applianceInventoryItems[0] = createInventoryItem(InetAddress.getByName("192.168.1.10"), IPV4_ADDR_LEN);
      applianceInventoryItems[1] = createInventoryItem(InetAddress.getByName("fe80::aa11:9305:4b35:d755"), IPV6_ADDR_LEN);
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    this.configProperties.put("appliance.inventoryItem", applianceInventoryItems);
  }

  @Override
  public void notify(PolicyEnforcementClient.Event event) {
    logger.info("#" + String.valueOf(this.notifyCount++) + " notify: " + event.getEventType());
    JsonFormat.Printer printer = JsonFormat.printer();
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("[");
    for (PolicyEnforcementClient.IntentRecord intentRecord : event.getIntentRecords()) {
      try {
        stringBuilder.append("{\"recordType\":\"").append(intentRecord.getRecordType().toString());
        stringBuilder.append(", \"position\":").append(intentRecord.getPosition());
        stringBuilder.append(", \"oldPosition\":").append(intentRecord.getOldPosition());
        stringBuilder.append(", \"previousIntentId\":\"").append(intentRecord.getPreviousIntentId());
        stringBuilder.append(", \"nextIntentId\":\"").append(intentRecord.getNextIntentId());
        stringBuilder.append("\", \"intent\":");
        stringBuilder.append(printer.print(intentRecord.getIntent())).append("}, ");
      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
      }
    }
    stringBuilder.append("]");
    logger.info("intentRecords=" + stringBuilder.toString());
    this.receivedEvent = event;
  }

  static class ExpectedUpdate {
    String intentId;
    PolicyEnforcementClient.RecordType recordType;
    String previousId;
    String nextId;
    int position;
    ExpectedUpdate(String intentId, PolicyEnforcementClient.RecordType recordType,
                   String previousId, String nextId, int position) {
      this.intentId = intentId;
      this.recordType = recordType;
      this.previousId = previousId;
      this.nextId = nextId;
      this.position = position;
    }
  }

  @Test
  public void testDeltaComputation() {
    KafkaConsumerFactory kafkaConsumerFactoryMock = mock(KafkaConsumerFactory.class);
    assertNotEquals(null, kafkaConsumerFactoryMock);
    KafkaConsumerRunner.kafkaConsumerFactory = kafkaConsumerFactoryMock;

    KafkaConsumer<String, byte[]> kafkaConsumerMock = mock(KafkaConsumer.class);
    assertNotEquals(null, kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.createInstance(any(Properties.class))).thenReturn(kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.getTopicName(any(Properties.class))).thenReturn(this.topicName);

    PartitionInfo partitionInfo = new PartitionInfo(this.topicName, this.partitionId, null, null, null);
    when(kafkaConsumerMock.partitionsFor(any(String.class))).thenReturn(Arrays.asList(partitionInfo));

    Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> consumerRecordsData = new HashMap<>();
    when(kafkaConsumerMock.poll(anyLong())).thenReturn(new ConsumerRecords<String, byte[]>(consumerRecordsData));

    PolicyEnforcementClientImpl client = new PolicyEnforcementClientImpl();
    client.init(this.configProperties, this);

    int inventoryFilterId = 123500;
    int intentId = 123700;
    byte[] inventoryItemIp = new byte[]{(byte) 192, (byte) 168, 1, 10};

    List<TetrationNetworkPolicyProto.InventoryFilter> inventoryFilterList = new ArrayList<>();
    TetrationNetworkPolicyProto.PortRange.Builder portRange = TetrationNetworkPolicyProto.PortRange.newBuilder();
    portRange.setStartPort(4234);
    portRange.setEndPort(4239);

    TetrationNetworkPolicyProto.ProtocolAndPorts.Builder protocolAndPorts = TetrationNetworkPolicyProto.ProtocolAndPorts.newBuilder();
    protocolAndPorts.setProtocolValue(TetrationNetworkPolicyProto.IPProtocol.TCP_VALUE);
    protocolAndPorts.addPortRanges(portRange);

    for (int i = 0; i < 6; ++i) {
      TetrationNetworkPolicyProto.AddressWithPrefix.Builder addressWithPrefix = TetrationNetworkPolicyProto.AddressWithPrefix.newBuilder();
      addressWithPrefix.setAddrFamily(TetrationNetworkPolicyProto.IPAddressFamily.IPv4);
      addressWithPrefix.setIpAddr(ByteString.copyFrom(inventoryItemIp));
      addressWithPrefix.setPrefixLength(IPV4_ADDR_LEN);
      TetrationNetworkPolicyProto.InventoryItem.Builder inventoryItem = TetrationNetworkPolicyProto.InventoryItem.newBuilder();
      inventoryItem.setIpAddress(addressWithPrefix);
      TetrationNetworkPolicyProto.InventoryFilter.Builder inventoryFilter = TetrationNetworkPolicyProto.InventoryFilter.newBuilder();
      inventoryFilter.setId(String.valueOf(inventoryFilterId++));
      inventoryFilter.addInventoryItems(inventoryItem);
      inventoryItemIp[3]++;
      inventoryFilterList.add(inventoryFilter.build());
    }

    List<TetrationNetworkPolicyProto.Intent> intentList = new ArrayList<>();
    for (int i = 1; i < 6; ++i) {
      TetrationNetworkPolicyProto.FlowFilter.Builder flowFilter = TetrationNetworkPolicyProto.FlowFilter.newBuilder();
      flowFilter.setConsumerFilterId(String.valueOf(inventoryFilterList.get(0).getId()));
      flowFilter.setProviderFilterId(String.valueOf(inventoryFilterList.get(i).getId()));
      flowFilter.addProtocolAndPorts(protocolAndPorts);
      TetrationNetworkPolicyProto.Intent.Builder intent = TetrationNetworkPolicyProto.Intent.newBuilder();
      intent.setAction(TetrationNetworkPolicyProto.Intent.Action.ALLOW);
      intent.setId(String.valueOf(intentId++));
      intent.setFlowFilter(flowFilter);
      intent.addTargetDevices(TetrationNetworkPolicyProto.Intent.TargetDevice.MIDDLE_BOXES);
      intentList.add(intent.build());
    }

    TetrationNetworkPolicyProto.NetworkPolicy.Builder networkPolicy = TetrationNetworkPolicyProto.NetworkPolicy.newBuilder();
    networkPolicy.addAllInventoryFilters(inventoryFilterList);

    // test: add intent0 = [intent0]
    TetrationNetworkPolicyProto.NetworkPolicy.Builder previousBuilder = networkPolicy.clone();
    networkPolicy.addIntents(intentList.get(0));
    client.notify(networkPolicy.build());
    assertEquals(this.receivedEvent.getEventType(), PolicyEnforcementClient.EventType.FULL_SNAPSHOT);
    List<PolicyEnforcementClient.IntentRecord> intentRecordList = this.receivedEvent.getIntentRecords();
    assertEquals(intentRecordList.size(), 1);
    validateRecordList(previousBuilder, networkPolicy.build());

    // test: add intent1 to the end: [intent0, intent1]
    previousBuilder = networkPolicy.clone();
    networkPolicy.addIntents(intentList.get(1));
    client.notify(networkPolicy.build());
    assertEquals(this.receivedEvent.getEventType(), PolicyEnforcementClient.EventType.INCREMENTAL_UPDATE);
    intentRecordList = this.receivedEvent.getIntentRecords();
    assertEquals(intentRecordList.size(), 1);
    validateRecordList(previousBuilder, networkPolicy.build());

    // test: add intent2 to the begin: [intent2, intent0, intent1]
    previousBuilder = networkPolicy.clone();
    networkPolicy.addIntents(0, intentList.get(2));
    client.notify(networkPolicy.build());
    assertEquals(this.receivedEvent.getEventType(), PolicyEnforcementClient.EventType.INCREMENTAL_UPDATE);
    intentRecordList = this.receivedEvent.getIntentRecords();
    assertEquals(intentRecordList.size(), 1);
    validateRecordList(previousBuilder, networkPolicy.build());

    // test: add intent3 after intent 0: [intent2, intent0, intent3, intent1]
    previousBuilder = networkPolicy.clone();
    networkPolicy.addIntents(2, intentList.get(3));
    client.notify(networkPolicy.build());
    assertEquals(this.receivedEvent.getEventType(), PolicyEnforcementClient.EventType.INCREMENTAL_UPDATE);
    intentRecordList = this.receivedEvent.getIntentRecords();
    assertEquals(intentRecordList.size(), 1);
    validateRecordList(previousBuilder, networkPolicy.build());

    // test: move intent1 after intent0: [intent2, intent0, intent1, intent3]
    previousBuilder = networkPolicy.clone();
    networkPolicy.removeIntents(3);
    networkPolicy.addIntents(2, intentList.get(1));
    client.notify(networkPolicy.build());
    assertEquals(this.receivedEvent.getEventType(), PolicyEnforcementClient.EventType.INCREMENTAL_UPDATE);
    intentRecordList = this.receivedEvent.getIntentRecords();
    assertEquals(intentRecordList.size(), 1);
    validateRecordList(previousBuilder, networkPolicy.build());

    // test: move intent2 after intent1: [intent0, intent1, intent2, intent3]
    previousBuilder = networkPolicy.clone();
    networkPolicy.removeIntents(0);
    networkPolicy.addIntents(2, intentList.get(2));
    client.notify(networkPolicy.build());
    assertEquals(this.receivedEvent.getEventType(), PolicyEnforcementClient.EventType.INCREMENTAL_UPDATE);
    intentRecordList = this.receivedEvent.getIntentRecords();
    assertEquals(intentRecordList.size(), 1);
    validateRecordList(previousBuilder, networkPolicy.build());

    // test: delete intent0: [intent1, intent2, intent3]
    previousBuilder = networkPolicy.clone();
    networkPolicy.removeIntents(0);
    client.notify(networkPolicy.build());
    assertEquals(this.receivedEvent.getEventType(), PolicyEnforcementClient.EventType.INCREMENTAL_UPDATE);
    intentRecordList = this.receivedEvent.getIntentRecords();
    assertEquals(intentRecordList.size(), 1);
    validateRecordList(previousBuilder, networkPolicy.build());

    // test: delete intent3: [intent1, intent2]
    previousBuilder = networkPolicy.clone();
    networkPolicy.removeIntents(2);
    client.notify(networkPolicy.build());
    assertEquals(this.receivedEvent.getEventType(), PolicyEnforcementClient.EventType.INCREMENTAL_UPDATE);
    intentRecordList = this.receivedEvent.getIntentRecords();
    assertEquals(intentRecordList.size(), 1);
    validateRecordList(previousBuilder, networkPolicy.build());

    // test: add intent0 to begin, intent3,4 to end: [intent0, intent1, intent2, intent3, intent4]
    previousBuilder = networkPolicy.clone();
    networkPolicy.addIntents(0, intentList.get(0));
    networkPolicy.addIntents(3, intentList.get(3));
    networkPolicy.addIntents(4, intentList.get(4));
    client.notify(networkPolicy.build());
    assertEquals(this.receivedEvent.getEventType(), PolicyEnforcementClient.EventType.INCREMENTAL_UPDATE);
    intentRecordList = this.receivedEvent.getIntentRecords();
    assertEquals(intentRecordList.size(), 3);
    validateRecordList(previousBuilder, networkPolicy.build());

    // test: reverse list of intents: [intent4, intent3, intent2, intent1, intent0]
    previousBuilder = networkPolicy.clone();
    networkPolicy.removeIntents(0);
    networkPolicy.removeIntents(0);
    networkPolicy.removeIntents(1);
    networkPolicy.removeIntents(1);
    networkPolicy.addIntents(0, intentList.get(3));
    networkPolicy.addIntents(0, intentList.get(4));
    networkPolicy.addIntents(intentList.get(1));
    networkPolicy.addIntents(intentList.get(0));
    client.notify(networkPolicy.build());
    assertEquals(this.receivedEvent.getEventType(), PolicyEnforcementClient.EventType.INCREMENTAL_UPDATE);
    intentRecordList = this.receivedEvent.getIntentRecords();
    assertEquals(intentRecordList.size(), 4);
    validateRecordList(previousBuilder, networkPolicy.build());

    // test: swap intent3 and intent1: [intent4, intent1, intent2, intent3, intent0]
    previousBuilder = networkPolicy.clone();
    networkPolicy.removeIntents(3);
    networkPolicy.removeIntents(1);
    networkPolicy.addIntents(1, intentList.get(1));
    networkPolicy.addIntents(3, intentList.get(3));
    client.notify(networkPolicy.build());
    assertEquals(this.receivedEvent.getEventType(), PolicyEnforcementClient.EventType.INCREMENTAL_UPDATE);
    intentRecordList = this.receivedEvent.getIntentRecords();
    assertEquals(intentRecordList.size(), 2);
    validateRecordList(previousBuilder, networkPolicy.build());

    // test: delete intent3 and intent2: [intent4, intent1, intent0]
    previousBuilder = networkPolicy.clone();
    networkPolicy.removeIntents(2);
    networkPolicy.removeIntents(2);
    client.notify(networkPolicy.build());
    assertEquals(this.receivedEvent.getEventType(), PolicyEnforcementClient.EventType.INCREMENTAL_UPDATE);
    intentRecordList = this.receivedEvent.getIntentRecords();
    assertEquals(intentRecordList.size(), 2);
    validateRecordList(previousBuilder, networkPolicy.build());

    //test: empty snapshot
    previousBuilder = networkPolicy.clone();
    networkPolicy.clearIntents();
    networkPolicy.clearInventoryFilters();
    client.notify(networkPolicy.build());
    assertEquals(this.receivedEvent.getEventType(), PolicyEnforcementClient.EventType.INCREMENTAL_UPDATE);
    intentRecordList = this.receivedEvent.getIntentRecords();
    assertEquals(intentRecordList.size(), 3);
    validateRecordList(previousBuilder, networkPolicy.build());
  }

  private void validateRecordList(TetrationNetworkPolicyProto.NetworkPolicy.Builder previousBuilder,
                                  TetrationNetworkPolicyProto.NetworkPolicy expectedVersion) {
    List<PolicyEnforcementClient.IntentRecord> intentRecordList = this.receivedEvent.intentRecords;
    // loop performs updates as delivered from tnp_client
    for (PolicyEnforcementClient.IntentRecord intentRecord: intentRecordList) {
      TetrationNetworkPolicyProto.Intent newIntent = intentRecord.intent;
      switch (intentRecord.getRecordType()) {
        case CREATE:
          previousBuilder.addIntents(intentRecord.position, newIntent);
          logger.info("validateRecordList: Created intent id=" + newIntent.getId() + " at position=" + intentRecord.position);
          break;
        case UPDATE:
          previousBuilder.removeIntents(intentRecord.oldPosition);
          previousBuilder.addIntents(intentRecord.position, newIntent);
          logger.info("validateRecordList: Updated intent id=" + newIntent.getId() + " oldPosition=" +
              intentRecord.oldPosition + " newPosition=" + intentRecord.position);
          break;
        case DELETE:
          previousBuilder.removeIntents(intentRecord.oldPosition);
          logger.info("validateRecordList: Deleted intent id=" + newIntent.getId() + " at position=" + intentRecord.oldPosition);
          break;
        default:
          assertTrue(false, "Invalid record type=" + intentRecord.getRecordType());
          break;
      }
    } // eof for
    // loop compares new version against expected
    assertEquals(previousBuilder.getIntentsCount(), expectedVersion.getIntentsCount());
    for (int i = 0; i < previousBuilder.getIntentsCount(); i++) {
      TetrationNetworkPolicyProto.Intent previousIntent = previousBuilder.getIntents(i);
      TetrationNetworkPolicyProto.Intent expectedIntent = expectedVersion.getIntents(i);
      assertTrue(previousIntent.getId().equalsIgnoreCase(expectedIntent.getId()),
          "Mismatch intents id=" + previousIntent.getId() + " at position=" + i +
              " expectedId=" + expectedIntent.getId());
    } // eof for
    // loop validates next and previous
    for (PolicyEnforcementClient.IntentRecord intentRecord: intentRecordList) {
      if (intentRecord.getRecordType() != PolicyEnforcementClient.RecordType.DELETE) {
        if (intentRecord.position > 0) {
          TetrationNetworkPolicyProto.Intent previous = previousBuilder.getIntents(intentRecord.position - 1);
          assertEquals(previous.getId(), intentRecord.previousIntentId,
              " Mismatch of previousIntentId=" + previous.getId() + " expected=" + intentRecord.previousIntentId);
        } else {
          assertTrue(intentRecord.previousIntentId == null || intentRecord.previousIntentId.isEmpty());
        }
        if (intentRecord.position + 1 < previousBuilder.getIntentsCount()) {
          TetrationNetworkPolicyProto.Intent next = previousBuilder.getIntents(intentRecord.position + 1);
          assertEquals(next.getId(), intentRecord.nextIntentId,
              " Mismatch of nextIntentId=" + next.getId() + " expected=" + intentRecord.previousIntentId);
        } else {
          assertTrue(intentRecord.nextIntentId == null || intentRecord.nextIntentId.isEmpty());
        }
      }
    } // eof for
    logger.info("validateRecordList: successful");
  }

  @Test
  public void testIpv6() throws UnknownHostException {
    KafkaConsumerFactory kafkaConsumerFactoryMock = mock(KafkaConsumerFactory.class);
    assertNotEquals(null, kafkaConsumerFactoryMock);
    KafkaConsumerRunner.kafkaConsumerFactory = kafkaConsumerFactoryMock;

    KafkaConsumer<String, byte[]> kafkaConsumerMock = mock(KafkaConsumer.class);
    assertNotEquals(null, kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.createInstance(any(Properties.class))).thenReturn(kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.getTopicName(any(Properties.class))).thenReturn(this.topicName);

    PartitionInfo partitionInfo = new PartitionInfo(this.topicName, this.partitionId, null, null, null);
    when(kafkaConsumerMock.partitionsFor(any(String.class))).thenReturn(Arrays.asList(partitionInfo));

    Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> consumerRecordsData = new HashMap<>();
    when(kafkaConsumerMock.poll(anyLong())).thenReturn(new ConsumerRecords<String, byte[]>(consumerRecordsData));

    PolicyEnforcementClientImpl client = new PolicyEnforcementClientImpl();
    client.init(this.configProperties, this);

    int inventoryFilterId = 123500;
    int intentId = 123700;
    InetAddress inetAddress = InetAddress.getByName("fe80::aa11:9305:4b35:d755");
    byte[] inventoryItemIp = inetAddress.getAddress();

    List<TetrationNetworkPolicyProto.InventoryFilter> inventoryFilterList = new ArrayList<>();
    TetrationNetworkPolicyProto.PortRange.Builder portRange = TetrationNetworkPolicyProto.PortRange.newBuilder();
    portRange.setStartPort(4234);
    portRange.setEndPort(4239);

    TetrationNetworkPolicyProto.ProtocolAndPorts.Builder protocolAndPorts = TetrationNetworkPolicyProto.ProtocolAndPorts.newBuilder();
    protocolAndPorts.setProtocolValue(TetrationNetworkPolicyProto.IPProtocol.TCP_VALUE);
    protocolAndPorts.addPortRanges(portRange);

    for (int i = 0; i < 2; ++i) {
      TetrationNetworkPolicyProto.AddressWithPrefix.Builder addressWithPrefix = TetrationNetworkPolicyProto.AddressWithPrefix.newBuilder();
      addressWithPrefix.setAddrFamily(TetrationNetworkPolicyProto.IPAddressFamily.IPv4);
      addressWithPrefix.setIpAddr(ByteString.copyFrom(inventoryItemIp));
      addressWithPrefix.setPrefixLength(IPV4_ADDR_LEN);
      TetrationNetworkPolicyProto.InventoryItem.Builder inventoryItem = TetrationNetworkPolicyProto.InventoryItem.newBuilder();
      inventoryItem.setIpAddress(addressWithPrefix);
      TetrationNetworkPolicyProto.InventoryFilter.Builder inventoryFilter = TetrationNetworkPolicyProto.InventoryFilter.newBuilder();
      inventoryFilter.setId(String.valueOf(inventoryFilterId++));
      inventoryFilter.addInventoryItems(inventoryItem);
      inventoryItemIp[inventoryItemIp.length - 1]++;
      inventoryFilterList.add(inventoryFilter.build());
    }

    TetrationNetworkPolicyProto.FlowFilter.Builder flowFilter = TetrationNetworkPolicyProto.FlowFilter.newBuilder();
    flowFilter.setConsumerFilterId(String.valueOf(inventoryFilterList.get(0).getId()));
    flowFilter.setProviderFilterId(String.valueOf(inventoryFilterList.get(1).getId()));
    flowFilter.addProtocolAndPorts(protocolAndPorts);
    TetrationNetworkPolicyProto.Intent.Builder intent = TetrationNetworkPolicyProto.Intent.newBuilder();
    intent.setAction(TetrationNetworkPolicyProto.Intent.Action.ALLOW);
    intent.setId(String.valueOf(intentId++));
    intent.setFlowFilter(flowFilter);
    intent.addTargetDevices(TetrationNetworkPolicyProto.Intent.TargetDevice.MIDDLE_BOXES);

    TetrationNetworkPolicyProto.NetworkPolicy.Builder networkPolicy = TetrationNetworkPolicyProto.NetworkPolicy.newBuilder();
    networkPolicy.addAllInventoryFilters(inventoryFilterList);

    // test: add intent0 = [intent0]
    TetrationNetworkPolicyProto.NetworkPolicy.Builder previousBuilder = networkPolicy.clone();
    networkPolicy.addIntents(intent);
    client.notify(networkPolicy.build());
    assertEquals(this.receivedEvent.getEventType(), PolicyEnforcementClient.EventType.FULL_SNAPSHOT);
    List<PolicyEnforcementClient.IntentRecord> intentRecordList = this.receivedEvent.getIntentRecords();
    assertEquals(intentRecordList.size(), 1);
    validateRecordList(previousBuilder, networkPolicy.build());
  }

  @Test
  public void testCatchAllPolicy() throws UnknownHostException {
    KafkaConsumerFactory kafkaConsumerFactoryMock = mock(KafkaConsumerFactory.class);
    assertNotEquals(null, kafkaConsumerFactoryMock);
    KafkaConsumerRunner.kafkaConsumerFactory = kafkaConsumerFactoryMock;

    KafkaConsumer<String, byte[]> kafkaConsumerMock = mock(KafkaConsumer.class);
    assertNotEquals(null, kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.createInstance(any(Properties.class))).thenReturn(kafkaConsumerMock);
    when(kafkaConsumerFactoryMock.getTopicName(any(Properties.class))).thenReturn(this.topicName);

    PartitionInfo partitionInfo = new PartitionInfo(this.topicName, this.partitionId, null, null, null);
    when(kafkaConsumerMock.partitionsFor(any(String.class))).thenReturn(Arrays.asList(partitionInfo));

    Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> consumerRecordsData = new HashMap<>();
    when(kafkaConsumerMock.poll(anyLong())).thenReturn(new ConsumerRecords<String, byte[]>(consumerRecordsData));

    PolicyEnforcementClientImpl client = new PolicyEnforcementClientImpl();
    client.init(this.configProperties, this);

    TetrationNetworkPolicyProto.CatchAllPolicy.Builder catchAllBuilder = TetrationNetworkPolicyProto.CatchAllPolicy.newBuilder();
    catchAllBuilder.setAction(TetrationNetworkPolicyProto.CatchAllPolicy.Action.ALLOW);
    TetrationNetworkPolicyProto.NetworkPolicy.Builder networkPolicy = TetrationNetworkPolicyProto.NetworkPolicy.newBuilder();
    networkPolicy.setCatchAll(catchAllBuilder);

    client.notify(networkPolicy.build());
    assertEquals(this.receivedEvent.getEventType(), PolicyEnforcementClient.EventType.FULL_SNAPSHOT);
    assertNotNull(this.receivedEvent.getCatchAllPolicyRecord());
    assertEquals(this.receivedEvent.getCatchAllPolicyRecord().getRecordType(), PolicyEnforcementClient.RecordType.CREATE);
    assertEquals(this.receivedEvent.getCatchAllPolicyRecord().getCatchAllPolicy().getAction(),
        TetrationNetworkPolicyProto.CatchAllPolicy.Action.ALLOW);

    catchAllBuilder.setAction(TetrationNetworkPolicyProto.CatchAllPolicy.Action.DROP);
    networkPolicy.setCatchAll(catchAllBuilder);
    client.notify(networkPolicy.build());
    assertEquals(this.receivedEvent.getEventType(), PolicyEnforcementClient.EventType.INCREMENTAL_UPDATE);
    assertNotNull(this.receivedEvent.getCatchAllPolicyRecord());
    assertEquals(this.receivedEvent.getCatchAllPolicyRecord().getRecordType(), PolicyEnforcementClient.RecordType.UPDATE);
    assertEquals(this.receivedEvent.getCatchAllPolicyRecord().getCatchAllPolicy().getAction(),
        TetrationNetworkPolicyProto.CatchAllPolicy.Action.DROP);

    networkPolicy.clearCatchAll();
    client.notify(networkPolicy.build());
    assertEquals(this.receivedEvent.getEventType(), PolicyEnforcementClient.EventType.INCREMENTAL_UPDATE);
    assertNotNull(this.receivedEvent.getCatchAllPolicyRecord());
    assertEquals(this.receivedEvent.getCatchAllPolicyRecord().getRecordType(), PolicyEnforcementClient.RecordType.DELETE);
  }
}
