/*
 * Copyright 2017 Cisco Systems, Inc.
 * Cisco Tetration
 */

package com.tetration.network_policy.enforcement;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tetration.tetration_network_policy.proto.TetrationNetworkPolicyProto.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka consumer runner handles receiving of Tetration messages by running one
 * receiver thread for all partitions in background. Even though only one partition
 * is used by tnp-publisher at the time being, this implementation supports multiple
 * partitions. The only requirement is that messages belonging to a snapshot of
 * Tetration policies need to be sent to the same partition and in order.
 * Once all messages belonging to a full snapshot of Tetration network policies
 * have been detected, the receiver thread will notify the given callback with the
 * entire list of received policies.
 * This class is used internally by PolicyEnforcementClient to retrieve the
 * list of policies. Post-processing of the same is done entirely by
 * PolicyEnforcementClient.
 */
public class KafkaConsumerRunner {
  private static Logger logger = LoggerFactory.getLogger(KafkaConsumerRunner.class);

  protected static KafkaConsumerFactory kafkaConsumerFactory = new KafkaConsumerFactory();

  /**
   * Callback to notify a full snapshot of received network policies
   */
  public interface PollCallback {
    void notify(TenantNetworkPolicy tenantNetworkPolicy);
  }

  /**
   * Construct kafka consumer runner and start consumer thread
   *
   * @param configProperties
   * @param callback
   */
  public KafkaConsumerRunner(Properties configProperties,
                             PollCallback callback) {
    this.configProperties = (Properties) configProperties.clone();
    this.partitionStates = new HashMap<>();
    this.pollCallback = callback;

    this.createAndInitKafkaConsumer();
    // start poller thread
    this.consumerThread = new Thread(new ConsumerThread());
    this.consumerThread.start();
    logger.info("Started kafka poller thread");
  }

  /**
   * Stop poller thread
   * This method sets the shutdown flag to signal poller thread to terminate
   * and waits until the poller thread exits
   */
  public void stopPoller() {
    logger.debug("Requesting kafka poller thread to terminate");
    shutdown.set(true);
    consumer.wakeup();
    try {
      this.consumerThread.join();
      logger.info("Kafka poller thread terminated");
    } catch (InterruptedException e) {
      e.printStackTrace();
      logger.error(e.getMessage());
    }
  }

  /**
   * Return topic name used for this Kafka consumer
   * @return Kafka topic name
   */
  public String getTopicName() {
    return this.topicName;
  }

  /**
   * Check if kafka consumer thread is running
   * @return true if kafka consumer thread is running
   */
  public boolean isConsumerThreadAlive() {
    return this.consumer != null && this.consumerThread != null && this.consumerThread.isAlive();
  }

  /**
   * Called by constructor and consumer thead to recreated kafka consumer and
   * initialize partition states and rewind position flags
   */
  private void createAndInitKafkaConsumer() {
    this.consumer = kafkaConsumerFactory.createInstance(this.configProperties);
    if (this.consumer == null) {
      throw new IllegalStateException("Could not create kafkaConsumer");
    }
    logger.info("Created kafka consumer to use tnp-publisher's messaging");

    this.topicName = kafkaConsumerFactory.getTopicName(this.configProperties);
    this.partitionStates.clear();

    // assign all partitions to this consumer as we need the seek function
    List<PartitionInfo> partitionInfos = this.consumer.partitionsFor(this.topicName);
    if (partitionInfos == null) {
      throw new IllegalArgumentException("Could not retrieve partition infos for topic=" + this.topicName);
    }
    List<TopicPartition> partitions = new ArrayList<>();
    for (PartitionInfo partitionInfo : partitionInfos) {
      TopicPartition aPartition = new TopicPartition(this.topicName,
          partitionInfo.partition());
      partitions.add(aPartition);
      this.partitionStates.put(Integer.valueOf(partitionInfo.partition()),
          new KafkaPartitionState());
    }
    this.consumer.assign(partitions);
    logger.debug("Assigned " + partitions.size() + " partitions to kafka consumer");
    // rewind all partitions offset to last message
    Map<TopicPartition, Long> endOffsets = this.consumer.endOffsets(partitions);
    for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
      long newOfs = entry.getValue().longValue() - 1;
      if (newOfs < 0) { // this partition is empty
        continue;
      }
      this.consumer.seek(entry.getKey(), newOfs);
      logger.debug("Set partition #" + entry.getKey() + "'s offset to " + newOfs);
    }
  }

  /**
   * This class is to track if a kafka partition needs to reset to first message
   * of last full snapshot, which is needed only at init, and to cache received
   * messages till end-message of full snapshot is detected.
   */
  private class KafkaPartitionState {
    // if set, we need to rewind to 1st msg of full snapshot
    public boolean rewindFirstMsg;
    /* cache pending messages of full snapshot until we see END message
     * Note tnp-publisher sends a full snapshot as a sequence of kafka messages
     * beginning with a START tag, followed by multiple UPDATE tag or none and
     * ending with an END tag. If a START tag is seen without a preceding END tag,
     * all received messages will be dropped.
     */
    public List<KafkaUpdate> pendingKafkaUpdates;

    public KafkaPartitionState() {
      this.rewindFirstMsg = true;
      this.pendingKafkaUpdates = new ArrayList<>();
    }

    /* called by poller thread if end-message is detected
     * this method consolidates all received network policies into one
     * network policy object and then calls the callback
     */
    public void processSnapshot() {
      KafkaUpdate firstKafkaUpdate = pendingKafkaUpdates.get(0);
      if (firstKafkaUpdate.getType() != KafkaUpdate.UpdateType.UPDATE_START) {
        logger.error("First message does not have START flag");
        return; // ignore this broken snapshot
      }
      long idx = 0;
      TenantNetworkPolicy.Builder tenantNetworkPolicy = TenantNetworkPolicy.newBuilder();
      NetworkPolicy.Builder networkPolicy = NetworkPolicy.newBuilder();
      for (KafkaUpdate kafkaUpdate : pendingKafkaUpdates) {
        if (idx != kafkaUpdate.getSequenceNum()) {
          logger.error("Message with invalid sequence nr=" + kafkaUpdate.getSequenceNum() +
              " found. Expected is " + idx);
          return; // ignore this inconsistent snapshot
        }
        TenantNetworkPolicy tnp = kafkaUpdate.getTenantNetworkPolicy();
        if (tnp.getNetworkVrfsCount() > 0) {
          tenantNetworkPolicy.addAllNetworkVrfs(tnp.getNetworkVrfsList());
        }
        if (tnp.getRootScopeId() != null && !tnp.getRootScopeId().isEmpty()) {
          tenantNetworkPolicy.setRootScopeId(tnp.getRootScopeId());
        }
        if (tnp.getTenantName() != null && !tnp.getTenantName().isEmpty()) {
          tenantNetworkPolicy.setTenantName((tnp.getTenantName()));
        }
        if (tnp.getScopesCount() > 0) {
          tenantNetworkPolicy.putAllScopes(tnp.getScopesMap());
        }
        for (NetworkPolicy np : tnp.getNetworkPolicyList()) {
          networkPolicy.addAllIntents(np.getIntentsList());
          networkPolicy.addAllInventoryFilters(np.getInventoryFiltersList());
          if (np.hasCatchAll()) {
            networkPolicy.setCatchAll(np.getCatchAll());
          }
        }
        idx++;
      }
      logger.debug("Calling callback from " + KafkaPartitionState.class.getName());
      try {
        tenantNetworkPolicy.addNetworkPolicy(networkPolicy);
        pollCallback.notify(tenantNetworkPolicy.build());
        logger.info("Callback from " + KafkaPartitionState.class.getName() + " returned");
      } catch (Exception e) {
        logger.error("Exception occurred in callback: " + e.getMessage());
      }
    }
  }

  private String topicName;
  private Properties configProperties;
  private KafkaConsumer<String, byte[]> consumer;
  private Map<Integer, KafkaPartitionState> partitionStates;
  private PollCallback pollCallback;
  private Thread consumerThread;
  // flag to request consumerThread to terminate
  private final AtomicBoolean shutdown = new AtomicBoolean(false);

  // kafka consumer thread proc to poll kafka messaging queue
  private class ConsumerThread implements Runnable {

    public void run() {
      final int waitTimeMillis = 1000;
      final int reconnectMaxCount = 5 * 60000 / waitTimeMillis; // try to reconnect for 5 min
      int reconnectCount = 0;

      logger.info("Started " + ConsumerThread.class.getName());

      while (!shutdown.get()) {
        if (consumer != null) {
          try {
            this.pollKafkaMessages();
            reconnectCount = 0; // connect and poll succeeded, reset counter
            continue; // things are alright
          } catch (OutOfMemoryError e) {
            logger.error("Exit thread proc: " + e.getMessage());
            closeKafkaConsumer();
            return;                // there's not much thing we can do if OOO happens
          } catch (WakeupException e) {
            if (shutdown.get()) {  // ignore exc if shutdown requested
              closeKafkaConsumer();
              return;              // terminate thread
            }
            logger.error("Exception occurred from kafka: " + e.getMessage());
          } catch (InvalidProtocolBufferException e) {
            logger.error("Protobuf exception occurred: " + e.getMessage());
          } catch (Exception e) {
            logger.error("Unexpected exception occurred: " + e.getMessage());
          } // eof try-catch
        }

        reconnectCount++;
        if (reconnectCount >= reconnectMaxCount) {
          logger.error("Max retries to connect Kafka failed. Exit thread proc");
          return;
        }

        // we get to this line only in case of exception: try reconnect kafka
        consumer = null;
        try {
          logger.info("Wait " + waitTimeMillis + " msec before reconnecting Kafka");
          Thread.sleep(waitTimeMillis);
          createAndInitKafkaConsumer();
        } catch (InterruptedException e) {
          logger.error("Exit thread proc: " + e.getMessage());
          break;
        } catch (Exception e) {
          logger.error("Reconnect Kafka failed: " + e.getMessage());
        }
      } // eof while
    }

    private void closeKafkaConsumer() {
      try {
        consumer.close();
        logger.info("Closed kafka consumer and exit thread proc");
      } catch (Exception e) {
        logger.error("Failed to close kafka consumer");
      }
    }

    private void pollKafkaMessages() throws InvalidProtocolBufferException {
      /* map partitionId to sequence number of last message seen
       * needed to calculate the offset of first message of
       * full snapshot
       */
      Map<Integer, Integer> rewindPartitions = new HashMap<>();

      ConsumerRecords<String, byte[]> records = consumer.poll(5000);
      if (records == null) {
        throw new IllegalStateException("records returned by poll is null");
      }
      if (records.count() > 0) {
        logger.debug("Kafka poll returns " + records.count() + " records");
      } else {
        logger.debug("NO messages received");
      }

      rewindPartitions.clear();
      /* loop collects received messages of full snapshot and notifies
       * callback if END tag is seen
       */
      for (ConsumerRecord<String, byte[]> record : records) {
        final Integer partitionId = Integer.valueOf(record.partition());
        KafkaPartitionState partitionState = partitionStates.get(partitionId);
        KafkaUpdate kafkaUpdate = KafkaUpdate.parseFrom(record.value());
        if (partitionState.rewindFirstMsg) {
          if (kafkaUpdate.getTypeValue() != KafkaUpdate.UpdateType.UPDATE_START_VALUE) { // this is not START message
            if (kafkaUpdate.getSequenceNum() == 0) {
              logger.error("Invalid sequence nr=0 for message with tag=" +
                  kafkaUpdate.getTypeValue() + " detected");
              // try with next message
            } else {
              rewindPartitions.put(partitionId, Integer.valueOf(kafkaUpdate.getSequenceNum()));
            }
            continue;
          }
          // this is START message, no need to rewind position
          partitionState.rewindFirstMsg = false;
          rewindPartitions.remove(partitionId);
        }

        if (kafkaUpdate.getTypeValue() == KafkaUpdate.UpdateType.UPDATE_START_VALUE) {
          logger.debug("Seen START record, partition id=" + partitionId);
          if (!partitionState.pendingKafkaUpdates.isEmpty()) {
            int lastIndex = partitionState.pendingKafkaUpdates.size() - 1;
            KafkaUpdate lastKafkaUpdate = partitionState.pendingKafkaUpdates.get(lastIndex);
            if (lastKafkaUpdate.getTypeValue() != KafkaUpdate.UpdateType.UPDATE_END_VALUE) {
              logger.warn("Removing " + partitionState.pendingKafkaUpdates.size() +
                  " messages without closing END, partition id=" + partitionId);
            }
            partitionState.pendingKafkaUpdates.clear();
          }
        }
        // append message to partition state's message list
        partitionState.pendingKafkaUpdates.add(kafkaUpdate);
      } // eof for

      // loop notifies callback if received END message
      for (Map.Entry<Integer, KafkaPartitionState> entry : partitionStates.entrySet()) {
        KafkaPartitionState partitionState = entry.getValue();
        if (rewindPartitions.containsKey(entry.getKey()) || partitionState.pendingKafkaUpdates.isEmpty()) {
          continue;
        }
        int lastIndex = partitionState.pendingKafkaUpdates.size() - 1;
        KafkaUpdate lastKafkaUpdate = partitionState.pendingKafkaUpdates.get(lastIndex);
        if (lastKafkaUpdate.getTypeValue() == KafkaUpdate.UpdateType.UPDATE_END_VALUE) {
          logger.debug("Seen END record, partition id=" + entry.getKey());
          partitionState.processSnapshot();
          partitionState.pendingKafkaUpdates.clear();
        }
      }

      // seek offset to first message if needed
      for (Map.Entry<Integer, Integer> entry : rewindPartitions.entrySet()) {
        KafkaPartitionState partitionState = partitionStates.get(entry.getKey());
        partitionState.rewindFirstMsg = false;
        TopicPartition partition = new TopicPartition(KafkaConsumerRunner.this.topicName,
            entry.getKey());
        long startOffset = consumer.position(partition) - entry.getValue().intValue() - 1;
        // move to first message of snapshot
        consumer.seek(partition, startOffset);
      }
    }
  }
}
