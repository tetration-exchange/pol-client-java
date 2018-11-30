/*
 * Copyright 2017 Cisco Systems, Inc.
 * Cisco Tetration
 */

package com.tetration.network_policy.enforcement;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Helper class to create a KafkaConsumer object with configuration specific to
 * tnp-publisher
 */
public class KafkaConsumerFactory {
  private static Logger logger = LoggerFactory.getLogger(KafkaConsumerFactory.class);

  /**
   * Create kafka consumer connecting tnp-publisher's kafka instance
   * Required config properties:
   * <ul>
   *   <li>tetration.user.credential - specifies local directory path,
   *   where the client certificates files are stored</li>
   * </ul>
   *
   * @param configProperties
   * @return instance of KafkaConsumer&lt;String, byte[]&gt;
   */
  public KafkaConsumer<String, byte[]> createInstance(Properties configProperties) {
    final String clientCertDirLocation = configProperties.getProperty("tetration.user.credential");
    if (clientCertDirLocation == null || clientCertDirLocation.isEmpty()) {
      throw new IllegalArgumentException("Dir location of client certificate files must be given");
    }

    final String brokerFileName = clientCertDirLocation + File.separator + "kafkaBrokerIps.txt";
    final String passphraseFilename = clientCertDirLocation + File.separator + "passphrase.txt";
    final String truststoreFileName = clientCertDirLocation + File.separator + "truststore.jks";
    final String keystoreFileName = clientCertDirLocation + File.separator + "keystore.jks";
    String passphrase;
    String kafkaBrokers;

    try {
      passphrase = readFile(passphraseFilename);
      if (passphrase == null || passphrase.isEmpty()) {
        logger.error("Reading passphrase from " + passphraseFilename + " failed");
        return null;
      }
      kafkaBrokers = readFile(brokerFileName);
      if (kafkaBrokers == null || kafkaBrokers.isEmpty()) {
        logger.error("Reading Kafka Brokers from " + brokerFileName + " failed");
        return null;
      }
    } catch (Exception ex) {
      logger.error("Read kafka config files failed due to exception " + ex.getMessage());
      return null;
    }

    String topicName = this.getTopicName(configProperties);
    if (topicName == null) {
      // error log already written by method call
      return null;
    }
    // extract group id from topic name
    String[] topicNameParts = topicName.split("-");
    if (topicNameParts.length < 2) {
      throw new IllegalArgumentException("Could not extract group id from topic name=" + topicName);
    }

    String zkIPs = getZooKeeperIPs(kafkaBrokers);
    logger.debug("zookeeper broker IPs=" + zkIPs);

    Properties props = new Properties();
    props.put("bootstrap.servers", kafkaBrokers);
    props.put("group.id", topicNameParts[topicNameParts.length - 1]);
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("enable.auto.commit", "false");
    props.put("security.protocol", "SSL");
    props.put("ssl.truststore.location", truststoreFileName);
    props.put("ssl.truststore.password", passphrase);
    props.put("ssl.keystore.location", keystoreFileName);
    props.put("ssl.keystore.password", passphrase);
    props.put("ssl.key.password", passphrase);
    props.put("zookeeper.connect", zkIPs);
    props.put("zookeeper.session.timeout.ms", "500");
    props.put("zookeeper.sync.time.ms", "250");
    props.put("auto.offset.reset", "latest");
    props.put("fetch.message.max.bytes", "10485760"); //10MB

    try {
      // retry with timeout of 120 secs
      return new RetryCallable<KafkaConsumer<String, byte[]>>("createKafkaConsumer", 12 , 10) {
        @Override
        public KafkaConsumer<String, byte[]> call() {
          return new KafkaConsumer<>(props);
        }
      }.retry();
    } catch (Exception e) {
      logger.error(e.getMessage());
      return null;
    }
  }

  /**
   * Read topic.txt from client certificate file dir
   * @param configProperties
   * @return Kafka topic name
   */
  public String getTopicName(Properties configProperties) {
    final String clientCertDirLocation = configProperties.getProperty("tetration.user.credential");
    if (clientCertDirLocation == null || clientCertDirLocation.isEmpty()) {
      throw new IllegalArgumentException("Dir location of client certificate files must be given");
    }

    final String topicFileName = clientCertDirLocation + File.separator + "topic.txt";

    try {
      String topicName = readFile(topicFileName);
      if (topicName == null || topicName.isEmpty()) {
        logger.error("Reading topic name from " + topicFileName + " failed");
        return null;
      }
      return topicName.trim();
    } catch (Exception ex) {
      logger.error("Failed to read file " + topicFileName +": " + ex.getMessage());
      return null;
    }
  }

  private static String getZooKeeperIPs(String kafkaBroker) {
    final String internalPort = "9092";
    final String externalPort = "9093";
    final String zkPort = "2181";
    if (kafkaBroker.contains(internalPort)) {
      return kafkaBroker.replaceAll(internalPort, zkPort);
    } else if (kafkaBroker.contains(externalPort)) {
      return kafkaBroker.replaceAll(externalPort, zkPort);
    }
    return "";
  }

  /**
   * Read given file and return its (byte[]) content as String
   *
   * @param fileName
   * @return String
   */
  private static String readFile(String fileName) {
    try {
      return new String(Files.readAllBytes(Paths.get(fileName)));
    } catch (IOException e) {
      logger.error("Could not read file " + fileName + ": " + e.getMessage());
      return null;
    }
  }
}
