/*
 * Copyright 2017 Cisco Systems, Inc.
 * Cisco Tetration
 */

package com.tetration.network_policy.enforcement;

import com.tetration.tetration_network_policy.proto.TetrationNetworkPolicyProto;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Interface of a Tetration policy enforcement client
 * A consumer of this interface should implement the callback interface
 * EventCallback in order to retrieve notification on updates of Tetration policies
 * from Kafka. After calling the method init() the consumer will receive
 * a full snapshot of Tetration policies intended for the appliance, on which
 * this client is running on or representing. Any subsequent
 * updates of Tetration policies will be delivered to the given callback
 * as changes to previous version. Thus, the consumer is responsible to
 * handle those two event types as needed.
 */
public interface PolicyEnforcementClient {
  public static String VERSION = "0.0.1-2018-04-25";

  public enum EventType {
    FULL_SNAPSHOT,       // event data represent full snapshot of policies
                         //   typical implementation is empty current configuration,
                         //   then deploy the received policies to appliance
    INCREMENTAL_UPDATE   // event data contain only policies updates
                         //   typical handling of this event is to compare
                         //   the received changes against current state of
                         //   appliance and perform appropriate actions
  }

  /**
   * An event consists of a list of InventoryFilterRecord and IntentRecord objects.
   * Each InventoryFilter/IntentRecord has a field recordType specifying if the
   * associated InventoryFilter/Intent object has been created, updated or deleted
   * in Tetration.
   * Note the list of IntentRecord objects represents list of intents in decreasing
   * order with first match semantic. The ordering is modeled in the same order
   * as if the intents are placed in a table with position starting from zero.
   * Thus, each IntentRecord contains position property specifying the target
   * position in the table und oldPosition. In addition to the table-semantic
   * previousIntentId and nextIntentId are provided to support the organization
   * of intents in a double-linked list manner.
   */
  public enum RecordType {
    CREATE,  // create a new intent
    UPDATE,  // update an existing intent
    DELETE   // delete an intent
  }

  /**
   * An InventoryFilter record represents a Tetration inventory filter and the
   * operation (create, update or delete) performed. Note that the provided
   * inventory filter object has its latest state.
   */
  public static class InventoryFilterRecord {
    /**
     * Return RecordType/operation (create, update or delete) performed
     * to the associated intent object
     *
     * @return type of this record object
     */
    public RecordType getRecordType() {
      return this.recordType;
    }

    /**
     * Return Inventory Filter object
     *
     * @return Tetration inventory filter object associated with this record
     */
    public TetrationNetworkPolicyProto.InventoryFilter getInventoryFilter() {
      return this.inventoryFilter;
    }

    public InventoryFilterRecord(RecordType recordType, TetrationNetworkPolicyProto.InventoryFilter inventoryFilter) {
      this.recordType = recordType;
      this.inventoryFilter = inventoryFilter;
    }

    protected RecordType recordType;
    protected TetrationNetworkPolicyProto.InventoryFilter inventoryFilter;

  }

  /**
   * An Intent record represents a Tetration intent and the operation (create,
   * update or delete) performed plus its target position as the intents be
   * placed in a table/array. Previous and next intent's id are provided
   * to support double-linked list implementation of intents. Note that
   * the provided intent object has its latest state.
   */
  public static class IntentRecord {
    /**
     * Return RecordType/operation (create, update or delete) performed
     * to the associated intent object
     *
     * @return type of this record object
     */
    public RecordType getRecordType() {
      return this.recordType;
    }

    /**
     * Return Intent object
     *
     * @return Tetration intent object associated with this record
     */
    public TetrationNetworkPolicyProto.Intent getIntent() {
      return this.intent;
    }

    /**
     * Return previous intent's id
     * @return previous intent's id
     */
    public String getPreviousIntentId() {
      return this.previousIntentId;
    }

    /**
     * Return intent's target position
     * Note that target position is not applicable for DELETE record type
     * @return intent's target position
     */
    public int getPosition() {
      return this.position;
    }

    /**
     * Return intent's old position, ie the current position before applying
     * the indicated modification
     * Note that old position is not applicable for CREATE record type
     * @return intent's position
     */
    public int getOldPosition() {
      return this.oldPosition;
    }

    /**
     * Return next intent's id
     * @return next intent's id
     */
    public String getNextIntentId() {
      return this.nextIntentId;
    }

    public IntentRecord(RecordType recordType, TetrationNetworkPolicyProto.Intent intent) {
      this.recordType = recordType;
      this.intent = intent;
      this.position = -1;
      this.oldPosition = -1;
      this.previousIntentId = null;
      this.nextIntentId = null;
    }

    protected RecordType recordType;
    protected TetrationNetworkPolicyProto.Intent intent;

    protected int position;
    protected int oldPosition;
    protected String previousIntentId;
    protected String nextIntentId;

  }

  /**
   * A CatchAllPolicy record represents the catch all rule and
   * operation (create, update or delete) performed. With regards
   * to policy enforcement the catch all rule, if present, specifies
   * the action to be taken when no intent has matched with network flows.
   */
  public static class CatchAllPolicyRecord {
    /**
     * Return RecordType/operation (create, update or delete) performed
     * to the associated CatchAllPolicy object
     *
     * @return type of this record object
     */
    public RecordType getRecordType() {
      return this.recordType;
    }

    /**
     * Return CatchAllPolicy object
     *
     * @return Tetration CatchAll object associated with this record
     */
    public TetrationNetworkPolicyProto.CatchAllPolicy getCatchAllPolicy() {
      return this.catchAllPolicy;
    }

    public CatchAllPolicyRecord(RecordType recordType, TetrationNetworkPolicyProto.CatchAllPolicy catchAllPolicy) {
      this.recordType = recordType;
      this.catchAllPolicy= catchAllPolicy;
    }

    protected RecordType recordType;
    protected TetrationNetworkPolicyProto.CatchAllPolicy catchAllPolicy;

  }

  /**
   * Event data provided to EventCallback
   */
  public static class Event {
    /**
     * Return event type, ie FULL_SNAPSHOT or INCREMENTAL_UPDATE
     *
     * @return event type, ie FULL_SNAPSHOT or INCREMENTAL_UPDATE
     */
    public EventType getEventType() {
      return this.eventType;
    }

    /**
     * Return list of IntentRecord objects for this event<br>
     * Note that the order of returned intents is priority based and in
     * reverse order. That means the first intent has highest priority,
     * while the last one's priority is lowest. Enforcement implementation
     * shall consider this first match approach.
     *
     * @return list of IntentRecord objects
     */
    public List<IntentRecord> getIntentRecords() {
      return intentRecords;
    }

    /**
     * Return list of InventoryFilterRecord objects for this event<br>
     * The inventory filter objects are referred by intent objects per their
     * id string. Any changes of inventory filter objects will be reported
     * by the returned list.
     * @return list of InventoryFilterRecord objects
     */
    public List<InventoryFilterRecord> getInventoryFilterRecords() {
      return inventoryFilterRecords;
    }

    /**
     * Return catch all policy record for this event
     * @return CatchAllPolicy object
     */
    public CatchAllPolicyRecord getCatchAllPolicyRecord() {
      return catchAllPolicyRecord;
    }

    /**
     * Return epoch time when this event object has been generated
     * Note that this timestamp is not equal to the time when backend
     * sent policies to Kafka
     * @return epoch time when this event object has been generated
     */
    public long getTimeInMillis() {
      return this.timeInMillis;
    }

    public Event(EventType evType, List<InventoryFilterRecord> inventoryFilterRecords,
          List<IntentRecord> intentRecords) {
      this.eventType = evType;
      this.inventoryFilterRecords = new LinkedList<>();
      if (inventoryFilterRecords != null) {
        this.inventoryFilterRecords.addAll(inventoryFilterRecords);
      }
      this.intentRecords = new LinkedList<>();
      if (intentRecords != null) {
        this.intentRecords.addAll(intentRecords);
      }
    }

    Event(EventType evType) {
      this(evType, null, null);
    }

    EventType eventType;
    List<IntentRecord> intentRecords;
    List<InventoryFilterRecord> inventoryFilterRecords;
    CatchAllPolicyRecord catchAllPolicyRecord;
    long timeInMillis;
  }

  /**
   * Callback interface to be implemented by consumer (usually appliance vendor)
   */
  public static interface EventCallback {
    /**
     * Called by Tetration policy enforcement client when it receives updates
     * of Tetration policies from Kafka.<br>
     * Note that the first event after a call to PolicyEnforcementClient.init()
     * always delivers a full snapshot of Tetration policies intended for this
     * appliance. Subsequent events will provide incremental changes against
     * previously received event.
     *
     * @param event
     */
    void notify(Event event);
  }

  /**
   * This method must be called by consumer during its init phase
   * in order to register a notification callback for policies updates.<br>
   * The given configProperties must provide the following data:
   * <dl>
   * <dt>tetration.user.credential</dt>
   * <dd>This specifies the directory, in which the Kafka data tap's
   * client certificate tar.gz file was extracted to. This method needs
   * read access to all extracted files in order to obtain Tetration root scope
   * and to access the allowed Kafka topic in order to receive Tetration network policies.
   * </dd>
   * <dt>appliance.inventoryItem</dt>
   * <dd>This specifies the appliance addresses. Only Intents matched against
   * them will be delivered to the given callback. This property value must
   * be either an object of
   * com.tetration.tetration_network_policy.proto.TetrationNetworkPolicyProto.InventoryItem,
   * which abstracts a single network address, subnet or address range,
   * or an array of InventoryItem objects
   * </dd>
   * </dl>
   *
   * @param configProperties
   * @param callback
   */
  public void init(Properties configProperties, EventCallback callback);

  /**
   * Stop event notification and release eg Kafka resources
   */
  public void release();

  /**
   * Return current network policy consisting of inventory filters and intents
   * matched with appliance address
   */
  public TetrationNetworkPolicyProto.NetworkPolicy getCurrentNetworkPolicy();

}
