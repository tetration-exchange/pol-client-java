/*
 * Copyright 2017 Cisco Systems, Inc.
 * Cisco Tetration
 */

package com.tetration.network_policy.enforcement;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.tetration.tetration_network_policy.proto.TetrationNetworkPolicyProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Implementation of PolicyEnforcementClient
 */
public class PolicyEnforcementClientImpl
    implements PolicyEnforcementClient, KafkaConsumerRunner.PollCallback {
  public static String VERSION = "0.0.1-2018-04-25";
  private static Logger logger = LoggerFactory.getLogger(PolicyEnforcementClientImpl.class);

  /* for debugging purpose:
   * if set, method notify() will dump network policy as received from Kafka
   */
  public static boolean traceNetworkPolicy = false;

  @Override
  public void init(Properties configProperties, EventCallback callback) {
    if (this.runState != RunState.UNKNOWN) {
      throw new IllegalStateException("Client object still active runState=" + this.runState);
    }
    Object applianceInventoryItemObject = configProperties.get("appliance.inventoryItem");
    if (applianceInventoryItemObject == null) {
      throw new IllegalArgumentException("configProperty \"appliance.inventoryItem\" must be given");
    }
    if (applianceInventoryItemObject instanceof TetrationNetworkPolicyProto.InventoryItem) {
      this.applianceInventoryItemList =  new ArrayList<>();
      this.applianceInventoryItemList.add((TetrationNetworkPolicyProto.InventoryItem) applianceInventoryItemObject);
    } else {
      // make sure we got an array of InventoryItem
      Class objectCls = applianceInventoryItemObject.getClass();
      if (objectCls.isArray() && objectCls.getComponentType().getName().equals(
          TetrationNetworkPolicyProto.InventoryItem.class.getName())) {
        this.applianceInventoryItemList =  Arrays.asList(
            (TetrationNetworkPolicyProto.InventoryItem[])applianceInventoryItemObject);
      } else {
        throw new IllegalArgumentException("configProperty \"appliance.inventoryItem\" must be an array " +
            "or instance of " + TetrationNetworkPolicyProto.InventoryItem.class.getName());
      }
    }
    this.kafkaConsumerRunner = new KafkaConsumerRunner(configProperties, this);
    this.eventCallback = callback;
    this.runState = RunState.ACTIVE;
    logger.info("Created kafka consumer object successfully");
  }

  @Override
  public void release() {
    if (this.runState != RunState.ACTIVE) {
      throw new IllegalStateException("Client object is not active runState=" + this.runState);
    }
    this.runState = RunState.RELEASE_REQUESTED;
    // shutdown kafka consumer
    this.kafkaConsumerRunner.stopPoller();
    this.currentIntentList.clear();
    this.currentIntentMap.clear();
    this.runState = RunState.UNKNOWN;
    logger.info("Released object of " + PolicyEnforcementClientImpl.class.getName());
  }

  @Override
  public TetrationNetworkPolicyProto.NetworkPolicy getCurrentNetworkPolicy() {
    Lock rLock = this.currentIntentsLock.readLock();
    try {
      if (!rLock.tryLock(5, TimeUnit.SECONDS)) {
        logger.error("Could not acquire readLock");
        return null;
      }
    } catch (InterruptedException e) {
      logger.error("Unexpected InterruptedException: " + e.getMessage());
      return null;
    }
    try {
      TetrationNetworkPolicyProto.NetworkPolicy.Builder builder =
          TetrationNetworkPolicyProto.NetworkPolicy.newBuilder();
      for (TetrationNetworkPolicyProto.InventoryGroup inventoryFilter: this.currentInventoryMap.values()) {
        // no copy of inventory filter necessary as protobuf objects are immutable
        builder.addInventoryFilters(inventoryFilter);
      }
      for (TetrationNetworkPolicyProto.Intent intent: this.currentIntentList) {
        // no copy of intent necessary as protobuf objects are immutable
        builder.addIntents(intent);
      }
      builder.setCatchAll(this.currentCatchAllPolicy);
      return builder.build();
    } catch (Exception e) {
      logger.error("Unexpected exception: " + e.getMessage());
      return null;
    } finally {
      rLock.unlock();
    }
  }

  public PolicyEnforcementClientImpl() {
    this.runState = RunState.UNKNOWN;
    this.currentIntentList = new LinkedList<>();
    this.currentIntentMap = new HashMap<>();
    this.currentInventoryMap = new HashMap<>();
    this.currentIntentsLock = new ReentrantReadWriteLock();
    this.errorList = new ArrayList<>();
    this.currentTenantPolicyMetaData = new TenantPolicyMetaData();
  }

  @Override
  public void notify(TetrationNetworkPolicyProto.TenantNetworkPolicy tenantNetworkPolicy) {
    // the provided tenantNetworkPolicy has only one networkPolicy element
    TetrationNetworkPolicyProto.NetworkPolicy networkPolicy = tenantNetworkPolicy.getNetworkPolicy(0);
    if (traceNetworkPolicy) {
      JsonFormat.Printer printer = JsonFormat.printer();
      try {
        logger.info("[BEGIN]\n" + printer.print(tenantNetworkPolicy) + "\n[END]\n");
      } catch (InvalidProtocolBufferException e) {
        logger.error("protobuf format error: " + e.getMessage());
      }
    }
    Lock wLock = this.currentIntentsLock.writeLock();
    try {
      if (!wLock.tryLock(10, TimeUnit.SECONDS)) {
        logger.error("Could not acquire writeLock");
        return;
      }
    } catch (InterruptedException e) {
      logger.error("Unexpected InterruptedException: " + e.getMessage());
      return;
    }
    Event result;
    try {
      if (this.currentIntentList.isEmpty() && this.currentCatchAllPolicy == null) {
        result = new Event(EventType.FULL_SNAPSHOT);
        // this is a full snapshot for first time or no matched intents found previously
        this.calculateCurrentSnapshot(networkPolicy);
        for (TetrationNetworkPolicyProto.InventoryGroup inventoryFilter: this.currentInventoryMap.values()) {
          InventoryFilterRecord inventoryFilterRecord = new InventoryFilterRecord(RecordType.CREATE, inventoryFilter);
          result.inventoryFilterRecords.add(inventoryFilterRecord);
          logger.info("InventoryFilterRecord: add inventory filter id=" + inventoryFilter.getId());
        }
        int position = 0;
        IntentRecord previousIntentRecord = null;
        for (TetrationNetworkPolicyProto.Intent intent : this.currentIntentList) {
          IntentRecord currentIntentRecord = new IntentRecord(RecordType.CREATE, intent);
          currentIntentRecord.position = position;
          currentIntentRecord.oldPosition = position;
          result.intentRecords.add(currentIntentRecord);
          if (previousIntentRecord != null) {
            previousIntentRecord.nextIntentId = intent.getId();
            currentIntentRecord.previousIntentId = previousIntentRecord.intent.getId();
          }
          previousIntentRecord = currentIntentRecord;
          logger.info("IntentRecord: add new intent id=" + intent.getId() + " at position=" + position);
          position++;
        }
        result.catchAllPolicyRecord = computeCatchAllPolicyDiff(null);
      } else {
        result = this.computeDeltaList(networkPolicy);
      }
    } finally {
      wLock.unlock();
    }

    this.currentTenantPolicyMetaData.tenantName = tenantNetworkPolicy.getTenantName();
    this.currentTenantPolicyMetaData.rootScopeId = tenantNetworkPolicy.getRootScopeId();
    this.currentTenantPolicyMetaData.networkVrfs = tenantNetworkPolicy.getNetworkVrfsList();
    this.currentTenantPolicyMetaData.scopes = tenantNetworkPolicy.getScopesMap();

    // notify client only if there's changes
    if (!result.intentRecords.isEmpty() || !result.inventoryFilterRecords.isEmpty() ||
        result.getCatchAllPolicyRecord() != null) {
      this.eventCallback.notify(result);
    }
  }

  @Override
  public TenantPolicyMetaData getCurrentTenantPolicyMetaData() {
    return this.currentTenantPolicyMetaData;
  }

  /**
   * Calculate intents changes (delta computation)
   */
  private Event computeDeltaList(TetrationNetworkPolicyProto.NetworkPolicy networkPolicy) {
    Event result = new Event(EventType.INCREMENTAL_UPDATE);
    /* calculate delta since last update
     * 1) save current list of matched inventory filters and intents: this is old version
     * 2) calculate list of matched intents derived from new snapshot: this becomes current version
     * 3) iterate list of inventory filters, compare old vs current and generate updates for inventoryRecords
     * 4) iterate old list of intents, if old intent is not in current list, generate intentRecord for delete
     *    after this loop old list is a subset of current list
     * 5) iterate current list of intents, if current intent is not in old list, generate intentRecord for create
     *    after this loop both old and current list have same length, and old set == current set
     * 6) iterate current list of intents, compare with old intent at same position, generate intentRecord for update
     *    if content is different or position has changed
     * NOTE if there are many order changes the result may not be optimal because current code
     * does not perform a full DP as this is more time consuming
     */
    // step 1)
    List<TetrationNetworkPolicyProto.Intent> oldIntentList = this.currentIntentList;
    Map<String, TetrationNetworkPolicyProto.Intent> oldIntentMap = this.currentIntentMap;
    Map<String, TetrationNetworkPolicyProto.InventoryGroup> oldInventoryMap = this.currentInventoryMap;
    TetrationNetworkPolicyProto.CatchAllPolicy oldCatchAllPolicy = this.currentCatchAllPolicy;
    // step 2)
    this.calculateCurrentSnapshot(networkPolicy);
    // step 3)
    for (TetrationNetworkPolicyProto.InventoryGroup inventoryFilter: this.currentInventoryMap.values()) {
      TetrationNetworkPolicyProto.InventoryGroup oldInventoryFilter = oldInventoryMap.get(inventoryFilter.getId());
      if (oldInventoryFilter != null) {
        if (!ProtoHelper.isEqual(oldInventoryFilter, inventoryFilter)) {
          InventoryFilterRecord inventoryFilterRecord = new InventoryFilterRecord(RecordType.UPDATE, inventoryFilter);
          result.inventoryFilterRecords.add(inventoryFilterRecord);
          logger.info("InventoryFilterRecord: update inventory filter id=" + inventoryFilter.getId());
        }
        oldInventoryMap.remove(inventoryFilter.getId());
      } else {
        InventoryFilterRecord inventoryFilterRecord = new InventoryFilterRecord(RecordType.CREATE, inventoryFilter);
        result.inventoryFilterRecords.add(inventoryFilterRecord);
        logger.info("InventoryFilterRecord: add inventory filter id=" + inventoryFilter.getId());
      }
    }
    for (TetrationNetworkPolicyProto.InventoryGroup oldInventoryFilter: oldInventoryMap.values()) {
      InventoryFilterRecord inventoryFilterRecord = new InventoryFilterRecord(RecordType.DELETE, oldInventoryFilter);
      result.inventoryFilterRecords.add(inventoryFilterRecord);
      logger.info("InventoryFilterRecord: delete inventory filter id=" + oldInventoryFilter.getId());
    }
    // step 4)
    int oldPosition = 0;
    ListIterator<TetrationNetworkPolicyProto.Intent> listIterator = oldIntentList.listIterator();
    while (listIterator.hasNext()) {
      TetrationNetworkPolicyProto.Intent oldIntent = listIterator.next();
      if (!this.currentIntentMap.containsKey(oldIntent.getId())) {
        // generate delete
        IntentRecord intentRecord = new IntentRecord(RecordType.DELETE, oldIntent);
        intentRecord.oldPosition = oldPosition;
        result.intentRecords.add(intentRecord);
        logger.info("IntentRecord: delete intent id=" + oldIntent.getId() + " at position=" + oldPosition);
        listIterator.remove();
      } else {
        oldPosition++;
      }
    } // eof while
    // step 5)
    oldPosition = 0;
    for (TetrationNetworkPolicyProto.Intent intent: this.currentIntentList) {
      if (!oldIntentMap.containsKey(intent.getId())) {
        IntentRecord intentRecord = new IntentRecord(RecordType.CREATE, intent);
        intentRecord.position = oldPosition;
        result.intentRecords.add(intentRecord);
        logger.info("IntentRecord: add new intent id=" + intent.getId() + " at position=" + oldPosition);
        oldIntentList.add(oldPosition, intent); //todo overflow is possible here
      }
      oldPosition++;
    }
    /* step 6) now both old and current list have same length
     * generate update for either content or position
     */
    if (oldIntentList.size() != this.currentIntentList.size()) {
      String msg = "oldIntentList.size=" + oldIntentList.size() +
          " != currentIntentList.size=" + this.currentIntentList.size();
      logger.error(msg);
      throw new IllegalStateException(msg);
    }
    for (int position = 0; position < oldIntentList.size();) {
      TetrationNetworkPolicyProto.Intent oldIntent = oldIntentList.get(position);
      TetrationNetworkPolicyProto.Intent intent = this.currentIntentList.get(position);
      if (oldIntent.getId().equalsIgnoreCase(intent.getId())) {
        // check for content update
        if (!ProtoHelper.isEqual(oldIntent, intent)) {
          IntentRecord intentRecord = new IntentRecord(RecordType.UPDATE, intent);
          intentRecord.position = position;
          intentRecord.oldPosition = position;
          result.intentRecords.add(intentRecord);
          logger.info("IntentRecord: update intent id=" + intent.getId());
        }
        position++;
        continue;
      }
      // old and current intent at same position have different id
      IntentRecord intentRecord = null;
      if (position + 1 < oldIntentList.size() &&
          oldIntentList.get(position + 1).getId().equalsIgnoreCase(intent.getId())) {
        // loop to search oldIntent in current list and move it to new position as in current list
        int newPosition = position + 1;
        listIterator = this.currentIntentList.listIterator(newPosition);
        while (listIterator.hasNext()) {
          TetrationNetworkPolicyProto.Intent intentInCurrentList = listIterator.next();
          if (intentInCurrentList.getId().equalsIgnoreCase(oldIntent.getId())) {
            oldIntentList.remove(position);
            oldIntentList.add(newPosition, oldIntent);
            intentRecord = new IntentRecord(RecordType.UPDATE, intentInCurrentList);
            intentRecord.position = newPosition;
            intentRecord.oldPosition = position;
            break;
          }
          newPosition++;
        } // eof while
        /* don't increment of position intentionally as we need to compare the content of
         * old and current intent despite its id-equality
         */
      } else {
        // loop to search currentIntent in old list and move it to current position
        oldPosition = position + 1;
        listIterator = oldIntentList.listIterator(oldPosition);
        while (listIterator.hasNext()) {
          TetrationNetworkPolicyProto.Intent intentInOldList = listIterator.next();
          if (intentInOldList.getId().equalsIgnoreCase(intent.getId())) {
            listIterator.remove();
            oldIntentList.add(position, intentInOldList);
            intentRecord = new IntentRecord(RecordType.UPDATE, intent);
            intentRecord.position = position;
            intentRecord.oldPosition = oldPosition;
            break;
          }
          oldPosition++;
        } // eof while
        position++;
      }
      // this check is to make sure position is valid
      if (intentRecord != null && intentRecord.position >= 0) {
        result.intentRecords.add(intentRecord);
        logger.info("IntentRecord: update intent id=" + intent.getId() + " with new position=" + position);
      } else { // should not happen to this line
        String msg;
        try {
          JsonFormat.Printer printer = JsonFormat.printer();
          msg = "Could not determine new position for oldIntent=" + printer.print(oldIntent) +
              " currentIntent=" + printer.print(intent) + " position=" + position;
        } catch (InvalidProtocolBufferException e) { // this should not happen at all
          msg = "Unexpected malformed protobuf object: " + e.getMessage();
        }
        throw new IllegalStateException(msg);
      }
    } // eof for
    // loop to set previous and next intent id for each intent record
    for (IntentRecord intentRecord: result.intentRecords) {
      if (intentRecord.recordType != RecordType.DELETE) {
        if (intentRecord.position > 0) {
          intentRecord.previousIntentId = this.currentIntentList.get(intentRecord.position - 1).getId();
        }
        if (intentRecord.position + 1 < this.currentIntentList.size()) {
          intentRecord.nextIntentId = this.currentIntentList.get(intentRecord.position + 1).getId();
        }
      }
    }
    // determine diff of catch all policy
    result.catchAllPolicyRecord = this.computeCatchAllPolicyDiff(oldCatchAllPolicy);

    return result;
  }

  /**
   * Determine if there is a change between new/current catch all policy with the given old version
   * and return a CatchAllPolicyRecord appropriately
   * @param oldCatchAllPolicy
   * @return CatchAllPolicyRecord if there is not
   */
  private CatchAllPolicyRecord computeCatchAllPolicyDiff(TetrationNetworkPolicyProto.CatchAllPolicy oldCatchAllPolicy) {
    if (oldCatchAllPolicy == null && this.currentCatchAllPolicy == null) {
      return null;
    }
    CatchAllPolicyRecord result = null;
    if (oldCatchAllPolicy != null && this.currentCatchAllPolicy != null) {
      if (oldCatchAllPolicy.getAction() != this.currentCatchAllPolicy.getAction()) {
        result = new CatchAllPolicyRecord(RecordType.UPDATE, this.currentCatchAllPolicy);
      }
    } else { // either one is not null
      if (oldCatchAllPolicy != null) { // delete catch all
        result = new CatchAllPolicyRecord(RecordType.DELETE, oldCatchAllPolicy);
      } else { // create catch all
        result = new CatchAllPolicyRecord(RecordType.CREATE, this.currentCatchAllPolicy);
      }
    }
    return result;
  }

  /**
   * This method determines inventory filters and intents that matched with
   * this client appliance and updates list of matched intents.<br>
   * Note currentIntentList will be cleared by this method!
   *
   * @param networkPolicy
   */
  private void calculateCurrentSnapshot(
      TetrationNetworkPolicyProto.NetworkPolicy networkPolicy) {
    Map<String, TetrationNetworkPolicyProto.InventoryGroup> inventoryFilterCache = new HashMap<>();

    /* helper class to keep track inventory items of inventory filter matched
     * with appliance address
     */
    class FilterAndMatchedItems {
      TetrationNetworkPolicyProto.InventoryGroup filter;
      List<TetrationNetworkPolicyProto.InventoryItem> matchedItems = new ArrayList<>();
      FilterAndMatchedItems(TetrationNetworkPolicyProto.InventoryGroup filter) {
        this.filter = filter;
      }
    }
    /* loop to gather matched inventory filters into current map and
     * to build a cache of remaining inventory filters needed by 2nd for loop
     */
    Map<String, FilterAndMatchedItems> matchedInventoryFilterMap = new HashMap<>();
    for (TetrationNetworkPolicyProto.InventoryGroup inventoryFilter :
        networkPolicy.getInventoryFiltersList()) {
      for (TetrationNetworkPolicyProto.InventoryItem inventoryItem :
          inventoryFilter.getInventoryItemsList()) {
        // loop determines if any appliance address matches with inventory item
        for (TetrationNetworkPolicyProto.InventoryItem applianceInventoryItem : this.applianceInventoryItemList) {
          if (ProtoHelper.intersect(applianceInventoryItem, inventoryItem)) {
            FilterAndMatchedItems filterAndMatchedItems = matchedInventoryFilterMap.get(inventoryFilter.getId());
            if (filterAndMatchedItems == null) {
              filterAndMatchedItems = new FilterAndMatchedItems(inventoryFilter);
              matchedInventoryFilterMap.put(inventoryFilter.getId(), filterAndMatchedItems);
            }
            filterAndMatchedItems.matchedItems.add(inventoryItem);
            break;
          }
        }
        // continue to determine further items matched with appliance address
      }
      // no match found for this filter
      if (!matchedInventoryFilterMap.containsKey(inventoryFilter.getId())) {
        inventoryFilterCache.put(inventoryFilter.getId(), inventoryFilter);
      }
    }
    /* loop examines intents: an intent is called matched if either consumer or provider
     * inventory filter (must be one of) has matched appliance addresses
     * if an intent is matched, this loop also resolves references to inventory filter
     * in protobuf flow filter object
     */
    this.currentIntentList = new LinkedList<>();
    this.currentIntentMap = new HashMap<>();
    this.currentInventoryMap = new HashMap<>();
    this.currentCatchAllPolicy = null;
    for (TetrationNetworkPolicyProto.Intent intent : networkPolicy.getIntentsList()) {
      TetrationNetworkPolicyProto.FlowFilter flowFilter = intent.getFlowFilter();
      if (!matchedInventoryFilterMap.containsKey(flowFilter.getConsumerFilterId()) &&
          !matchedInventoryFilterMap.containsKey(flowFilter.getProviderFilterId())) {
        continue; // this intent is not for the appliance
      }
      FilterAndMatchedItems consumerFilterAndMatchedItems =
          matchedInventoryFilterMap.get(flowFilter.getConsumerFilterId());
      TetrationNetworkPolicyProto.InventoryGroup consumerFilter;
      boolean consumerMatched;
      if (consumerFilterAndMatchedItems != null) {
        consumerMatched = true;
        consumerFilter = consumerFilterAndMatchedItems.filter;
      } else {
        consumerMatched = false;
        consumerFilter = inventoryFilterCache.get(flowFilter.getConsumerFilterId());
      }
      if (consumerFilter == null) {
        final String msg = "Consumer filter id=" + flowFilter.getConsumerFilterId() +
            " not found for intent id=" + intent.getId();
        logger.error(msg);
        synchronized (this.errorList) {
          this.errorList.add(msg);
        }
        continue; // ignore this broken intent
      }
      FilterAndMatchedItems providerFilterAndMatchedItems =
          matchedInventoryFilterMap.get(flowFilter.getProviderFilterId());
      TetrationNetworkPolicyProto.InventoryGroup providerFilter;
      /* if consumer (exclusive) or provider has matched with appliance address,
       * build new filter with matched items only
       * otherwise, ie if both consumer and provider match with appliance address,
       * we return the inventoryFilter as is
       */
      if (providerFilterAndMatchedItems != null) {
        providerFilter = providerFilterAndMatchedItems.filter;
        if (!consumerMatched) {
          TetrationNetworkPolicyProto.InventoryGroup.Builder filterBuilder =
              providerFilter.toBuilder().clone();
          filterBuilder.clearInventoryItems();
          filterBuilder.addAllInventoryItems(providerFilterAndMatchedItems.matchedItems);
          providerFilter = filterBuilder.build();
        }
      } else {
        providerFilter = inventoryFilterCache.get(flowFilter.getProviderFilterId());
        if (consumerMatched) {
          TetrationNetworkPolicyProto.InventoryGroup.Builder filterBuilder =
              consumerFilter.toBuilder().clone();
          filterBuilder.clearInventoryItems();
          filterBuilder.addAllInventoryItems(consumerFilterAndMatchedItems.matchedItems);
          consumerFilter = filterBuilder.build();
        }
      }
      if (providerFilter == null) {
        final String msg = "Provider filter id=" + flowFilter.getProviderFilterId() +
            " not found for intent id=" + intent.getId();
        logger.error(msg);
        synchronized (this.errorList) {
          this.errorList.add(msg);
        }
        continue; // ignore this broken intent
      }
      // rebuild intent, flow filter with found consumer and provider inventory filter
      TetrationNetworkPolicyProto.FlowFilter.Builder flowFilterBuilder = flowFilter.toBuilder();
      TetrationNetworkPolicyProto.Intent.Builder intentBuilder = intent.toBuilder();
      intentBuilder.setFlowFilter(flowFilterBuilder);
      TetrationNetworkPolicyProto.Intent matchedIntent = intentBuilder.build();
      this.currentIntentList.add(matchedIntent);
      this.currentIntentMap.put(matchedIntent.getId(), matchedIntent);
      this.currentInventoryMap.put(consumerFilter.getId(), consumerFilter);
      this.currentInventoryMap.put(providerFilter.getId(), providerFilter);
    } // eof for intentList
    if (networkPolicy.hasCatchAll()) {
      this.currentCatchAllPolicy = networkPolicy.getCatchAll();
    }
  }

  /**
   * For debugging purpose only!!!
   * Return list of errors detected during processing of kafka updates
   * Note this method clears the internal error list at the end.
   * @return
   */
  public List<String> getErrorList() {
    List<String> result;
    synchronized (this.errorList) {
      result =  new ArrayList<>(this.errorList);
      this.errorList.clear();
    }
    return result;
  }

  /**
   * Check if kafka consumer thread running
   */
  public boolean isKafkaConsumerRunning() {
    return this.kafkaConsumerRunner != null && this.kafkaConsumerRunner.isConsumerThreadAlive();
  }

  private enum RunState {
    UNKNOWN,
    ACTIVE,
    RELEASE_REQUESTED
  }

  private RunState runState;
  private KafkaConsumerRunner kafkaConsumerRunner;
  private List<TetrationNetworkPolicyProto.Intent> currentIntentList;
  private Map<String, TetrationNetworkPolicyProto.Intent> currentIntentMap;
  private Map<String, TetrationNetworkPolicyProto.InventoryGroup> currentInventoryMap;
  private TetrationNetworkPolicyProto.CatchAllPolicy currentCatchAllPolicy;
  private ReadWriteLock currentIntentsLock;
  private List<TetrationNetworkPolicyProto.InventoryItem> applianceInventoryItemList;
  private EventCallback eventCallback;
  /* list of error messages occurred during processing kafka updates
   * eg not found inventory filter id
   * client can call getErrorList() to check if there's any errors detected
   */
  private List<String> errorList;
  private TenantPolicyMetaData currentTenantPolicyMetaData;
}
