/*
 * Copyright 2017 Cisco Systems, Inc.
 * Cisco Tetration
 */

package com.tetration.network_policy.enforcement;

import com.tetration.tetration_network_policy.proto.TetrationNetworkPolicyProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

/**
 * Helper class providing methods to check content equality of inventory filters,
 * items and addresses
 */
public class ProtoHelper {
  private static Logger logger = LoggerFactory.getLogger(ProtoHelper.class);

  /**
   * Calculate network end address of given subnet
   *
   * @param address
   * @param prefixLength
   * @return InetAddress
   * @throws UnknownHostException
   */
  public static InetAddress getSubnetEndAddress(InetAddress address, int prefixLength) throws UnknownHostException {
    final byte[] INV_BIT_MASK = {(byte)255, 127, 63, 31, 15, 7, 3, 1};
    if (prefixLength < 0) {
      throw new IllegalArgumentException(
          "prefixLength must be a positive number");
    }
    byte[] bytes = address.getAddress();
    int numBits = bytes.length << 3;
    if (prefixLength > numBits) {
      throw new IllegalArgumentException(
          "Invalid prefixLength (given address length is " + numBits + " bits)");
    }
    int firstByteIdx = prefixLength >> 3;
    if (firstByteIdx < bytes.length) {
      bytes[firstByteIdx] |= INV_BIT_MASK[prefixLength & 7];
      for (int i = firstByteIdx + 1; i < bytes.length; ++i) {
        bytes[i] = (byte)255;
      }
    }
    return InetAddress.getByAddress(bytes);
  }

  /**
   * Compare two addresses
   * @param lhs
   * @param rhs
   * @return -1 if lhs &lt; rhs, 1 if lhs &gt; rhs, otherwise 0
   */
  public static int compareAddress(InetAddress lhs, InetAddress rhs) {
    byte[] lhsBytes = lhs.getAddress();
    byte[] rhsBytes = rhs.getAddress();
    if (lhsBytes.length < rhsBytes.length) {
      return -1;
    }
    if (lhsBytes.length > rhsBytes.length) {
      return 1;
    }
    BigInteger lhsBigInt = new BigInteger(1, lhsBytes);
    BigInteger rhsBigInt = new BigInteger(1, rhsBytes);
    return lhsBigInt.compareTo(rhsBigInt);
  }

  /**
   * Internal class for address range based on InetAddress
   */
  static class AddressRange {
    InetAddress startInetAddress;
    InetAddress endInetAddress;

    /**
     * Helper method to convert to address range object, which is used mainly to
     * check if an inventory item matches with appliance addresses (also an inventory item)
     * For a single address the returned address range has the same start and end address.
     * For a subnet the returned address range has the given start address and the subnets
     * end address.
     * @return address range that is equivalent to this inventory item
     */
    static AddressRange convert(TetrationNetworkPolicyProto.InventoryItem inventoryItem) throws UnknownHostException {
      TetrationNetworkPolicyProto.InventoryItem.AddressCase addressCase = inventoryItem.getAddressCase();
      if (addressCase == TetrationNetworkPolicyProto.InventoryItem.AddressCase.ADDRESS_RANGE) {
        return new AddressRange(inventoryItem.getAddressRange());
      } else if (addressCase == TetrationNetworkPolicyProto.InventoryItem.AddressCase.IP_ADDRESS) {
        return new AddressRange(inventoryItem.getIpAddress());
      } else if (addressCase == TetrationNetworkPolicyProto.InventoryItem.AddressCase.LB_SERVICE) {
        return new AddressRange(inventoryItem.getLbService().getVip());
      } else {
       throw new IllegalArgumentException("Invalid addressCase=" + addressCase.getNumber());
      }
    }

    /**
     * Convert protobuf AddressWithPrefix to AddressRange
     * @param addressProto
     * @throws UnknownHostException
     */
    AddressRange(TetrationNetworkPolicyProto.AddressWithPrefix addressProto) throws UnknownHostException {
      switch (addressProto.getAddrFamilyValue()) {
        case TetrationNetworkPolicyProto.IPAddressFamily.IPv4_VALUE:
          startInetAddress = Inet4Address.getByAddress(addressProto.getIpAddr().toByteArray());
          break;
        case TetrationNetworkPolicyProto.IPAddressFamily.IPv6_VALUE:
          startInetAddress = Inet6Address.getByAddress(addressProto.getIpAddr().toByteArray());
          break;
        default:
          throw new IllegalArgumentException("Invalid addrFamilyValue=" + addressProto.getAddrFamilyValue());
      }
      endInetAddress = getSubnetEndAddress(startInetAddress, addressProto.getPrefixLength());
    }

    /**
     * Convert protobuf AddressWithRange to AddressRange
     * @param addressWithRange
     * @throws UnknownHostException
     */
    AddressRange(TetrationNetworkPolicyProto.AddressWithRange addressWithRange) throws UnknownHostException {
      TetrationNetworkPolicyProto.IPAddressFamily addressFamily = addressWithRange.getAddrFamily();
      if (addressFamily == TetrationNetworkPolicyProto.IPAddressFamily.IPv4) {
        startInetAddress = Inet4Address.getByAddress(addressWithRange.getStartIpAddr().toByteArray());
        endInetAddress = Inet4Address.getByAddress(addressWithRange.getEndIpAddr().toByteArray());
      } else if (addressFamily == TetrationNetworkPolicyProto.IPAddressFamily.IPv6) {
        startInetAddress = Inet6Address.getByAddress(addressWithRange.getStartIpAddr().toByteArray());
        endInetAddress = Inet6Address.getByAddress(addressWithRange.getEndIpAddr().toByteArray());
      } else {
        throw new IllegalArgumentException("Invalid addrFamilyValue=" + addressFamily.getNumber());
      }
    }

    /**
     * Check if given addres range overlaps with this address range, ie
     * the intersection is not empty
     * @param addressRange
     * @return true if given address range overlaps with this address range
     */
    public boolean intersect(AddressRange addressRange) {
      return (ProtoHelper.compareAddress(this.startInetAddress, addressRange.startInetAddress) >= 0 &&
          ProtoHelper.compareAddress(this.startInetAddress, addressRange.endInetAddress) <= 0) ||
          (ProtoHelper.compareAddress(addressRange.startInetAddress, this.startInetAddress) >= 0 &&
              ProtoHelper.compareAddress(addressRange.startInetAddress, this.endInetAddress) <= 0);
    }
  }

  /**
   * Check if given two inventory items addresses overlap, ie their intersection is
   * not empty
   * @param item1
   * @param item2
   * @return true if two inventory items addresses overlap
   */
  public static boolean intersect(TetrationNetworkPolicyProto.InventoryItem item1,
                                  TetrationNetworkPolicyProto.InventoryItem item2) {
    try {
      AddressRange addressRange1 = AddressRange.convert(item1);
      AddressRange addressRange2 = AddressRange.convert(item2);
      return addressRange1.intersect(addressRange2);
    } catch (UnknownHostException e) {
      return false;
    }
  }

  /**
   * This is needed as the protobuf generated equals() does not work correctly
   * to detect changes in nested lists as we can not rely on the order of items
   * returned by backend.
   * @param lhs
   * @param rhs
   * @return true if both intents are equal to each other
   */
  public static boolean isEqual(TetrationNetworkPolicyProto.Intent lhs, TetrationNetworkPolicyProto.Intent rhs) {
    List<TetrationNetworkPolicyProto.KeyValue> lhsTags = lhs.getTagsList();
    List<TetrationNetworkPolicyProto.KeyValue> rhsTags = rhs.getTagsList();
    /* note ignore meta data intentionally here because the version number is mostly not equal
     * which causes the compare to be false negative
     */
    boolean result = lhs.getId().equals(rhs.getId()) &&
        lhs.getAction() == rhs.getAction() &&
        lhsTags.size() == rhsTags.size() && lhsTags.containsAll(rhsTags) &&
        rhsTags.containsAll(lhsTags);
    return result && isEqual(lhs.getFlowFilter(), rhs.getFlowFilter());
  }

  private static boolean isEqual(TetrationNetworkPolicyProto.FlowFilter lhs, TetrationNetworkPolicyProto.FlowFilter rhs) {
    boolean result = lhs.getConsumerFilterId().equals(rhs.getConsumerFilterId()) &&
        lhs.getProviderFilterId().equals(rhs.getProviderFilterId()) &&
        isEqual(lhs.getProtocolAndPortsList(), rhs.getProtocolAndPortsList());
    return result;
  }

  private static boolean isEqual(List<TetrationNetworkPolicyProto.ProtocolAndPorts> lhs,
                                 List<TetrationNetworkPolicyProto.ProtocolAndPorts> rhs) {
    if (lhs.size() != rhs.size()) {
      return false;
    }
    // caution: this is O(n^2)
    for (TetrationNetworkPolicyProto.ProtocolAndPorts protocolAndPorts: lhs) {
      if (!contains(rhs, protocolAndPorts)) {
        return false;
      }
    }
    /* second reverse check might not be necessary as backend will not generate
     * duplicates in one list
     * let be defensive here
     */
    for (TetrationNetworkPolicyProto.ProtocolAndPorts protocolAndPorts: rhs) {
      if (!contains(lhs, protocolAndPorts)) {
        return false;
      }
    }
    return true;
  }

  private static boolean contains(List<TetrationNetworkPolicyProto.ProtocolAndPorts> list,
                                  TetrationNetworkPolicyProto.ProtocolAndPorts item) {
    for (TetrationNetworkPolicyProto.ProtocolAndPorts protocolAndPorts: list) {
      if (protocolAndPorts.getProtocol() == item.getProtocol() &&
          protocolAndPorts.getPortRangesCount() == item.getPortRangesCount() &&
          protocolAndPorts.getPortsCount() == item.getPortsCount() &&
          protocolAndPorts.getPortRangesList().containsAll(item.getPortRangesList()) &&
          item.getPortRangesList().containsAll(protocolAndPorts.getPortRangesList()) &&
          protocolAndPorts.getPortsList().containsAll(item.getPortsList()) &&
          item.getPortsList().containsAll(protocolAndPorts.getPortsList())
          ) {
        return true;
      }
    }
    return false;
  }

  public static boolean isEqual(TetrationNetworkPolicyProto.InventoryFilter lhs, TetrationNetworkPolicyProto.InventoryFilter rhs) {
    if (lhs == null && rhs == null) {
      return true;
    }
    if (lhs == null || rhs == null) {
      return false;
    }
    List<TetrationNetworkPolicyProto.InventoryItem> lhsInventoryItems = lhs.getInventoryItemsList();
    List<TetrationNetworkPolicyProto.InventoryItem> rhsInventoryItems = rhs.getInventoryItemsList();
    if (!lhs.getId().equals(rhs.getId()) ||
        lhsInventoryItems.size() != rhsInventoryItems.size()) {
      return false;
    }
    for (TetrationNetworkPolicyProto.InventoryItem inventoryItem: lhsInventoryItems) {
      if (!contains(rhsInventoryItems, inventoryItem)) {
        return false;
      }
    }
    for (TetrationNetworkPolicyProto.InventoryItem inventoryItem: rhsInventoryItems) {
      if (!contains(lhsInventoryItems, inventoryItem)) {
        return false;
      }
    }
    return true;
  }

  private static boolean contains(List<TetrationNetworkPolicyProto.InventoryItem> list,
                                  TetrationNetworkPolicyProto.InventoryItem item) {
    for (TetrationNetworkPolicyProto.InventoryItem inventoryItem: list) {
      TetrationNetworkPolicyProto.InventoryItem.AddressCase addressCase = inventoryItem.getAddressCase();
      if (addressCase == TetrationNetworkPolicyProto.InventoryItem.AddressCase.IP_ADDRESS) {
        if (isEqual(inventoryItem.getIpAddress(), item.getIpAddress())) {
          return true;
        }
      } else if (addressCase == TetrationNetworkPolicyProto.InventoryItem.AddressCase.ADDRESS_RANGE) {
        if (isEqual(inventoryItem.getAddressRange(), item.getAddressRange())) {
          return true;
        }
      } else if (addressCase == TetrationNetworkPolicyProto.InventoryItem.AddressCase.LB_SERVICE) {
        if (isEqual(inventoryItem.getLbService().getVip(), item.getLbService().getVip()) &&
            inventoryItem.getLbService().getName().equals(item.getLbService().getName()) &&
            inventoryItem.getLbService().getUrl().equals(item.getLbService().getUrl())) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean isEqual(TetrationNetworkPolicyProto.AddressWithRange lhs,
                                 TetrationNetworkPolicyProto.AddressWithRange rhs) {
    return lhs.getAddrFamily().equals(rhs.getAddrFamily()) &&
        Arrays.equals(lhs.getStartIpAddr().toByteArray(), rhs.getStartIpAddr().toByteArray()) &&
        Arrays.equals(lhs.getEndIpAddr().toByteArray(), rhs.getEndIpAddr().toByteArray());
  }

  private static boolean isEqual(TetrationNetworkPolicyProto.AddressWithPrefix lhs,
                                 TetrationNetworkPolicyProto.AddressWithPrefix rhs) {
    return lhs.getAddrFamily().equals(rhs.getAddrFamily()) &&
        lhs.getPrefixLength() == rhs.getPrefixLength() &&
        Arrays.equals(lhs.getIpAddr().toByteArray(), rhs.getIpAddr().toByteArray());
  }
}
