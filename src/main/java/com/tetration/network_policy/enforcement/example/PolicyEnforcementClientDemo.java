/*
 * Copyright 2017 Cisco Systems, Inc.
 * Cisco Tetration
 */

package com.tetration.network_policy.enforcement.example;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.ByteString;
import com.tetration.network_policy.enforcement.PolicyEnforcementClient;
import com.tetration.network_policy.enforcement.PolicyEnforcementClientImpl;
import com.tetration.tetration_network_policy.proto.TetrationNetworkPolicyProto;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

public class PolicyEnforcementClientDemo implements PolicyEnforcementClient.EventCallback {
  @Override
  public void notify(PolicyEnforcementClient.Event event) {
    System.out.println("Received event from " + event.getClass().getName());
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    System.out.println("\tjson: " + gson.toJson(event));
  }

  public static void main(String[] args) throws UnknownHostException {
    if (args.length < 2 || args.length > 3) {
      System.out.println("Usage: java " + PolicyEnforcementClientDemo.class.getName() +
          " <client_cert_dir> <appliance_ip> [<appliance_end_ip>]");
      System.exit(-1);
    }
    PolicyEnforcementClientDemo clientDemo = new PolicyEnforcementClientDemo();
    PolicyEnforcementClient enforcementClient = new PolicyEnforcementClientImpl();
    Properties configProperties = new Properties();
    configProperties.put("tetration.user.credential", args[0]);
    TetrationNetworkPolicyProto.AddressWithPrefix.Builder addressBuilder =
        TetrationNetworkPolicyProto.AddressWithPrefix.newBuilder();
    String address;
    int prefixLength = -1; // default for single addr
    if (args[1].contains("/")) {
      String[] parts = args[1].split("/");
      if (parts.length > 2) {
        System.err.println("Invalid subnet format: " + args[1]);
        System.exit(-1);
      }
      address = parts[0];
      if (parts.length == 2) {
        prefixLength = Integer.valueOf(parts[1]);
      }
    } else {
      address = args[1];
    }
    InetAddress inetAddress = InetAddress.getByName(address);
    if (inetAddress instanceof Inet4Address) {
      if (prefixLength < 0) {
        addressBuilder.setPrefixLength(32);
      } else {
        if (prefixLength > 32) {
          System.err.println("Invalid prefix length: " + args[1]);
          System.exit(-1);
        }
        addressBuilder.setPrefixLength(prefixLength);
      }
      addressBuilder.setAddrFamily(TetrationNetworkPolicyProto.IPAddressFamily.IPv4);
    } else {
      if (prefixLength < 0) {
        addressBuilder.setPrefixLength(128);
      } else {
        if (prefixLength > 128) {
          System.err.println("Invalid prefix length: " + args[1]);
          System.exit(-1);
        }
        addressBuilder.setPrefixLength(prefixLength);
      }
      addressBuilder.setAddrFamily(TetrationNetworkPolicyProto.IPAddressFamily.IPv6);
    }
    addressBuilder.setIpAddr(ByteString.copyFrom(inetAddress.getAddress()));
    TetrationNetworkPolicyProto.InventoryItem.Builder inventoryItemBuilder =
        TetrationNetworkPolicyProto.InventoryItem.newBuilder();
    if (args.length > 2) { // addr range
      if (prefixLength >= 0) {
        System.err.println("Specification of prefix length not allowed for address range");
        System.exit(-1);
      }
      TetrationNetworkPolicyProto.AddressWithRange.Builder addrRangeBuilder =
          TetrationNetworkPolicyProto.AddressWithRange.newBuilder();
      addrRangeBuilder.setAddrFamily(addressBuilder.getAddrFamily());
      addrRangeBuilder.setStartIpAddr(ByteString.copyFrom(inetAddress.getAddress()));
      InetAddress endInetAddress = InetAddress.getByName(args[2]);
      if (endInetAddress.getAddress().length != inetAddress.getAddress().length) {
        System.err.println("Mismatch of IPv4/6 addresses: " + args[1] + " - " + args[2]);
        System.exit(-1);
      }
      addrRangeBuilder.setEndIpAddr(ByteString.copyFrom(endInetAddress.getAddress()));
      inventoryItemBuilder.setAddressRange(addrRangeBuilder);
    } else { // single addr or subnet
      inventoryItemBuilder.setIpAddress(addressBuilder);
    }
    configProperties.put("appliance.inventoryItem", inventoryItemBuilder.build());
    enforcementClient.init(configProperties, clientDemo);
    while (true) {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
        break;
      }
    }
    System.exit(0);
  }
}
