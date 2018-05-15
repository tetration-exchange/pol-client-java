/*
 * Copyright 2017 Cisco Systems, Inc.
 * Cisco Tetration
 */

package com.tetration.network_policy.enforcement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ProtoHelperTest {
  protected static Logger logger = LoggerFactory.getLogger(ProtoHelperTest.class);

  @Test()
  public void testGetSubnetEndAddress4() {
    InetAddress address = null;
    try {
      address = InetAddress.getByAddress(new byte[] {(byte)192, (byte)168, 64, 0});
      Assert.assertEquals(ProtoHelper.getSubnetEndAddress(address, 18),
          InetAddress.getByAddress(new byte[] {(byte)192, (byte)168, 127, (byte)255}));
      Assert.assertEquals(ProtoHelper.getSubnetEndAddress(address, 19),
          InetAddress.getByAddress(new byte[] {(byte)192, (byte)168, 95, (byte)255}));
      Assert.assertEquals(ProtoHelper.getSubnetEndAddress(address, 23),
          InetAddress.getByAddress(new byte[] {(byte)192, (byte)168, 65, (byte)255}));
      Assert.assertEquals(ProtoHelper.getSubnetEndAddress(address, 24),
          InetAddress.getByAddress(new byte[] {(byte)192, (byte)168, 64, (byte)255}));
      Assert.assertEquals(ProtoHelper.getSubnetEndAddress(address, 25),
          InetAddress.getByAddress(new byte[] {(byte)192, (byte)168, 64, 127}));
      Assert.assertEquals(ProtoHelper.getSubnetEndAddress(address, 32),
          address);
      Assert.assertEquals(ProtoHelper.getSubnetEndAddress(address, 0),
          InetAddress.getByAddress(new byte[] {(byte)255, (byte)255, (byte)255, (byte)255}));
    } catch (UnknownHostException e) {
      Assert.fail(e.getMessage());
    }
    try {
      ProtoHelper.getSubnetEndAddress(address, 33);
      Assert.fail("Invalid prefixLength not caught by getSubnetEndAddress()");
    } catch (IllegalArgumentException e) {
      // this is expected
    } catch (UnknownHostException e) {
      Assert.fail(e.getMessage());
    }
    try {
      ProtoHelper.getSubnetEndAddress(address, -1);
      Assert.fail("Invalid prefixLength not caught by getSubnetEndAddress()");
    } catch (IllegalArgumentException e) {
      // this is expected
    } catch (UnknownHostException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test()
  public void testGetSubnetEndAddress6() {
    final byte[] BIT_SET = {(byte)128, 64, 32, 16, 8, 4, 2 ,1};
    byte[] addrBytes = new byte[16];
    InetAddress address = null;
    try {
      address = InetAddress.getByAddress(addrBytes);
      byte[] expectedAddrBytes = new byte[16];
      for (int i = 128; i >= 0; --i) {
        InetAddress expectedAddress = InetAddress.getByAddress(expectedAddrBytes);
        Assert.assertEquals(ProtoHelper.getSubnetEndAddress(address, i), expectedAddress);
        if (i > 0) {
          expectedAddrBytes[(i - 1) >> 3] |= BIT_SET[(i - 1) & 7];
        }
      }
    } catch (UnknownHostException e) {
      Assert.fail(e.getMessage());
    }
    try {
      ProtoHelper.getSubnetEndAddress(address, 129);
      Assert.fail("Invalid prefixLength not caught by getSubnetEndAddress()");
    } catch (IllegalArgumentException e) {
      // this is expected
    } catch (UnknownHostException e) {
      Assert.fail(e.getMessage());
    }
  }
}
