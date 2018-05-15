/*
 * Copyright 2017 Cisco Systems, Inc.
 * Cisco Tetration
 */

package com.tetration.network_policy.enforcement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * Abstract helper class to retry function call of eg Rest API.
 * Subclass needs to implement method T call().
 * @param <T> return type of function call
 */
public abstract class RetryCallable<T> implements Callable<T> {
  private static Logger logger = LoggerFactory.getLogger(RetryCallable.class);

  private String callerName;
  private int maxRetries;
  private long delaySecs;

  public RetryCallable(
      String callerName,
      int maxRetries,
      long delaySecs) {
    if (maxRetries < 1) {
      throw new IllegalArgumentException("maxRetries must be >= 1");
    }
    this.callerName = callerName;
    this.maxRetries = maxRetries;
    this.delaySecs = delaySecs;
  }

  /**
   * Call the implemented call() method and retry if any exception occurred
   * until maxRetries exceeded
   * @return
   */
  public T retry() throws Exception {
    Exception previousE = null;
    for (int count = 0; count < this.maxRetries; ++count) {
      try {
        T result = this.call();
        if (count > 0) {
          logger.info("Call to " + this.callerName + " took " + count +
              " retries and " + (count * this.delaySecs) + "secs");
        }
        return result;
      } catch (Exception e) {
        logger.error("Exception occurred: " + e.getMessage());
        previousE = e;
      }
      try {
        logger.debug("Retrying " + this.callerName + " in " + this.delaySecs + " secs");
        Thread.sleep(this.delaySecs * 1000);
      } catch (InterruptedException e) {
        throw e;
      }
    } // eof for
    throw previousE;
  }
}
