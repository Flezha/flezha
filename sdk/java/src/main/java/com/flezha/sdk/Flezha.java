package com.flezha.sdk;

import java.util.concurrent.Executors;
import java.util.concurrent.ArrayBlockingQueue;

public class Flezha {
  protected String id;
  protected ArrayBlockingQueue<FlezhaMessage> rxChannel;
  private static final executor = Executors.newVirtualThreadPerTaskExecutor();

  public enum FlezhaMessage {
    SOMETHING
  }

  private Flezha(String id) {
    this.id = id;

    rxChannel = new ArrayBlockingQueue<FlezhaMessage>();
    txChannel = new ArrayBlockingQueue<FlezhaMessage>();

    executor.submit(() -> {
      var computeEngine = new ComputeEngine(id, rxChannel, txChannel);
    });
  }

  public static Flezha create(String id) {
    return new Flezha(id);
  }
}