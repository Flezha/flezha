package com.flezha.sdk;

import java.util.concurrent.*;
import com.flezha.sdk.Flezha.FlezhaMessage;

public class Flezha {
  protected String id;
  protected LinkedBlockingQueue<FlezhaMessage> rxChannel;
  protected LinkedBlockingQueue<FlezhaMessage> txChannel;
  private static final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

  public enum FlezhaMessage {
    SOMETHING
  }

  private Flezha(String id) {
    this.id = id;

    rxChannel = new LinkedBlockingQueue<FlezhaMessage>();
    txChannel = new LinkedBlockingQueue<FlezhaMessage>();

    executor.submit(() -> {
      var computeEngine = new ComputeEngine(id, rxChannel, txChannel);
    });
  }

  public static Flezha create(String id) {
    return new Flezha(id);
  }
}