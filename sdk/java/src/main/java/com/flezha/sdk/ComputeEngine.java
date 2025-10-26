package com.flezha.sdk;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.*;
import com.flezha.sdk.Flezha.FlezhaMessage;

class ComputeEngine {
  protected String id;
  protected List<Processor> processors;
  protected LinkedBlockingQueue<FlezhaMessage> rxChannel;
  protected LinkedBlockingQueue<FlezhaMessage> txChannel;

  public ComputeEngine(String id, LinkedBlockingQueue<FlezhaMessage> rxChannel, LinkedBlockingQueue<FlezhaMessage> txChannel) {
    this.id = id;
    this.processors = new ArrayList<>();
    this.rxChannel = rxChannel;
    this.txChannel = txChannel;
  }
}