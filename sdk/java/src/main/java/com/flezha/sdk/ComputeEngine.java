package com.flezha.sdk;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.*;

class ComputeEngine {
  protected String id;
  protected List<Processor> processors;
  protected ArrayBlockingQueue<FlezhaMessage> rxChannel;
  protected ArrayBlockingQueue<FlezhaMessage> txChannel;

  public ComputeEngine(String id, ArrayBlockingQueue<FlezhaMessage> rxChannel, ArrayBlockingQueue<FlezhaMessage> txChannel) {
    this.id = id;
    this.processors = new ArrayList<>();
    this.rxChannel = rxChannel;
    this.txChannel = txChannel;
  }
}