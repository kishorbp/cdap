package com.continuuity.fabric.engine.memory;

import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.fabric.engine.NativeSimpleExecutor;
import com.continuuity.fabric.operations.impl.Modifier;
import com.continuuity.fabric.operations.queues.QueueConsumer;
import com.continuuity.fabric.operations.queues.QueueEntry;
import com.continuuity.fabric.operations.queues.QueuePartitioner;

public class MemorySimpleExecutor implements NativeSimpleExecutor {

  private final MemorySimpleEngine engine;

  public MemorySimpleExecutor(MemorySimpleEngine engine) {
    this.engine = engine;
  }

  @Override
  public byte[] readRandom(byte[] key) {
    return this.engine.get(generateRandomOrderKey(key));
  }

  @Override
  public void writeRandom(byte [] key, byte [] value) {
    this.engine.put(generateRandomOrderKey(key), value);
  }

  @Override
  public Map<byte[],byte[]> readOrdered(byte [] key) {
    return this.engine.getAsMap(key);
  }

  /**
   *
   * @param startKey inclusive
   * @param endKey exclusive
   * @return
   */
  @Override
  public Map<byte[],byte[]> readOrdered(byte [] startKey, byte [] endKey) {
    return this.engine.get(startKey, endKey);
  }

  /**
   *
   * @param startKey inclusive
   * @param limit
   * @return
   */
  @Override
  public Map<byte[],byte[]> readOrdered(byte [] startKey, int limit) {
    return this.engine.get(startKey, limit);
  }

  @Override
  public void writeOrdered(byte [] key, byte [] value) {
    this.engine.put(key, value);
  }

  @Override
  public long readCounter(byte[] key) {
    return this.engine.getCounter(generateRandomOrderKey(key));
  }

  @Override
  public boolean compareAndSwap(byte [] key,
      byte [] expectedValue, byte [] newValue) {
    return this.engine.compareAndSwap(
        generateRandomOrderKey(key), expectedValue, newValue);
  }

  @Override
  public void readModifyWrite(byte [] key, Modifier<byte[]> modifier) {
    this.engine.readModifyWrite(generateRandomOrderKey(key), modifier);
  }

  @Override
  public long increment(byte [] key, long amount) {
    return this.engine.increment(generateRandomOrderKey(key), amount);
  }

  @Override
  public long getCounter(byte [] key) {
    return this.engine.getCounter(generateRandomOrderKey(key));
  }

  @Override
  public boolean queuePush(byte [] queueName, byte [] queueEntry) {
    return this.engine.queuePush(generateRandomOrderKey(queueName), queueEntry);
  }

  @Override
  public boolean queueAck(byte [] queueName, QueueEntry queueEntry) {
    return this.engine.queueAck(generateRandomOrderKey(queueName), queueEntry);
  }

  @Override
  public QueueEntry queuePop(byte [] queueName, QueueConsumer consumer,
      QueuePartitioner partitioner) throws InterruptedException {
    return this.engine.queuePop(generateRandomOrderKey(queueName), consumer,
        partitioner);
  }

  // Private helper methods


  /**
   * Generates a 4-byte hash of the specified key and returns a copy of the
   * specified key with the hash prepended to it.
   * @param key
   * @return 4-byte-hash(key) + key
   */
  private byte[] generateRandomOrderKey(byte [] key) {
    byte [] hash = Bytes.toBytes(Bytes.hashCode(key));
    return Bytes.add(hash, key);
  }
}
