package com.continuuity.fabric.operations.memory;

import java.util.List;
import java.util.Map;

import com.continuuity.fabric.access.DelegationToken;
import com.continuuity.fabric.engine.NativeTransactionalExecutor;
import com.continuuity.fabric.engine.memory.MemoryTransactionalExecutor.Transaction;
import com.continuuity.fabric.engine.transactions.RowTracker;
import com.continuuity.fabric.engine.transactions.TransactionOracle;
import com.continuuity.fabric.operations.SyncReadTimeoutException;
import com.continuuity.fabric.operations.TransactionalOperationExecutor;
import com.continuuity.fabric.operations.WriteOperation;
import com.continuuity.fabric.operations.impl.CompareAndSwap;
import com.continuuity.fabric.operations.impl.Increment;
import com.continuuity.fabric.operations.impl.OrderedRead;
import com.continuuity.fabric.operations.impl.OrderedWrite;
import com.continuuity.fabric.operations.impl.Read;
import com.continuuity.fabric.operations.impl.ReadModifyWrite;
import com.continuuity.fabric.operations.impl.Write;
import com.continuuity.fabric.operations.queues.QueueEntry;
import com.continuuity.fabric.operations.queues.QueuePop;
import com.continuuity.fabric.operations.queues.QueuePush;

public abstract class MemoryTransactionalOperationExecutor
implements TransactionalOperationExecutor {

  private final NativeTransactionalExecutor executor;

  private final TransactionOracle oracle;
  
  private final RowTracker rowTracker;

  public MemoryTransactionalOperationExecutor(
      NativeTransactionalExecutor executor,
      TransactionOracle oracle,
      RowTracker rowTracker) {
    this.executor = executor;
    this.oracle = oracle;
    this.rowTracker = rowTracker;
  }

  @Override
  public boolean execute(List<WriteOperation> writes) {
    // Open the transaction
    long txid = oracle.getWriteTxid();
    // Write them in order, for now
    for (WriteOperation write : writes) {
      DelegationToken token;
    }
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(Write write) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public byte[] execute(Read read) throws SyncReadTimeoutException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueueEntry execute(QueuePop pop) throws SyncReadTimeoutException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<byte[], byte[]> execute(OrderedRead orderedRead) throws SyncReadTimeoutException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean execute(ReadModifyWrite rmw) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(OrderedWrite write) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(QueuePush push) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(Increment inc) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(CompareAndSwap cas) {
    // TODO Auto-generated method stub
    return false;
  }


}
