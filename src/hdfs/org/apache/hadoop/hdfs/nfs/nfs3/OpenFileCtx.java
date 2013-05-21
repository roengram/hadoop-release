/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.nfs.nfs3;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.InvalidParameterException;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.io.BytesWritable.Comparator;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.IdUserGroup;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.WriteStableHow;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.Nfs3Status;
import org.apache.hadoop.nfs.nfs3.request.WRITE3Request;
import org.apache.hadoop.nfs.nfs3.response.WRITE3Response;
import org.apache.hadoop.nfs.nfs3.response.WccAttr;
import org.apache.hadoop.nfs.nfs3.response.WccData;
import org.apache.hadoop.oncrpc.XDR;
import org.jboss.netty.channel.Channel;

/**
 * OpenFileCtx saves the context of one HDFS file output stream. Access to it is
 * synchronized by its member lock.
 */
class OpenFileCtx {
  public static final Log LOG = LogFactory.getLog(OpenFileCtx.class);
  
  /**
   * Lock to synchronize OpenFileCtx changes. Thread should get this lock before
   * any read/write operation to an OpenFileCtx object
   */
  private final ReentrantLock ctxLock;

  // The stream status. False means the stream is closed.
  private boolean activeState;
  // The stream write-back status. True means one thread is doing write back.
  private boolean asyncStatus;

  private final FSDataOutputStream fos;
  private final Nfs3FileAttributes latestAttr;
  private long nextOffset;

  private final SortedMap<OffsetRange, WriteCtx> pendingWrites;
  
  // The last write, commit request or write-back event. Updating time to keep
  // output steam alive.
  private long lastAccessTime;
  
  // Pending writes water mark for dump, 1MB
  private static int DUMP_WRITE_WATER_MARK = 1024 * 1024; 
  private FileOutputStream dumpOut;
  private long nonSequentialWriteInMemory;
  private boolean enabledDump;
  private RandomAccessFile raf;
  private final String dumpFilePath;
  
  private void updateLastAccessTime() {
    lastAccessTime = System.currentTimeMillis();
  }

  private boolean checkStreamTimeout(long streamTimeout) {
    return System.currentTimeMillis() - lastAccessTime > streamTimeout;
  }
  
  // Increase or decrease the memory occupation of non-sequential writes
  private long updateNonSequentialWriteInMemory(long count) {
    nonSequentialWriteInMemory += count;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Update nonSequentialWriteInMemory by " + count + " new value:"
          + nonSequentialWriteInMemory);
    }
    if (nonSequentialWriteInMemory < 0) {
      throw new InvalidParameterException(
          "nonSequentialWriteInMemory is negative after update with count "
              + count);
    }
    return nonSequentialWriteInMemory;
  }
  
  SortedMap<OffsetRange, WriteCtx> getPendingWrites() {
    return pendingWrites;
  }
  
  OpenFileCtx(FSDataOutputStream fos, Nfs3FileAttributes latestAttr,
      String dumpFilePath) {
    this.fos = fos;
    this.latestAttr = latestAttr;
    pendingWrites = new TreeMap<OffsetRange, WriteCtx>();
    updateLastAccessTime();
    activeState = true;
    asyncStatus = false;
    dumpOut = null;
    raf = null;
    nonSequentialWriteInMemory = 0;
    this.dumpFilePath = dumpFilePath;  
    enabledDump = dumpFilePath == null ? false: true;
    ctxLock = new ReentrantLock();
  }

  // Make a copy of the latestAttr
  public Nfs3FileAttributes copyLatestAttr() {
    Nfs3FileAttributes ret;
    ctxLock.lock();
    ret = new Nfs3FileAttributes(latestAttr);
    ctxLock.unlock();
    return ret;
  }

  public long getNextOffset() {
    long ret;
    ctxLock.lock();
    ret = nextOffset;
    ctxLock.unlock();
    return ret;
  }
  
  // Get flushed offset. Note that flushed data may not be persisted.
  private long getFlushedOffset() throws IOException {
    return fos.getPos();
  }
  
  // Check if need to dump the new writes
  private void checkDump(long count) {
    assert (ctxLock.isLocked());
    if (!enabledDump) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Do nothing, dump is disabled.");
      }
      return;
    }
    updateNonSequentialWriteInMemory(count);
    if (nonSequentialWriteInMemory < DUMP_WRITE_WATER_MARK) {
      return;
    }

    // Create dump outputstream for the first time
    if (dumpOut == null) {
      LOG.info("Create dump file:" + dumpFilePath);
      File dumpFile = new File(dumpFilePath);
      try {
        if (dumpFile.exists()) {
          throw new RuntimeException("The dump file should not exist:" + dumpFilePath);
          // TODO: handle error
        }
        dumpOut = new FileOutputStream(dumpFile);
        if (dumpFile.createNewFile()) {
          LOG.error("Can't create dump file:" + dumpFilePath);
        }
      } catch (IOException e) {
        LOG.error("Got failure when creating dump stream " + dumpFilePath
            + " with error:" + e);
        enabledDump = false;
        if (dumpOut != null) {
          try {
            dumpOut.close();
          } catch (IOException e1) {
            LOG.error("Can't close dump stream " + dumpFilePath
                + " with error:" + e);
          }
        }
        return;
      }
    }
    // Get raf for the first dump
    if (raf == null) {
      try {
        raf = new RandomAccessFile(dumpFilePath, "r");
      } catch (FileNotFoundException e) {
        LOG.error("Can't get random access to file " + dumpFilePath);
        // Disable dump
        enabledDump = false;
        return;
      }
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Start dump, current write number:" + pendingWrites.size());
    }
    Iterator<OffsetRange> it = pendingWrites.keySet().iterator();
    while (it.hasNext()) {
      OffsetRange key = it.next();
      WriteCtx writeCtx = pendingWrites.get(key);
      try {
        long dumpedDataSize = writeCtx.dumpData(dumpOut, raf);
        if (dumpedDataSize > 0) {
          if(dumpedDataSize != writeCtx.getCount()) {
            throw new RuntimeException("Dumped size, " + dumpedDataSize
                + ", is not write size:" + writeCtx.getCount());
          }
          updateNonSequentialWriteInMemory(-dumpedDataSize);
          
          // In test, noticed some Linux client sends a batch (e.g., 1MB)
          // of reordered writes and won't send more writes until it gets
          // responses of the previous batch. So when dump the request, send
          // back reply also.
          WccData fileWcc = new WccData(latestAttr.getWccAttr(), latestAttr);
          WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3_OK,
              fileWcc, (int) dumpedDataSize, writeCtx.getStableHow(),
              Nfs3Constant.WRITE_COMMIT_VERF);
          Nfs3Utils.writeChannel(writeCtx.getChannel(),
              response.send(new XDR(), writeCtx.getXid()));
          writeCtx.setReplied(true);
        }
      } catch (IOException e) {
        LOG.error("Dump data failed:" + writeCtx + " with error:" + e);
        // Disable dump
        enabledDump = false;
        return;
      }
    }
    if (nonSequentialWriteInMemory != 0) {
      throw new RuntimeException(
          "After dump, nonSequentialWriteInMemory is not zero: "
              + nonSequentialWriteInMemory);
      //TODO: better handling.
    }
  }
  
  private boolean checkRepeatedWriteRequest(WRITE3Request request,
      Channel channel, int xid) {
    OffsetRange range = new OffsetRange(request.getOffset(),
        request.getOffset() + request.getCount());
    WriteCtx writeCtx = pendingWrites.get(range);
    if (writeCtx== null) {
      return false;
    } else {
      if (xid != writeCtx.getXid()) {
        LOG.warn("Got a repeated request, same range, with a different xid:"
            + xid + " xid in old request:" + writeCtx.getXid());
        //TODO: better handling.
      }
      return true;  
    }
    
  }
  
  public void receivedNewWrite(DFSClient dfsClient, WRITE3Request request,
      Channel channel, int xid, AsyncDataService asyncDataService,
      IdUserGroup iug) {
    long offset = request.getOffset();
    int count = request.getCount();
    WriteStableHow stableHow = request.getStableHow();

    ctxLock.lock();
    
    if (!activeState) {
       LOG.info("OpenFileCtx is inactive, fileId:"+request.getHandle().getFileId());
       WccData fileWcc = new WccData(latestAttr.getWccAttr(), latestAttr);
       WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3ERR_IO,
           fileWcc, 0, stableHow, Nfs3Constant.WRITE_COMMIT_VERF);
       Nfs3Utils.writeChannel(channel, response.send(new XDR(), xid));
       return;
    }
    
    // Handle repeated write requests(same xid or not)
    if (checkRepeatedWriteRequest(request, channel, xid)) {
      LOG.debug("Repeated unstable write request: xid=" + xid + " reqeust:"
          + request + " just drop it.");
      // Update the write time first
      updateLastAccessTime();
      ctxLock.lock();
      return;
    }
    
    // Get file length, fail non-append call
    WccAttr preOpAttr = latestAttr.getWccAttr();
    LOG.info("requesed offset=" + offset + " and current filesize="
        + preOpAttr.getSize());

    //assert(request.getStableHow() == WriteStableHow.UNSTABLE);
    long nextOffset = getNextOffset();
    if (offset == nextOffset) {
      LOG.info("Add to the list, update nextOffset and notify the writer,"
          + " nextOffset:" + nextOffset);
      addWrite(new WriteCtx(request.getHandle(), request.getOffset(),
          request.getCount(), request.getStableHow(), request.getData(),
          channel, xid, true, WriteCtx.NO_DUMP));
      nextOffset = offset + count;
      // Create an async task and change openFileCtx status to indicate async
      // task pending
      if (!asyncStatus) {
        asyncStatus = true;
        asyncDataService.execute(new AsyncDataService.WriteBackTask(this));
      }
      
      // Update the write time first
      updateLastAccessTime();
      ctxLock.unlock();

      // Send response immediately for unstable write
      if (request.getStableHow() == WriteStableHow.UNSTABLE) {
        WccData fileWcc = new WccData(preOpAttr, latestAttr);
        WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3_OK,
            fileWcc, count, stableHow, Nfs3Constant.WRITE_COMMIT_VERF);
        Nfs3Utils.writeChannel(channel, response.send(new XDR(), xid));
      }

    } else if (offset > nextOffset) {
      LOG.info("Add new write to the list but not update nextOffset:"
          + nextOffset);
      addWrite(new WriteCtx(request.getHandle(), request.getOffset(),
          request.getCount(), request.getStableHow(), request.getData(),
          channel, xid, true, WriteCtx.ALLOW_DUMP));
      // Check if need to dump some pending requests to file
      checkDump(request.getCount());
      updateLastAccessTime();
      ctxLock.unlock();
      
      // Send response immediately for unstable write
      if (request.getStableHow() == WriteStableHow.UNSTABLE) {
        WccData fileWcc = new WccData(preOpAttr, latestAttr);
        WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3_OK,
            fileWcc, count, stableHow, Nfs3Constant.WRITE_COMMIT_VERF);
        Nfs3Utils.writeChannel(channel, response.send(new XDR(), xid));
      }

    } else {
      // offset < nextOffset
      WccData wccData = new WccData(preOpAttr, null);
      WRITE3Response response;

      if (offset + count > nextOffset) {
        // Haven't noticed any partial overwrite out of a sequential file
        // write requests, so treat it as a real random write, no support.
        response = new WRITE3Response(Nfs3Status.NFS3ERR_INVAL, wccData, 0,
            WriteStableHow.UNSTABLE, 0);
      } else {
        response = processPerfectOverWrite(dfsClient, offset, count, stableHow,
            request.getData(), Nfs3Utils.getFileIdPath(request.getHandle()),
            wccData, iug);
      }
      
      updateLastAccessTime();
      ctxLock.unlock();
      Nfs3Utils.writeChannel(channel, response.send(new XDR(), xid));
    }
  }
  
  /**
   * Honor 2 kinds of overwrites: 1). support some application like touch(write
   * the same content back to change mtime), 2) client somehow sends the same
   * write again in a different RPC.
   */
  private WRITE3Response processPerfectOverWrite(DFSClient dfsClient,
      long offset, int count, WriteStableHow stableHow, byte[] data,
      String path, WccData wccData, IdUserGroup iug) {
    assert (ctxLock.isLocked());
    WRITE3Response response = null;

    // Read the content back
    byte[] readbuffer = new byte[count];

    int readCount = 0;
    FSDataInputStream fis = null;
    try {
      fis = new DFSClient.DFSDataInputStream(dfsClient.open(path));
      readCount = fis.read((int) offset, readbuffer, 0, count);
      if (readCount < count) {
        LOG.error("Can't read back " + count + " bytes, partial read size:"
            + readCount);
        return response = new WRITE3Response(Nfs3Status.NFS3ERR_IO, wccData, 0,
            stableHow, Nfs3Constant.WRITE_COMMIT_VERF);
      }

    } catch (IOException e) {
      LOG.info("Read failed when processing possible perfect overwrite, path="
          + path + " error:" + e);
      return response = new WRITE3Response(Nfs3Status.NFS3ERR_IO, wccData, 0,
          stableHow, Nfs3Constant.WRITE_COMMIT_VERF);
    } finally {
      if (fis != null) {
        try {
          fis.close();
        } catch (IOException e) {
          LOG.error("Can't close inputstream for " + path + " error:" + e);
        }
      }
    }

    // Compare with the request
    Comparator comparator = new Comparator();
    if (comparator.compare(readbuffer, 0, readCount, data, 0, count) != 0) {
      response = new WRITE3Response(Nfs3Status.NFS3ERR_INVAL, wccData, 0,
          stableHow, 0);
    } else {
      // Same content, updating the mtime, return success
      Nfs3FileAttributes postOpAttr = null;
      try {
        dfsClient.setTimes(path, System.currentTimeMillis(), -1);
        postOpAttr = Nfs3Utils.getFileAttr(dfsClient, path, iug);
      } catch (IOException e) {
        LOG.info("Got error when processing perfect overwrite, path=" + path
            + " error:" + e);
        return response = new WRITE3Response(Nfs3Status.NFS3ERR_IO, wccData,
            0, stableHow, 0);
      }

      wccData.setPostOpAttr(postOpAttr);
      response = new WRITE3Response(Nfs3Status.NFS3_OK, wccData, count,
          stableHow, 0);
    }
    return response;
  }
  
  public final static int COMMIT_FINISHED = 0;
  public final static int COMMIT_WAIT = 1;
  public final static int COMMIT_INACTIVE_CTX = 2;
  public final static int COMMIT_ERROR = 3;

  /**
   * return 0: committed, 1: can't commit, need more wait, 2, ctx not active, 3,
   * error encountered
   */
  public int checkCommit(long commitOffset) {
    ctxLock.lock();
    if (!activeState) {
      ctxLock.unlock();
      return COMMIT_INACTIVE_CTX;
    }
    if (commitOffset == 0) {
      // Commit whole file
      commitOffset = getNextOffset();
    }

    long flushed = 0;
    try {
      flushed = getFlushedOffset();
    } catch (IOException e) {
      LOG.error("Can't get flushed offset, error:" + e);
      ctxLock.unlock();
      return COMMIT_ERROR;
    }

    LOG.info("getFlushedOffset=" + flushed + " commitOffset=" + commitOffset);
    if (flushed < commitOffset) {
      // Keep stream active
      updateLastAccessTime();
      ctxLock.unlock();
      return COMMIT_WAIT;
    }

    int ret = COMMIT_WAIT;
    try {
      fos.sync();
      // Nothing to do for metadata since attr related change is pass-through
      ret = COMMIT_FINISHED;
    } catch (IOException e) {
      LOG.error("Got stream error during data sync:" + e);
      // Do nothing. Stream will be closed eventually by StreamMonitor.
      ret = COMMIT_ERROR;
    }

    // Keep stream active
    updateLastAccessTime();
    ctxLock.unlock();
    return ret;
  }
  
  private void addWrite(WriteCtx writeCtx) {
    long offset = writeCtx.getOffset();
    int count = writeCtx.getCount();
    
    ctxLock.lock();
    SortedMap<OffsetRange, WriteCtx> writes = getPendingWrites();
    writes.put(new OffsetRange(offset, offset + count), writeCtx);
    ctxLock.unlock();
  }
  
  
  /**
   * Check stream status to decide if it should be closed
   * @return true, remove stream; false, keep stream
   */
  public boolean streamCleanup(long fileId, long streamTimeout) {
    if (streamTimeout < WriteManager.MINIMIUM_STREAM_TIMEOUT) {
      throw new InvalidParameterException("StreamTimeout" + streamTimeout
          + "ms is less than MINIMIUM_STREAM_TIMEOUT "
          + WriteManager.MINIMIUM_STREAM_TIMEOUT + "ms");
    }
    if (!ctxLock.tryLock()) {
      // Another thread is working on it
      return false;
    }
    boolean flag = false;

    // Check the stream timeout
    if (checkStreamTimeout(streamTimeout)) {
      LOG.info("closing stream for fileId:" + fileId);
      cleanup();
      flag = true;
    }
    ctxLock.unlock();
    return flag;
  }
  
  // Invoked by AsynDataService to do the write back
  public void executeWriteBack() {
    ctxLock.lock();
    try {
      if (!asyncStatus) {
        // This should never happen.
        LOG.fatal("The openFileCtx has false async status");
        System.exit(-1);
      }
      if (getPendingWrites().isEmpty()) {
        // This should never happen.
        LOG.fatal("The asyn write task has no pendding writes! fileId:"
            + latestAttr.getFileId());
        System.exit(-2);
      }

      doWrites();

    } catch (IOException e) {
      LOG.info("got exception when writing back:" + e);
    } finally {
      // Always reset the async status so another async task can be created
      // for this file
      asyncStatus = false;
      ctxLock.unlock();
    }
  }

  private void doSingleWrite(final WriteCtx writeCtx) {
    assert(ctxLock.isLocked());
    Channel channel = writeCtx.getChannel();
    int xid = writeCtx.getXid();

    long offset = writeCtx.getOffset();
    int count = writeCtx.getCount();
    WriteStableHow stableHow = writeCtx.getStableHow();
    byte[] data = null;
    try {
      data = writeCtx.getData();
    } catch (IOException e1) {
      LOG.error("Failed to get request data offset:" + offset + " count:"
          + count + " error:" + e1);
      // Cleanup everything
      cleanup();
    }
    assert (data.length == count);

    FileHandle handle = writeCtx.getHandle();
    LOG.info("do write, fileId: " + handle.getFileId() + " offset: " + offset
        + " length:" + count + " stableHow:" + stableHow.getValue());

    try {
      fos.write(data, 0, count);

      if (fos.getPos() != (offset + count)) {
        throw new IOException("output stream is out of sync, pos="
            + fos.getPos() + " and nextOffset should be" + (offset + count));
      }
      nextOffset = fos.getPos();

      // Reduce memory occupation size if request was allowed dumped
      if (writeCtx.getDataState() == WriteCtx.ALLOW_DUMP) {
        updateNonSequentialWriteInMemory(-count);
      }
      
      if (!writeCtx.getReplied()) {
        WccAttr preOpAttr = latestAttr.getWccAttr();
        WccData fileWcc = new WccData(preOpAttr, latestAttr);
        WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3_OK,
            fileWcc, count, stableHow, Nfs3Constant.WRITE_COMMIT_VERF);
        Nfs3Utils.writeChannel(channel, response.send(new XDR(), xid));
      }

    } catch (IOException e) {
      LOG.error("Error writing to fileId " + handle.getFileId() + " at offset "
          + offset + " and length " + data.length, e);
     if (!writeCtx.getReplied()) {
        WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3ERR_IO);
        Nfs3Utils.writeChannel(channel, response.send(new XDR(), xid));
        // Keep stream open. Either client retries or SteamMonitor closes it.
      }
     
      LOG.info("Clean up open file context for fileId: "
          + latestAttr.getFileid());
      cleanup();
    }
  }

  private void cleanup() {
    activeState = false;
    
    // Close stream
    try {
      if (fos != null) {
        fos.close();
      }
    } catch (IOException e) {
      LOG.info("Can't close stream for fileId:" + latestAttr.getFileid()
          + ", error:" + e);
    }
    
    // Reply error for pending writes
    LOG.info("There are " + pendingWrites.size() + " pending writes.");
    WccAttr preOpAttr = latestAttr.getWccAttr();
    while (!pendingWrites.isEmpty()) {
      OffsetRange key = pendingWrites.firstKey();
      LOG.info("Fail pending write: (" + key.getMin() + "," + key.getMax()
          + "), nextOffset=" + getNextOffset());
      // TODO: may not need to reply if it's already replied
      WriteCtx writeCtx = pendingWrites.remove(key);
      WccData fileWcc = new WccData(preOpAttr, latestAttr);
      WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3ERR_IO,
          fileWcc, 0, writeCtx.getStableHow(), Nfs3Constant.WRITE_COMMIT_VERF);
      Nfs3Utils.writeChannel(writeCtx.getChannel(),
          response.send(new XDR(), writeCtx.getXid()));
    }
    
    // Cleanup dump file
    if (dumpOut!=null){
      try {
        dumpOut.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    if (raf!=null) {
      try {
        raf.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    File dumpFile = new File(dumpFilePath);
    dumpFile.delete();
  }
  
  private void doWrites() throws IOException {
    assert(ctxLock.isLocked());
    long nextOffset;
    OffsetRange key;
    WriteCtx writeCtx;
    SortedMap<OffsetRange, WriteCtx> pendingWrites = getPendingWrites();

    // Any single write failure can change activeState to false
    while (!pendingWrites.isEmpty() && activeState) {
      // Get the next sequential write
      nextOffset = getNextOffset();
      key = pendingWrites.firstKey();
      if (LOG.isTraceEnabled()) {
        LOG.trace("key.getMin()=" + key.getMin() + " nextOffset=" + nextOffset);
      }

      if (key.getMin() > nextOffset) {
        LOG.info("The next sequencial write has not arrived yet");
        return;

      } else if (key.getMin() < nextOffset && key.getMax() > nextOffset) {
        // Can't handle overlapping write. Didn't see it in tests yet.
        throw new IOException("Got a overlapping write (" + key.getMin() + ","
            + key.getMax() + "), nextOffset=" + nextOffset);

      } else {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Remove write(" + key.getMin() + "-" + key.getMax()
              + ") from the list");
        }
        writeCtx = pendingWrites.remove(key);
        // Do the write
        doSingleWrite(writeCtx);
        updateLastAccessTime();
      }
    }
  }
}