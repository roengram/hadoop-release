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

import java.io.IOException;
import java.security.InvalidParameterException;
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
  private long lastWriteTime;

  SortedMap<OffsetRange, WriteCtx> getPendingWrites() {
    return pendingWrites;
  }
  
  OpenFileCtx(FSDataOutputStream fos, Nfs3FileAttributes latestAttr) {
    this.fos = fos;
    this.latestAttr = latestAttr;
    this.pendingWrites = new TreeMap<OffsetRange, WriteCtx>();
    this.lastWriteTime = System.currentTimeMillis();
    this.activeState = true;
    this.asyncStatus = false;
    this.ctxLock =  new ReentrantLock();
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
  
  public void receivedNewWrite(DFSClient dfsClient, WRITE3Request request,
      Channel channel, int xid, AsyncDataService asyncDataService,
      IdUserGroup iug) {
    long offset = request.getOffset();
    int count = request.getCount();
    WriteStableHow stableHow = request.getStableHow();

    ctxLock.lock();
    
    // Get file length, fail non-append call
    WccAttr preOpAttr = latestAttr.getWccAttr();
    LOG.info("requesed offset=" + offset + " and current filesize="
        + preOpAttr.getSize());

    //assert(request.getStableHow() == WriteStableHow.UNSTABLE);
    long nextOffset = getNextOffset();
    if (offset == nextOffset) {
      LOG.info("Add to the list, update nextOffset and notify the writer,"
          + " nextOffset:" + nextOffset);
      addWrite(new WriteCtx(request, channel, xid, true));
      nextOffset = offset + count;
      // Create an async task and change openFileCtx status to indicate async
      // task pending
      if (!asyncStatus) {
        asyncStatus = true;
        asyncDataService.execute(new AsyncDataService.WriteBackTask(this));
      }
      ctxLock.unlock();

      // Send response immediately for unstable write
      if (request.getStableHow() == WriteStableHow.UNSTABLE) {
        WccData fileWcc = new WccData(preOpAttr, latestAttr);
        WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3_OK,
            fileWcc, count, stableHow, Nfs3Constant.WRITE_COMMIT_VERF);
        Nfs3Utils.writeChannel(channel, response.send(new XDR(), xid));
      }
      return;
    } else if (offset > nextOffset) {
      LOG.info("Add new write to the list but not update nextOffset:"
          + nextOffset);
      addWrite(new WriteCtx(request, channel, xid, false));

      ctxLock.unlock();

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
            count, stableHow, 0);
      }

      wccData.setPostOpAttr(postOpAttr);
      response = new WRITE3Response(Nfs3Status.NFS3_OK, wccData, count,
          stableHow, 0);
    }
    return response;
  }
  
  /** 
   * return 0: committed, 1: can't commit, need more wait, 2, ctx not active
   */
  public int checkCommit(long commitOffset) {
    ctxLock.lock();
    if (!activeState) {
      ctxLock.unlock();
      return 2;
    }
    if (commitOffset == 0) {
      // Commit whole file
      commitOffset = getNextOffset();
    }

    try {
      LOG.info("getFlushedOffset=" + getFlushedOffset() + " commitOffset="
          + commitOffset);
      if (getFlushedOffset() >= commitOffset) {
        fos.sync();
        // Nothing to do for metadata since attr related change is pass-through
        return 0;
      }
    } catch (IOException e) {
      LOG.error("Got stream error during data sync:" + e);
      // Do nothing. Stream will be closed eventually by StreamMonitor.
      return 1;
    } finally {
      ctxLock.unlock();
    }
    return 1;
  }
  
  public void addWrite(WriteCtx writeCtx) {
    long offset = writeCtx.getRequest().getOffset();
    int count = writeCtx.getRequest().getCount();
    
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
    if (System.currentTimeMillis() - lastWriteTime > streamTimeout) {
      LOG.info("closing stream for fileId:" + fileId);
      try {
        fos.close();
      } catch (IOException e) {
        LOG.info("Can't close stream for fileId:" + fileId + ", error:" + e);
      }
      // Reply error for pending writes
      while (!pendingWrites.isEmpty()) {
        OffsetRange key = pendingWrites.firstKey();
        LOG.info("Fail pending write: (" + key.getMin() + "," + key.getMax()
            + "), nextOffset=" + getNextOffset());
        WriteCtx writeCtx = pendingWrites.remove(key);

        WRITE3Response response = new WRITE3Response(Nfs3Status.NFS3ERR_IO);
        Nfs3Utils.writeChannel(writeCtx.getChannel(),
            response.send(new XDR(), writeCtx.getXid()));
      }

      activeState = false;
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
    WRITE3Request request = writeCtx.getRequest();
    Channel channel = writeCtx.getChannel();
    int xid = writeCtx.getXid();

    long offset = request.getOffset();
    int count = request.getCount();
    WriteStableHow stableHow = request.getStableHow();
    byte[] data = request.getData();
    assert (data.length == count);

    FileHandle handle = request.getHandle();
    LOG.info("do write, fileId: " + handle.getFileId() + " offset: " + offset
        + " length:" + count + " stableHow:" + stableHow.getValue());

    try {
      fos.write(data, 0, count);

      if (fos.getPos() != (offset + count)) {
        throw new IOException("output stream is out of sync, pos="
            + fos.getPos() + " and nextOffset should be" + (offset + count));
      }
      nextOffset = fos.getPos();

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
    }
  }

  private void doWrites() throws IOException {
    assert(ctxLock.isLocked());
    long nextOffset;
    OffsetRange key;
    WriteCtx writeCtx;
    SortedMap<OffsetRange, WriteCtx> pendingWrites = getPendingWrites();

    while (!pendingWrites.isEmpty()) {
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
        lastWriteTime = System.currentTimeMillis();
      }
    }
  }
}