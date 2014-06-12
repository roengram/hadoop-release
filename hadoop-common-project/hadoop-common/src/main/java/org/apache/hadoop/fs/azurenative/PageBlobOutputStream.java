package org.apache.hadoop.fs.azurenative;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.*;

import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.azurenative.StorageInterface.*;

import com.microsoft.windowsazure.storage.OperationContext;
import com.microsoft.windowsazure.storage.StorageException;
import com.microsoft.windowsazure.storage.blob.BlobRequestOptions;
import com.microsoft.windowsazure.storage.blob.CloudPageBlob;

import static org.apache.hadoop.fs.azurenative.PageBlobFormatHelpers.*;

/**
 * An output stream that write file data to a page blob stored using ASV's
 * custom format.
 */
final class PageBlobOutputStream extends OutputStream implements Syncable {
  /**
   * The maximum number of raw bytes Azure Storage allows us to upload in a
   * single request (4 MB).
   */
  private static final int MAX_RAW_BYTES_PER_REQUEST = 4 * 1024 * 1024;
  /**
   * The maximum number of pages Azure Storage allows us to upload in a
   * single request.
   */
  private static final int MAX_PAGES_IN_REQUEST =
      MAX_RAW_BYTES_PER_REQUEST / PAGE_SIZE;
  /**
   * The maximum number of data bytes (header not included) we can upload
   * in a single request. I'm limiting it to (N - 1) pages to account for
   * the possibility that we may have to rewrite the previous request's
   * last page.
   */
  private static final int MAX_DATA_BYTES_PER_REQUEST =
      PAGE_DATA_SIZE * (MAX_PAGES_IN_REQUEST - 1);

  /**
   * Capacity of the IO queue (threads would have to block if it's filled).
   * Note: Threads do not block if it is filled but are rejected and throws.
   *       with an  RejectedExecutionException as such.
   * TODO: Temporary fix is to use an unbounded LinkedBlockingQueeu. The real
   *       fix should perform internal hsync when queue is approaching capacity.
   */
  private static final int OUTSTANDING_IO_CAPACITY = 150;

  private final CloudPageBlobWrapper blob;
  private final OperationContext opContext;

  /**
   * If the IO thread encounters an error, it'll store it here.
   */
  private volatile IOException lastError;

  /**
   * The current byte offset we're at in the blob (how many bytes we've
   * uploaded to the server).
   */
  private long currentBlobOffset;
  /**
   * The data in the last page that we wrote to the server, in case we have to
   * overwrite it in the new request.
   */
  private byte[] previousLastPageDataWritten = new byte[0];
  /**
   * The current buffer we're writing to before sending to the server.
   */
  private ByteArrayOutputStream outBuffer;
  /**
   * The task queue for writing to the server.
   */
  // private final ArrayBlockingQueue<Runnable> ioQueue;
  private final LinkedBlockingQueue<Runnable> ioQueue;
  /**
   * The thread pool we're using for writing to the server. Note that the IO
   * write is NOT designed for parallelism, so there can only be one thread
   * in that pool (I'm using the thread pool mainly for the lifetime management
   * capabilities, otherwise I'd have just used a simple Thread).
   */
  private final ThreadPoolExecutor ioThreadPool;

  /**
   * Constructs an output stream over the given page blog.
   *
   * @param blob the blob that this stream is associated with.
   * @param opContext an object used to track the execution of the operation
   * @throws StorageException if anything goes wrong creating the blob.
   */
  public PageBlobOutputStream(final CloudPageBlobWrapper blob,
      final OperationContext opContext) throws StorageException {
    this.blob = blob;
    this.outBuffer = new ByteArrayOutputStream();
    this.opContext = opContext;
    // this.ioQueue = new ArrayBlockingQueue<Runnable>(OUTSTANDING_IO_CAPACITY);
    this.ioQueue = new LinkedBlockingQueue<Runnable>();
    
    // As explained above: the IO writes are not designed for parallelism,
    // so we only have one thread in this thread pool.
    this.ioThreadPool = new ThreadPoolExecutor(1, 1, 2, TimeUnit.SECONDS,
        ioQueue);

    blob.create(1024l * 1024l * 1024l * 1024l, // TODO: validate this
        new BlobRequestOptions(), opContext);
  }

  private void checkStreamState() throws IOException {
    if (lastError != null) {
      throw lastError;
    }
  }

  /**
   * Closes this output stream and releases any system resources associated with
   * this stream. If any data remains in the buffer it is committed to the
   * service.
   */
  @Override
  public void close() throws IOException {
    flush();
    checkStreamState();
    ioThreadPool.shutdown();
    try {
      if (!ioThreadPool.awaitTermination(10, TimeUnit.MINUTES)) {
        throw new IOException("Timed out waiting for IO requests to finish");
      }
    } catch (InterruptedException e) {
    }

    // (Clever trick shamelessly copied from Azure SDK)
    this.lastError = new IOException("Stream is already closed.");
  }

  /**
   * A single write request for data to write to Azure storage.
   */
  private class WriteRequest implements Runnable {
    private final byte[] dataPayload;
    private final CountDownLatch doneSignal = new CountDownLatch(1);

    public WriteRequest(byte[] dataPayload) {
      this.dataPayload = dataPayload;
    }

    public void waitTillDone() throws InterruptedException {
      doneSignal.await();
    }

    @Override
    public void run() {
      try {
        runInternal();
      } finally {
        doneSignal.countDown();
      }
    }

    private void runInternal() {
      if (lastError != null) {
        // We're already in an error state, no point doing anything.
        return;
      }
      if (dataPayload.length == 0) {
        // Nothing to do.
        return;
      }

      // Since we have to rewrite the last request's last page's data
      // (may be empty), total data size is our data plus whatever was
      // left from there.
      final int totalDataBytes = dataPayload.length +
          previousLastPageDataWritten.length;
      // Calculate the total number of pages we're writing to the server.
      final int numberOfPages = (totalDataBytes / PAGE_DATA_SIZE) +
          (totalDataBytes % PAGE_DATA_SIZE == 0 ? 0 : 1);
      // Fill up the raw bytes we're writing.
      byte[] rawPayload = new byte[numberOfPages * PAGE_SIZE];
      // Keep track of the size of the last page we uploaded.
      int currentLastPageDataSize = -1;
      for (int page = 0; page < numberOfPages; page++) {
        // Our current byte offset in the data.
        int dataOffset = page * PAGE_DATA_SIZE;
        // Our current byte offset in the raw buffer.
        int rawOffset = page * PAGE_SIZE;
        // The size of the data in the current page.
        final short currentPageDataSize = (short)Math.min(PAGE_DATA_SIZE,
            totalDataBytes - dataOffset);
        // Save off this page's size as the potential last page's size.
        currentLastPageDataSize = currentPageDataSize;

        // Write out the page size in the header.
        final byte[] header = fromShort(currentPageDataSize);
        System.arraycopy(header, 0, rawPayload, rawOffset, header.length);
        rawOffset += header.length;

        int bytesToCopyFromDataPayload = currentPageDataSize;
        if (dataOffset < previousLastPageDataWritten.length) {
          // First write out the last page's data.
          final int bytesToCopyFromLastPage = Math.min(currentPageDataSize,
              previousLastPageDataWritten.length - dataOffset);
          System.arraycopy(previousLastPageDataWritten, dataOffset,
              rawPayload, rawOffset, bytesToCopyFromLastPage);
          bytesToCopyFromDataPayload -= bytesToCopyFromLastPage;
          rawOffset += bytesToCopyFromLastPage;
          dataOffset += bytesToCopyFromLastPage;
        }

        if (dataOffset >= previousLastPageDataWritten.length) {
          // Then write the current payload's data.
          System.arraycopy(dataPayload, dataOffset - previousLastPageDataWritten.length,
              rawPayload, rawOffset, bytesToCopyFromDataPayload);
        }
      }

      // Raw payload constructed, ship it off to the server.
      writePayloadToServer(rawPayload);

      // Post-send bookkeeping.
      currentBlobOffset += rawPayload.length;
      if (currentLastPageDataSize < PAGE_DATA_SIZE) {
        // Partial page, save it off so it's overwritten in the next request.
        final int startOffset = (numberOfPages - 1) * PAGE_SIZE + PAGE_HEADER_SIZE;
        previousLastPageDataWritten = Arrays.copyOfRange(rawPayload,
            startOffset,
            startOffset + currentLastPageDataSize);
        // Since we're rewriting this page, set our current offset in the server
        // to that page's beginning.
        currentBlobOffset -= PAGE_SIZE;
      } else {
        // It wasn't a partial page, we won't need to rewrite it.
        previousLastPageDataWritten = new byte[0];
      }
    }

    /**
     * Writes the given raw payload to Azure Storage at the current blob
     * offset.
     */
    private void writePayloadToServer(byte[] rawPayload) {
      final ByteArrayInputStream wrapperStream = 
                  new ByteArrayInputStream(rawPayload);
      try {
        blob.uploadPages(wrapperStream, currentBlobOffset, rawPayload.length, 
            withMD5Checking(), PageBlobOutputStream.this.opContext);
      } catch (IOException ex) {
        lastError = ex;
      } catch (StorageException ex) {
       lastError = new IOException(ex);
      }
    }
  }

  private synchronized void flushIOBuffers()  {
    if (outBuffer.size() == 0) {
      return;
    }
    ioThreadPool.execute(new WriteRequest(outBuffer.toByteArray()));
    outBuffer = new ByteArrayOutputStream();
  }

  /**
   * Flushes this output stream and forces any buffered output bytes to be
   * written out. If any data remains in the buffer it is committed to the
   * service.
   */
  @Override
  public void flush() throws IOException {
    checkStreamState();
    flushIOBuffers();
  }

  /**
   * Writes b.length bytes from the specified byte array to this output stream.
   *
   * @param data
   *          the byte array to write.
   *
   * @throws IOException
   *           if an I/O error occurs. In particular, an IOException may be
   *           thrown if the output stream has been closed.
   */
  @Override
  public void write(final byte[] data) throws IOException {
    write(data, 0, data.length);
  }

  /**
   * Writes length bytes from the specified byte array starting at offset to
   * this output stream.
   *
   * @param data
   *          the byte array to write.
   * @param offset
   *          the start offset in the data.
   * @param length
   *          the number of bytes to write.
   * @throws IOException
   *           if an I/O error occurs. In particular, an IOException may be
   *           thrown if the output stream has been closed.
   */
  @Override
  public void write(final byte[] data, final int offset, final int length)
      throws IOException {
    if (offset < 0 || length < 0 || length > data.length - offset) {
      throw new IndexOutOfBoundsException();
    }

    writeInternal(data, offset, length);
  }

  /**
   * Writes the specified byte to this output stream. The general contract for
   * write is that one byte is written to the output stream. The byte to be
   * written is the eight low-order bits of the argument b. The 24 high-order
   * bits of b are ignored.
   *
   * @param byteVal
   *          the byteValue to write.
   * @throws IOException
   *           if an I/O error occurs. In particular, an IOException may be
   *           thrown if the output stream has been closed.
   */
  @Override
  public void write(final int byteVal) throws IOException {
    write(new byte[] { (byte) (byteVal & 0xFF) });
  }

  /**
   * Writes the data to the buffer and triggers writes to the service as needed.
   *
   * @param data
   *          the byte array to write.
   * @param offset
   *          the start offset in the data.
   * @param length
   *          the number of bytes to write.
   * @throws IOException
   *           if an I/O error occurs. In particular, an IOException may be
   *           thrown if the output stream has been closed.
   */
  private synchronized void writeInternal(final byte[] data, int offset,
      int length) throws IOException {
    while (length > 0) {
      checkStreamState();
      final int availableBufferBytes = MAX_DATA_BYTES_PER_REQUEST
          - this.outBuffer.size();
      final int nextWrite = Math.min(availableBufferBytes, length);

      outBuffer.write(data, offset, nextWrite);
      offset += nextWrite;
      length -= nextWrite;

      if (outBuffer.size() == MAX_DATA_BYTES_PER_REQUEST) {
        flushIOBuffers();
      }
    }
  }

  // TODO: Right now we treat hsync and hflush as the same.
  // hflush and hsync is the same. hsync invokes hflush. hflush
  // guarantees that flushed data become visible to new readers.
  // It is not guaranteed that data has been flushed to persistent
  // store on the datanode. So using hflush may lose some data
  // if the datanode failures happen. hsync is designed to guarantee
  // that all data writes to the storage are completed but is not
  // implemented now.
  
  @Override
  public void hsync() throws IOException {
    // Sync deprecated in favor of hflush.
  	flush();
  	Runnable[] ioQueueSnapshot = ioQueue.toArray(new Runnable[0]);
  	if (ioQueueSnapshot.length == 0) {
  	  return;
  	}
  	WriteRequest lastRequest =
  				(WriteRequest) ioQueueSnapshot[ioQueueSnapshot.length - 1];
  	try {
  		lastRequest.waitTillDone();
  	} catch (InterruptedException e) {
  	  // Yield, we've been interrupted.
  	}
  }
  
  @Override
  public void hflush() throws IOException {
    flush();
  }
  
  @Deprecated
  public void sync() throws IOException {
    // Sync has been deprecated in favor of hflush.
    hflush();
  }

  // For unit testing purposes: kill the IO threads.
  void killIoThreads() {
    ioThreadPool.shutdownNow();
  }
}
