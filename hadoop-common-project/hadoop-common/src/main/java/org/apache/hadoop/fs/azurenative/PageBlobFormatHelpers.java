package org.apache.hadoop.fs.azurenative;

import java.nio.*;

import com.microsoft.windowsazure.storage.blob.BlobRequestOptions;

/**
 * Constants and helper methods for ASV's custom data format in page blobs.
 */
class PageBlobFormatHelpers {
  public static final short PAGE_SIZE = 512;
  public static final short PAGE_HEADER_SIZE = 2;
  public static final short PAGE_DATA_SIZE = PAGE_SIZE - PAGE_HEADER_SIZE;

  /**
   * Stores the given short as a two-byte array.
   */
  public static byte[] fromShort(short s) {
    return ByteBuffer.allocate(2).putShort(s).array();
  }

  /**
   * Retrieves a short from the given two bytes.
   */
  public static short toShort(byte firstByte, byte secondByte) {
    return ByteBuffer.wrap(new byte[] { firstByte, secondByte })
        .getShort();
  }

  public static BlobRequestOptions withMD5Checking() {
    BlobRequestOptions options = new BlobRequestOptions();
    options.setUseTransactionalContentMD5(true);
    return options;
  }
}
