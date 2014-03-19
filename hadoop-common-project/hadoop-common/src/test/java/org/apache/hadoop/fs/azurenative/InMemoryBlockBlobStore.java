package org.apache.hadoop.fs.azurenative;

import java.io.*;
import java.util.*;

/**
 * A simple memory key-value store to help mock the
 * Windows Azure Storage implementation for unit testing.
 */
public class InMemoryBlockBlobStore {
  private final HashMap<String, Entry> blobs =
      new HashMap<String, Entry>();
  private HashMap<String, String> containerMetadata;

  public synchronized Iterable<String> getKeys() {
    return new ArrayList<String>(blobs.keySet());
  }

  public static class ListBlobEntry {
    private final String key;
    private final HashMap<String, String> metadata;
    private final int contentLength;

    ListBlobEntry(String key, HashMap<String, String> metadata, int contentLength) {
      this.key = key;
      this.metadata = metadata;
      this.contentLength = contentLength;
    }

    public String getKey() {
      return key;
    }

    public HashMap<String, String> getMetadata() {
      return metadata;
    }

    public int getContentLength() {
      return contentLength;
    }
  }

  /**
   * List all the blobs whose key starts with the given prefix.
   * @param prefix The prefix to check.
   * @param includeMetadata If set, the metadata in the returned listing will
   *                        be populated; otherwise it'll be null.
   * @return The listing.
   */
  public synchronized Iterable<ListBlobEntry> listBlobs(String prefix,
      boolean includeMetadata) {
    ArrayList<ListBlobEntry> list = new ArrayList<ListBlobEntry>();
    for (Map.Entry<String, Entry> entry : blobs.entrySet()) {
      if (entry.getKey().startsWith(prefix)) {
        list.add(new ListBlobEntry(
            entry.getKey(),
            includeMetadata ?
                new HashMap<String, String>(entry.getValue().metadata) :
                  null,
            entry.getValue().content.length));
      }
    }
    return list;
  }

  public synchronized byte[] getContent(String key) {
    return blobs.get(key).content;
  }

  @SuppressWarnings("unchecked")
  public synchronized void setContent(String key, byte[] value,
      HashMap<String, String> metadata) {
    blobs.put(key, new Entry(value, (HashMap<String, String>)metadata.clone()));
  }
  
  public OutputStream upload(final String key,
      final HashMap<String, String> metadata) {
    setContent(key, new byte[0], metadata);
    return new ByteArrayOutputStream() {
      @Override
      public void flush()
          throws IOException {
        super.flush();
        setContent(key, toByteArray(), metadata);
      }
    };
  }

  public synchronized void copy(String sourceKey, String destKey) {
    blobs.put(destKey, blobs.get(sourceKey));
  }

  public synchronized void delete(String key) {
    blobs.remove(key);
  }

  public synchronized boolean exists(String key) {
    return blobs.containsKey(key);
  }

  @SuppressWarnings("unchecked")
  public synchronized HashMap<String, String> getMetadata(String key) {
    return (HashMap<String, String>)blobs.get(key).metadata.clone();
  }

  public synchronized HashMap<String, String> getContainerMetadata() {
    return containerMetadata;
  }

  public synchronized void setContainerMetadata(
      HashMap<String, String> metadata) {
    containerMetadata = metadata;
  }

  private static class Entry {
    private byte[] content;
    private HashMap<String, String> metadata;
 
    public Entry(byte[] content, HashMap<String, String> metadata) {
      this.content = content;
      this.metadata = metadata;
    }
  }
}
