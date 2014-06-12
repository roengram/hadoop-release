package org.apache.hadoop.fs.azurenative;

import java.io.*;
import java.net.*;
import java.util.*;

import com.microsoft.windowsazure.storage.*;
import com.microsoft.windowsazure.storage.blob.*;

/**
 * A real implementation of the Azure interaction layer that
 * just redirects calls to the Windows Azure storage SDK.
 */
class StorageInterfaceImpl extends StorageInterface {
  private CloudBlobClient serviceClient;

  @Override
  public void setRetryPolicyFactory(final RetryPolicyFactory retryPolicyFactory) {
    serviceClient.setRetryPolicyFactory(retryPolicyFactory);
  }

  @Override
  public void setTimeoutInMs(int timeoutInMs) {
    serviceClient.setTimeoutInMs(timeoutInMs);
  }

  @Override
  public void createBlobClient(CloudStorageAccount account) {
    serviceClient = account.createCloudBlobClient();
  }

  @Override
  public void createBlobClient(URI baseUri) {
    serviceClient = new CloudBlobClient(baseUri);
  }

  @Override
  public void createBlobClient(URI baseUri,
      StorageCredentials credentials) {
    serviceClient = new CloudBlobClient(baseUri, credentials);
  }

  @Override
  public StorageCredentials getCredentials() {
    return serviceClient.getCredentials();
  }


  @Override
  public CloudBlobContainerWrapper getContainerReference(String uri)
      throws URISyntaxException, StorageException {
    return new CloudBlobContainerWrapperImpl(
        serviceClient.getContainerReference(uri));
  }


  //
  // WrappingIterator
  //

  /**
   * This iterator wraps every ListBlobItem as they come from the
   * listBlobs() calls to their proper wrapping objects.
   */
  private static class WrappingIterator implements Iterator<ListBlobItem> {
    private final Iterator<ListBlobItem> present;

    public WrappingIterator(Iterator<ListBlobItem> present) {
      this.present = present;
    }

    public static Iterable<ListBlobItem> Wrap(
        final Iterable<ListBlobItem> present) {
      return new Iterable<ListBlobItem>() {
        @Override
        public Iterator<ListBlobItem> iterator() {
          return new WrappingIterator(present.iterator());
        }
      };
    }

    @Override
    public boolean hasNext() {
      return present.hasNext();
    }

    @Override
    public ListBlobItem next() {
      ListBlobItem unwrapped = present.next();
      if (unwrapped instanceof CloudBlobDirectory) {
        return new CloudBlobDirectoryWrapperImpl(
            (CloudBlobDirectory)unwrapped);
      } else if (unwrapped instanceof CloudBlockBlob) {
        return new CloudBlockBlobWrapperImpl(
            (CloudBlockBlob)unwrapped);
      } else if (unwrapped instanceof CloudPageBlob) {
        return new CloudPageBlobWrapperImpl(
            (CloudPageBlob)unwrapped);
      } else {
        return unwrapped;
      }
    }

    @Override
    public void remove() {
      present.remove();
    }
  }

  //
  // CloudBlobDirectoryWrapperImpl
  //
  static class CloudBlobDirectoryWrapperImpl extends CloudBlobDirectoryWrapper {
    private final CloudBlobDirectory directory;

    public CloudBlobDirectoryWrapperImpl(CloudBlobDirectory directory) {
      this.directory = directory;
    }

    @Override
    public URI getUri() {
      return directory.getUri();
    }

    @Override
    public Iterable<ListBlobItem> listBlobs(String prefix,
        boolean useFlatBlobListing, EnumSet<BlobListingDetails> listingDetails,
        BlobRequestOptions options, OperationContext opContext)
        throws URISyntaxException, StorageException {
      return WrappingIterator.Wrap(
          directory.listBlobs(prefix, useFlatBlobListing,
          listingDetails, options, opContext));
    }

    @Override
    public CloudBlobContainer getContainer() throws URISyntaxException,
        StorageException {
      return directory.getContainer();
    }

    @Override
    public CloudBlobDirectory getParent() throws URISyntaxException,
        StorageException {
      return directory.getParent();
    }

    @Override
    public StorageUri getStorageUri() {
      return directory.getStorageUri();
    }

  }

  //
  // CloudBlobContainerWrapperImpl
  //

  static class CloudBlobContainerWrapperImpl extends CloudBlobContainerWrapper {
    private final CloudBlobContainer container;

    public CloudBlobContainerWrapperImpl(CloudBlobContainer container) {
      this.container = container;
    }

    @Override
    public String getName() {
        return container.getName();
    }
    @Override
    public boolean exists(OperationContext opContext) throws StorageException {
      return container.exists(AccessCondition.generateEmptyCondition(), null, opContext);
    }

    @Override
    public void create(OperationContext opContext) throws StorageException {
      container.create(null, opContext);
    }

    @Override
    public HashMap<String, String> getMetadata() {
      return container.getMetadata();
    }

    @Override
    public void setMetadata(HashMap<String, String> metadata) {
      container.setMetadata(metadata);
    }

    @Override
    public void downloadAttributes(OperationContext opContext)
        throws StorageException {
      container.downloadAttributes(AccessCondition.generateEmptyCondition(), null, opContext);
    }

    @Override
    public void uploadMetadata(OperationContext opContext)
        throws StorageException {
      container.uploadMetadata(AccessCondition.generateEmptyCondition(), null, opContext);
    }

    @Override
    public CloudBlobDirectoryWrapper getDirectoryReference(String relativePath)
        throws URISyntaxException, StorageException {

      CloudBlobDirectory dir = container.getDirectoryReference(relativePath);
      return new CloudBlobDirectoryWrapperImpl(dir);
    }

    @Override
    public CloudBlobWrapper getBlockBlobReference(String relativePath)
        throws URISyntaxException, StorageException {

      return new CloudBlockBlobWrapperImpl(container.getBlockBlobReference(relativePath));
    }

    @Override
    public CloudBlobWrapper getPageBlobReference(String relativePath)
        throws URISyntaxException, StorageException {
      return new CloudPageBlobWrapperImpl(
          container.getPageBlobReference(relativePath));
    }
  }

  static abstract class CloudBlobWrapperImpl implements CloudBlobWrapper {
    protected final CloudBlob blob;

    public URI getUri() {
      return blob.getUri();
    }

    protected CloudBlobWrapperImpl(CloudBlob blob) {
      this.blob = blob;
    }

    @Override
    public HashMap<String, String> getMetadata() {
      return blob.getMetadata();
    }

    @Override
    public void delete(OperationContext opContext)
        throws StorageException {
      blob.delete(DeleteSnapshotsOption.NONE, null, null, opContext);
    }

    @Override
    public boolean exists(OperationContext opContext)
        throws StorageException {
      return blob.exists(null, null, opContext);
    }

    @Override
    public void downloadAttributes(
        OperationContext opContext) throws StorageException {
      blob.downloadAttributes(null, null, opContext);
    }

    @Override
    public BlobProperties getProperties() {
      return blob.getProperties();
    }

    @Override
    public void setMetadata(HashMap<String, String> metadata) {
      blob.setMetadata(metadata);
    }

    @Override
    public InputStream openInputStream(
        BlobRequestOptions options,
        OperationContext opContext) throws StorageException {
      return blob.openInputStream(null, options, opContext);
    }

    public OutputStream openOutputStream(
        BlobRequestOptions options,
        OperationContext opContext) throws StorageException {
      return ((CloudBlockBlob)blob).openOutputStream(null, options, opContext);
    }

    public void upload(InputStream sourceStream, OperationContext opContext)
        throws StorageException, IOException {
      blob.upload(sourceStream, 0, null, null, opContext);
    }

    @Override
    public CloudBlobContainer getContainer() throws URISyntaxException,
        StorageException {
      return blob.getContainer();
    }

    @Override
    public CloudBlobDirectory getParent() throws URISyntaxException,
        StorageException {
      return blob.getParent();
    }

    @Override
    public void uploadMetadata(OperationContext opContext)
        throws StorageException {
      blob.uploadMetadata(null, null, opContext);
    }

    public void uploadProperties(OperationContext opContext)
        throws StorageException {
      blob.uploadProperties(null, null, opContext);
    }

    @Override
    public void setStreamMinimumReadSizeInBytes(int minimumReadSizeBytes) {
      blob.setStreamMinimumReadSizeInBytes(minimumReadSizeBytes);
    }

    @Override
    public void setWriteBlockSizeInBytes(int writeBlockSizeBytes) {
      blob.setStreamWriteSizeInBytes(writeBlockSizeBytes);
    }

    @Override
    public StorageUri getStorageUri() {
      return blob.getStorageUri();
    }

    @Override
    public CopyState getCopyState() {
      return blob.getCopyState();
    }

    @Override
    public void startCopyFromBlob(CloudBlobWrapper sourceBlob,
        OperationContext opContext)
            throws StorageException, URISyntaxException {
      blob.startCopyFromBlob(((CloudBlobWrapperImpl)sourceBlob).blob,
          null, null, null, opContext);
    }

    @Override
    public void downloadRange(long offset, long length, OutputStream outStream,
        BlobRequestOptions options, OperationContext opContext)
            throws StorageException, IOException {

      blob.downloadRange(offset, length, outStream, null, options, opContext);
    }
  }

  //
  // CloudBlockBlobWrapperImpl
  //

  static class CloudBlockBlobWrapperImpl extends CloudBlobWrapperImpl implements CloudBlockBlobWrapper {
    public CloudBlockBlobWrapperImpl(CloudBlockBlob blob) {
      super(blob);
    }

    public OutputStream openOutputStream(
        BlobRequestOptions options,
        OperationContext opContext) throws StorageException {
      return ((CloudBlockBlob)blob).openOutputStream(null, options, opContext);
    }

    public void upload(InputStream sourceStream, OperationContext opContext)
        throws StorageException, IOException {
      blob.upload(sourceStream, 0, null, null, opContext);
    }

    public void uploadProperties(OperationContext opContext)
        throws StorageException {
      blob.uploadProperties(null, null, opContext);
    }

  }

  static class CloudPageBlobWrapperImpl extends CloudBlobWrapperImpl implements CloudPageBlobWrapper {
    public CloudPageBlobWrapperImpl(CloudPageBlob blob) {
      super(blob);
    }

    public void create(final long length, BlobRequestOptions options,
        OperationContext opContext) throws StorageException {
      ((CloudPageBlob)blob).create(length, null, options, opContext);
    }

    public void uploadPages(final InputStream sourceStream, final long offset,
        final long length, BlobRequestOptions options, OperationContext opContext)
        throws StorageException, IOException {
      ((CloudPageBlob)blob).uploadPages(sourceStream, offset, length, null,
          options, opContext);
    }

    public ArrayList<PageRange> downloadPageRanges(BlobRequestOptions options,
        OperationContext opContext) throws StorageException {
      return ((CloudPageBlob)blob).downloadPageRanges(
          null, options, opContext);
    }
  }
}
