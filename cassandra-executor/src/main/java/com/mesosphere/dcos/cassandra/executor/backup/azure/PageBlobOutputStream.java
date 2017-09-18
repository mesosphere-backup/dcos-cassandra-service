package com.mesosphere.dcos.cassandra.executor.backup.azure;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudPageBlob;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.util.HashMap;

/**
 * OutputStream for working with Azure PageBlob.  PageBlobs stores everything in 512 pages.
 * The Azure Java libraries manage most of the buffer management.  It does nothing to
 * help manage the same sized files.  The most significant functionality of this
 * class is to set the originalSize metadata in the blob.
 * This class also ensures that 512 pages are padded in.  This is an Azure
 * requirement.  Without it the last page fragment is never sent to Azure.
 * The CloudPageBlob.OutputStream has buffer optimizations which are defeated by
 * the default super doing byte by byte copies, hense the override.
 * It is designed to work in combination of PageBlobInputStream
 * but it isn't required as long as the "originalSize" metadata is saved.
 */
public class PageBlobOutputStream extends FilterOutputStream {

  private static final int PAGE_BLOB_PAGE_SIZE = 512;
  public static final String ORIGINAL_SIZE_KEY = "originalSize";

  private CloudPageBlob pageBlob;

  private long count = 0;
  private long currentPageSize = PAGE_BLOB_PAGE_SIZE;

  private volatile IOException lastError;
  private boolean closed = false;

  /**
   * Creates an output stream filter built on top of the Azure
   * PageBlob output stream.
   *
   * @param pageBlob The Azure blob reference which has the underlying output
   *                 stream to be assigned to
   *                 the field <tt>this.out</tt> for later use, or
   *                 <code>null</code> if this instance is to be
   *                 created without an underlying stream.
   */
  public PageBlobOutputStream(CloudPageBlob pageBlob) throws StorageException {
    // we don't know the total size, we default to a page
    this(pageBlob, PAGE_BLOB_PAGE_SIZE);
  }

  public PageBlobOutputStream(CloudPageBlob pageBlob, long initialPageSize) throws StorageException {
    // to set the size we need to calc but that can't be the first line when we need to construct the super:(
    super(pageBlob.openWriteNew(initialPageSize));
    this.pageBlob = pageBlob;
    resize(initialPageSize);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    internalWrite(b, off, len);
  }

  @Override
  public void write(int b) throws IOException {
    internalWrite(b);
  }

  @Override
  public void flush() throws IOException {
    // cannot flush untill close
  }

  @Override
  public void close() throws IOException {

    this.checkStreamState();
    if(closed)
      return;

    // top off page size
    // flush write does NOT occur unless full page is written
    if (count < currentPageSize) {
      byte[] pad = new byte[(int) (currentPageSize - count)];
      writePad(pad);
    }
    try {
      super.flush();
      super.close();
    } finally {
      closed = true;
    }
    uploadMetadata();
  }


  private void writePad(byte[] pad) throws IOException {
    out.write(pad);
  }

  private void internalWrite(byte[] b, int off, int len) throws IOException {
    internalWrite(b, off, len, false);
  }

  private void internalWrite(byte[] b, int off, int len, boolean isPad) throws IOException {
    this.checkStreamState();
    ensureCapacity(count + len);
    out.write(b, off, len);  // optimized in the azure driver
    if (!isPad) {
      count += len;
    }
  }

  private void internalWrite(int b) throws IOException {
    this.checkStreamState();
    ensureCapacity(count + 1);
    out.write(b);
    count += 1;
  }

  private void uploadMetadata() throws AzureIOException {
    pageBlob.setMetadata(fileMetaData(count));

    try {
      pageBlob.uploadMetadata();
    } catch (StorageException e) {
      throw new AzureIOException("Unable to upload metadata", e);
    }
  }

  private HashMap<String, String> fileMetaData(long count) {
    HashMap<String, String> metadata = new HashMap<>(1);
    metadata.put(ORIGINAL_SIZE_KEY, count + "");
    return metadata;
  }

  private void checkStreamState() throws IOException {
    if (lastError != null) {
      throw lastError;
    }
  }

  private void ensureCapacity(long minCapacity) {
    if (minCapacity > currentPageSize) {
      resize(minCapacity);
    }
  }

  private void resize(long streamSize) {
    if (streamSize > currentPageSize) {
      currentPageSize = roundToPageBlobSize(streamSize);
      try {
        pageBlob.resize(currentPageSize);
      } catch (StorageException e) {
        this.lastError = new AzureIOException("Unable to resize blob.", e);
      }
    }
  }

  private static long roundToPageBlobSize(long size) {
    return (size + PAGE_BLOB_PAGE_SIZE - 1) & ~(PAGE_BLOB_PAGE_SIZE - 1);
  }
}
