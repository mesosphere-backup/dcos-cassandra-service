package com.mesosphere.dcos.cassandra.executor.backup.azure;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilterInputStream;
import java.io.IOException;

import static com.mesosphere.dcos.cassandra.executor.backup.azure.PageBlobOutputStream.ORIGINAL_SIZE_KEY;

/**
 * InputStream for working with Azure PageBlob.  PageBlobs stores everything in 512 pages.
 * The Azure Java libraries manage most of the buffer management.  It does nothing to
 * help manage the same sized files.  The most significant functionality of this
 * class is to retrieve the originalSize metadata from the blob and to cut off streaming
 * and the end of that byte.  It is designed to work in combination of PageBlobOutputStream
 * but it isn't required as long as the "originalSize" metadata is saved.
 */
public class PageBlobInputStream extends FilterInputStream {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private static final int EOF = -1;
  private CloudBlob pageBlob;

  // it is expected that the EOF is saved in the metadata
  // if not, then read until out of pages.
  private long totalsize = EOF;
  private long sizeRemaining;

  private volatile IOException lastError;

  /**
   * Creates a <code>FilterInputStream</code>
   * by assigning the  argument <code>in</code>
   * to the field <code>this.in</code> so as
   * to remember it for later use.
   *
   * @param pageBlob The Azure blob reference which has the underlying input stream,
   *                 or <code>null</code> if this instance is to be created without
   *                 an underlying stream.
   */
  public PageBlobInputStream(CloudBlob pageBlob) throws StorageException {
    super(pageBlob.openInputStream());
    this.pageBlob = pageBlob;
    setOriginalNonPageAlignedStreamSize();
  }

  /**
   * Sets up the original non page aligned size of the file from the PageBlob
   * from the meta-data.
   *
   * @throws StorageException
   */
  private void setOriginalNonPageAlignedStreamSize() throws StorageException {
    try {
      pageBlob.downloadAttributes();
      String size = pageBlob.getMetadata().get(ORIGINAL_SIZE_KEY);
      try {
        totalsize = Long.parseLong(size);
        sizeRemaining = totalsize;
      } catch (Exception e) {
        logger.error("Size meta-data missing.");
      }
    } catch (StorageException e) {
      this.lastError = new AzureIOException("Unable to download attributes from Page Blob.", e);
      throw e;
    }
  }

  @Override
  public int read() throws IOException {
    getReadsize(1);
    return super.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {

    this.checkStreamState();

    if (totalsize != EOF && sizeRemaining <= 0) {
      return EOF;
    }

    int readsize = getReadsize(len);
    final int numberOfBytesRead = in.read(b, off, readsize);
    return numberOfBytesRead;
  }

  private int getReadsize(int len) {
    int readsize = len;
    if (totalsize != EOF) {
      readsize = (int) Math.min(readsize, sizeRemaining);
      sizeRemaining -= readsize;
    }
    return readsize;
  }

  private void checkStreamState() throws IOException {
    if (lastError != null) {
      throw lastError;
    }
  }

  @Override
  public boolean markSupported() {
    return false;  // currently not supported (azure stream does)
    // Not tested best not to allow it unless we know it works.
  }
}
