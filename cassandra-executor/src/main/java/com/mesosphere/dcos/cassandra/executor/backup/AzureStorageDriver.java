package com.mesosphere.dcos.cassandra.executor.backup;

import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreContext;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class AzureStorageDriver implements BackupStorageDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(AzureStorageDriver.class);

    public static final int DEFAULT_PART_SIZE_UPLOAD = 4 * 1024 * 1024; // Chunk size set to 4MB

    public static final int DEFAULT_PART_SIZE_DOWNLOAD = 4 * 1024 * 1024; // Chunk size set to 4MB

    @Override
    public void upload(BackupContext ctx) throws IOException {
        final String endpointProtocol = "http";
        final String accountName = "mohitpage";
        final String accountKey = "o7tXASUgqS+/BHi2Oh1bbHYVbfnp3Hsjqy3vEE6VccfQmNcr1aUEiWE2UnTdXlrcIuNsI5h2KSyxkB7DEQarYA==";
        final String storageConnectionString = "DefaultEndpointsProtocol=" + endpointProtocol
                + ";AccountName=" + accountName
                + ";AccountKey=" + accountKey;
        final String containerName = StringUtils.lowerCase("cassandra-backups");
        BlobOutputStream blobOutputStream = null;
        FileInputStream fileInputStream = null;
        BlobInputStream blobInputStream = null;
        FileOutputStream fileOutputStream = null;
        try {
            final int buffSize = 512 * 512;

            final CloudStorageAccount account = CloudStorageAccount.parse(storageConnectionString);
            CloudBlobClient serviceClient = account.createCloudBlobClient();
            final CloudBlobContainer container = serviceClient.getContainerReference(containerName);
            container.createIfNotExists();

            final CloudPageBlob blob = container.getPageBlobReference("task1.log");
            final File sourceFile = new File("/tmp/task1.log");

            long sourceFileLength = sourceFile.length();
            fileInputStream = new FileInputStream(sourceFile);
            byte[] buffer = new byte[buffSize];
            int buffLen = 0;

            if (sourceFileLength % 512 != 0) {
                sourceFileLength = sourceFileLength + (512 - sourceFileLength % 512);
            }

            blobOutputStream = blob.openWriteNew(sourceFileLength);
            while ((buffLen = fileInputStream.read(buffer)) != -1) {
                if (buffLen % buffSize != 0) {
                    final int paddingBits = buffSize - buffLen % buffSize;

                    for (int i = buffLen; i < buffSize; i++) {
                        buffer[i] = 0;
                    }
                    buffLen = buffLen + paddingBits - 1;
                }

                blobOutputStream.write(buffer);

                buffer = new byte[buffSize];
            }

            // Download
            final File destFile = new File(sourceFile.getParentFile(), "task1.log.tmp");
            blob.downloadToFile("/tmp/task1.log.tmp");
//            blobInputStream = blob.openInputStream();
//            fileOutputStream = new FileOutputStream(destFile);
//
//            byte[] lastReadBuffer = null;
//            int bytesRead = -1;
//            while ((bytesRead = blobInputStream.read(buffer)) != -1) {
////                System.out.println(bytesRead);
////                if (lastReadBuffer != null) {
//                    fileOutputStream.write(buffer);
////                }
////
////                lastReadBuffer = Arrays.copyOf(buffer, buffer.length);
////                buffer = new byte[buffSize];
//            }
//
//            if (lastReadBuffer != null) {
//                int i = 0;
//                for (i = 0; i < lastReadBuffer.length; i++) {
//                    if (lastReadBuffer[i] == 10) {
//                        break;
//                    }
//                }
//                fileOutputStream.write(lastReadBuffer, 0, i);
//            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(blobOutputStream);
            IOUtils.closeQuietly(fileInputStream);
            IOUtils.closeQuietly(fileOutputStream);
            IOUtils.closeQuietly(blobInputStream);
        }
    }

    @Override
    public void download(RestoreContext ctx) throws IOException {
    }

    public static void main(String[] args) throws Exception {
        final AzureStorageDriver azureStorageDriver = new AzureStorageDriver();
        azureStorageDriver.upload(null);
    }
}
