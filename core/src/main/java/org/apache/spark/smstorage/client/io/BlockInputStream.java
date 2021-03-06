package org.apache.spark.smstorage.client.io;

import java.io.IOException;
import java.io.InputStream;

/**
 * 
 * @author haihua.wang
 * 
 */
public abstract class BlockInputStream extends InputStream {
  
  protected boolean isClosed = false;
  
  public static InputStream getInputStream(Boolean local, String type, int entryId, int size) throws IOException {
    if (local) {
      return LocalBlockInputStream.getLocalInputStream(type, entryId, size);
    } else {
      return null;
    }
  }
  
  @Override
  public abstract void close() throws IOException;

  @Override
  public abstract int read() throws IOException;

  @Override
  public abstract int read(byte[] b) throws IOException;

  @Override
  public abstract int read(byte[] b, int offset, int lengt) throws IOException;

  @Override
  public abstract long skip(long n) throws IOException;

}
