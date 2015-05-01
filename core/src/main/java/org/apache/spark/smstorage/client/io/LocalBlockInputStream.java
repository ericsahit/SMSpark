package org.apache.spark.smstorage.client.io;

import java.io.IOException;
import java.io.InputStream;

public abstract class LocalBlockInputStream extends BlockInputStream {
  // 共享存储入口
  protected String bufferEntry;

  public LocalBlockInputStream(String bufferEntry) {
    this.bufferEntry = bufferEntry;
  }
  
  public static LocalBlockInputStream getLocalInputStream(String type, String bufferEntry) {
    //TODO: add MmapLocalBlockInputStream support
    return new ShmLocalBlockInputStream(bufferEntry);
  }
  
  /**
   * skip some byte for read
   * 
   * @param position
   * @return
   * @throws IOException
   */
  public abstract long seek(long position) throws IOException;
  
}
