package org.apache.spark.smstorage.client.io;

import java.io.IOException;
import java.io.InputStream;

public abstract class LocalBlockInputStream extends BlockInputStream {
  // 共享存储入口
  protected int entryId;
  protected int size;
  protected int currPos;
  
  public LocalBlockInputStream(int entryId, int size) {
    this.entryId = entryId;
    this.size = size;
    currPos = 0;
  }
  
  public static LocalBlockInputStream getLocalInputStream(String type, int entryId, int size) throws IOException {
    //TODO: add MmapLocalBlockInputStream support
    return new ShmLocalBlockInputStream(entryId, size);
  }
  
  public abstract byte[] readFully(int len) throws IOException;
  
  /**
   * skip some byte for read
   * 
   * @param position
   * @return
   * @throws IOException
   */
  public abstract long seek(long position) throws IOException;
  
}
