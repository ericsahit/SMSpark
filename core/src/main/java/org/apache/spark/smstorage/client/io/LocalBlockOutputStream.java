/**
 * 
 */
package org.apache.spark.smstorage.client.io;

import java.io.IOException;

/**
 * @author hwang
 *
 */
public abstract class LocalBlockOutputStream extends BlockOutputStream {
  
  protected int entryId;
  
  protected int size;
  
  protected int currPos;
  

  /**
   * 
   */
  public LocalBlockOutputStream(int entryId, int size) {
    this.entryId = entryId;
    this.size = size;
    currPos = 0;
  }
  
  public static LocalBlockOutputStream getLocalOutputStream(String type, int entryId, int size) throws IOException {
    return new ShmgetLocalBlockOutputStream(entryId, size);
  }

}
