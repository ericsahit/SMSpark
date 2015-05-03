/**
 * 
 */
package org.apache.spark.smstorage.client.io;

import java.io.IOException;

import org.apache.spark.smstorage.sharedmemory.ShmgetAccesser;

/**
 * @author hwang
 *
 */
public class ShmgetLocalBlockOutputStream extends LocalBlockOutputStream {
  
  private ShmgetAccesser accesser;

  /**
   * @param entryId
   * @param size
   */
  public ShmgetLocalBlockOutputStream(int entryId, int size) {
    super(entryId, size);
    accesser = ShmgetAccesser.getInstance("client");
  }

  @Override
  public void write(int b) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void close() throws IOException {
    if (!isClosed) {
      
      isClosed = true;
    }
  }

  /* (non-Javadoc)
   * @see org.apache.spark.smstorage.client.io.BlockOutputStream#write(byte[])
   */
  @Override
  public void write(byte[] b) throws IOException {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see org.apache.spark.smstorage.client.io.BlockOutputStream#write(byte[], int, int)
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    // TODO Auto-generated method stub

  }

}
