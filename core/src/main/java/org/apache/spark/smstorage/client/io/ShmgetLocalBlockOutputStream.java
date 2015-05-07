/**
 * 
 */
package org.apache.spark.smstorage.client.io;

import java.io.IOException;

import org.apache.spark.Logging;
import org.apache.spark.smstorage.sharedmemory.ShmgetAccesser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hwang
 *
 */
public class ShmgetLocalBlockOutputStream extends LocalBlockOutputStream {
  
  //private static Logger LOG = Logging.log_;
  private static final Logger LOG = LoggerFactory.getLogger(ShmgetLocalBlockOutputStream.class);
  
  private ShmgetAccesser accesser;
  
  private int shmgetId = -1; 

  /**
   * @param entryId
   * @param size
   */
  public ShmgetLocalBlockOutputStream(int entryId, int size) {
    super(entryId, size);
    accesser = ShmgetAccesser.getInstance("client");
    if (entryId < 0) {
      LOG.warn("Init shmget failed, bad entryId: " + entryId);
      return;
    }
    shmgetId = accesser.shmat(entryId);
    LOG.info("Init shmget successfully entryId:" + entryId);
  }

  @Override
  public void write(int b) throws IOException {
    //accesser.write(shmgetId, new byte[]{b}, );
  }

  @Override
  public void close() throws IOException {
    if (!isClosed) {
      accesser.shmdt(shmgetId);
      isClosed = true;
    }
  }

  @Override
  public void write(byte[] b) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    if (currPos + b.length > size) {
      //throw new IndexOutOfBoundsException(String.format("write out of bound. currPos: %d, arr len: %d, size: %d", currPos, b.length, size));
      throw new IOException(String.format("write out of bound. currPos: %d, arr len: %d, size: %d", currPos, b.length, size));
    }
    accesser.write(shmgetId, b, b.length);
    currPos += b.length;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    write(b);
  }

}
