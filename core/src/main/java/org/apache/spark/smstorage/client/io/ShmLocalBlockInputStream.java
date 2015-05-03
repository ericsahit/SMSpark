/**
 * 
 */
package org.apache.spark.smstorage.client.io;

import java.io.IOException;

import org.apache.spark.Logging;
import org.apache.spark.smstorage.sharedmemory.ShmgetAccesser;
import org.slf4j.Logger;

/**
 * @author hwang
 *
 */
public class ShmLocalBlockInputStream extends LocalBlockInputStream {
  
  private static Logger LOG = Logging.log_;
  
  private int shmgetId = -1;
  
  private int bufSize = 64 * 1024;
  
  private ShmgetAccesser accesser;
  
  /**
   * @param bufferEntry
   */
  public ShmLocalBlockInputStream(int entryId, int size) throws IOException {
    super(entryId, size);
    
    accesser = ShmgetAccesser.getInstance("client");
    if (entryId < 0) {
      throw new IOException("bad entryId: " + entryId);
    }
    shmgetId = accesser.shmat(entryId);
    LOG.info("init shmget successfully at id:" + entryId);
  }

  /* (non-Javadoc)
   * @see cn.edu.bjut.smspark.client.io.LocalBlockInputStream#seek(long)
   */
  @Override
  public long seek(long position) throws IOException {
    return 0;
  }

  /* (non-Javadoc)
   * @see cn.edu.bjut.smspark.client.io.BlockInputStream#close()
   */
  @Override
  public void close() throws IOException {
    if (!isClosed && shmgetId >= 0) {
      accesser.shmdt(shmgetId);
      isClosed = true;
    }
  }

  /* (non-Javadoc)
   * @see cn.edu.bjut.smspark.client.io.BlockInputStream#read()
   */
  @Override
  public int read() throws IOException {
    return 0;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  /**
   * TODO：使用缓存来读取
   */
  @Override
  public int read(byte[] b, int offset, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (offset < 0 || len < 0 || len > b.length - offset) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }
    
    int ret = Math.min(size - currPos, len);
    if (ret <= 0) {
      close();
      return -1;
    }
    byte[] buf = accesser.read(shmgetId, ret);
    System.arraycopy(buf, 0, b, offset, len);//这里有一次数组拷贝
    currPos += ret;
    return ret;
  }
  
  /* (non-Javadoc)
   * @see cn.edu.bjut.smspark.client.io.BlockInputStream#skip(long)
   */
  @Override
  public long skip(long n) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public byte[] readFully(int len) throws IOException {
    int ret = Math.min(size - currPos, len);
    if (ret <= 0) {
      close();
      return null;
    }
    byte[] buf = accesser.read(shmgetId, ret);
    currPos += ret;
    return buf;
  }

}
