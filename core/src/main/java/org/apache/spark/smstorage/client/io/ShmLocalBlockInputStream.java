/**
 * 
 */
package org.apache.spark.smstorage.client.io;

import java.io.IOException;

/**
 * @author hwang
 *
 */
public class ShmLocalBlockInputStream extends LocalBlockInputStream {

  /**
   * @param bufferEntry
   */
  public ShmLocalBlockInputStream(String bufferEntry) {
    super(bufferEntry);
    // TODO Auto-generated constructor stub
  }

  /* (non-Javadoc)
   * @see cn.edu.bjut.smspark.client.io.LocalBlockInputStream#seek(long)
   */
  @Override
  public long seek(long position) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see cn.edu.bjut.smspark.client.io.BlockInputStream#close()
   */
  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
  }

  /* (non-Javadoc)
   * @see cn.edu.bjut.smspark.client.io.BlockInputStream#read()
   */
  @Override
  public int read() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see cn.edu.bjut.smspark.client.io.BlockInputStream#read(byte[])
   */
  @Override
  public int read(byte[] b) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see cn.edu.bjut.smspark.client.io.BlockInputStream#read(byte[], int, int)
   */
  @Override
  public int read(byte[] b, int offset, int lengt) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see cn.edu.bjut.smspark.client.io.BlockInputStream#skip(long)
   */
  @Override
  public long skip(long n) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

}
