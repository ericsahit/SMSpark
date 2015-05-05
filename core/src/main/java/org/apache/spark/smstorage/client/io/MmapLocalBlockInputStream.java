/**
 * 
 */
package org.apache.spark.smstorage.client.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hwang
 *
 */
public class MmapLocalBlockInputStream extends LocalBlockInputStream {
  
  //TODO: common logger
  private static final Logger LOG = LoggerFactory.getLogger(MmapLocalBlockInputStream.class);
  
  private ByteBuffer buffer = null;

  /**
   * @param bufferEntry
   */
  public MmapLocalBlockInputStream(int entryId, int size) throws IOException {
    super(entryId, size);
    // TODO Auto-generated constructor stub
    
    buffer = getMappedByteBuffer(String.valueOf(entryId), -1);
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
    if (!isClosed) {
      isClosed = true;
    }
  }

  /* (non-Javadoc)
   * @see cn.edu.bjut.smspark.client.io.BlockInputStream#read()
   */
  @Override
  public int read() throws IOException {
    if (buffer.remaining() == 0) {
      close();
      return -1;
    }
    return buffer.get();
  }

  /* (non-Javadoc)
   * @see cn.edu.bjut.smspark.client.io.BlockInputStream#read(byte[])
   */
  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  /* (non-Javadoc)
   * @see cn.edu.bjut.smspark.client.io.BlockInputStream#read(byte[], int, int)
   */
  @Override
  public int read(byte[] b, int offset, int length) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (offset < 0 || length < 0 || length > b.length - offset) {
      throw new IndexOutOfBoundsException();
    } else if (length == 0) {
      return 0;
    }
    
    int readLength = Math.min(buffer.remaining(), length);
    if (readLength == 0) {
      close();
      return -1;
    }
    buffer.put(b, offset, readLength);
    return readLength;
  }

  /* (non-Javadoc)
   * @see cn.edu.bjut.smspark.client.io.BlockInputStream#skip(long)
   */
  @Override
  public long skip(long n) throws IOException {
    // TODO Auto-generated method stub
    return -1;
  }
  
  private static ByteBuffer getMappedByteBuffer(String fileName, long fileLength) throws IOException {
    RandomAccessFile file = null;
    try {
      file = new RandomAccessFile(fileName, "r");
      long len = file.length();
      if (len <= 0 || (fileLength != -1 && fileLength != len)) {
        throw new IOException(String.format("expect length: %d, actually length: %d", fileLength, len));
      }
      
      FileChannel fileChannel = file.getChannel();
      final MappedByteBuffer buf = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, len);
      //TODO: mark access block 
      return buf;
    } catch (FileNotFoundException e) {
      LOG.info(fileName + " not found on disk");
    } catch (IOException e) {
      LOG.info(fileName + " read file error: " + e.toString());
    } finally {
      file.close();
    }
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.spark.smstorage.client.io.LocalBlockInputStream#readFully(int)
   */
  @Override
  public byte[] readFully(int len) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

}
