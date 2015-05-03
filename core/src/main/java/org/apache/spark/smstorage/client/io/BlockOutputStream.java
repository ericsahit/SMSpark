package org.apache.spark.smstorage.client.io;

import java.io.IOException;
import java.io.OutputStream;

public abstract class BlockOutputStream extends OutputStream {
  
  protected boolean isClosed = false;
  

  public BlockOutputStream() {
  }

  @Override
  public abstract void write(int b) throws IOException;
  
  @Override
  public abstract void close() throws IOException;
  
  @Override
  public abstract void write(byte[] b) throws IOException;
  
  @Override
  public abstract void write(byte[] b, int off, int len) throws IOException;

}
