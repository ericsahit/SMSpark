/**
 * 
 */
package org.apache.spark.smstorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.smstorage.client.io.LocalBlockInputStream;
import org.apache.spark.smstorage.client.io.LocalBlockOutputStream;
import org.apache.spark.smstorage.sharedmemory.ShmgetAccesser;
import org.junit.Assert;

/**
 * @author hwang
 * 共享存储的数据管理机制测试，申请了多个共享存储空间
 */
public class MultiShmgetTest {
  
  private ShmgetAccesser accesser;
  
  private Map<Integer, Integer> shmIdList = new HashMap<Integer, Integer>();
  
  /**
   * 
   */
  public MultiShmgetTest() {
    accesser = ShmgetAccesser.getInstance("worker");
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws IOException {
    int MB = 1024*1024;
    MultiShmgetTest test = new MultiShmgetTest();
    test.testWrite(8*MB);
    test.testWrite(64*MB);
    test.testWrite(128*MB);
    
    test.testReadAll();
    
    test.closeAll();
    
  }
  
  public void testReadAll() throws IOException {
    for (Entry<Integer, Integer> entry: shmIdList.entrySet()) {
      testRead(entry.getKey(), entry.getValue());
    }
  }
  
  public void closeAll() {
    for (Entry<Integer, Integer> entry: shmIdList.entrySet()) {
      accesser.releaseSpace(entry.getValue());
    }
  }
  
  private void testWrite(int size) throws IOException {
    
    //int size = 10*1024*1024;
    int entryId = accesser.applySpace(size);
    shmIdList.put(size, entryId);
    Assert.assertTrue(entryId > 0);
    System.out.println("init shmid: "+entryId+", size: "+size);
    
    int writeId = accesser.shmat(entryId);
    Assert.assertTrue(writeId >= 0 && writeId <=255);
    
    byte[] buf = generateArr(size);
    
    LocalBlockOutputStream os = LocalBlockOutputStream.getLocalOutputStream("shmget", entryId, size);
    Assert.assertTrue(os != null);
    os.write(buf);
    os.close();
  }
  
  private void testRead(int size, int entryId) throws IOException {
    LocalBlockInputStream in = LocalBlockInputStream.getLocalInputStream("shmget", entryId, size);
    Assert.assertTrue(in != null);
    
    byte[] arr = in.readFully(size);
    
    SMStorageReadTest.printByteArr(arr, 100);
    SMStorageReadTest.printByteArrLast(arr, 100);
    
    in.close();
  }
  
  private byte[] generateArr(int size) {
    
    byte[] buf = new byte[size];
    
    for (int i = 0; i < buf.length; i++) {
      buf[i] = 123;
    }
    buf[0]=(byte)size;
    buf[size-1]=(byte)(size+1);
    
    return buf;
  }

  
}
