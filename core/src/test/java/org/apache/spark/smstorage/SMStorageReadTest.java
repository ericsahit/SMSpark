/**
 * 
 */
package org.apache.spark.smstorage;

import java.io.IOException;
import java.io.InputStream;

import org.apache.spark.smstorage.client.io.LocalBlockInputStream;
import org.apache.spark.smstorage.client.io.MmapLocalBlockInputStream;
import org.apache.spark.smstorage.sharedmemory.ShmgetAccesser;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author hwang
 * 另一个进程写入共享内存，本程序测试读取功能
 * 需要传入shmId，这样就可以进行读取
 */
public class SMStorageReadTest {
  
  private ShmgetAccesser accesser;
  private static int MB = 1024*1024;
  private static int GB = 1024*MB;
  private int bufSize = 1024*64;
  private int shmId;
  
  int applySize;
  
  private static final Logger LOG = LoggerFactory.getLogger(SMStorageReadTest.class);
  
  
  @Before
  public void setUp() {
    accesser = ShmgetAccesser.getInstance("client");
    applySize = 100*MB;
    shmId = 720915;//另一个进程申请了shmid 720915
    //shmId = Integer.parseInt(accesser.applySpace(applySize));
  }
  
  @After
  public void close() {
    //accesser.releaseSpace(String.valueOf(shmId));
  }
  
  @Test
  public void test() {
	  printByteArr(null, 0);
  }
  
  

  /**
   * 
   */
  public SMStorageReadTest() {
    
  }
  
  @Test
  public void testRead() {
    //-------------------------read--------------------------//
    int shmgetId;
    shmgetId = accesser.shmat(shmId);
    System.out.println("begin read shmid: "+shmId);
    
    int remaining = applySize;
    int readLen = 0;
    long readTimeBegin = System.currentTimeMillis();
    
    while (remaining > 0) {
      readLen = Math.min(remaining, bufSize);
      
      accesser.read(shmgetId, readLen);
      
      remaining -= readLen;
    }
    //byte[] extraResult = accesser.read(shmgetId, 1024);
    //printByteArr(extraResult, 100);
    
    long readTimeEnd = System.currentTimeMillis();
    System.out.println("read end, length: "+(applySize-remaining)+", time spent: "+(readTimeEnd-readTimeBegin));
    
    accesser.shmdt(shmgetId);
  }
  
  @Test
  public void testReadOnce() {
	    //-------------------------read--------------------------//
	    int shmgetId;
	    shmgetId = accesser.shmat(shmId);
	    System.out.println("begin read shmid: "+shmId);
	    
	    int remaining = applySize;
	    int readLen = 0;
	    long readTimeBegin = System.currentTimeMillis();
	    
	    byte[] result = accesser.read(shmgetId, applySize);
	    //accesser.read(shmgetId, applySize);
	    //System.gc();
	    printByteArr(result, 100);
	    printByteArrLast(result, 100);
	    
	    long readTimeEnd = System.currentTimeMillis();
	    System.out.println("read once end, length: "+(applySize-remaining)+", time spent: "+(readTimeEnd-readTimeBegin));
	    
	    accesser.shmdt(shmgetId);
  }
  
  @Test
  public void testReadInputStream() throws IOException {
	    InputStream in = LocalBlockInputStream.getLocalInputStream("shmget", shmId, applySize);
	    Assert.assertTrue(in != null);
	    
	    byte[] arr = new byte[applySize];
	    
	    in.read(arr);
	    
	    printByteArr(arr, 100);
	    printByteArrLast(arr, 100);
	    //printByteArrLast(arr, 1000);
	    
	    in.close();
	    
	  }
  
  @Test
  public void testReadInputStream2() throws IOException {
    LocalBlockInputStream in = LocalBlockInputStream.getLocalInputStream("shmget", shmId, applySize);
    Assert.assertTrue(in != null);
    
    byte[] arr = in.readFully(applySize);
    
    printByteArr(arr, 100);
    printByteArrLast(arr, 100);
    //printByteArrLast(arr, 1000);
    
    in.close();
  }
  
  public static void printByteArr(byte[] arr, int len) {
	  
	  if (arr==null||arr.length==0) {
		  return;
	  }
	  
	  
	    for (int i = 0; i < len; i++) {
	    	System.out.print(arr[i]);
		}
	    System.out.println();
  }
  
  public static void printByteArrLast(byte[] arr, int len) {
	    
	    if (arr==null||arr.length==0) {
	        return;
	    }
	      for (int i = 0; i < len; i++) {
	          System.out.print(arr[arr.length-1-i]);
	      }
	      System.out.println();
	  }

}
