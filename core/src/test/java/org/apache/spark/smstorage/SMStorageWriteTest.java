/**
 * 
 */
package org.apache.spark.smstorage;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.smstorage.client.io.LocalBlockInputStream;
import org.apache.spark.smstorage.client.io.LocalBlockOutputStream;
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
public class SMStorageWriteTest {
  
  private ShmgetAccesser accesser;
  private static int MB = 1024*1024;
  private static int GB = 1024*MB;
  private int bufSize = 1024*64;
  private List<Integer> shmIds;
  private int shmId;
  
  int applySize;
  
  private static final Logger LOG = LoggerFactory.getLogger(SMStorageWriteTest.class);
  
  
  @Before
  public void setUp() {
    accesser = ShmgetAccesser.getInstance("worker");
    applySize = 100*MB;
    //shmId = 720915;
    shmIds = new ArrayList<Integer>();
    shmId = accesser.applySpace(applySize);
    shmIds.add(shmId);
  }
  
  @After
  public void close() {
    for (int id: shmIds) {
      accesser.releaseSpace(id);
    }
  }
  
  @Test
  public void test() {
	  printByteArr(null, 0);
  } 

  /**
   * 
   */
  public SMStorageWriteTest() {
    
  }
  
  @Test
  public void testWriteOutputStream() throws IOException {
    
    int size = 10*MB;
    int entryId = accesser.applySpace(size);
    shmIds.add(entryId);
    Assert.assertTrue(entryId > 0);
    
    int writeId = accesser.shmat(entryId);
    Assert.assertTrue(writeId >= 0 && writeId <=255);
    
    byte[] buf = new byte[size];
    
    for (int i = 0; i < buf.length; i++) {
      buf[i] = 123;
    }
    buf[0]=122;
    buf[size-1]=124;
    
    LocalBlockOutputStream os = LocalBlockOutputStream.getLocalOutputStream("shmget", entryId, size);
    Assert.assertTrue(os != null);
    os.write(buf);
    os.close();
  }
  
  @Test
  public void testReadAfterWrite() throws IOException {
    int size = 10*MB;
    LocalBlockInputStream in = LocalBlockInputStream.getLocalInputStream("shmget", shmIds.get(1), 10*MB);
    Assert.assertTrue(in != null);
    
    byte[] arr = in.readFully(size);
    
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
