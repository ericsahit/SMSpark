/**
 * 
 */
package org.apache.spark.smstorage;

import java.io.IOException;
import java.io.InputStream;

import org.apache.spark.smstorage.client.io.LocalBlockInputStream;
import org.apache.spark.smstorage.sharedmemory.ShmgetAccesser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author hwang
 *
 */
public class SMStorageTest {
  
  private ShmgetAccesser accesser;
  private static int MB = 1024*1024;
  private static int GB = 1024*MB;
  private int bufSize = MB;
  private int shmId;
  
  int applySize = 100*MB;
  
  
  @Before
  public void setUp() {
    accesser = ShmgetAccesser.getInstance("worker");
    
    shmId = accesser.applySpace(applySize);
    printByteArr(null, 0);
  }
  
  @After
  public void close() {
    String.valueOf(shmId);
  }
  
  

  /**
   * 
   */
  public SMStorageTest() {
    
  }
  
  @Test
  public void testWrite() {

    Assert.assertTrue(shmId > 0);
    
    int shmgetId = accesser.shmat(shmId);
    Assert.assertTrue(shmgetId >= 0 && shmgetId <=255);
    
    int currentPosn = 0;
    byte[] buffer = new byte[bufSize];
    for (int i = 0; i < buffer.length; i++) {
        buffer[i] = 123;
    }
    
    long writeStartTime = System.currentTimeMillis();
    int count = 0;
    do {
        accesser.write(shmgetId, buffer, bufSize); // ½«bufferÖÐµÄÊý¾ÝÐ´Èëµ½¹²ÏíÄÚ´æ¿Õ¼äÖÐ
        currentPosn += bufSize;
    } while (currentPosn < applySize);
    
    long writeEndTime = System.currentTimeMillis();
    System.out.println(" len:"+applySize+",write len:"+currentPosn+", write time: "+(writeEndTime-writeStartTime));
    
    accesser.shmdt(shmgetId);
    
    
  }
  
  /**
   * 1MB缓冲区：
   * len:104857600,write len:104857600, write time: 87
   * begin read shmid: 557073
   * read end, length: 104857600, time spent: 134
   * begin read shmid: 557073
   * read once end, length: 0, time spent: 95
   * 
   * 使用缓冲区读，和一次读时间差不多
   * 
   * 64KB缓冲区（性能最佳）：
   *  len:104857600,write len:104857600, write time: 44
begin read shmid: 557073
read end, length: 104857600, time spent: 73
begin read shmid: 557073
read once end, length: 0, time spent: 99
   * 
   */
  
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
    long readTimeEnd = System.currentTimeMillis();
    System.out.println("read end, length: "+(applySize-remaining)+", time spent: "+(readTimeEnd-readTimeBegin));
    
    //这里SHR会降低100M，RES也会降低100M
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
        
        //这里执行完后RES和SHR都会增加，RES增加200MB，SHR增加100MB
        byte[] result = accesser.read(shmgetId, applySize);
        //accesser.read(shmgetId, applySize);
        
        //printByteArr(result, 1000);
        
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
    
    printByteArrLast(arr, 1000);
    
    in.close();
    
  }
  
  @Test
  public void testReadInputStream2() throws IOException {
    LocalBlockInputStream in = LocalBlockInputStream.getLocalInputStream("shmget", shmId, applySize);
    Assert.assertTrue(in != null);
    
    byte[] arr = in.readFully(applySize);
    
    printByteArrLast(arr, 1000);
    
    in.close();
  }
  
  private void printByteArr(byte[] arr, int len) {
    
    if (arr==null||arr.length==0) {
        return;
    }
      for (int i = 0; i < len; i++) {
          System.out.print(arr[i]);
      }
      System.out.println();
  }
  
  private void printByteArrLast(byte[] arr, int len) {
    
    if (arr==null||arr.length==0) {
        return;
    }
      for (int i = 0; i < len; i++) {
          System.out.print(arr[arr.length-1-i]);
      }
      System.out.println();
  }
  

}
