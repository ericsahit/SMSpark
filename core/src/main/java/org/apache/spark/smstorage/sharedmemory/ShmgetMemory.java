/**
 * 
 */
package org.apache.spark.smstorage.sharedmemory;

import java.util.BitSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author hwang
 * 使用Shmget方式访问共享内存的接口
 * 在服务器端和客户端都需要使用到共享内存的接口
 */
public class ShmgetMemory {
  
  
  /**
   * 申请一块共享存储空间，
   * @param name 
   * @param key 
   * @param size 共享存储空间的大小
   * @return
   */
  public native int init(String name, int key, long size);//申请共享内存空间

  public native int libshmat(int shm_id);//挂载共享内存

  /**
   * 断开共享内存挂载
   * @param id 
   */
  public native void libshmdt(int id);//

  //size从int修改成long
  public native void writeData(int id, byte buf[], int size);//将数据写入共享内存

  public native byte[] readData(int id, int size);//从共享内存中读取数据

  public native void release(int shm_id);//释放共享内存空间
  
  
  private static final int MAX_CACHE_COUNT = 1024;
  
  private static final String SM_NAME = "smspark";
  
  private static ShmgetMemory instance;
  
  private ConcurrentHashMap<String, Integer> entry2InternalId;
  
  //private static final long 
  
  private BitSet keySet;//存储entryKey的Set
  
  static {//改动默认的PATH
    System.setProperty("java.library.path", "/opt/lib");//动态链接库的目录
    System.load("/opt/lib/JniShm.so");//访问共享内存的动态链接库
  }

  /**
   * 
   */
  public ShmgetMemory() {
    this("client");
  }
  
  public ShmgetMemory(String type) {
    if (type == "worker") {
      keySet = new BitSet(MAX_CACHE_COUNT);
      entry2InternalId = new ConcurrentHashMap<String, Integer>(MAX_CACHE_COUNT/2);
    }
  }
  
  public ShmgetMemory getInstance(String type) {
    if (instance == null) {
      synchronized (instance) {
        instance = new ShmgetMemory(type);
      }
    }
    return instance;
  }
  
  //申请size大小的共享内存空间，并且申请id
  public synchronized String applySpace(int size) {
    int internalId = -1;
    int entryKey = getNewEntryKey();
    String entryStr =null;
    if (entryKey >= 0) {
      
      internalId = init(SM_NAME, entryKey, size);
      if (internalId >= 0) {
        entryStr = String.valueOf(entryKey);
        entry2InternalId.put(entryStr, internalId);
      }
    }
    
    return entryStr;
  }
  
  private int getNewEntryKey() {
    int key = -1;
    //synchronized (keySet) {
      for (int i = 0; i < MAX_CACHE_COUNT; i ++) {
        if (!keySet.get(i)) {
          keySet.set(i);
          key = i;
          break;
        }
      }
    //}
    
    return key;
  }
  
  public synchronized void releaseSpace(String entryStr) {
    int entryKey = -1;
    try {
      entryKey = Integer.parseInt(entryStr);
    } catch(NumberFormatException e) {
      return;
    }
    if (entry2InternalId.containsKey(entryKey)) {
      int internalId = entry2InternalId.get(entryKey);
      release(internalId);
      keySet.clear(internalId);
      entry2InternalId.remove(entryKey);
    }
  }
  
  //------------------method for client---------------------//
  public byte[] read(String entryStr, int size) {
    return readData(Integer.parseInt(entryStr), size);
  }
  
  public void write(String entryStr, byte[] buf, int len) {
    writeData(Integer.parseInt(entryStr), buf, len);
  }
  
}
