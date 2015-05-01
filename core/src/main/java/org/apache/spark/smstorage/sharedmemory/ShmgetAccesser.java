/**
 * 
 */
package org.apache.spark.smstorage.sharedmemory;

import java.util.BitSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.math3.exception.OutOfRangeException;

/**
 * @author hwang
 * 使用Shmget方式访问共享内存的接口
 * 在服务器端和客户端都需要使用到共享内存的接口
 */
public class ShmgetAccesser {
  
  
  /**
   * 申请一块共享存储空间，返回entryKey
   * @param name 
   * @param key index key
   * @param size 要申请的共享存储空间的大小，不能超过2GB
   * @return
   */
  public static native int init(String name, int key, long size);//申请共享内存空间

  public static native int libshmat(int shm_id);//挂载共享内存

  /**
   * 断开共享内存挂载
   * @param id 
   */
  public native void libshmdt(int id);//

  //修改为static方法
  public static native void writeData(int id, byte buf[], int len);//将数据写入共享内存

  public static native byte[] readData(int id, int len);//从共享内存中读取数据

  public native void release(int shm_id);//释放共享内存空间
  
  
  
  private static final int GB = 1024 * 1024 * 1024;
  
  private static final int MAX_CACHE_COUNT = 1024;
  
  private static final String SM_NAME = "smspark";
  
  private static ShmgetAccesser instance;
  
  private ConcurrentHashMap<String, Integer> entryKey2indexKey;
  
  //private static final long 
  
  private BitSet indexKeySet;//存储entryKey的Set
  
  static {//改动默认的PATH
    System.setProperty("java.library.path", "/opt/lib");//动态链接库的目录
    System.load("/opt/lib/JniShm.so");//访问共享内存的动态链接库
  }

  public ShmgetAccesser() {
    indexKeySet = new BitSet(MAX_CACHE_COUNT);
    entryKey2indexKey = new ConcurrentHashMap<String, Integer>(MAX_CACHE_COUNT/2);
  }
  
  public static ShmgetAccesser getInstance(String type) {
    if (instance == null) {
      synchronized (instance) {
        instance  = new ShmgetAccesser();
      }
    }
    return instance;
  }
  
  //------------------method for client---------------------//
  public static byte[] read(int entry, int len) {
    return readData(entry, len);
  }
  
  public static void write(int entry, byte[] buf, int len) {
    writeData(entry, buf, len);
  }
  
  public static int shmat(int entry) {
    return libshmat(entry);
  }
  
  public static void shmdt(int entry) {
    libshmat(entry);
  }
  
  //------------------method for manager---------------------//
  
  //申请size大小的共享内存空间，并且申请id
  public synchronized String applySpace(int size) {
    
    if (size >= GB) {
      throw new OutOfRangeException(size, 0, GB);
    }
    
    int entryKey = -1;
    int indexKey = getNewIndexKey();
    String entryStr = null;
    if (indexKey >= 0) {
      
      entryKey = init(SM_NAME, entryKey, size);
      if (entryKey >= 0) {
        entryStr = String.valueOf(entryKey);
        entryKey2indexKey.put(entryStr, entryKey);
      }
    }
    
    return entryStr;
  }
  
  private int getNewIndexKey() {
    int key = -1;
    //synchronized (keySet) {
      for (int i = 0; i < MAX_CACHE_COUNT; i ++) {
        if (!indexKeySet.get(i)) {
          indexKeySet.set(i);
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
    if (entryKey2indexKey.containsKey(entryKey)) {
      int indexKey = entryKey2indexKey.get(entryKey);
      release(entryKey);
      indexKeySet.clear(indexKey);
      entryKey2indexKey.remove(entryKey);
    }
  }
  
}
