/**
 * 
 */
package org.apache.spark.smstorage.sharedmemory;

import java.util.BitSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.math3.exception.OutOfRangeException;

/**
 * @author Wang Haihua
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
  public native int init(String name, int key, long size);//申请共享内存空间

  public native int libshmat(int shm_id);//挂载共享内存

  /**
   * 断开共享内存挂载
   * @param id 使用libshmat方法返回的dt
   */
  public native void libshmdt(int id);//断开共享内存挂载

  public native void writeData(int id, byte b[], int len);//将数据写入共享内存

  public native byte[] readData(int id, int len);//从共享内存中读取数据

  public native void release(int shm_id);//释放共享内存空间
  
  private static final int GB = 1024 * 1024 * 1024;
  
  private static final int MAX_CACHE_COUNT = 1024;
  
  /**
   * name的目录一定需要存在，否在会出现诡异问题
   * ****在部署时候，需要使用mkdir来创建这个特殊的目录
   */
  //private static final String SM_NAME = "/home/hadoop/develop/lib";
  private static final String SM_NAME = "/data/hadoopspark";
  
  private static ShmgetAccesser instance;
  
  private static volatile Object lockObj = new Object();
  
  private ConcurrentHashMap<Integer, Integer> entryKey2indexKey;
  
  //private static final long 
  
  private BitSet indexKeySet;//存储entryKey的Set
  
  static {
    //改动默认的PATH
    //System.setProperty("java.library.path", "/opt/lib");//动态链接库的目录
    //System.load("/home/hadoop/develop/lib/ShmgetAccesser.so");//访问共享内存的动态链接库
    /**
     * ****改动默认的PATH，在conf/Spark-env.sh中设置native library的路径
     * export SPARK_WORKER_OPTS="-Djava.library.path=/data/hadoopspark/spark-hadoop2.3/lib/native"
     */
      System.loadLibrary("ShmgetAccesser.so");
  }

    /**
     * 初始化方法，对于worker和executor角色，初始化方法不同。
     * worker：需要存储和管理本节点上的共享存储数据
     * executor：通过存储接口，去读取共享存储中的数据
     * @param type worker OR executor
     */
  private ShmgetAccesser(String type) {
    if (type == "worker") {
      indexKeySet = new BitSet(MAX_CACHE_COUNT);
      entryKey2indexKey = new ConcurrentHashMap<Integer, Integer>(MAX_CACHE_COUNT/2);
    } else {
      indexKeySet = new BitSet();
      entryKey2indexKey = new ConcurrentHashMap<Integer, Integer>();
    }
  }
  
  public static ShmgetAccesser getInstance(String type) {
    if (instance == null) {
      synchronized (lockObj) {
        instance  = new ShmgetAccesser(type);
      }
    }
    return instance;
  }
  
  //------------------method for client---------------------//
  public byte[] read(int idx, int len) {
    return readData(idx, len);
  }
  
  public void write(int shmgetId, byte[] buf, int len) {
    writeData(shmgetId, buf, len);
  }
  
  public int shmat(int entryId) {
    return libshmat(entryId);
  }
  
  public void shmdt(int shmgetId) {
    libshmdt(shmgetId);
  }
  
  //------------------method for manager---------------------//
  
  //申请size大小的共享内存空间，并且申请id
  public synchronized int applySpace(int size) {
    
    if (size >= GB) {
      throw new OutOfRangeException(size, 0, GB);
    }
    
    int entryKey = -1;
    int indexKey = getNewIndexKey();
    //String entryStr = null;
    if (indexKey >= 0) {
      
      entryKey = init(SM_NAME, indexKey, size);
      if (entryKey >= 0) {
        //entryStr = String.valueOf(entryKey);
        entryKey2indexKey.put(entryKey, indexKey);
      }
    }
    
    return entryKey;
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
  
  public synchronized void releaseSpace(int entryId) {
    if (entryId < 0) {
      return;
    }
    int entryKey = -1;
    if (entryKey2indexKey.containsKey(entryId)) {
      int indexKey = entryKey2indexKey.get(entryId);
      release(entryId);
      indexKeySet.clear(indexKey);
      entryKey2indexKey.remove(entryKey);
    }
  }
  
}
