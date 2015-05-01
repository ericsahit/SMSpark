/**
 * 
 */
package org.apache.spark.smstorage.sharedmemory;

/**
 * @author hwang
 * 共享存储的管理组件，可以实现Shmget和mmap两种方式。
 */
public abstract class SMemoryManager {
  
  ShmgetAccesser accesser;

  /**
   * 
   */
  public SMemoryManager() {
    accesser = ShmgetAccesser.getInstance("worker");
  }
  
  public String applySpace(int size) {
    return accesser.applySpace(size);
  }
  
  public void realseSpace(String entryStr) {
    accesser.releaseSpace(entryStr);
  }
  
  
  

}
