/**
 * 
 */
package org.apache.spark.smstorage.sharedmemory;

/**
 * @author hwang
 * 共享存储的管理组件，可以实现Shmget和mmap两种方式。
 */
public class SMemoryManager {
  
  ShmgetAccesser accesser;

  /**
   * 
   */
  public SMemoryManager() {
    accesser = ShmgetAccesser.getInstance("worker");
  }
  
  public int applySpace(int size) {
    return accesser.applySpace(size);
  }
  
  public void realseSpace(int entryId) {
    accesser.releaseSpace(entryId);
  }
  
  
  

}
