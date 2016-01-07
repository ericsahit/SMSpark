/**
 * 
 */
package org.apache.spark.smstorage.sharedmemory;

/**
 * @author Wang Haihua
 * 共享存储的管理组件，可以实现Shmget和mmap两种方式。
 * 目前的实现方式使用shmget方式，测试时候考虑mmap方式
 */
public class SMemoryManager {

    /**
     * 共享存储管理组件，全局单例
     */
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
