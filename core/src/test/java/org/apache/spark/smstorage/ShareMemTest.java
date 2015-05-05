package org.apache.spark.smstorage;

//import org.apache.hadoop.mapred.datacache.JniShm;

public class ShareMemTest {
	public static void main(String[] args) {
	  
	  
	  
		
		JniShm shm = JniShm.getInstance();
		
		int mb = 1024*1024;
		int gb = 1024*1024*1024;
		//long size = 1024*mb;
		long size = 128*mb;
		int bufferSize = 8*1024;//1MB
		
		int shmId = shm.getCacheId(size);
		
		int shmgetId = shm.libshmat(shmId);// ¹ÒÔØ¹²ÏíÄÚ´æ
		System.out.println("shmId: "+shmId+" readid: "+shmgetId);
		
		int currentPosn = 0;
		byte[] buffer = new byte[bufferSize];
		for (int i = 0; i < buffer.length; i++) {
			buffer[i] = 123;
		}
		
		long writeStartTime = System.currentTimeMillis();
		int count = 0;
		do {
			//shm.writeData(id, buffer, bufferLength);
			shm.writeCache(shmgetId, buffer, bufferSize); // ½«bufferÖÐµÄÊý¾ÝÐ´Èëµ½¹²ÏíÄÚ´æ¿Õ¼äÖÐ
			currentPosn += bufferSize;

		} while (currentPosn < size);
		
		long writeEndTime = System.currentTimeMillis();
		System.out.println(" len:"+size+",write len:"+currentPosn+", write time: "+(writeEndTime-writeStartTime));
		//shm.writeCache(id, buffer, bufferLength); // ½«bufferÖÐµÄÊý¾ÝÐ´Èëµ½¹²ÏíÄÚ´æ¿Õ¼äÖÐ
		shm.shmdt(shmgetId); // ¶Ï¿ª¹²ÏíÄÚ´æ
		
		
		//-------------------------read--------------------------//
		
		if ((shmgetId = shm.shmat(shmId)) < 0) {
			System.out.println("shmat error");
			return;
		}
		System.out.println("begin read shmid: "+shmId);
		
	    long remainLength = size;
	    int readLength = 0;
	    long readTimeBegin = System.currentTimeMillis();
	    
	    while (remainLength > 0) {
	    	//System.out.println("remaining: "+remainLength);
			if (remainLength > bufferSize)
				readLength = bufferSize;
			else
				readLength = (int) remainLength;
	    	
	    	shm.readCache(shmgetId, readLength);
	    	

//	      while (readTime++ < 1000 && blockBuf.remaining() > 0) {
//	    	  System.out.print(blockBuf.get());
//	      }
	      //System.out.println();
	      
	      // Success
	    	remainLength -= readLength;
	    }
	    
	    long readTimeEnd = System.currentTimeMillis();
	    
		System.out.println("read end, length: "+(size-remainLength)+", time spent: "+(readTimeEnd-readTimeBegin));
		//TimeUnit.MINUTES.SECONDS.sleep(30);
		shm.releaseCache(shmId);
	}
}
