#include "org_apache_spark_smstorage_sharedmemory_ShmgetAccesser.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/shm.h>
#include <sys/sem.h>
#include <sys/ipc.h>
#include <string.h>
#include <time.h>

//max data count, maybe not enough for some applications.
const int MAX_CACHE_COUNT = 256;
bool bitset[MAX_CACHE_COUNT] = { false };
char* shmaddr[MAX_CACHE_COUNT];
char* curaddr[MAX_CACHE_COUNT];
/*
 * Class:     org_apache_spark_smstorage_sharedmemory_ShmgetAccesser
 * Method:    init
 * Signature: (Ljava/lang/String;IJ)I
 */
JNIEXPORT jint JNICALL Java_org_apache_spark_smstorage_sharedmemory_ShmgetAccesser_init(
		JNIEnv *env, jobject obj, jstring jstr, jint key, jlong size) {
	int shm_Id;
	int shmkey;
	const char* name;
	name = env->GetStringUTFChars(jstr, false);
	shmkey = ftok(name, key);
	shm_Id = shmget(shmkey, size, IPC_CREAT | 0666);
	if (shm_Id == -1) {
		printf("create shm error!\n");
		//should not exit?
		exit(1);
	}
	env->ReleaseStringUTFChars(jstr, name);
	return shm_Id;
}

/*
 * Class:     org_apache_spark_smstorage_sharedmemory_ShmgetAccesser
 * Method:    libshmat
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_apache_spark_smstorage_sharedmemory_ShmgetAccesser_libshmat(
		JNIEnv * env, jobject obj, jint shm_Id) {
	int id = -1;
	char * shm_addr = (char *) shmat(shm_Id, NULL, 0);
	if (!shm_addr) {
		printf("shmat error!\n");
		exit(1);
	}
	for (int i = 0; i < MAX_CACHE_COUNT; i++) {
		if (!bitset[i]) {
			bitset[i] = true;
			id = i;
			break;
		}
	}
	curaddr[id] = shm_addr;
	shmaddr[id] = shm_addr;
	return id;
}

/*
 * Class:     org_apache_spark_smstorage_sharedmemory_ShmgetAccesser
 * Method:    libshmdt
 * Signature: (I)V
 */
JNIEXPORT void Java_org_apache_spark_smstorage_sharedmemory_ShmgetAccesser_libshmdt
(JNIEnv * env, jobject obj, jint id) {
	shmdt(shmaddr[id]);
	bitset[id] = false;
}

/*
 * Class:     org_apache_spark_smstorage_sharedmemory_ShmgetAccesser
 * Method:    writeData
 * Signature: (I[BI)V
 */
JNIEXPORT void JNICALL Java_org_apache_spark_smstorage_sharedmemory_ShmgetAccesser_writeData
(JNIEnv * env, jobject obj, jint id, jbyteArray jbArr, jint len) {
	jbyte* data = new jbyte[len + 1];
	if (len < 0) {
		printf("argument error!\n");
		exit(1);
	}
	if (jbArr != NULL ) {
		env->GetByteArrayRegion(jbArr, 0,len,(jbyte*) data );
		data[len]='\0';
		char* cur = curaddr[id];
		memcpy(cur, data, len);
		cur += len;
		curaddr[id] = cur;
	} else {
		printf("write data error!\n");
		exit(1);
	}
	delete[] data;
}

/*
 * Class:     org_apache_spark_smstorage_sharedmemory_ShmgetAccesser
 * Method:    readData
 * Signature: (II)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_smstorage_sharedmemory_ShmgetAccesser_readData(
		JNIEnv * env, jobject obj, jint id, jint len) {
	jbyte* tmpByteArr = new jbyte[len + 1];
	if (len > 0) {
		char* cur = curaddr[id];
		memcpy(tmpByteArr, cur, len);
		tmpByteArr[len] = '\0';
		cur += len;
		curaddr[id] = cur;
	} else {
		printf("argument error!\n");
	}
	jbyteArray jbArr = env->NewByteArray(len);
	env->SetByteArrayRegion(jbArr, 0, len, (jbyte*) tmpByteArr);
	delete[] tmpByteArr;
	return jbArr;
}

/*
 * Class:     org_apache_spark_smstorage_sharedmemory_ShmgetAccesser
 * Method:    release
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_org_apache_spark_smstorage_sharedmemory_ShmgetAccesser_release
(JNIEnv *env, jobject obj, jint shm_Id) {
	if (shmctl(shm_Id, IPC_RMID, 0) == -1) {
		perror("delete error");
		exit(0);
	}
}

