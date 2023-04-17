#include "zc_io.h"

#include <fcntl.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

// The zc_file struct is analogous to the FILE struct that you get from fopen.

double ceil(double num) {
  int inum = (int)num;
  if (num == (float)inum) {
    return inum;
  }
  return inum + 1;
}

typedef struct linked_list_queue linked_list_queue;
typedef struct queue queue;

struct linked_list_queue {
  int pageIndex;
  int numPages;
  struct linked_list_queue* next;
};

struct queue {
  struct linked_list_queue* head;
  struct linked_list_queue* tail;
};

struct zc_file {
  int fileDescriptor;
  size_t fileSize;
  size_t offset;
  size_t prevOffset;

  sem_t fileLock;  // mutex to protect from any edits to page
  sem_t fileWriterMutex;
  void* memoryAddress;
  int numOfPages;
  int numOfCurrentReaders;

  int bonusPageToUnlock;

  int* numOfCurrentPageReaders;
  sem_t* pageReaderMutex;  // mutex to protect from any edits to page during read
  sem_t* pageWriterMutex;  // mutex to protect from any edits to page during write
  int isBonus;

  queue readPageQueue;  // keep track of pages that are being read
  queue writePageQueue;
  sem_t readPageQueueMutex;
  sem_t writePageQueueMutex;
};

/**************
 * Exercise 1 *
 **************/

void push_queue(struct queue* q, int pageIndex, int numPages) {
  struct linked_list_queue* new_node = malloc(sizeof(struct linked_list_queue));
  new_node->pageIndex = pageIndex;
  new_node->numPages = numPages;
  new_node->next = NULL;

  if (q->head == NULL) {
    q->head = new_node;
    q->tail = new_node;
  } else {
    q->tail->next = new_node;
    q->tail = new_node;
  }
}

void pop_queue(struct queue* q, int* pageIndexResult, int* numPagesResult) {
  if (q->head == NULL) {
    pageIndexResult = NULL;
    numPagesResult = NULL;
  }

  struct linked_list_queue* temp = q->head;
  int pageIndex = temp->pageIndex;
  int numPages = temp->numPages;
  q->head = q->head->next;
  free(temp);
  *pageIndexResult = pageIndex;
  *numPagesResult = numPages;
}

int buffer = 10;

zc_file* zc_open(const char* path) {
  struct zc_file* zc = malloc(sizeof(struct zc_file));
  int fileDescriptor = open(path, O_CREAT | O_RDWR, 0644);

  struct stat fileStatus;

  fstat(fileDescriptor, &fileStatus);

  size_t fileSize = fileStatus.st_size;

  void* memoryAddress = mmap(NULL, fileSize, PROT_READ | PROT_WRITE, MAP_SHARED, fileDescriptor, 0);

  zc->fileDescriptor = fileDescriptor;
  zc->fileSize = fileSize;
  zc->offset = 0;
  zc->memoryAddress = memoryAddress;
  zc->isBonus = 0;
  zc->readPageQueue = (struct queue){NULL, NULL};
  zc->writePageQueue = (struct queue){NULL, NULL};

  int pageSize = sysconf(_SC_PAGE_SIZE);

  int numberOfPages = ceil(fileSize / pageSize) + buffer;
  zc->numOfPages = numberOfPages;

  zc->pageReaderMutex = malloc(numberOfPages * sizeof(sem_t));

  zc->pageWriterMutex = malloc(numberOfPages * sizeof(sem_t));
  zc->numOfCurrentPageReaders = malloc(numberOfPages * sizeof(int));
  zc->numOfCurrentReaders = 0;
  sem_init(&zc->fileLock, 0, 1);
  sem_init(&zc->fileWriterMutex, 0, 1);

  for (int i = 0; i < numberOfPages; i++) {
    zc->numOfCurrentPageReaders[i] = 0;
    sem_init(&zc->pageReaderMutex[i], 0, 1);
    sem_init(&zc->pageWriterMutex[i], 0, 1);
  }

  sem_init(&zc->readPageQueueMutex, 0, 1);
  sem_init(&zc->writePageQueueMutex, 0, 1);

  return zc;
}

int zc_close(zc_file* file) {

  int ans = munmap(file->memoryAddress, file->fileSize);

  // destroy mutexes

  for (int i = 0; i < file->numOfPages; i++) {
    sem_destroy(&file->pageReaderMutex[i]);
    sem_destroy(&file->pageWriterMutex[i]);
  }

  free(file->pageReaderMutex);
  free(file->pageWriterMutex);
  free(file->numOfCurrentPageReaders);
  // deallocate zc_file

  free(file);

  return ans;
}

const char* zc_read_start(zc_file* file, size_t* size) {
  // copy necessary information and pass the lock to the next reader
  sem_wait(&file->fileLock);
  // if is first reader, lock write
  if (file->numOfCurrentReaders == 0) {
    sem_wait(&file->fileWriterMutex);
  }

  file->numOfCurrentReaders++;

  size_t fileSizeLeftToRead = file->fileSize - file->offset;

  if (fileSizeLeftToRead < *size) {
    *size = fileSizeLeftToRead;
  }

  file->prevOffset = file->offset;
  file->offset += *size;

  char* ans = (char*)(file->memoryAddress + file->prevOffset);

  sem_post(&file->fileLock);

  return ans;
}

void zc_read_end(zc_file* file) {
  if (file->isBonus) {
    int currPageIndex = 0;
    int numPages = 1;
    pop_queue(&file->readPageQueue, &currPageIndex, &numPages);
    for (int i = 0; i < numPages; i++) {

      sem_wait(&file->pageReaderMutex[currPageIndex]);
      if (file->numOfCurrentPageReaders[currPageIndex] == 1) {
        sem_post(&file->pageWriterMutex[currPageIndex]);
      }
      file->numOfCurrentPageReaders[currPageIndex]--;
      int currPageIndexTemp = currPageIndex;
      currPageIndex++;
      sem_post(&file->pageReaderMutex[currPageIndexTemp]);
    }
    return;
  }

  sem_wait(&file->fileLock);

  if (file->numOfCurrentReaders == 1) {
    sem_post(&file->fileWriterMutex);
  }

  file->numOfCurrentReaders--;
  sem_post(&file->fileLock);
}

char* zc_write_start(zc_file* file, size_t size) {
  sem_wait(&file->fileWriterMutex);
  sem_wait(&file->fileLock);
  if (file->offset + size > file->fileSize) {
    // lock the file as we need to resize the file and init more semaphores
    // double confimation that the file size is not enough

    size_t newFileSize = file->offset + size;
    ftruncate(file->fileDescriptor, newFileSize);

    void* newMemoryAddress = NULL;
    if (file->fileSize == 0) {
      newMemoryAddress = mmap(NULL, newFileSize, PROT_READ | PROT_WRITE, MAP_SHARED, file->fileDescriptor, 0);
    } else {
      newMemoryAddress = mremap(file->memoryAddress, file->fileSize, newFileSize, MREMAP_MAYMOVE);
    }

    file->memoryAddress = newMemoryAddress;

    file->fileSize = newFileSize;
  }

  file->prevOffset = file->offset;
  file->offset += size;

  char* ans = (char*)(file->memoryAddress + file->prevOffset);

  sem_post(&file->fileLock);

  return ans;
}

void zc_write_end(zc_file* file) {
  if (file->isBonus) {
    sem_wait(&file->writePageQueueMutex);
    int currPageIndex = 0;
    int numPages = 1;
    pop_queue(&file->writePageQueue, &currPageIndex, &numPages);
    sem_post(&file->writePageQueueMutex);
    msync(file->memoryAddress + currPageIndex * sysconf(_SC_PAGE_SIZE), sysconf(_SC_PAGE_SIZE), MS_SYNC);
    for (int i = 0; i < numPages; i++) {
      int currPageIndexTemp = currPageIndex;
      currPageIndex++;
      sem_post(&file->pageWriterMutex[currPageIndexTemp]);
    }

    return;
  }

  msync(file->memoryAddress, file->fileSize, MS_SYNC);
  sem_post(&file->fileWriterMutex);
}

/**************
 * Exercise 2 *
 **************/

off_t zc_lseek(zc_file* file, long offset, int whence) {
  sem_wait(&file->fileLock);
  int ans = -1;
  int pageSize = sysconf(_SC_PAGE_SIZE);
  int currPageIndex = (int)ceil((double)file->offset / pageSize);

  sem_wait(&file->pageReaderMutex[currPageIndex]);
  sem_wait(&file->pageWriterMutex[currPageIndex]);

  switch (whence) {
    case SEEK_SET:
      ans = file->offset = offset;
      break;
    case SEEK_CUR:
      ans = file->offset += offset;
      break;
    case SEEK_END:
      ans = file->offset = file->fileSize + offset;
      break;
    default:
      break;
  }

  sem_post(&file->pageReaderMutex[currPageIndex]);
  sem_post(&file->pageWriterMutex[currPageIndex]);

  sem_post(&file->fileLock);
  return ans;
}

/**************
 * Exercise 3 *
 **************/

int zc_copyfile(const char* source, const char* dest) {
  zc_file* source_zc = zc_open(source);
  zc_file* dest_zc = zc_open(dest);
  size_t size_of_source = source_zc->fileSize;

  const char* source_content = zc_read_start(source_zc, &size_of_source);
  zc_read_end(source_zc);
  char* dest_content = zc_write_start(dest_zc, size_of_source);
  zc_write_end(dest_zc);

  memcpy(dest_content, source_content, size_of_source);
  zc_close(source_zc);
  zc_close(dest_zc);

  return 0;
}

/**************
 * Bonus Exercise *
 **************/

const char* zc_read_offset(zc_file* file, size_t* size, long offset) {
  // To implement
  sem_wait(&file->fileLock);

  file->isBonus = 1;
  int pageSize = sysconf(_SC_PAGE_SIZE);
  int currPageIndex = (int)ceil((double)offset / pageSize);

  char* ans = (char*)(file->memoryAddress + offset);
  size_t fileSizeLeftToRead = file->fileSize - file->offset;

  int numPages = 0;
  if (fileSizeLeftToRead < *size) {
    numPages = (int)ceil((double)fileSizeLeftToRead / pageSize);
  } else {
    numPages = (int)ceil((double)*size / pageSize);
  }

  push_queue(&file->readPageQueue, currPageIndex, numPages);
  sem_post(&file->fileLock);
  // // lock reader to update the number of readers
  if (fileSizeLeftToRead < *size) {
    *size = fileSizeLeftToRead;
  }

  for (int i = 0; i < numPages; i++) {
    int currPage = currPageIndex + i;
    sem_wait(&file->pageReaderMutex[currPage]);
    if (file->numOfCurrentPageReaders[currPage] == 0) {
      sem_wait(&file->pageWriterMutex[currPage]);
    }
    file->numOfCurrentPageReaders[currPage]++;
    sem_post(&file->pageReaderMutex[currPage]);
  }

  return ans;
}

char* zc_write_offset(zc_file* file, size_t size, long offset) {
  file->isBonus = 1;

  sem_wait(&file->fileLock);
  int pageSize = sysconf(_SC_PAGE_SIZE);
  int currPageIndex = (int)ceil((double)offset / pageSize);

  int numPages = 0;
  numPages = (int)ceil((double)size / pageSize);

  sem_post(&file->fileLock);
  // lock whole file (among writers) for a possible update to the number of pages
  sem_wait(&file->fileLock);
  if (file->offset + size > file->fileSize) {
    size_t newFileSize = file->offset + size;
    ftruncate(file->fileDescriptor, newFileSize);

    int newPageNum = (int)ceil((double)newFileSize / pageSize) + buffer;

    // copy old page locks to new page locks
    sem_t* newPageReaderMutex = malloc(newPageNum * sizeof(sem_t));
    sem_t* newPageWriterMutex = malloc(newPageNum * sizeof(sem_t));
    int* newNumOfCurrentPageReaders = malloc(newPageNum * sizeof(int));

    for (int i = 0; i < file->numOfPages; i++) {
      newPageReaderMutex[i] = file->pageReaderMutex[i];
      newPageWriterMutex[i] = file->pageWriterMutex[i];
      newNumOfCurrentPageReaders[i] = file->numOfCurrentPageReaders[i];
    }

    // init new page locks

    for (int i = file->numOfPages; i < newPageNum; i++) {
      // sem_init(&zc->pageLock[i], 0, 1);
      newNumOfCurrentPageReaders[i] = 0;
      sem_init(&newPageReaderMutex[i], 0, 1);
      sem_init(&newPageWriterMutex[i], 0, 1);
    }

    // free old page locks

    free(file->pageReaderMutex);
    free(file->pageWriterMutex);
    free(file->numOfCurrentPageReaders);

    // update zc_file

    file->pageReaderMutex = newPageReaderMutex;
    file->pageWriterMutex = newPageWriterMutex;

    file->numOfPages = newPageNum;

    void* newMemoryAddress = NULL;
    if (file->fileSize == 0) {
      newMemoryAddress = mmap(NULL, newFileSize, PROT_READ | PROT_WRITE, MAP_SHARED, file->fileDescriptor, 0);
    } else {
      newMemoryAddress = mremap(file->memoryAddress, file->fileSize, newFileSize, MREMAP_MAYMOVE);
    }

    file->memoryAddress = newMemoryAddress;
    file->fileSize = newFileSize;
  }

  char* ans = (char*)(file->memoryAddress + offset);

  push_queue(&file->writePageQueue, currPageIndex, numPages);

  sem_post(&file->fileLock);

  for (int i = 0; i < numPages; i++) {
    int currPage = currPageIndex + i;
    sem_wait(&file->pageWriterMutex[currPage]);
  }

  return ans;
}