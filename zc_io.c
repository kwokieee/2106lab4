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

  //   sem_t* pageLock;         // mutex to protect from any edits to page
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

void push_queue(struct queue* q, int pageIndex) {
  struct linked_list_queue* new_node = malloc(sizeof(struct linked_list_queue));
  new_node->pageIndex = pageIndex;
  new_node->next = NULL;

  if (q->head == NULL) {
    q->head = new_node;
    q->tail = new_node;
  } else {
    q->tail->next = new_node;
    q->tail = new_node;
  }
}

int pop_queue(struct queue* q) {
  if (q->head == NULL) {
    return -1;
  }

  struct linked_list_queue* temp = q->head;
  int pageIndex = temp->pageIndex;
  q->head = q->head->next;
  free(temp);
  return pageIndex;
}

int buffer = 10;  // not sure why this works, lolololloolol, do I init less semaphores than needed?

zc_file* zc_open(const char* path) {
  struct zc_file* zc = malloc(sizeof(struct zc_file));
  int fileDescriptor = open(path, O_CREAT | O_RDWR, 0644);

  struct stat fileStatus;

  fstat(fileDescriptor, &fileStatus);

  size_t fileSize = fileStatus.st_size == 0 ? 4 : fileStatus.st_size;  // todo:: @byran figure what the ternary is for

  void* memoryAddress = mmap(NULL, fileSize, PROT_READ | PROT_WRITE, MAP_SHARED, fileDescriptor, 0);

  zc->fileDescriptor = fileDescriptor;
  zc->fileSize = fileSize;
  zc->offset = 0;
  zc->memoryAddress = memoryAddress;
  zc->isBonus = 0;

  int pageSize = sysconf(_SC_PAGE_SIZE);

  int numberOfPages = ceil(fileSize / pageSize) + buffer;
  zc->numOfPages = numberOfPages;

  zc->pageReaderMutex = malloc(numberOfPages * sizeof(sem_t));

  zc->pageWriterMutex = malloc(numberOfPages * sizeof(sem_t));
  zc->numOfCurrentPageReaders = malloc(numberOfPages * sizeof(int));
  // zc->numberOfCurrentReaders = malloc(numberOfPages * sizeof(int));
  zc->numOfCurrentReaders = 0;
  sem_init(&zc->fileLock, 0, 1);
  sem_init(&zc->fileWriterMutex, 0, 1);
  fprintf(stderr, "number of pages: %d\n", numberOfPages);

  for (int i = 0; i < numberOfPages; i++) {
    // sem_init(&zc->pageLock[i], 0, 1);
    zc->numOfCurrentPageReaders[i] = 0;
    sem_init(&zc->pageReaderMutex[i], 0, 1);
    sem_init(&zc->pageWriterMutex[i], 0, 1);
  }

  sem_init(&zc->readPageQueueMutex, 0, 1);
  sem_init(&zc->writePageQueueMutex, 0, 1);

  return zc;
  // return NULL;
}

int zc_close(zc_file* file) {
  int ans = munmap(file->memoryAddress, file->fileSize);

  // destroy mutexes

  for (int i = 0; i < file->numOfPages; i++) {
    // sem_destroy(&file->pageLock[i]);
    sem_destroy(&file->pageReaderMutex[i]);
    sem_destroy(&file->pageWriterMutex[i]);
  }

  // free(file->pageLock);
  free(file->pageReaderMutex);
  free(file->pageWriterMutex);
  // deallocate zc_file

  free(file);

  return ans;
}

const char* zc_read_start(zc_file* file, size_t* size) {
  // copy necessary information and pass the lock to the next reader
  sem_wait(&file->fileLock);
  int pageSize = sysconf(_SC_PAGE_SIZE);
  int currPageIndex = (int)ceil((double)file->offset / pageSize);

  fprintf(stderr, "offset %ld\n", file->offset);
  fprintf(stderr, "file size %ld\n", file->fileSize);
  // if (file->offset >= file->fileSize) {
  //   return NULL;
  // }

  fprintf(stderr, "locking %d in start \n", currPageIndex);

  // sem_wait(&file->pageReaderMutex[currPageIndex]);

  fprintf(stderr, "size %ld\n", *size);
  // if is first reader, lock write
  if (file->numOfCurrentReaders == 0) {
    fprintf(stderr, "wait for writer\n");
    sem_wait(&file->fileWriterMutex);
  }

  file->numOfCurrentReaders++;

  fprintf(stderr, "unlocking %d in start\n", currPageIndex);
  // sem_post(&file->pageReaderMutex[currPageIndex]);

  size_t fileSizeLeftToRead = file->fileSize - file->offset;

  if (fileSizeLeftToRead < *size) {
    *size = fileSizeLeftToRead;
  }

  file->prevOffset = file->offset;
  file->offset += *size;

  fprintf(stderr, "hello 5 world\n");
  fprintf(stderr, "size %ld\n", *size);

  char* ans = (char*)(file->memoryAddress + file->prevOffset);

  sem_post(&file->fileLock);

  return ans;

  // return (char*)file->memoryAddress + file->offset;
}

void zc_read_end(zc_file* file) {
  // To implement
  if (file->isBonus) {
    int currPageIndex = pop_queue(&file->readPageQueue);

    sem_wait(&file->pageReaderMutex[currPageIndex]);

    if (file->numOfCurrentPageReaders[currPageIndex] == 1) {
      fprintf(stderr, "unlock writer\n");
      sem_post(&file->pageWriterMutex[currPageIndex]);
    }

    file->numOfCurrentPageReaders[currPageIndex]--;

    sem_post(&file->pageReaderMutex[currPageIndex]);

    return;
  }

  sem_wait(&file->fileLock);
  // int pageSize = sysconf(_SC_PAGE_SIZE);
  // int currPageIndex = (int)ceil((double)file->prevOffset / pageSize);

  // fprintf(stderr, "locking %d in end\n", currPageIndex);
  // sem_wait(&file->pageReaderMutex[currPageIndex]);

  // if is last reader, unlock write
  if (file->numOfCurrentReaders == 1) {
    fprintf(stderr, "unlock writer\n");
    // sem_post(&file->pageWriterMutex[currPageIndex]);
    sem_post(&file->fileWriterMutex);
  }

  // fprintf(stderr, "unlocking %d in end\n", currPageIndex);

  file->numOfCurrentReaders--;
  // sem_post(&file->pageReaderMutex[currPageIndex]);
  sem_post(&file->fileLock);
}

char* zc_write_start(zc_file* file, size_t size) {
  sem_wait(&file->fileWriterMutex);
  sem_wait(&file->fileLock);

  fprintf(stderr, "hello 1 world\n");
  int pageSize = sysconf(_SC_PAGE_SIZE);
  int currPageIndex = (int)ceil((double)file->offset / pageSize);

  // if size + current offset > file size, increase file size

  if (file->offset + size > file->fileSize) {
    fprintf(stderr, "resizing file\n");
    // lock the file as we need to resize the file and init more semaphores
    // double confimation that the file size is not enough

    size_t newFileSize = file->offset + size;
    ftruncate(file->fileDescriptor, newFileSize);

    void* newMemoryAddress = mremap(file->memoryAddress, file->fileSize, newFileSize, MREMAP_MAYMOVE);

    file->memoryAddress = newMemoryAddress;

    file->fileSize = newFileSize;
  }

  fprintf(stderr, "locking reader %d in write start\n", currPageIndex);
  // sem_wait(&file->pageReaderMutex[currPageIndex]);
  fprintf(stderr, "locking writer %d in write start\n", currPageIndex);

  file->prevOffset = file->offset;
  file->offset += size;

  char* ans = (char*)(file->memoryAddress + file->prevOffset);

  sem_post(&file->fileLock);

  return ans;
}

void zc_write_end(zc_file* file) {
  // To implement
  if (file->isBonus) {
    sem_wait(&file->writePageQueueMutex);
    int currPageIndex = pop_queue(&file->writePageQueue);
    sem_post(&file->writePageQueueMutex);

    // writes to the page at address
    msync(file->memoryAddress + currPageIndex * sysconf(_SC_PAGE_SIZE), sysconf(_SC_PAGE_SIZE), MS_SYNC);

    // unlock the page
    sem_post(&file->pageWriterMutex[currPageIndex]);

    return;
  }

  // sem_wait(&file->fileLock);
  msync(file->memoryAddress, file->fileSize, MS_SYNC);
  sem_post(&file->fileWriterMutex);
  // sem_post(&file->fileLock);
}

/**************
 * Exercise 2 *
 **************/

off_t zc_lseek(zc_file* file, long offset, int whence) {
  // To implement
  sem_wait(&file->fileLock);
  fprintf(stderr, "oi\n");
  int ans = -1;

  int pageSize = sysconf(_SC_PAGE_SIZE);
  int currPageIndex = (int)ceil((double)file->offset / pageSize);

  fprintf(stderr, "locking %d in lseek\n", currPageIndex);

  sem_wait(&file->pageReaderMutex[currPageIndex]);
  sem_wait(&file->pageWriterMutex[currPageIndex]);

  switch (whence) {
    case SEEK_SET:
      fprintf(stderr, "SEEK_SET\n");
      ans = file->offset = offset;
      fprintf(stderr, "SEEK_SET\n");

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
  //   // To implement
  //   return -1;

  // This function copies the content of source into dest. It will return 0 on success and -1 on failure. You should make use of the function calls you implemented in the previous exercises, and should not use any user buffers to achieve this. Do ftruncate the destination file so they have the same size.c

  zc_file* source_zc = zc_open(source);
  zc_file* dest_zc = zc_open(dest);

  size_t size_of_source = source_zc->fileSize;
  // no need to truncate, since write_start will do it
  // ftruncate(dest_zc->fileDescriptor, size_of_source);

  const char* source_content = zc_read_start(source_zc, &size_of_source);
  char* dest_content = zc_write_start(dest_zc, size_of_source);
  zc_write_end(dest_zc);

  dest_zc = source_zc;
  memcpy(dest_content, source_content, size_of_source);

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

  sem_t* readerMutex = &file->pageReaderMutex[currPageIndex];
  sem_t* writerMutex = &file->pageWriterMutex[currPageIndex];
  char* ans = (char*)(file->memoryAddress + offset);
  size_t fileSizeLeftToRead = file->fileSize - file->offset;
  sem_post(&file->fileLock);
  // lock reader to update the number of readers
  sem_wait(readerMutex);
  // if is first reader, lock write
  if (file->numOfCurrentPageReaders[currPageIndex] == 0) {
    sem_wait(writerMutex);
  }

  file->numOfCurrentPageReaders[currPageIndex]++;
  if (fileSizeLeftToRead < *size) {
    *size = fileSizeLeftToRead;
  }

  sem_post(readerMutex);

  return ans;
}

char* zc_write_offset(zc_file* file, size_t size, long offset) {
  file->isBonus = 1;

  int pageSize = sysconf(_SC_PAGE_SIZE);
  int currPageIndex = (int)ceil((double)offset / pageSize);

  sem_wait(&file->pageWriterMutex[currPageIndex]);
  // lock whole file (among writers) for a possible update to the number of pages
  sem_wait(&file->fileLock);
  if (file->offset + size > file->fileSize) {
    size_t newFileSize = file->offset + size;
    ftruncate(file->fileDescriptor, newFileSize);

    int newPageNum = (int)ceil((double)newFileSize / pageSize) + buffer;

    // copy old page locks to new page locks
    sem_t* newPageReaderMutex = malloc(newPageNum * sizeof(sem_t));
    sem_t* newPageWriterMutex = malloc(newPageNum * sizeof(sem_t));

    for (int i = 0; i < file->numOfPages; i++) {
      newPageReaderMutex[i] = file->pageReaderMutex[i];
      newPageWriterMutex[i] = file->pageWriterMutex[i];
    }

    // init new page locks

    for (int i = file->numOfPages; i < newPageNum; i++) {
      // sem_init(&zc->pageLock[i], 0, 1);
      sem_init(&newPageReaderMutex[i], 0, 1);
      sem_init(&newPageWriterMutex[i], 0, 1);
    }

    // free old page locks

    free(file->pageReaderMutex);
    free(file->pageWriterMutex);

    // update zc_file

    file->pageReaderMutex = newPageReaderMutex;
    file->pageWriterMutex = newPageWriterMutex;

    file->numOfPages = newPageNum;

    void* newMemoryAddress = mremap(file->memoryAddress, file->fileSize, newFileSize, MREMAP_MAYMOVE);

    file->memoryAddress = newMemoryAddress;
    file->fileSize = newFileSize;
  }

  char* ans = (char*)(file->memoryAddress + offset);

  sem_post(&file->fileLock);

  return ans;

  // To implement

  // simply set offset to the given offset

  //   return NULL;
}