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

struct zc_file {
  int fileDescriptor;
  size_t fileSize;
  size_t offset;
  size_t prevOffset;
  void* memoryAddress;
  int numOfPages;

  //   sem_t* pageLock;         // mutex to protect from any edits to page
  sem_t* pageReaderMutex;  // mutex to protect from any edits to page during read
  sem_t* pageWriterMutex;  // mutex to protect from any edits to page during write

  int* numberOfCurrentReaders;  // count for number of readers on page

  // Insert the fields you need here.

  /* Some suggested fields :
      - pointer to the virtual memory space
      - offset from the start of the virtual memory
      - total size of the file
      - file descriptor to the opened file
      - mutex for access to the memory space and number of readers
  */
};

/**************
 * Exercise 1 *
 **************/

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

  int pageSize = sysconf(_SC_PAGE_SIZE);

  int numberOfPages = ceil(fileSize / pageSize);
  zc->numOfPages = numberOfPages;
  // init mutexes

  // zc->pageReaderMutex = malloc(numberOfPages * sizeof(sem_t*));
  // zc->pageWriterMutex = malloc(numberOfPages * sizeof(sem_t*));
  // zc->numberOfCurrentReaders = malloc(numberOfPages * sizeof(int*));
  zc->pageReaderMutex = malloc(numberOfPages * sizeof(sem_t));

  zc->pageWriterMutex = malloc(numberOfPages * sizeof(sem_t));

  zc->numberOfCurrentReaders = malloc(numberOfPages * sizeof(int));

  fprintf(stderr, "number of pages: %d\n", numberOfPages);
  for (int i = 0; i < numberOfPages; i++) {
    // sem_init(&zc->pageLock[i], 0, 1);
    sem_init(&zc->pageReaderMutex[i], 0, 1);
    sem_init(&zc->pageWriterMutex[i], 0, 1);
    zc->numberOfCurrentReaders[i] = 0;
  }

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
  int pageSize = sysconf(_SC_PAGE_SIZE);

  int currPageIndex = (int)ceil((double)file->offset / pageSize);

  // print current page index
  // printf("currPageIndex: %d", currPageIndex);
  //   sem_wait(file->pageLock[currPageIndex]);
  // lock read
  fprintf(stderr, "locking %d in start \n", currPageIndex);

  sem_wait(&file->pageReaderMutex[currPageIndex]);

  fprintf(stderr, "size %ld\n", *size);
  // if is first reader, lock write
  if (file->numberOfCurrentReaders[currPageIndex] == 0) {
    fprintf(stderr, "wait for writer\n");
    sem_wait(&file->pageWriterMutex[currPageIndex]);
  }

  file->numberOfCurrentReaders[currPageIndex]++;

  fprintf(stderr, "unlocking %d in start\n", currPageIndex);
  sem_post(&file->pageReaderMutex[currPageIndex]);

  //   sem_post(file->pageLock[currPageIndex]);

  size_t fileSizeLeftToRead = file->fileSize - file->offset;

  // // filesize left to read cannot be negative
  // if (fileSizeLeftToRead < 0) {
  //   fileSizeLeftToRead = 0;
  // }

  if (fileSizeLeftToRead < *size) {
    *size = fileSizeLeftToRead;
  }

  file->prevOffset = file->offset;
  file->offset += *size;
  

  // read from memory at (file->memoryAddress + file->offset)

  // fprintf(stderr, "returned ans %s\n", (char*)(file->memoryAddress));
  fprintf(stderr, "hello 5 world\n");
  fprintf(stderr, "size %ld\n", *size);
  return (char*)(file->memoryAddress + file->prevOffset);

  // return (char*)file->memoryAddress + file->offset;
}

void zc_read_end(zc_file* file) {
  // To implement
  int pageSize = sysconf(_SC_PAGE_SIZE);
  int currPageIndex = (int)ceil((double)file->prevOffset / pageSize);

  fprintf(stderr, "locking %d in end\n", currPageIndex);
  sem_wait(&file->pageReaderMutex[currPageIndex]);

  // if is last reader, unlock write
  if (file->numberOfCurrentReaders[currPageIndex] == 1) {
    fprintf(stderr, "unlock writer\n");
    sem_post(&file->pageWriterMutex[currPageIndex]);
  }

  fprintf(stderr, "unlocking %d in end\n", currPageIndex);

  file->numberOfCurrentReaders[currPageIndex]--;
  sem_post(&file->pageReaderMutex[currPageIndex]);
}

char* zc_write_start(zc_file* file, size_t size) {
  int pageSize = sysconf(_SC_PAGE_SIZE);
  int currPageIndex = (int)ceil((double)file->offset / pageSize);

  sem_wait(&file->pageReaderMutex[currPageIndex]);
  sem_wait(&file->pageWriterMutex[currPageIndex]);

  size_t fileSizeLeftToWrite = file->fileSize - file->offset;

  // // filesize left to read cannot be negative
  // if (fileSizeLeftToWrite < (size_t) 0) {
  //   fileSizeLeftToWrite = 0;
  // }

  if (fileSizeLeftToWrite < size) {
    size_t newFileSize = file->fileSize + size - fileSizeLeftToWrite;
    fprintf(stderr, "newFileSize: %ld", newFileSize);
    ftruncate(file->fileDescriptor, newFileSize);

    void* newMemoryAddress = mremap(file->memoryAddress, file->fileSize, newFileSize, MREMAP_MAYMOVE);

    file->memoryAddress = newMemoryAddress;
    file->fileSize = newFileSize;
  }
  file->prevOffset = file->offset;
  file->offset += size;

  return (char*)file->memoryAddress + file->prevOffset;
}

void zc_write_end(zc_file* file) {
  // To implement
  msync(file->memoryAddress, file->fileSize, MS_SYNC);

  int pageSize = sysconf(_SC_PAGE_SIZE);
  int currPageIndex = (int)ceil((double)file->prevOffset / pageSize);
  sem_post(&file->pageReaderMutex[currPageIndex]);
  sem_post(&file->pageWriterMutex[currPageIndex]);
}

/**************
 * Exercise 2 *
 **************/

off_t zc_lseek(zc_file* file, long offset, int whence) {
  // To implement

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

  sem_post(&file->pageReaderMutex[pageSize]);
  sem_post(&file->pageWriterMutex[pageSize]);

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

  zc_write_start(dest_zc, size_of_source);
  zc_write_end(dest_zc);

  memcpy(dest_content, source_content, size_of_source);

  return 0;
}

/**************
 * Bonus Exercise *
 **************/

const char* zc_read_offset(zc_file* file, size_t* size, long offset) {
  // To implement

  // simply set offset to the given offset

  zc_lseek(file, offset, SEEK_SET);
  const char* ans = zc_read_start(file, size);
  zc_read_end(file);
  return ans;
  //   return NULL;
}

char* zc_write_offset(zc_file* file, size_t size, long offset) {
  // To implement

  // simply set offset to the given offset
  zc_lseek(file, offset, SEEK_SET);
  char* ans = zc_write_start(file, size);
  zc_write_end(file);
  return ans;
  //   return NULL;
}