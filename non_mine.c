#include "zc_io.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>
#include <semaphore.h>

// The zc_file struct is analogous to the FILE struct that you get from fopen.

struct zc_file
{
  zc_page *pages;
  int fd;
  void *addr;
  size_t size;
  size_t offset;
  size_t totalSize;
  int noOfPages;
};

struct zc_page
{
  sem_t readerSem;
  sem_t writerSem;
  sem_t mutex;
  int count;
};

/**************
 * Exercise 1 *
 **************/

zc_file *zc_open(const char *path)
{
  //init variables
  struct stat s;
  struct zc_file *zc = malloc(sizeof(struct zc_file));
  int fd;
  void *addr;
  size_t fsize;

  //open file
  if ((fd = open(path, O_CREAT | O_RDWR, 0644)) == -1)
  {
    perror("Error opening file...");
  }

  // get stat of file
  fstat(fd, &s);
  fsize = s.st_size == 0 ? 4 : s.st_size;
  if ((addr = mmap(NULL, fsize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)) == MAP_FAILED)
  {
    perror("Error mapping...");
  }

  zc->addr = addr;
  zc->size = fsize;
  zc->totalSize = fsize;
  zc->offset = 0;
  zc->fd = fd;
  zc->noOfPages = (int) ceil((double) fsize / getpagesize());

  for (int i = 0; i < zc->noOfPages; i++) {
    zc->pages[i].count = 0;
    sem_init(zc->pages[i].mutex, 0, 1);
    sem_init(zc->pages[i].writerSem, 0, 1);
    sem_init(zc->pages[i].readerSem, 0, 1);
  }

  return zc;
}

int zc_close(zc_file *file)
{
  return munmap(file->addr, file->size);
}

const char *zc_read_start(zc_file *file, size_t *size)
{
  sem_wait(&file->pages[(int) ceil((double) file->offset / getpagesize()) - 1].readerSem);
  sem_wait(&file->pages[(int) ceil((double) file->offset / getpagesize()) - 1].mutex);
  if (++(file->pages[(int) ceil((double) file->offset / getpagesize()) - 1].count) == 1) {
    sem_wait(&file->pages[(int) ceil((double) file->offset / getpagesize()) - 1].writerSem);
  }
  sem_post(&file->pages[(int) ceil((double) file->offset / getpagesize()) - 1].mutex);
  sem_post(&file->pages[(int) ceil((double) file->offset / getpagesize()) - 1].readerSem);

  int off = file->offset;

  if (file->size < *size)
  {
    *size = file->size;
    file->size = 0;
  }
  else
  {
    file->size -= *size;
  }
  file->offset += *size;

  return (char *)file->addr + off;
}

void zc_read_end(zc_file *file)
{
  // To implement
  sem_wait(&file->pages[(int) ceil((double) file->offset / getpagesize()) - 1].mutex);
  if (--file->pages[(int) ceil((double) file->offset / getpagesize()) - 1].count == 0) {
    sem_post(&file->pages[(int) ceil((double) file->offset / getpagesize()) - 1].writerSem);
  }
  sem_post(&file->pages[(int) ceil((double) file->offset / getpagesize()) - 1].mutex);
}

/**************
 * Exercise 2 *
 **************/

char *zc_write_start(zc_file *file, size_t size)
{
  sem_wait(&file->pages[(int) ceil((double) file->offset / getpagesize()) - 1].readerSem);
  sem_wait(&file->pages[(int) ceil((double) file->offset / getpagesize()) - 1].writerSem);
  int off = file->offset;

  if (file->size < size) {
    size_t oldsize = file->offset + file->size;
    size_t additional = size - file->size;
    if (ftruncate(file->fd, oldsize + additional) == -1) {
      perror("Error truncating file...");
    }

    void* newaddr = mremap(file->addr, oldsize, oldsize + additional, MREMAP_MAYMOVE);
    file->addr = newaddr;
    file->size += additional;
    file->totalSize += additional;
  }
  file->size -= size;
  file->offset += size;

  return (char *)file->addr + off;
}

void zc_write_end(zc_file *file)
{
  msync(file->addr, file->offset + file->size, MS_SYNC);
  sem_post(&file->pages[(int) ceil((double) file->offset / getpagesize()) - 1].writerSem);
  sem_post(&file->pages[(int) ceil((double) file->offset / getpagesize()) - 1].readerSem);
}

/**************
 * Exercise 3 *
 **************/

off_t zc_lseek(zc_file *file, long offset, int whence)
{
  int off = file->offset;
  sem_wait(&file->pages[(int) ceil((double) off / getpagesize()) - 1].readerSem);
  sem_wait(&file->pages[(int) ceil((double) off / getpagesize()) - 1].writerSem);
  if (whence == SEEK_SET) {
    // file->offset = offset;
    file->offset = offset;
    sem_post(&file->pages[(int) ceil((double) off / getpagesize()) - 1].writerSem);
    sem_post(&file->pages[(int) ceil((double) off / getpagesize()) - 1].readerSem);
    return file->offset;
  } else if (whence == SEEK_CUR) {
    file->offset += offset;
    sem_post(&file->pages[(int) ceil((double) off / getpagesize()) - 1].writerSem);
    sem_post(&file->pages[(int) ceil((double) off / getpagesize()) - 1].readerSem);
    return file->offset;
  } else if (whence == SEEK_END) {
    file->offset = file->totalSize + offset;
    sem_post(&file->pages[(int) ceil((double) off / getpagesize()) - 1].writerSem);
    sem_post(&file->pages[(int) ceil((double) off / getpagesize()) - 1].readerSem);
    return file->offset;
  } else {
    sem_post(&file->pages[(int) ceil((double) off / getpagesize()) - 1].writerSem);
    sem_post(&file->pages[(int) ceil((double) off / getpagesize()) - 1].readerSem);
    return -1;
  }
}

/**************
 * Exercise 5 *
 **************/

int zc_copyfile(const char *source, const char *dest)
{
  struct stat sc;
  int src = open(source, O_RDWR | O_CREAT, 0644);
  int dst = open(dest, O_RDWR | O_CREAT, 0644);
  fstat(src, &sc);

  if (copy_file_range(src, NULL, dst, NULL, sc.st_size, 0) == -1) {
    return -1;
  } else {
    return 0;
  }
}
