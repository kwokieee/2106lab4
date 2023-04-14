// CS2106 Lab 5

#ifndef ZC_IO_H
#define ZC_IO_H

#include <stddef.h>
#include <sys/types.h>

// THERE IS NO NEED TO CHANGE THIS HEADER FILE

typedef struct zc_file zc_file;

// Exercise 1
zc_file* zc_open(const char* path);
int zc_close(zc_file* file);

const char* zc_read_start(zc_file* file, size_t* size);
void zc_read_end(zc_file* file);

char* zc_write_start(zc_file* file, size_t size);
void zc_write_end(zc_file* file);

// Exercise 2
off_t zc_lseek(zc_file* file, long offset, int whence);

// Exercise 3
int zc_copyfile(const char* source, const char* dest);

// Bonus Exercise
const char* zc_read_offset(zc_file* file, size_t* size, long offset);
char* zc_write_offset(zc_file* file, size_t size, long offset);
#endif
