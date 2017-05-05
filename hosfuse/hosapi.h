#ifndef _hosAPI_H
#define _hosAPI_H

#include <curl/curl.h>
#include <curl/easy.h>

#define BUFFER_INITIAL_SIZE 4096
#define MAX_HEADER_SIZE 8192
#define MAX_PATH_SIZE (1024 + 256 + 3)
#define MAX_URL_SIZE (MAX_PATH_SIZE * 3)
#define MAX_BODY_SIZE (1<<20)

typedef struct curl_slist curl_slist;

typedef struct dir_entry
{
  char *name;
  char *full_name;
  char *content_type;
  off_t size;
  time_t last_modified;
  int isdir;
  struct dir_entry *next;
} dir_entry;

void hos_init();
void hos_set_credentials(char *username, char *server);
int hos_object_read_fp(const char *path, FILE *fp);
int hos_object_write_fp(const char *path, FILE *fp);
int hos_list_directory(const char *path, dir_entry **);
int hos_delete_object(const char *path);
int hos_copy_object(const char *src, const char *dst);
int hos_create_directory(const char *label);
int hos_object_truncate(const char *path, off_t size);
off_t hos_file_size(int fd);
void hos_debug(int dbg);
void hos_free_dir_list(dir_entry *dir_list);

void debugf(char *fmt, ...);
#endif

