#define _GNU_SOURCE
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#ifdef __linux__
#include <alloca.h>
#endif
#include <pthread.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/time.h>
#include "hosapi.h"
#include "config.h"

#define REQUEST_RETRIES 4

static CURL *curl_pool[1024];
static int curl_pool_count = 0;
static int debug = 0;
static pthread_mutex_t pool_mut;

static CURL *get_connection()
{
  pthread_mutex_lock(&pool_mut);
  CURL *curl = curl_pool_count ? curl_pool[--curl_pool_count] : curl_easy_init();
  if (!curl)
  {
    debugf("curl alloc failed");
    abort();
  }
  pthread_mutex_unlock(&pool_mut);
  return curl;
}

static void return_connection(CURL *curl)
{
  pthread_mutex_lock(&pool_mut);
  curl_pool[curl_pool_count++] = curl;
  pthread_mutex_unlock(&pool_mut);
}

size_t body_to_string(void *ptr, size_t sz, size_t nmemb, void *body){
  size_t prev_len = strlen(body);
  size_t new_len = prev_len + sz*nmemb;
  if(new_len >= MAX_BODY_SIZE)
    new_len = MAX_BODY_SIZE;
  memcpy(body + prev_len, ptr, new_len - prev_len);
  ((char*)body)[new_len] = 0; //null terminate
  return  sz*nmemb;
}

static int send_request(char *method, const char *path, char *body, FILE *up_fp, FILE *down_fp)
{
  char url[MAX_URL_SIZE];
  char *slash;
  long response = -1;
  int tries = 0;

  char *encoded = curl_escape(path, 0);
  while ((slash = strstr(encoded, "%2F")) || (slash = strstr(encoded, "%2f")))
  {
    *slash = '/';
    memmove(slash+1, slash+3, strlen(slash+3)+1);
  }
  strncpy(url, reconnect_args.server, MAX_URL_SIZE);
  strncat(url, encoded, MAX_URL_SIZE);
  curl_free(encoded);

  // retry on failures
  for (tries = 0; tries < REQUEST_RETRIES; tries++)
  {
    CURL *curl = get_connection(path);
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_HEADER, 0);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "interwebz_dogeplorer/pro 6.9");
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10);
    curl_easy_setopt(curl, CURLOPT_VERBOSE, debug);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method);

    if(up_fp){
      rewind(up_fp);
      curl_easy_setopt(curl, CURLOPT_UPLOAD, 1);
      curl_easy_setopt(curl, CURLOPT_INFILESIZE, hos_file_size(fileno(up_fp)));
      curl_easy_setopt(curl, CURLOPT_READDATA, up_fp);
    }
    if(down_fp){
      rewind(down_fp); // make sure the file is ready for a-writin'
      fflush(down_fp);
      if (ftruncate(fileno(down_fp), 0) < 0)
      {
        debugf("ftruncate failed.");
        abort();
      }
      curl_easy_setopt(curl, CURLOPT_WRITEDATA, down_fp);
    }else if(body){
      // MAX_BODY_SIZE
      body[0] = 0;
       curl_easy_setopt(curl, CURLOPT_WRITEDATA, body);
       curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, body_to_string);
    }

    curl_easy_perform(curl);
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response);
    curl_easy_reset(curl);
    return_connection(curl);
    if (response >= 200 && response < 400)
      return response;
    sleep(8 << tries); // backoff
  }
  return response;
}

void split_path(const char *path, char *bucket, char *object){
  char *c=path;
  if(*c == '/')
    c++;
  while(*c != '/' || *c != 0){
    *bucket = *c;
    c++; bucket++;
  }
  *bucket = 0;
  if(*c == '/')
    c++;
  while(*c != 0){
    *object = *c;
    c++; object++;
  }
  *object = 0;
}

/*
 * Public interface
 */

void hos_init()
{
  curl_global_init(CURL_GLOBAL_ALL);
  pthread_mutex_init(&pool_mut, NULL);
  curl_version_info_data *cvid = curl_version_info(CURLVERSION_NOW);
}

int hos_object_read_fp(const char *path, FILE *fp)
{
  char url[MAX_URL_SIZE], bucket[MAX_PATH_SIZE], object[MAX_PATH_SIZE];
  split_path(path, bucket, object);
  if(bucket[0] == 0 || object[0] == 0)
    return 0;
  snprintf(url, MAX_URL_SIZE, "/putObject?bucketKey=%s&objectKey=%s", bucket, object);
  fflush(fp);
  rewind(fp);
  int response = send_request("PUT", url, NULL, fp, NULL);
  return (response >= 200 && response < 300);
}

int hos_object_write_fp(const char *path, FILE *fp)
{
  char url[MAX_URL_SIZE], bucket[MAX_PATH_SIZE], object[MAX_PATH_SIZE];
  split_path(path, bucket, object);
  if(bucket[0] == 0 || object[0] == 0)
    return 0;
  snprintf(url, MAX_URL_SIZE, "/getObject?bucketKey=%s&objectKey=%s", bucket, object);
  int response = send_request("GET", url, NULL, NULL, fp);
  fflush(fp);
  if ((response >= 200 && response < 300) || ftruncate(fileno(fp), 0))
    return 1;
  rewind(fp);
  return 0;
}

int hos_object_truncate(const char *path, off_t size)
{
  int success = 0;
  if (size == 0)
  {
    FILE *fp = fopen("/dev/null", "r");
    success = hos_object_read_fp(path, fp);
    fclose(fp);
  }
  else
  {
    success = 0;
  }
  return success;
}

int hos_list_directory(const char *path, dir_entry **dir_list)
{
  char bucket[MAX_PATH_SIZE], object[MAX_PATH_SIZE];
  char body[MAX_BODY_SIZE], url[MAX_URL_SIZE];
  int prefix_length = 0;
  int response = 0;
  int retval = 0;
  int entry_count = 0;
  int list_buckets = 0;
  *dir_list = NULL;
  split_path(path, bucket, object);
  if(bucket[0] == 0){
    snprintf(url, MAX_URL_SIZE, "/listBuckets");
    list_buckets = 1;
  }else if(object[0] == 0){
    snprintf(url, MAX_URL_SIZE, "/listObjects?bucketKey=%s", bucket);
  }else{
    // object
    snprintf(body, MAX_BODY_SIZE, "{\"objectList\":[{\"1\":\"%s\"}]}", object);
  }

  response = send_request("GET", url, body, NULL, NULL);
  if(response < 200 || response >= 400)
    return 0;
  char *json_stream = 0;
  int bytes_read = 0;
  if(list_buckets)
    json_stream = &body[0] + strlen("{\"objectList\":[");
  else
    json_stream = &body[0] + strlen("{\"bucketList\":[");

  if(*json_stream==']') // no objects in this bucket
    return 1;

  json_stream++; //skip {
  char entry_name[MAX_PATH_SIZE], entry_index[MAX_PATH_SIZE];
  while(1){
    sscanf(json_stream, "\"%s\":\"%s\"%n", entry_index, entry_name, &bytes_read);
    if(*json_stream == ',')
      json_stream++;
    entry_count++;

    dir_entry *de = (dir_entry *)malloc(sizeof(dir_entry));
    de->next = NULL;
    de->size = 0;
    de->last_modified = time(NULL);
    de->name = strdup(entry_name);
    if(list_buckets){
      de->content_type = strdup("application/directory");
      de->isdir = 1;
      asprintf(&(de->full_name), "%s", entry_name);
    }else{
      de->isdir = 0;
      de->content_type = strdup("application/data");
      asprintf(&(de->full_name), "%s/%s", bucket, entry_name);
    }
  }

  debugf("entry count: %d", entry_count);

  return 1;
}

void hos_free_dir_list(dir_entry *dir_list)
{
  while (dir_list)
  {
    dir_entry *de = dir_list;
    dir_list = dir_list->next;
    free(de->name);
    free(de->full_name);
    free(de->content_type);
    free(de);
  }
}

int hos_delete_object(const char *path)
{
  char url[MAX_URL_SIZE], bucket[MAX_PATH_SIZE], object[MAX_PATH_SIZE];
  split_path(path, bucket, object);
  int response;
  if(bucket[0] != 0 || object[0] == 0){
    snprintf(url, MAX_URL_SIZE, "/deleteBucket?bucketKey=%s", bucket);
    response = send_request("DELETE", url, NULL, NULL, NULL);
  }else{
    if(bucket[0] == 0 || object[0] == 0)
      return 0;
    snprintf(url, MAX_URL_SIZE, "/deleteObject?bucketKey=%s&objectKey=%s", bucket, object);
    response = send_request("GET", url, NULL, NULL, NULL);
  }
  return (response >= 200 && response < 300);
}

int hos_copy_object(const char *src, const char *dst)
{
  // int response = send_request("PUT", dst_encoded, NULL, NULL, headers);
  // return (response >= 200 && response < 300);
  return 0;
}

int hos_create_directory(const char *path)
{
  char url[MAX_URL_SIZE], bucket[MAX_PATH_SIZE], object[MAX_PATH_SIZE];
  split_path(path, bucket, object);
  if(bucket[0] == 0 || object[0] != 0)
    return 0;
  snprintf(url, MAX_URL_SIZE, "/createBucket?bucketKey=%s", bucket);
  int response = send_request("PUT", url, NULL, NULL, NULL);
  return (response >= 200 && response < 300);
}

off_t hos_file_size(int fd)
{
  struct stat buf;
  fstat(fd, &buf);
  return buf.st_size;
}

void hos_debug(int dbg)
{
  debug = dbg;
}


static struct {
  char username[MAX_HEADER_SIZE], server[MAX_URL_SIZE];
} reconnect_args;

void hos_set_credentials(char *username, char *server)
{
  strncpy(reconnect_args.username, username, sizeof(reconnect_args.username));
  strncpy(reconnect_args.server, server, sizeof(reconnect_args.server));
}

void debugf(char *fmt, ...)
{
  if (debug)
  {
    va_list args;
    va_start(args, fmt);
    fputs("!!! ", stderr);
    vfprintf(stderr, fmt, args);
    va_end(args);
    putc('\n', stderr);
  }
}

