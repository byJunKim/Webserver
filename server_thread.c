#include "request.h"
#include "server_thread.h"
#include "common.h"
#include "pthread.h"
#include <math.h>

#define AVERAGE_FILE_SIZE 12288

struct data_object {
    struct file_data *data_in_cache;
    int requested;
    struct data_object *next;

};

struct queue_object {
    struct file_data *data_in_queue;
    unsigned int hash_val;
    struct queue_object *next;

};

struct server {
    int nr_threads;
    int max_requests;
    int max_cache_size;
    int cache_in_use;
    int exiting;
    pthread_t *worker_threads;
    int *request_queue;
    struct data_object **cache;
    struct queue_object *cache_queue_head;
    struct queue_object *cache_queue_tail;
    /* add any other parameters you need */
};

struct rq_object {
    pthread_t *thread;
    struct rq_object* next;

};

struct request_queue {
    struct rq_object* head;

};

struct arguments {
    //    void* thread_main;
    struct server* sv;
    pthread_t tid;

};

int NUMBER_OF_FILES;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t llock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t full;
pthread_cond_t empty;
int in;
int out;


/* static functions */


void server_receive(struct server *sv);
struct data_object* isItInCache(struct server* sv, char* name, unsigned int hash_val);
unsigned int getHash(const char* source);
int deleteFromCache(struct server* sv, char* name);
void addToCache(struct server *sv, struct file_data *data, unsigned int hash_val);
void updateLRU(struct server *sv, char *file_name);
void delete_LRU_from_queue(struct server* sv);

/* initialize file data */
static struct file_data *
file_data_init(void) {
    struct file_data *data;

    data = Malloc(sizeof (struct file_data));
    data->file_name = NULL;
    data->file_buf = NULL;
    data->file_size = 0;
    return data;
}

/* free all file data */
static void
file_data_free(struct file_data *data) {
    free(data->file_name);
    free(data->file_buf);
    free(data);
}


//consumer calls this function

static void
do_server_request(struct server *sv, int connfd) {

    int ret;
    struct request *rq = NULL;
    struct file_data *data = NULL;


    data = file_data_init();

    rq = request_init(connfd, data);


    if (!rq) {
                file_data_free(data);
        return;
    }

    if (NUMBER_OF_FILES == 0) {
        ret = request_readfile(rq);
        if (ret == 0) {
            request_destroy(rq);
            return;
        }
        request_sendfile(rq);
        request_destroy(rq);
        return;

    }

    //if reached here, it means the requested file was not in the cache
    struct data_object *data_in_cache;
    unsigned int hash_val;


    /* read file, 
     * fills data->file_buf with the file contents,
     * data->file_size with file size. */



    hash_val = getHash(data->file_name);
    data_in_cache = isItInCache(sv, data->file_name, hash_val);
    if (data_in_cache != NULL) {

        data_in_cache->requested = 1;

        request_set_data(rq, data_in_cache->data_in_cache);
        request_sendfile(rq);

        data_in_cache->requested = 0;

        
        pthread_mutex_lock(&llock);
        updateLRU(sv, data->file_name);
        pthread_mutex_unlock(&llock);

        request_destroy(rq);
        return;
    }




    ret = request_readfile(rq);
    if (ret == 0) {
        request_destroy(rq);
        return;
    }
    request_sendfile(rq);

    //check if the new file can be added to the cache without evicting

    pthread_mutex_lock(&llock);
    if (data->file_size <= (sv->max_cache_size - sv->cache_in_use)) {
        //can be added
        if (isItInCache(sv, data->file_name, hash_val) == NULL) {
            addToCache(sv, data, hash_val);
        } else {
            updateLRU(sv, data->file_name);
        }
    } else {

        //cannot be added
        //might need to evict least recently used file
        if (isItInCache(sv, data->file_name, hash_val) == NULL) {
            if (data->file_size <= sv->max_cache_size) {
                while (sv->cache_queue_head != NULL && data->file_size > sv->max_cache_size - sv->cache_in_use) {
                    struct data_object *ptr = isItInCache(sv, sv->cache_queue_head->data_in_queue->file_name, sv->cache_queue_head->hash_val);
                    //now can be added after evicting one
                    //remove it from the cache
                    if(ptr->requested == 0){
                        deleteFromCache(sv, sv->cache_queue_head->data_in_queue->file_name);
                        //succeed in deletion
                        delete_LRU_from_queue(sv);
                    }
                }
                addToCache(sv, data, hash_val);
            }
        } else {
            updateLRU(sv, data->file_name);
        }
    }
    
    
    pthread_mutex_unlock(&llock);
    request_destroy(rq);

    return;
}

/* entry point functions */

struct server *
server_init(int nr_threads, int max_requests, int max_cache_size) {

    in = 0;
    out = 0;
    struct server *sv;

    sv = Malloc(sizeof (struct server));
    sv->nr_threads = nr_threads;
    sv->max_requests = max_requests + 1;
    sv->max_cache_size = max_cache_size;
    sv->cache_in_use = 0;
    sv->exiting = 0;
    sv->cache_queue_head = NULL;
    sv->cache_queue_tail = NULL;




    if (nr_threads > 0 || max_requests > 0 || max_cache_size > 0) {
        NUMBER_OF_FILES = (int) floor(sv->max_cache_size / AVERAGE_FILE_SIZE);
        sv->worker_threads = (pthread_t*) malloc(sizeof (pthread_t) * nr_threads);
        pthread_mutex_init(&lock, NULL);
        pthread_mutex_init(&llock, NULL);
        pthread_cond_init(&full, NULL);
        pthread_cond_init(&empty, NULL);
    }


    /* Lab 4: create queue of max_request size when max_requests > 0 */
    if (max_requests > 0) {
        sv->request_queue = (int*) malloc(sizeof (int) * sv->max_requests);
    }

    /* Lab 5: init server cache and limit its size to max_cache_size */

    if (sv->max_cache_size > 0) {
        sv->cache = (struct data_object**) malloc(sizeof (struct data_object*) * (NUMBER_OF_FILES + 1));
    }

    /* Lab 4: create worker threads when nr_threads > 0 */

    pthread_mutex_lock(&lock);

    if (nr_threads > 0) {

        for (unsigned i = 0; i < nr_threads; ++i) {
            pthread_create(&((sv->worker_threads)[i]), NULL, (void *) server_receive, (void *) sv);
        }

    }
    pthread_mutex_unlock(&lock);
    return sv;
}

//producer

void
server_request(struct server *sv, int connfd) {

    pthread_mutex_lock(&lock);
    if (sv->nr_threads == 0) { /* no worker threads */
        pthread_mutex_unlock(&lock);
        do_server_request(sv, connfd);
    } else {
        /*  Save the relevant info in a buffer and have one of the
         *  worker threads do the work. */

        while ((in - out + sv->max_requests) % sv->max_requests == sv->max_requests - 1) {
            pthread_cond_wait(&full, &lock);
        }

        //save port number
        sv->request_queue[in] = connfd;
        in = (in + 1) % sv->max_requests;
        pthread_cond_signal(&empty);
        pthread_mutex_unlock(&lock);
    }

    return;

}

void
server_exit(struct server *sv) {
    /* when using one or more worker threads, use sv->exiting to indicate to
     * these threads that the server is exiting. make sure to call
     * pthread_join in this function so that the main server thread waits
     * for all the worker threads to exit before exiting. */

    pthread_mutex_lock(&lock);

    sv->exiting = 1;

    pthread_cond_broadcast(&full);
    pthread_cond_broadcast(&empty);

    pthread_mutex_unlock(&lock);

    for (unsigned i = 0; i < sv->nr_threads; ++i) {
        pthread_join(sv->worker_threads[i], NULL);
    }

    

    pthread_cond_destroy(&full);
    pthread_cond_destroy(&empty);
    free(sv->request_queue);
    
    struct data_object *ptr;
    struct queue_object *temp = sv->cache_queue_head;
    while(temp != NULL){
        ptr = isItInCache(sv, temp->data_in_queue->file_name, getHash(temp->data_in_queue->file_name));
        temp = temp->next;
        file_data_free(ptr->data_in_cache);
        free(ptr);
    }
    
    while (sv->cache_queue_head != NULL) {
        temp = sv->cache_queue_head;
        sv->cache_queue_head = sv->cache_queue_head->next;
        free(temp);
    }
    
    if (NUMBER_OF_FILES != 0) {
        free(sv->cache);
    }
    free(sv->worker_threads);


    /* make sure to free any allocated resources */
    free(sv);

    return;
}




//consumer

void server_receive(struct server *sv) {


    int connfd;
    while (1) {
        pthread_mutex_lock(&lock);

        while (in == out) {

            if (sv->exiting == 1) {

                pthread_mutex_unlock(&lock);
                return;
            }
            pthread_cond_wait(&empty, &lock);
        }


        connfd = sv->request_queue[out];
        out = (out + 1) % sv->max_requests;
        pthread_cond_signal(&full);

        pthread_mutex_unlock(&lock);
        do_server_request(sv, connfd);
    }

    return;

}


//hash function citation
//���Simple string hashing algorithm implementation,��� Code Review Stack Exchange. [Online]. Available: https://codereview.stackexchange.com/questions/85556/simple-string-hashing-algorithm-implementation. [Accessed: 24-Sep-2018].

unsigned int getHash(const char* source) {

    if (NUMBER_OF_FILES == 0) {
        //cache cant store a file
        return 0;
    }

    if (source == NULL) {
        return 0;
    }

    unsigned int hash = 0;
    while (*source != '\0') {
        char c = *source++;
        int a = c - '0';
        hash = (hash * 10) + a;
    }
    return hash % (NUMBER_OF_FILES);
}

struct data_object* isItInCache(struct server* sv, char* name, unsigned int hash_val) {

    if (sv->cache[hash_val] != NULL) {

        struct data_object* temp = sv->cache[hash_val];

        while (temp != NULL) {

            if (strcmp(temp->data_in_cache->file_name, name) == 0) {
                return temp;
            }
            temp = temp->next;
        }
    }

    //if it does not exist
    return NULL;

}

int deleteFromCache(struct server* sv, char* name) {

    int size;
    unsigned int hash_val = getHash(name);
    if (sv->cache[hash_val] != NULL) {

        struct data_object* temp = sv->cache[hash_val];
        struct data_object* follower = NULL;

        while (temp != NULL) {
            if (strcmp(temp->data_in_cache->file_name, name) == 0) {
                if (follower == NULL) {
                    follower = temp;
                    size = sv->cache[hash_val]->data_in_cache->file_size;
                    sv->cache[hash_val] = sv->cache[hash_val]->next;
                    follower->next = NULL;
//                    file_data_free(follower->data_in_cache);
//                    free(follower);
                    sv->cache_in_use = sv->cache_in_use - size;

                    return 1;
                } else { //follower is not NULL

                    follower->next = temp ->next;
                    temp->next = NULL;
                    size = temp->data_in_cache->file_size;
//                    file_data_free(temp->data_in_cache);
//                    free(temp);
                    sv->cache_in_use = sv->cache_in_use - size;
                    return 1;
                }
            }
            follower = temp;
            temp = temp->next;
        }
    }

    //failure
    return 0;
}

void addToCache(struct server *sv, struct file_data *data, unsigned int hash_val) {


    struct data_object *object = (struct data_object*) malloc(sizeof (struct data_object));
    object->data_in_cache = data;
    object->requested = 0;
    object->next = NULL;

    struct queue_object *queue_object = (struct queue_object*) malloc(sizeof (struct queue_object));
    queue_object->data_in_queue = data;
    queue_object->hash_val = hash_val;
    queue_object->next = NULL;

    if (sv->cache_queue_head == NULL) {
        sv->cache_queue_head = queue_object;
        sv->cache_queue_tail = queue_object;
    } else {
        sv->cache_queue_tail->next = queue_object;
        sv->cache_queue_tail = sv->cache_queue_tail->next;
    }


    if (sv->cache[hash_val] == NULL) {

        sv->cache[hash_val] = object;

        sv->cache_in_use = sv->cache_in_use + data->file_size;

        return;
    } else {

        struct data_object *ptr = sv->cache[hash_val];
        while (ptr->next != NULL) {
            ptr = ptr->next;
        }

        sv->cache_in_use = sv->cache_in_use + data->file_size;
        ptr->next = object;
        return;
    }

}

//if data is reachable from cache, it should be reachable from the queue as well

void updateLRU(struct server *sv, char *file_name) {

    struct queue_object *ptr = sv->cache_queue_head;
    struct queue_object *follower = NULL;

    while (ptr != NULL) {
        if(strcmp(ptr->data_in_queue->file_name, file_name) != 0) break;
        follower = ptr;
        ptr = ptr->next;

    }
    if(ptr == NULL)
        return;

    if (follower == NULL) {
        //head is used

        if (ptr->next == NULL) {
            //there is only one object in the queue -> nothing to change
            return;
        }

        sv->cache_queue_head = sv->cache_queue_head->next;
        sv->cache_queue_tail->next = ptr;
        ptr->next = NULL;
        sv->cache_queue_tail = sv->cache_queue_tail->next;
        return;

    } else {
        //in the middle
        //head doesnt need to change

        if (ptr->next == NULL) {
            //nothing to update
            return;
        }

        follower->next = ptr->next;
        ptr->next = NULL;
        sv->cache_queue_tail->next = ptr;
        sv->cache_queue_tail = sv->cache_queue_tail->next;
        ptr->next = NULL;
        return;
    }



}

void delete_LRU_from_queue(struct server* sv) {

    struct queue_object *ptr = sv->cache_queue_head;

    sv->cache_queue_head = sv->cache_queue_head->next;

    free(ptr);

    return;

}