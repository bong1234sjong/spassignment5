/*--------------------------------------------------------------------*/
/* rwlock.c                                                           */
/* Author: Junghan Yoon, KyoungSoo Park                               */
/* Modified by: (Your Name)                                           */
/*--------------------------------------------------------------------*/
#include "rwlock.h"
/*--------------------------------------------------------------------*/
/* free to add header files and global variables */
struct entry
{
    pthread_t tid;
    int is_writer;
};
/*--------------------------------------------------------------------*/
struct uctx
{
    /* free to use */
    pthread_cond_t condvar;
    int head, tail, size;
    struct entry entries[RING_SIZE];
};
/*--------------------------------------------------------------------*/
int rwlock_init(rwlock_t *rw, int delay)
{
    TRACE_PRINT();
/*--------------------------------------------------------------------*/
    /* edit here */
    if(rw->uctx != NULL || rw == NULL) {
        return -1;
    }
    if((pthread_mutex_init(&rw->lock, NULL)) < 0) {
        return -1;
    }
    rw->current_readers = 0;
    rw->current_writers = 0;
    rw->delay = delay;
    struct uctx *u = rw->uctx = malloc(sizeof(struct uctx));
    u->head = u->size = u->tail = 0;
    if((pthread_cond_init(&u->condvar, NULL)) < 0) {
        return -1;
    }

    return 0;
/*--------------------------------------------------------------------*/
}
/*--------------------------------------------------------------------*/
int rwlock_read_lock(rwlock_t *rw, int quick)
{
    TRACE_PRINT();
/*--------------------------------------------------------------------*/
    /* edit here */
    struct uctx *u = (struct uctx *)rw->uctx;
    if(quick) {
        pthread_mutex_lock(&rw->lock);
        while(rw->current_writers != 0) {
            pthread_cond_wait(&u->condvar, &rw->lock);
        }
        rw->current_readers++;
        pthread_mutex_unlock(&rw->lock);
    } else if (quick == 0) {
        pthread_mutex_lock(&rw->lock);
        u->entries[u->tail].tid = pthread_self();
        u->entries[u->tail].is_writer = 0;
        u->size++;
        u->tail = (u->tail + 1) % RING_SIZE;
        while(rw->current_writers != 0 || !pthread_equal(u->entries[u->head].tid, pthread_self())) {
            pthread_cond_wait(&u->condvar, &rw->lock);
        }
        u->size--;
        u->head = (u->head + 1) % RING_SIZE;
        rw->current_readers++;   
        pthread_cond_broadcast(&u->condvar);
        pthread_mutex_unlock(&rw->lock);
    }
    
    return 0;
/*--------------------------------------------------------------------*/
}
/*--------------------------------------------------------------------*/
int rwlock_read_unlock(rwlock_t *rw)
{
    TRACE_PRINT();
    if (!rw)
    {
        errno = EINVAL;
        return -1;
    }
    sleep(rw->delay);
/*--------------------------------------------------------------------*/
    /* edit here */
    struct uctx *u = (struct uctx *)rw->uctx;
    pthread_mutex_lock(&rw->lock);
    rw->current_readers--;
    if(rw->current_readers == 0) {
        pthread_cond_broadcast(&u->condvar);
    }
    pthread_mutex_unlock(&rw->lock);
    return 0;
/*--------------------------------------------------------------------*/
}
/*--------------------------------------------------------------------*/
int rwlock_write_lock(rwlock_t *rw)
{
    TRACE_PRINT();
/*--------------------------------------------------------------------*/
    /* edit here */
    struct uctx *u = (struct uctx *)rw->uctx;
    pthread_mutex_lock(&rw->lock);
    u->entries[u->tail].tid = pthread_self();
    u->entries[u->tail].is_writer = 1;
    u->size++;
    u->tail = (u->tail + 1) % RING_SIZE;
    while(rw->current_readers != 0 || rw->current_writers != 0 || !pthread_equal(u->entries[u->head].tid, pthread_self())) {
        pthread_cond_wait(&u->condvar, &rw->lock);
    }
    u->size--;
    u->head = (u->head + 1) % RING_SIZE;
    rw->current_writers++;   
    pthread_mutex_unlock(&rw->lock);
    return 0;
/*--------------------------------------------------------------------*/
}
/*--------------------------------------------------------------------*/
int rwlock_write_unlock(rwlock_t *rw)
{
    TRACE_PRINT();
    if (!rw)
    {
        errno = EINVAL;
        return -1;
    }
    sleep(rw->delay);
/*--------------------------------------------------------------------*/
    /* edit here */
    struct uctx *u = (struct uctx *)rw->uctx;
    pthread_mutex_lock(&rw->lock);
    rw->current_writers--;
    if(rw->current_writers == 0) {
        pthread_cond_broadcast(&u->condvar);
    }
    pthread_mutex_unlock(&rw->lock);
    return 0;
/*--------------------------------------------------------------------*/
}
/*--------------------------------------------------------------------*/
int rwlock_destroy(rwlock_t *rw)
{
    TRACE_PRINT();
/*--------------------------------------------------------------------*/
    /* edit here */
    struct uctx *u = (struct uctx *)rw->uctx;
    pthread_cond_destroy(&u->condvar);
    free(rw->uctx);
    pthread_mutex_destroy(&rw->lock);
    return 0;
/*--------------------------------------------------------------------*/
}