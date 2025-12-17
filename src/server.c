/*--------------------------------------------------------------------*/
/* server.c                                                           */
/* Author: Junghan Yoon, KyoungSoo Park                               */
/* Modified by: (Your Name)                                           */
/*--------------------------------------------------------------------*/
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <errno.h>
#include <getopt.h>
#include <signal.h>
#include <sys/time.h>
#include "common.h"
#include "skvslib.h"
/*--------------------------------------------------------------------*/
/* free to add header files and global variables */
#include "netdb.h"
/*--------------------------------------------------------------------*/
struct thread_args
{
    int listenfd;
    int idx;
    struct skvs_ctx *ctx;

    /*----------------------------------------------------------------*/
    /* free to use */

    /*----------------------------------------------------------------*/
};
/*--------------------------------------------------------------------*/
volatile static sig_atomic_t g_shutdown = 0;
/*--------------------------------------------------------------------*/
void *handle_client(void *arg)
{
    TRACE_PRINT();
    struct thread_args *args = (struct thread_args *)arg;
    struct skvs_ctx *ctx = args->ctx;
    int idx = args->idx;
    int listenfd = args->listenfd;
    /*----------------------------------------------------------------*/
    /* free to add any variables */
    int connfd, rused = 0, readb, clientclose, wb, wused;
    char rbuf[BUF_SIZE], wbuf[BUF_SIZE];
    size_t wlen, rlen;
    /*----------------------------------------------------------------*/

    free(args);
    fprintf(stdout, "%dth worker ready\n", idx);

    /*----------------------------------------------------------------*/
    /* edit here */
    while(g_shutdown != 1) {
        if ((connfd = accept(listenfd, NULL, NULL )) < 0) {
            continue;
        }
        setsockopt(connfd, SOL_SOCKET, SO_RCVTIMEO, (int *)TIMEOUT, sizeof(int));
        setsockopt(connfd, SOL_SOCKET, SO_SNDTIMEO, (int *)TIMEOUT, sizeof(int));
        wused = 0;
        while(1) {
            if ((readb = read(connfd, rbuf + rused, BUF_SIZE - rused)) <= 0) {
                clientclose = 1;
                break;
            }
            rused = rused + readb;
            if(rbuf[rused-1] == '\n') {
                if(rused == 1) {
                    clientclose = 1;
                    break;
                }
                rlen = (size_t)rused;
                rused = 0;
                skvs_serve(ctx, rbuf, rlen, wbuf, &wlen);
                while(wused < wlen) {
                    if ((wb = write(connfd, wbuf + wused, wlen - wused)) <= 0) {
                        clientclose = 1;
                        break;
                    }
                    wused = wused + wb;
                }
            }
        }
        if (clientclose) {
            close(connfd);
            clientclose = 0;
        }
    }
    close(connfd);
    //The socket should close immediately if the client feeds an empty line or EOF.

    /*----------------------------------------------------------------*/

    return NULL;
}
/*--------------------------------------------------------------------*/
/* Signal handler for SIGINT */
void handle_sigint(int sig)
{
    g_shutdown = 1;
}
/*--------------------------------------------------------------------*/
int main(int argc, char *argv[])
{
    size_t hash_size = DEFAULT_HASH_SIZE;
    char *ip = DEFAULT_ANY_IP;
    int port = DEFAULT_PORT, opt;
    int num_threads = NUM_THREADS;
    int delay = RWLOCK_DELAY;
    /*----------------------------------------------------------------*/
    /* free to declare any variables */
    struct skvs_ctx *hashtable;
    struct addrinfo hints, *listp, *p;
    int listenfd, optval = 1;
    char strport[6];
    pthread_t tid[NUM_THREADS];
    /*----------------------------------------------------------------*/

    /* parse command line options */
    while ((opt = getopt(argc, argv, "p:t:s:d:h")) != -1)
    {
        switch (opt)
        {
        case 'p':
            port = atoi(optarg);
            if (port < 1025 || port > 65535)
            {
                fprintf(stderr, "Invalid port number: %d\n", port);
                exit(EXIT_FAILURE);
            }
            break;
        case 't':
            num_threads = atoi(optarg);
            if (num_threads < 1 || num_threads > NUM_THREADS)
            {
                fprintf(stderr, "Invalid number of threads: %d\n",
                        num_threads);
                exit(EXIT_FAILURE);
            }
            break;
        case 's':
            hash_size = atoi(optarg);
            if (hash_size <= 0)
            {
                fprintf(stderr, "Invalid hash size: %zu\n", hash_size);
                exit(EXIT_FAILURE);
            }
            break;
        case 'd':
            delay = atoi(optarg);
            if (delay < 0)
            {
                fprintf(stderr, "Invalid rwlock delay: %d\n", delay);
                exit(EXIT_FAILURE);
            }
            break;
        case 'h':
        default:
            fprintf(stdout, "Usage: %s [-p port (%d)] "
                            "[-t num_threads (%d)] "
                            "[-d rwlock_delay (%d)] "
                            "[-s hash_size (%d)]\n",
                    argv[0],
                    DEFAULT_PORT,
                    NUM_THREADS,
                    RWLOCK_DELAY,
                    DEFAULT_HASH_SIZE);
            exit(EXIT_FAILURE);
        }
    }

    /*----------------------------------------------------------------*/
    /* edit here */
    if ((hashtable = skvs_init(hash_size, delay)) == NULL) {
        fprintf(stderr, "Skvs_init Failed to Allocate Hash Table");
        exit(EXIT_FAILURE);
    }
    //Create socketaddr struct
    snprintf(strport, sizeof(strport), "%d", port);
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_NUMERICHOST | AI_ADDRCONFIG | AI_NUMERICSERV;
    //REMEMBER HANDLE FAILURE
    getaddrinfo(ip, strport, &hints, &listp);
    //Walk through list and bind
    for(p = listp; p; p = p->ai_next) {
        if((listenfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) < 0 ){
            continue;
        }
        //Eliminates "Already in use"
        setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, (const void *)&optval, sizeof(int));
        //Bind descriptor to address
        if (bind(listenfd, p->ai_addr, p->ai_addrlen) == 0)
            break; /* Success */
        close(listenfd); /* Bind failed, try the next */
    }

    freeaddrinfo(listp);
    if(!p) {
        return -1;
    }

    if(listen(listenfd, NUM_BACKLOG) < 0) {
        close(listenfd);
        return -1;
    }
    signal(SIGINT, handle_sigint);
    //THREADS
    for(int i = 0; i < NUM_THREADS; i++) {
        struct thread_args *pt_arg = malloc(sizeof(struct thread_args));
        pt_arg->ctx = hashtable;
        pt_arg->listenfd = listenfd;
        pt_arg->idx = i;
        if((pthread_create(&tid[i], NULL, handle_client, pt_arg)) < 0) {
            free(pt_arg);
        }
    }
    // "Please use the SIGINT handler only to signal worker threads to exit their loops (e.g., by setting a shutdown flag).
    // The hash dump should be performed by the main thread after all worker threads have exited."
    close(listenfd);
    hash_dump(hashtable->table);
    hash_destroy(hashtable->table);

    /*----------------------------------------------------------------*/

    return 0;
}
/*--------------------------------------------------------------------*/