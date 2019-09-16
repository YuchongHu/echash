#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/types.h>
#include <pthread.h>
#include <errno.h>

#include <libmemcached/memcached.h>

pthread_mutex_t printmutex;

//gcc -std=c++11 scale.cpp -o scale -lmemcached -lpthread

#define NTHREADS 1

static uint32_t op_load = 0, op_test = 0;

enum operation { op_set = 0, op_get, op_del };
enum scale_op {none = 0, out, in, repair};

typedef struct query
{
    enum operation op;
    char *key;
    char *value;
} query;


query **queries;

uint32_t dget_mysql = 0;
double time_dget_mysql = 0.0;

typedef struct
{
    uint32_t tid;

    enum scale_op sop;
    uint32_t num;
    uint32_t num_ops;

    uint32_t num_sets;
    double time_sets;

    uint32_t num_gets;
    double time_gets;

    uint32_t num_miss;
    uint32_t num_hits;

    double throughput;
    double throughput_sets;
    double throughput_gets;

    double time;
} thread_param;

typedef struct result_t
{
    double total_throughput;
    double total_throughput_sets;
    double total_throughput_gets;

    double total_time;

    uint32_t total_hits;
    uint32_t total_miss;
    uint32_t total_gets;
    uint32_t total_sets;
    uint32_t num_threads;
} result_t;


static double timeval_diff(struct timeval *start,
                           struct timeval *end)
{
    double r = end->tv_sec - start->tv_sec;
    if (end->tv_usec > start->tv_usec)
        r += (end->tv_usec - start->tv_usec) / 1000000.0;
    else if (end->tv_usec < start->tv_usec)
        r -= (start->tv_usec - end->tv_usec) / 1000000.0;
    return r;
}


static uint32_t ECH_set(struct ECHash_st *ECH, char *key, char *val)
{
    memcached_return_t rc;

    rc = ECHash_set(ECH, key, strlen(key), val, strlen(val), (time_t) 0, (uint32_t) 0);

    if (rc != MEMCACHED_SUCCESS)
    {
        return 1;
    }
    return 0;
}


static char *ECH_get(struct ECHash_st *ECH, char *key)
{
    memcached_return_t rc = MEMCACHED_FAILURE;
    char *val;
    size_t len;
    uint32_t flag;
    val = ECHash_get(ECH, key, strlen(key), &len, &flag, &rc);

    return val;
}

static void queries_init()
{
    FILE *fin_para;
    if(!(fin_para = fopen("para.txt", "r")))
    {
        printf("The para.txt open failed.\n");
        exit(-1);
    }
    char path[100]={0};
    fscanf(fin_para,"Workloads Path=%s", path);

    char path1[100], path2[100];
    sprintf(path1, "%s/ycsb_set.txt", path);
    sprintf(path2, "%s/ycsb_test.txt", path);

    FILE *fin_set, *fin_test;
    if(!(fin_set = fopen(path1, "r")))
    {
        printf("The ycsb_set.txt open failed.\n");
        exit(-1);
    }

    if(!(fin_test = fopen(path2, "r")))
    {
        printf("The ycsb_test.txt open failed.\n");
        exit(-1);
    }

    uint32_t load_set = 0, load_test = 0, get_test = 0;

    fscanf(fin_set, "Operationcount=%u", &op_load);
    fscanf(fin_test, "Operationcount=%u", &op_test);

    queries = (struct query **)calloc(op_load + op_test, sizeof(struct query *));

    for(uint32_t i = 0; i < op_load; i++)
    {
        queries[i] = (struct query *)malloc(sizeof(struct query));
    }
    for(uint32_t i = op_load; i < op_load + op_test; i++)
    {
        queries[i] = (struct query *)malloc(sizeof(struct query));
    }
    char tmp[10240];
    uint32_t count = 0;
    fseek(fin_set, 0, SEEK_SET);

    while((!feof(fin_set)) && fgets(tmp, 10240, fin_set))
    {

        char key[250] = {0};
        char value[10240] = {0};
        if(sscanf(tmp, "INSERT %s %[^\n]", key, value))
        {
            queries[count]->op = op_set;
            queries[count]->key = (char *)malloc((strlen(key) + 1) * sizeof(char));
            queries[count]->value = (char *)malloc((strlen(value) + 1) * sizeof(char));

            memcpy(queries[count]->key, key, strlen(key) + 1);
            memcpy(queries[count]->value, value, strlen(value) + 1);
            count++;
        }
        else if(sscanf(tmp, "LOAD_INSERT=%u", &load_set))
        {
            ;
        }
        else
            ;
    }


    fseek(fin_test, 0, SEEK_SET);
    while((!feof(fin_test)) && fgets(tmp, 10240, fin_test))
    {
        char key[250] = {0};
        char value[10240] = {0};
        if(sscanf(tmp, "INSERT %s %[^\n]", key, value))
        {
            queries[count]->op = op_set;
            queries[count]->key = (char *)malloc((strlen(key) + 1) * sizeof(char));
            queries[count]->value = (char *)malloc((strlen(value) + 1) * sizeof(char));

            memcpy(queries[count]->key, key, strlen(key) + 1);
            memcpy(queries[count]->value, value, strlen(value) + 1);
            count++;


        }
        else if(sscanf(tmp, "READ %s", key))
        {
            queries[count]->op = op_get;
            queries[count]->key = (char *)malloc((strlen(key) + 1) * sizeof(char));
            queries[count]->value = 0;

            memcpy(queries[count]->key, key, strlen(key) + 1);
            count++;
        }
        else if(sscanf(tmp, "RUN_INSERT=%u", &load_test))
            ;
        else if(sscanf(tmp, "RUN_READ=%u", &get_test))
            ;
        else
            ;
    }

    fclose(fin_set);
    fclose(fin_test);

    fprintf(stderr, "\nqueries_init...done\n\n");
}


static void *queries_exec(void *param)
{
    struct ECHash_st *ECH;
    ECHash_init(&ECH);

    FILE *fin_config;
    char conf[1024];
    if(!(fin_config = fopen("config.txt", "r")))
    {
        printf("The config file not exist.\n");
        exit(-1);
    }
    while(fgets(conf, 1024, fin_config) && (!feof(fin_config)))
    {
        char ip[50]={0};
        uint32_t port=0;
        uint32_t ring=0;

        sscanf(conf,"%s %u %u", ip, &port, &ring);
        if(strncmp(ip,"#Scale",6)==0)
            break;
        
        if(ip[0]=='#' || ip[0]==0)
            continue;

        //fprintf(stderr,"%s, %u ,%u\n",ip, port,ring);

        ECHash_init_addserver(ECH, ip, port,ring);
    }
    fclose(fin_config);

    struct timeval tv_s, tv_e, tv_ts, tv_te;
    thread_param *p = (thread_param *) param;
    pthread_mutex_lock (&printmutex);
    //fprintf(stderr, "start benching using thread%"PRIu64"\n", p->tid);
    pthread_mutex_unlock (&printmutex);

    p->time = p->time_sets = p->time_gets = 0;

    for (uint32_t i = 0 ; i < op_load; i++)
    {
        enum operation op = queries[i]->op;
        char *key = queries[i]->key;

        if (op == op_set)
        {
            gettimeofday(&tv_ts, NULL);
            ECH_set(ECH, key, queries[i]->value);
            p->num_sets++;
            gettimeofday(&tv_te, NULL);
            p->time_sets += timeval_diff(&tv_ts, &tv_te);
        }
    }

    //test scale out/in S
    fin_config = fopen("config.txt", "r");
    if(p->sop == out)
    {
        //add servers
        double time_add = 0;
        struct timeval begin, end;

        while(fgets(conf, 1024, fin_config) && (!feof(fin_config)))
        {
            char ip[50]={0};
            uint32_t port=0;
            uint32_t ring=0;

            sscanf(conf,"%s %u %u", ip, &port, &ring);
            if(strncmp(ip,"#Scale_Out",10)!=0)
                continue;
        
            uint32_t tmp=0;
            while(tmp < p->num && fgets(conf, 1024, fin_config) && (!feof(fin_config)))
            {
                int n=sscanf(conf,"%s %u %u", ip, &port, &ring);
                if(n!=3)
                    break;
                tmp++;
                //fprintf(stderr,"%s, %u ,%u n=%d\n",ip, port,ring);

                gettimeofday(&begin, NULL);
                ECHash_addserver(ECH, ip, port, ring);
                gettimeofday(&end, NULL);
                time_add = timeval_diff(&begin, &end);
                fprintf(stderr, "[%u]. ADD SERVER time=%f s.\n\n", tmp, time_add);
            }
            fprintf(stderr, "\nAll scale out %u nodes\n",tmp>p->num?p->num:tmp);
            break;
        }
        fclose(fin_config);
    }
    else if(p->sop == in)
    {
        double time_remove = 0;
        struct timeval begin, end;

        while(fgets(conf, 1024, fin_config) && (!feof(fin_config)))
        {
            char ip[50]={0};
            uint32_t port=0;
            uint32_t ring=0;

            sscanf(conf,"%s %u %u", ip, &port, &ring);
            if(strncmp(ip,"#Scale_In",9)!=0)
                continue;
        
            uint32_t tmp=0;
            while(tmp < p->num && fgets(conf, 1024, fin_config) && (!feof(fin_config)))
            {
                int n=sscanf(conf,"%s %u %u", ip, &port, &ring);
                if(n!=3)
                    break;
                tmp++;
                //fprintf(stderr,"%s, %u ,%u\n",ip, port,ring);

                gettimeofday(&begin, NULL);
                ECHash_remove_server(ECH, ip, port, ring);
                gettimeofday(&end, NULL);
                time_remove = timeval_diff(&begin, &end);
                fprintf(stderr, "[%u] REMOVE SERVER time=%f s.\n\n", tmp, time_remove);
            }
            fprintf(stderr, "\nAll scale in %u nodes\n",tmp>p->num?p->num:tmp);
            break;
        }
        fclose(fin_config);

    }


    uint32_t nops = p->num_gets + p->num_sets;
    p->throughput = nops / p->time;
    p->throughput_sets = p->num_sets / p->time_sets;
    p->throughput_gets = p->num_gets / p->time_gets;

    uint32_t stat[7] = {0};
    double stat2[2] = {0};

    ECHash_stat(ECH, stat, stat2);
    ECHash_destroy(ECH);

    fprintf(stderr, "queries_exec...done\n\n");
    pthread_exit(NULL);

}

int main(int argc, char *argv[])
{
    if(argc != 3)
    {
        printf("Parameter error,should be ./test out/in num\n");
        exit(-1);
    }

    queries_init();

    pthread_t threads[NTHREADS];
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);;

    pthread_mutex_init(&printmutex, NULL);

    thread_param tp[NTHREADS];

    enum  scale_op sop_tmp = none;
    uint32_t num=0;
    sscanf(argv[2], "%u", &num);
    if(strcmp(argv[1], "out") == 0)
    {
        sop_tmp = out;
    }
    else if(strcmp(argv[1], "in") == 0)
    {
        sop_tmp = in;
    }

    for (uint32_t t = 0; t < NTHREADS; t++)
    {
        //tp[t].queries = queries;
        tp[t].tid     = t;
        tp[t].sop     = sop_tmp;
        tp[t].num     = num;
        tp[t].num_ops = op_load + op_test;
        tp[t].num_sets = tp[t].num_gets = tp[t].num_miss = tp[t].num_hits = 0;
        tp[t].time = tp[t].throughput = 0.0;
        int rc = pthread_create(&threads[t], &attr, queries_exec, (void *) &tp[t]);
        if (rc)
        {
            perror("failed: pthread_create\n");
            exit(-1);
        }
    }

    result_t result;
    result.total_time = 0.0;;

    result.total_throughput = 0.0;
    result.total_throughput_sets = 0.0;
    result.total_throughput_gets = 0.0;
    result.total_hits = 0;
    result.total_miss = 0;
    result.total_gets = 0;
    result.total_sets = 0;
    result.num_threads = NTHREADS;

    for (uint32_t t = 0; t < NTHREADS; t++)
    {
        void *status;
        int rc = pthread_join(threads[t], &status);
        if (rc)
        {
            perror("error, pthread_join\n");
            exit(-1);
        }
        result.total_time = (result.total_time > tp[t].time) ? result.total_time : tp[t].time;
        result.total_throughput += tp[t].throughput;
        result.total_throughput_sets += tp[t].throughput_sets;
        result.total_throughput_gets += tp[t].throughput_gets;
        result.total_hits += tp[t].num_hits;
        result.total_miss += tp[t].num_miss;
        result.total_gets += tp[t].num_gets;
        result.total_sets += tp[t].num_sets;
    }


    pthread_attr_destroy(&attr);
    fprintf(stderr, "\nover\n\n");
    for(uint32_t i = 0; i < op_load + op_test; i++)
    {
        free(queries[i]->key);
        free(queries[i]->value);
        free(queries[i]);
    }

    free(queries);
    return 0;
}