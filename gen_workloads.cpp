#include <stdio.h>
#include <stdlib.h>
//#include <openssl/sha.h>

#include <string.h>
#include <stdint.h>
#include <time.h>

#define LENGTH 256

static int inc_v = 1;

void creat_str(char *s, int length)
{
    srand(inc_v);
    for(int i = 0; i < length; i++)
    {
        //33-122,'!'->'z'
        *(s + i) = rand() % 89 + 33;

        if(*(s + i) == '\'' || *(s + i) == '\\')
            *(s + i) = '1';
    }
    s[length] = '\0';
    inc_v++;
}


int main()
{
    FILE *fin_load, *fin_run, *fout_set, *fout_test;
    if(!(fin_load = fopen("ycsb_load.load", "r")))
    {
        printf("The source file *.load not exist.\n");
        exit(-1);
    }

    if(!(fin_run = fopen("ycsb_run.run", "r")))
    {
        printf("The source file *.run not exist.\n");
        exit(-1);
    }

    if(!(fout_set = fopen("ycsb_set.txt", "w+")))
    {
        printf("The ycsb_set.txt create failed.\n");
        exit(-1);
    }

    if(!(fout_test = fopen("ycsb_test.txt", "w+")))
    {
        printf("The ycsb_test.txt create failed.\n");
        exit(-1);
    }

    //read by line
    char tmp[10240];

    unsigned int insert_load = 0, insert_run = 0, read_run = 0;
    unsigned int op_all_load = 0, op_all_run = 0;


    while(fgets(tmp, 10240, fin_load) && (!feof(fin_load)))
    {
        char key[250] = {0};
        char value[LENGTH + 1] = {0};

        if(sscanf(tmp, "INSERT table %s [ field0=%[^\n]", key, value))
        {
            insert_load++;
            //int len=strlen(value);
            creat_str(value, LENGTH);
            //value[LENGTH]=0;
            //printf("key=%s\nvalue=%s\n\n",key,value);
            fprintf(fout_set, "INSERT\t%s\t%s\n", key, value);
        }
        else if(sscanf(tmp, "\"operationcount\"=\"%u\"", &op_all_load))
        {
            fprintf(fout_set, "Operationcount=%u\n\n", op_all_load);
        }
        else if(sscanf(tmp, "INSERT table %s [ field0=%[^\n]", key, value))
        {
            insert_load++;
            //int len=strlen(value);
            creat_str(value, LENGTH);
            //value[LENGTH]=0;
            //printf("key=%s\nvalue=%s\n\n",key,value);
            fprintf(fout_set, "UPDATE\t%s\t%s\n", key, value);
        }
        else
            ;
    }

    fprintf(fout_set, "\n\nLOAD_INSERT=%u", insert_load);


    while(fgets(tmp, 10240, fin_run) && (!feof(fin_run)))
    {
        char key[250] = {0};
        //char value[1000]={0};
        char value[LENGTH + 1] = {0};

        if(sscanf(tmp, "INSERT table %s [ field0=%[^\n]", key, value))
        {
            insert_run++;
            //value[0]='{';
            //int len=strlen(value);
            //value[len-2]='}';
            //value[len-1]=0;

            //memcpy(value+1,v,LENGTH-2);
            //value[LENGTH-1]='}';
            value[LENGTH] = 0;
            fprintf(fout_test, "INSERT\t%s\t%s\n", key, value);
        }
        else if(sscanf(tmp, "READ table %s [", key))
        {
            read_run++;

            fprintf(fout_test, "READ\t%s\n", key);
        }
        else if(sscanf(tmp, "\"operationcount\"=\"%u\"", &op_all_run))
        {
            fprintf(fout_test, "Operationcount=%u\n\n", op_all_run);
        }
        else if(sscanf(tmp, "UPDATE table %s [ field0=%[^\n]", key, value))
        {
            insert_run++;
            //value[0]='{';
            //int len=strlen(value);
            //value[len-2]='}';
            //value[len-1]=0;

            //memcpy(value+1,v,LENGTH-2);
            //value[LENGTH-1]='}';
            value[LENGTH] = 0;
            fprintf(fout_test, "UPDATE\t%s\t%s\n", key, value);
        }
        else
            ;
    }

    //free(v);
    fprintf(fout_test, "\n\nRUN_INSERT=%u\nRUN_READ=%u", insert_run, read_run);

    fclose(fin_load);
    fclose(fin_run);
    fclose(fout_set);
    fclose(fout_test);

    return 0;
}