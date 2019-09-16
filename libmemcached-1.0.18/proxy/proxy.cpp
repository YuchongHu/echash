
//test
//always read

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
//#include <pthread.h>
#include <signal.h>
#include <sys/stat.h>
#include <libmemcached/memcached.h>

//24        27=>add 3
#define NODE 24
#define NUM  10000
#define RING_SIZE 5

// //0=>no.1=>during the backup
int backup_flag=0;

enum status{
			conn_wait,
			conn_write,
			conn_read,
			conn_close,
			conn_dead,
			conn_backup
		};

struct MRmemcached_st* MRmemc;
int listenfd,connfd;
int listenfd_b,connfd_b;


void transport_file()
{
    //4 Finite State Machine
	//char send_buf[1024]={0};

	char buffer[1024]={0};
    system("tar -zcvf /home/node/libmemcached-1.0.18/backup.tar.gz ~/libmemcached-1.0.18/config/");

    //char file_name[256]={0};
    //the path is stable
    FILE *fin=fopen("/home/node/libmemcached-1.0.18/backup.tar.gz","r");
    if(fin)
    {
        int length=0;
        while((length = fread(buffer, sizeof(char), 1024, fin)) > 0)   
        {
        	printf("NULL,length=%d\n",length); 
            if(send(connfd_b, buffer, length, 0) < 0)   
            {   
                printf("Send File: Failed\n");   
                break;   
            }

            bzero(buffer, 1024);   
        }   
    }
    else
    {
        printf("Not found.\n");
    }

    fclose(fin);
    system("rm -f /home/node/libmemcached-1.0.18/backup.tar.gz");
}


void backup(int sig)
{
    if(SIGALRM == sig)
    {
        printf("Invoke backup_metadata().\n");
        MRmemcached_backup(MRmemc);
		//sleep(30);

		// printf("Invoke transport_file().\n");
		transport_file();

		//finish
		backup_flag=0;
        alarm(50);    //we contimue set the timer
    }

    return ;
}


int main()
{
	memcached_return_t rc;

	int k,i=0;

	rc=MRmemcached_init(&MRmemc);

	//create the backup dir
	if(access("/home/node/libmemcached-1.0.18/config/",F_OK)!=1)
	{
		mkdir("/home/node/libmemcached-1.0.18/config/", 0777);
	}

	for(k=0;k<NODE;k++)
	{
		MRmemcached_init_addserver(MRmemc,"127.0.0.1", 11211+k, k%RING_SIZE);
	}


	unsigned int origin=MRmemc->total_num_server;
	//printf("server_instance=%u\n\n",MRmemc->total_num_server);

    listenfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = inet_addr("192.168.184.199");
    serv_addr.sin_port = htons(1234);
	
    bind(listenfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
	
    listen(listenfd, 20);

    struct sockaddr_in clnt_addr;
    socklen_t clnt_addr_size = sizeof(clnt_addr);
    connfd = accept(listenfd, (struct sockaddr*)&clnt_addr, &clnt_addr_size);


    //conect to Proxy_B

    listenfd_b = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

    struct sockaddr_in serv_addr_b;
    memset(&serv_addr_b, 0, sizeof(serv_addr_b));
    serv_addr_b.sin_family = AF_INET;
    serv_addr_b.sin_addr.s_addr = inet_addr("192.168.184.199");
    serv_addr_b.sin_port = htons(1235);
	
    bind(listenfd_b, (struct sockaddr*)&serv_addr_b, sizeof(serv_addr_b));
	
    listen(listenfd_b, 20);

    struct sockaddr_in clnt_addr_b;
    socklen_t clnt_addr_size_b = sizeof(clnt_addr_b);
    connfd_b = accept(listenfd_b, (struct sockaddr*)&clnt_addr_b, &clnt_addr_size_b);


    signal(SIGALRM, backup); //relate the signal and function
    alarm(1);    //trigger the timer


    //4 Finite State Machine
	char send_buf[1024]={0};
	char receive_buf[1024]={0};//key+value

	enum  status status_now=conn_wait;
    int get_once=0;
	int dget=0;
	int dget_success=0;
	//occur the >2 server failed
	int dget_failed=0;
	//not the failed making the dget unsuccess,just the key is in the waitting list ,waitting for encode.
	int dget_not_encode=0;

	enum status status_before;

	while(status_now!=conn_dead)
	{
		if(backup_flag==1)
		{
			//save the normal status
			status_before=status_now;
			status_now=conn_backup;

			memcpy(send_buf,"BACKUP_START",12);
			send(connfd,send_buf,sizeof(send_buf),0);
			printf("[SEND]=> %s\n",send_buf);
			memset(send_buf,0,1024);
		}
		switch(status_now)
		{
			case conn_wait:
							{
								//turn to conn_read;
								status_now=conn_read;
								memset(receive_buf,0,1024);
								//sleep(1);
								break;
							}
			case conn_backup:
							{
								//wait the backup==0
								while(backup_flag==1)
								{
									sleep(1);
									printf("During backup...\n");
								}
								memcpy(send_buf,"BACKUP_OVER",11);
								send(connfd,send_buf,sizeof(send_buf),0);
								printf("[SEND]=> %s\n",send_buf);
								memset(send_buf,0,1024);

								status_now=status_before;
								//begin_backup=clock();
								//sleep(1);
								break;
							}
			case conn_read:
							{
								//receive message
								recv(connfd,receive_buf,sizeof(receive_buf),0);
								if(receive_buf)
								{
									printf("[RECV]<= %s\n",receive_buf);

									//judge the op,set or get
									char op[5]={0};
									sscanf(receive_buf,"%s",op);

									//set,=>wait
									if(strcmp(op,"SET")==0)
									{
										char key[256]={0};
										char value[1024]={0};
										//SET key value
										sscanf(receive_buf,"SET %s %[^\n]",key,value);
									
										rc=MRmemcached_set(MRmemc,key, strlen(key),value, strlen(value),0,0);
										if(rc==MEMCACHED_SUCCESS)
										{
											printf("\nSET_OP success:[%s]==>{%s}\n",key,value);
											memcpy(send_buf,"SET OK",6);
										}
										else
										{
											printf("\nSET_OP failed:[%s]==>{%s}\n",key,value);
											memcpy(send_buf,"SET NOT OK",10);
										}
										status_now=conn_write;	
									}
									else if(strcmp(op,"GET")==0)//get,=>write
									{
										size_t value_length;
										uint32_t  flags;
										char key[256]={0};
										sscanf(receive_buf,"GET %s",key);

										char *value= MRmemcached_get(MRmemc,key, strlen(key),&value_length,&flags,&rc,&dget);

										//turn to conn_write
										status_now=conn_write;

										//fill the send_buf
										//memcpy(send_buf,value,value_length);

										if(dget==1)
										{
											dget_success++;
											//printf("\nDGET:[%s] ==>{%s}.\n",key,value);
											memcpy(send_buf,value,value_length);
										}
										else if(dget==-1)
										{
											dget_failed++;
											//printf("Dget failed.\n");
											memcpy(send_buf,"DGET failed",11);
										}
										else if(dget==0)
										{
											get_once++;
											//printf("\nGET:[%s] ==>{%s}.\n",key,value);
											memcpy(send_buf,value,value_length);
										}
										else //-2
										{
											dget_not_encode++;
											//printf("Dget KV not encode.\n");
											memcpy(send_buf,"DGET failed",11);
										}

									}
									else if(strcmp(op,"QUIT")==0)
									{
										//turn to conn_close
										status_now=conn_close;
									}
									else //other issue
										;

								}
								else
								{
									//turn to conn_wait
									status_now=conn_wait;
								}
								break;
							}
			case conn_write:
							{
								//send message
								//scanf("%s",send_buf);
								send(connfd,send_buf,sizeof(send_buf),0);
								printf("[SEND]=> %s\n",send_buf);
								memset(send_buf,0,1024);

								//turn to conn_read
								status_now=conn_wait;
								break;
							}
			case conn_close:
							{
								printf("\n\nconn_close\n");
								close(connfd);
								status_now=conn_dead;
								break;
							}
		}
	}

	MRmemcached_destroy(MRmemc);
	close(listenfd);

	return 0;

}