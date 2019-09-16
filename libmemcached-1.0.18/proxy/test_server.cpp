#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>


enum status{conn_wait,conn_write,conn_read,conn_close};

int main()
{
    int listenfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    
    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr)); 
    serv_addr.sin_family = AF_INET; 
    serv_addr.sin_addr.s_addr = inet_addr("192.168.184.199");
    serv_addr.sin_port = htons(1234);  //any avilable server
	
	
    bind(listenfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
	
    listen(listenfd, 20);

    struct sockaddr_in clnt_addr;
    socklen_t clnt_addr_size = sizeof(clnt_addr);
    int connfd = accept(listenfd, (struct sockaddr*)&clnt_addr, &clnt_addr_size);


//1,normal
	//char buf[5][20]={"Hello World!0","Hello World!1","Hello World!2","Hello World!3","Hello World!4"};
    //char str[40] = "Hello World!";
    //write(connfd, str, sizeof(str));
	//int i=0;
/*
	while(i<5)
	{
		send(connfd,buf[i],sizeof(buf[i]),0);		
		i++;
	}	
*/


//2,send&receive together
/*
	char send_buf[20];
	char buf[20];
	while(i<20)
	{
		//send message
		sprintf(send_buf,"%d%d%d%d%d%d",i,i,i,i,i,i);
		send(connfd,send_buf,sizeof(send_buf),0);
		printf("SEND %s\n",send_buf);
		memset(send_buf,0,sizeof(send_buf));
		i++;
		
		//receive message
		recv(connfd,buf,sizeof(buf),0);
		printf("GET %s\n",buf);
	}

*/


//3,commuication
	// char send_buf[20];
	// char receive_buf[20];
	// while(1)
	// {
	// 	//send message
	// 	scanf("%s",send_buf);
	// 	send(connfd,send_buf,sizeof(send_buf),0);
	// 	printf("SEND %s\n",send_buf);

	// 	//receive message
	// 	recv(connfd,receive_buf,sizeof(receive_buf),0);
	// 	printf("GET %s\n",receive_buf);
	// 	if(strcmp(receive_buf,"STOP")==0)
	// 		break;		
	// }


//4 Finite State Machine
	char send_buf[20];
	char receive_buf[20];

	enum  status status_now=conn_wait;

	while(status_now!=conn_close)
	{
		switch(status_now)
		{
			case conn_wait:
							{
								//turn to conn_read;
								status_now=conn_read;
								//sleep(1);
								break;
							}
			case conn_read:
							{
								//receive message
								recv(connfd,receive_buf,sizeof(receive_buf),0);
								if(receive_buf)
								{
									printf("[GET]<= %s\n",receive_buf);
									if(strcmp(receive_buf,"QUIT")==0)
									{
										//turn to close
										status_now=conn_close;
									}
									//turn to conn_write
									status_now=conn_write;
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

								//getchar();

								scanf("%[^\n]",send_buf);
								getchar();

								//printf("s=%s\n",send_buf);
								send(connfd,send_buf,sizeof(send_buf),0);
								printf("[SEND]=>%s\n",send_buf);

								//turn to conn_read
								status_now=conn_read;
								memset(send_buf,0,20);
								
								break;
							}
			case conn_close:
							{
								printf("\n\nconn_close\n");
								break;
							}
		}	
	}


	printf("OVER\n\n");

    close(connfd);
    close(listenfd);
    return 0;
}
