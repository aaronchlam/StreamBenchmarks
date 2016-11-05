#include<stdio.h> //printf
#include<string.h>    //strlen
#include<sys/socket.h>    //socket
#include<arpa/inet.h> //inet_addr
#include<netdb.h> //hostent
#include <pthread.h>

int sock;
struct sockaddr_in server;




int hostname_to_ip(char * hostname , char* ip)
{
    struct hostent *he;
    struct in_addr **addr_list;
    int i;
         
    if ( (he = gethostbyname( hostname ) ) == NULL) 
    {
        // get the host info
        herror("gethostbyname");
        return 1;
    }
 
    addr_list = (struct in_addr **) he->h_addr_list;
     
    for(i = 0; addr_list[i] != NULL; i++) 
    {
        //Return the first one;
        strcpy(ip , inet_ntoa(*addr_list[i]) );
        return 0;
    }
     
    return 1;
}

void *consume( void  ){
char server_reply[2000];
while(recv(sock , server_reply , 2000 , 0))
    {

    }
}


 
int main(int argc , char *argv[])
{
    int port;
    char* host = malloc(1000); 
    sscanf(argv[1],"%d",&port);
    host = argv[2];
    char * ip = malloc(1000);
    hostname_to_ip(host,ip);
    pthread_t t1,t2, t3;
    //Create socket
    sock = socket(AF_INET , SOCK_STREAM , 0);
    if (sock == -1)
    {
        printf("Could not create socket");
    }
    puts("Socket created");
     
    server.sin_addr.s_addr = inet_addr(ip);
    server.sin_family = AF_INET;
    server.sin_port = htons( port );
 
    if (connect(sock , (struct sockaddr *)&server , sizeof(server)) < 0)
    {
        perror("connect failed. Error");
        return 1;
    }
     
    puts("Connected\n");
    pthread_create( &t1, NULL, consume, NULL);
   // pthread_create( &t2, NULL, consume, NULL);
   // pthread_create( &t3, NULL, consume, NULL);
    
     pthread_join( t1, NULL);
    // pthread_join( t2, NULL);
     //pthread_join( t3, NULL);

   close(sock);
    return 0;
}
