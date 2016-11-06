/*
    C socket server example
*/
#define _POSIX_C_SOURCE 200809L
 
#include<stdio.h>
#include<string.h>    //strlen
#include<sys/socket.h>
#include<arpa/inet.h> //inet_addr
#include<unistd.h>    //write
#include <stdlib.h>
#include<math.h>
//#include <time.h>
#include <pthread.h>
#include <sys/time.h>
#include<semaphore.h>
#include <unistd.h>

const char *geoListAll[] = {
            "AF", "AX", "AL", "DZ", "AS", "AD", "AO", "AI", "AQ", "AG", "AR", "AM","AW", "AC", "AU", "AT", "AZ", "BS", "BH", "BB",
            "BD", "BY", "BE", "BZ", "BJ", "BM", "BT", "BW", "BO", "BA", "BV", "BR","IO", "BN", "BG", "BF", "BI", "KH", "CM", "CA", 
            "CV", "KY", "CF", "TD", "CL", "CN", "CX", "CC", "CO", "KM", "CG", "CD","CK", "CR", "CI", "HR", "CU", "CY", "CZ", "CS",
            "DK", "DJ", "DM", "DO", "TP", "EC", "EG", "SV", "GQ", "ER", "EE", "ET","EU", "FK", "FO", "FJ", "FI", "FR", "FX", "GF",
            "PF", "TF", "MK", "GA", "GM", "GE", "DE", "GH", "GI", "GB", "GR", "GL","GD", "GP", "GU", "GT", "GG", "GN", "GW", "GY"};
char **reducedGeoList;
unsigned long benchmarkCount;
unsigned long geoIndex=0;
int geoArraySize;
int maxPrice = 100;
char ** buffer;
int port;
unsigned long logInterval;
sem_t sem;
char * statsPath;
unsigned long sleepTime;
int socket_desc , client_sock , c , read_size;
struct sockaddr_in server , client;
typedef struct LogInfo {
    unsigned long long key;
    unsigned long value;
}  logInfo;

logInfo**  producerLog;
logInfo** consumerLog;
FILE *consumerFile;
FILE *producerFile;



unsigned long long  get_current_time_with_ms (void)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	unsigned long long millisecondsSinceEpoch = (unsigned long long)(tv.tv_sec) * 1000 + (unsigned long long)(tv.tv_usec) / 1000;
    return  millisecondsSinceEpoch;
}



void initializeGeoList( double d){
	
	int allSize = sizeof(geoListAll)/sizeof(geoListAll[0]);
	geoArraySize = allSize * fabs(d);
	reducedGeoList = (char**)malloc(sizeof(char*) * geoArraySize);
	if (d > 0){
		for(int i = 0; i < geoArraySize; i ++){
			*(reducedGeoList + i) = malloc(sizeof(geoListAll[i]));
			   strcpy(reducedGeoList[i], geoListAll[i]);
		}	
	} else {
		for(int i = allSize - geoArraySize,b=0; i < allSize; i ++,b++){
                        *(reducedGeoList + b) = malloc(sizeof(geoListAll[i]));
                        strcpy(reducedGeoList[b], geoListAll[i]);
        }	
	}
}

char* generateJsonString(void){
	char * newJson = malloc(100);
	sprintf(newJson,"{\"geo\":\"%s\",\"price\":\"%d\",\"ts\":\"%llu\"}\n", reducedGeoList[geoIndex] , rand() % maxPrice,get_current_time_with_ms());
	geoIndex++;
	geoIndex = geoIndex % geoArraySize;	
	return newJson;
}

void nsleep(long us)
{
        struct timespec wait;
            //printf("Will sleep for is %ld\n", diff); //This will take extra ~70 microseconds        
        wait.tv_sec = us / (1000 * 1000);
        wait.tv_nsec = (us % (1000 * 1000)) * 1000;
        nanosleep(&wait, NULL);
}
void *produce( void  )
{
    int logIndex = 0;
    producerLog = malloc(((benchmarkCount/logInterval) +1) * sizeof (*producerLog));
    producerLog[logIndex] = malloc(sizeof(logInfo));
    producerLog[logIndex]->value = 0;
    producerLog[logIndex]->key = get_current_time_with_ms()/1000;
    fprintf(producerFile, "%llu, %lu, %lu \n", producerLog[logIndex]->key, producerLog[logIndex]->value, 0);
    unsigned long long startTime =     producerLog[logIndex]->key;
    for (unsigned long i = 0; i < benchmarkCount; i++){

         buffer[i] = generateJsonString();
         if(i % logInterval == 0){
            unsigned long long sec  = get_current_time_with_ms()/1000;
            if (producerLog[logIndex]->key != get_current_time_with_ms()/1000){
                logIndex++;
            }
            producerLog[logIndex] = malloc(sizeof(logInfo));
            producerLog[logIndex]->value = i;
            producerLog[logIndex]->key = sec;
            
            unsigned long long interval =   producerLog[logIndex]->key - startTime;
            if(interval != 0){
                 unsigned long currentThroughput = i / interval;
                 fprintf(producerFile, "%llu, %lu, %lu \n", producerLog[logIndex]->key, producerLog[logIndex]->value, currentThroughput);
            }
            printf("%lu tuples produced\n", i );
         }
         sem_post(&sem);
	if(sleepTime){
        	nsleep(sleepTime );
        }
    }
    logIndex++;
    producerLog[logIndex] = NULL;
    fclose(producerFile);
}



void *consume( void  )
{
     if (client_sock < 0)
     {
         perror("accept failed");
         return (void*)1;
     }
     puts("Connection accepted");
     
     
     // sending tuples
    int logIndex = 0;
    consumerLog = malloc(((benchmarkCount/logInterval) +3) * sizeof (*consumerLog));
    consumerLog[logIndex] = malloc(sizeof(logInfo));
    consumerLog[logIndex]->value = 0;
    consumerLog[logIndex]->key = get_current_time_with_ms()/1000;
    
    fprintf(consumerFile, "%llu, %lu, %lu \n", consumerLog[logIndex]->key, consumerLog[logIndex]->value, 0);
    unsigned long long startTime = consumerLog[logIndex]->key;

    for (unsigned long i = 0; i < benchmarkCount; i ++){
        sem_wait(&sem);
        write(client_sock , buffer[i] , strlen(buffer[i]));
       	if(i % logInterval == 0){
            unsigned long long sec  = get_current_time_with_ms()/1000;
            if (consumerLog[logIndex]->key != get_current_time_with_ms()/1000){
                logIndex++;
            }
            consumerLog[logIndex] = malloc(sizeof(logInfo));
            consumerLog[logIndex]->value = i;
            consumerLog[logIndex]->key = sec;
          
            unsigned long long interval = consumerLog[logIndex]->key - startTime;
            if(interval != 0){
                unsigned long currentThroughput = i / interval;
                fprintf(consumerFile, "%llu, %lu, %lu \n", consumerLog[logIndex]->key, consumerLog[logIndex]->value, currentThroughput);  
            }
           printf("%lu tuples sent from buffer\n", i );
        }
     free(buffer[i]);
    }
    logIndex++;
    consumerLog[logIndex]=NULL;

    fclose(consumerFile);

     if(read_size == 0)
     {
         puts("Client disconnected");
     }
     else if(read_size == -1)
     {
         perror("recv failed");
     }
}

void fireServerSocket(void){
    socket_desc = socket(AF_INET , SOCK_STREAM , 0);
    if (socket_desc == -1)
    {
        printf("Could not create socket");
    }
    puts("Socket created");
    //Prepare the sockaddr_in structure
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_port = htons( port );
    //Bind
    if( bind(socket_desc,(struct sockaddr *)&server , sizeof(server)) < 0)
    {
        //print the error message
    perror("bind failed. Error");
    return ;
    }
    puts("bind done");
    //Listen
    listen(socket_desc , 3);
    //Accept and incoming connection
    puts("Waiting for incoming connections...");
    c = sizeof(struct sockaddr_in);

         //accept connection from an incoming client
    client_sock = accept(socket_desc, (struct sockaddr *)&client, (socklen_t*)&c);
}

void initLogFiles(void){
    char * producerFP = malloc(2000);
    char hostname[1024];
    gethostname(hostname, 1024);
    sprintf(producerFP, "%sproducer-%s-%d.csv",statsPath,hostname,port  );
 

    char * consumerFP = malloc(2000);
    sprintf(consumerFP, "%sconsumer-%s-%d.csv",statsPath,hostname,port  );
    
    producerFile = fopen(producerFP, "w");
    consumerFile = fopen(consumerFP, "w");
    if (consumerFile == NULL || producerFile == NULL)  {
         printf("Error opening file!\n");
         exit(1);
    } 
}

int main(int argc , char *argv[])
{
    double partitionSize;
    statsPath = malloc(1000);
    pthread_t producer, consumer;
    sscanf(argv[1],"%lf",&partitionSize);
    sscanf(argv[2],"%lu",&benchmarkCount); 
    sscanf(argv[4],"%d",&port); 
    sscanf(argv[3],"%lu",&logInterval);
    statsPath = argv[5];
    sscanf(argv[6],"%lu",&sleepTime);
    initializeGeoList( partitionSize);
    int seed = 123;
    srand(seed);
    
    sem_init(&sem, 0 , 0);
    buffer = malloc (benchmarkCount * sizeof(*buffer));   
    
    fireServerSocket(); 
    initLogFiles();
    pthread_create( &producer, NULL, produce, NULL);
    pthread_create( &consumer, NULL, consume, NULL);

     pthread_join( producer, NULL);
     pthread_join( consumer, NULL);
    free(buffer);
    return 0;
         //Send ehe message back to client
}

