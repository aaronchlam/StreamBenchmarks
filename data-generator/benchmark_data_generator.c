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
            "AF","AX" ,"AL","DZ","AS","AD","AO","AI","AQ","AG","AR","AM","AW","AC","AU","AT","AZ","BS","BH","BB",
            "BD","BY","BE","BZ", "BJ","BM","BT","BW","BO","BA","BV","BR","IO","BN","BG","BF","BI","KH","CM","CA",
            "CV","KY","CF","TD","CL","CN","CX","CC","CO","KM","CG","CD","CK","CR","CI","HR", "CU","CY","CZ","CS",
            "DK","DJ","DM","DO","TP","EC","EG","SV","GQ","ER","EE","ET","EU","FK","FO","FJ","FI","FR","FX","GF",
            "PF","TF","MK","GA","GM","GE","DE","GH","GI","GB","GR","GL","GD","GP","GU","GT","GG","GN","GW","GY",
            "HT","HM","HN","HK","HU","IS","IN","ID","IR","IQ","IE","IL","IM","IT","JE","JM","JP","JO","KZ","KE",
            "KI","KP","KR","KW","KG","LA","LV","LB","LI","LR","LY","LS","LT","LU","MO","MG","MW","MY","MV","ML",
            "MT","MH","MQ","MR","MU","YT","MX","FM","MC","MD","MN","ME","MS","MA","MZ","MM","NA","NR","NP","NL"

            
            };
char **reducedGeoList;
char ** reducedGeoListRemaining;
unsigned long benchmarkCount;
unsigned long geoIndex=0;
unsigned long geoIndexRemaining=0;
int geoArraySize;
int geoArraySizeRemaining;
int maxPrice = 1000;
char ** buffer;
int port;
unsigned long logInterval;
sem_t sem;
unsigned long sleepTime;
int socket_desc , client_sock , c , read_size;
struct sockaddr_in server , client;
typedef struct LogInfo {
    unsigned long long key;
    unsigned long value;
    unsigned long throughput;
}  logInfo;

logInfo**  producerLog;
logInfo** consumerLog;
FILE *consumerFile;
FILE *producerFile;

char * producerFP;
char * consumerFP;
int dataGeneratedAfterEachSleep;
int sustainability_limit;
int backpressure_limit;
double skew;
unsigned long selectivityOfSkewedData;
int consumedTuplesPerTimeUnit = 0;
int consumerLogInterval;


void openFiles(){
    char hostnameAndPort[1024];
    char hostname[500];
    hostname[500] = '\0';
    hostnameAndPort[1024] = '\0';
    gethostname(hostname, 1023);
    sprintf(hostnameAndPort, "/share/hadoop/jkarimov/workDir/StreamBenchmarks/data-generator/stats/%s-%d.csv", hostname, port);
    consumerFile = fopen(hostnameAndPort, "a");
    if (consumerFile == NULL)
    {
            printf("Error opening file!\n");
            exit(1);
    }
}

unsigned long long  get_current_time_with_ms (void)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	unsigned long long millisecondsSinceEpoch = (unsigned long long)(tv.tv_sec) * 1000 + (unsigned long long)(tv.tv_usec) / 1000;
    return  millisecondsSinceEpoch;
}

int msleep(unsigned long milisec)
{
       struct timespec req={0};
       time_t sec=(int)(milisec/1000);
       milisec=milisec-(sec*1000);
       req.tv_sec=sec;
       req.tv_nsec=milisec*1000000L;
       while(nanosleep(&req,&req)==-1)
           continue;
       return 1;
}

void initializeGeoList( double d){
	int allSize = sizeof(geoListAll)/sizeof(geoListAll[0]);
	geoArraySize = allSize * fabs(d);
        geoArraySizeRemaining = allSize * (1 - fabs(d));
	reducedGeoList = (char**)malloc(sizeof(char*) * geoArraySize);

        reducedGeoListRemaining = (char **) malloc (sizeof(char*) * geoArraySizeRemaining   ) ;
        if (d > 0){
		for(int i = 0; i < geoArraySize; i ++){
			*(reducedGeoList + i) = malloc(sizeof(geoListAll[i]));
			   strcpy(reducedGeoList[i], geoListAll[i]);
		}

		for(int i = geoArraySize +1 ,b=0; i < allSize; i ++,b++){
                        *(reducedGeoListRemaining + b) = malloc(sizeof(geoListAll[i]));
                        strcpy(reducedGeoListRemaining[b], geoListAll[i]);
                }	
	} else {
		for(int i = allSize - geoArraySize,b=0; i < allSize; i ++,b++){
                        *(reducedGeoList + b) = malloc(sizeof(geoListAll[i]));
                        strcpy(reducedGeoList[b], geoListAll[i]);
                }

		for(int i = 0; i < geoArraySize; i ++){
			*(reducedGeoListRemaining + i) = malloc(sizeof(geoListAll[i]));
			   strcpy(reducedGeoListRemaining[i], geoListAll[i]);
		}

	}
}

char* generateJsonString( unsigned long currentIndex){
	char * newJson = malloc(100);
        if(skew != 1.0 && currentIndex % selectivityOfSkewedData == 0 ){
	        sprintf(newJson,"{\"key\":\"%s\",\"value\":\"%d\",\"ts\":\"%llu\"}\n", reducedGeoListRemaining[geoIndexRemaining] , rand() % maxPrice,get_current_time_with_ms());
	        geoIndexRemaining++;
	        geoIndexRemaining = geoIndexRemaining % geoArraySizeRemaining;	
	        return newJson;
        }
        else{
	        sprintf(newJson,"{\"key\":\"%s\",\"value\":\"%d\",\"ts\":\"%llu\"}\n", reducedGeoList[geoIndex] , rand() % maxPrice,get_current_time_with_ms());
            geoIndex++;
 	        geoIndex = geoIndex % geoArraySize;	
	        return newJson;
        }
}

void *produce( void  )
{
    int logIndex = 0;
    producerLog = malloc(((benchmarkCount/logInterval) +1) * sizeof (*producerLog));
    producerLog[logIndex] = malloc(sizeof(logInfo));
    producerLog[logIndex]->value = 0;
    producerLog[logIndex]->key = get_current_time_with_ms()/1000;
    producerLog[logIndex]->throughput = 0;

    unsigned long long startTime =     producerLog[logIndex]->key;
    
    int queue_size ;
    int backpressure_tolerance_iteration = backpressure_limit/sustainability_limit;
    for (unsigned long i = 0; i < benchmarkCount; ){
	for(int k = 0; k < dataGeneratedAfterEachSleep && i < benchmarkCount; k++ ,i++){
         buffer[i] = generateJsonString(i);
         if(i % logInterval == 0){
              sem_getvalue(&sem, &queue_size);
              if(queue_size > sustainability_limit){
                if(backpressure_tolerance_iteration == 0 || queue_size > backpressure_limit ){
                     printf("Cannot sustain the input data rate \n");
                     exit(0);
                }
                else {
                    backpressure_tolerance_iteration --;
                    printf("The system can tolerate backpressure for %d additional iterations \n",backpressure_tolerance_iteration);
                }
            } else {
                backpressure_tolerance_iteration = backpressure_limit / sustainability_limit;
            }


            unsigned long long sec  = get_current_time_with_ms()/1000;
            if (producerLog[logIndex]->key != get_current_time_with_ms()/1000){
                logIndex++;
            }
            producerLog[logIndex] = malloc(sizeof(logInfo));
            producerLog[logIndex]->value = i;
            producerLog[logIndex]->key = sec;
            producerLog[logIndex]->throughput = 0;
            unsigned long long interval =   producerLog[logIndex]->key - startTime;
            if(interval != 0){
                 producerLog[logIndex]->throughput = i /  interval;
           }
            printf("Producer info - %llu, %lu, %lu \n", producerLog[logIndex]->key, producerLog[logIndex]->value, producerLog[logIndex]->throughput );
         }
         sem_post(&sem);
        }


	if(sleepTime != 0){
        	msleep(sleepTime );
        }
    }
    logIndex++;
    producerLog[logIndex] = NULL;

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
    consumerLog[logIndex]->throughput = 0;    
    unsigned long long startTime = consumerLog[logIndex]->key;

    for (unsigned long i = 0; i < benchmarkCount; i ++){
        sem_wait(&sem);
        write(client_sock , buffer[i] , strlen( buffer[i] ));
       	if(i % logInterval == 0){
                      
            unsigned long long sec  = get_current_time_with_ms()/1000;
            if (consumerLog[logIndex]->key != get_current_time_with_ms()/1000){
                logIndex++;
            }
            consumerLog[logIndex] = malloc(sizeof(logInfo));
            consumerLog[logIndex]->value = i;
            consumerLog[logIndex]->key = sec;
            consumerLog[logIndex]->throughput = 0;
            unsigned long long interval = consumerLog[logIndex]->key - startTime;
            if(interval != 0){
                 consumerLog[logIndex]->throughput = i / interval;
	        }
            printf("Consumer log - %llu, %lu, %lu \n", consumerLog[logIndex]->key,consumerLog[logIndex]->value,consumerLog[logIndex]->throughput );
        }
        if (i % consumerLogInterval == 0) {
            openFiles();
            fprintf(consumerFile, "%lu,%d\n", get_current_time_with_ms()/1000,consumedTuplesPerTimeUnit);
            consumedTuplesPerTimeUnit = 0;
            fclose(consumerFile);
        } else {
            consumedTuplesPerTimeUnit ++;
        }
     free(buffer[i]);
    }
    logIndex++;
    consumerLog[logIndex]=NULL;

 
    printf("All data read by system \n");
    msleep(1000 * 1000);

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






int main(int argc , char *argv[])
{
    pthread_t producer, consumer;
    sscanf(argv[1],"%lf",&skew);
    sscanf(argv[2],"%lu",&benchmarkCount); 
    sscanf(argv[4],"%d",&port); 
    sscanf(argv[3],"%lu",&logInterval);
    sscanf(argv[5],"%lu",&sleepTime);
    sscanf(argv[6],"%d",&dataGeneratedAfterEachSleep);
    sscanf(argv[7],"%d",&sustainability_limit);
    sscanf(argv[8],"%d",&backpressure_limit);
    sscanf(argv[9],"%lu",&selectivityOfSkewedData);
    sscanf(argv[10],"%d",&consumerLogInterval);
    initializeGeoList( skew);
    int seed = 123;
    srand(seed);
    sem_init(&sem, 0 , 0);
    buffer = malloc (benchmarkCount * sizeof(*buffer));   
    fireServerSocket(); 
    //initLogFiles();
    pthread_create( &producer, NULL, produce, NULL);
    pthread_create( &consumer, NULL, consume, NULL);

     pthread_join( producer, NULL);
     pthread_join( consumer, NULL);
    free(buffer);
    return 0;
         //Send ehe message back to client
}

