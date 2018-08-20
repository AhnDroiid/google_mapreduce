#include <stdio.h>

#include "mapreduce.h"

#include <stdlib.h>

#include <assert.h>

#include <string.h>

#include <dirent.h>

#include <unistd.h>

#include <pthread.h>

typedef struct __data

{

    char * key;

    int value;

	struct __data * next;

}Data;

typedef struct __ListHead

{

	Data * headP;

}Head;

 

///////***thread pool queue declaration***//////

 

typedef struct __thread_data

{

    int thread_num;

	int done;

	Mapper mapper;

	Reducer reducer;

	Data * cur;

	char * route;

     struct __thread_data * next;

}th_data;

typedef struct __Queue

{

    th_data * first;

    th_data * end;

    int num;

}Queue; // thread queue 구현을 위한 Queue data structure declaration

 

void QueueInit(Queue * q)

{

    q->num = 0;

    q->first = q->end = NULL;

}

int IsEmpty(Queue * q)

{

    if(q->num == 0)

    	return 1;

    return 0;

}

void Enqueue(Queue * q, th_data * t)

{

	

	if(IsEmpty(q))

    {

		q->first = t;

		q->end = t;

		q->num++;

		return;

	}

	q->end->next = t;

	q->end = t;

	q->num++; 

	

	return;

	

}

void Dequeue(Queue * q)

{

	

    if(IsEmpty(q))

	    exit(1);

	else

	{

		//th_data * temp;

		//temp = q->first;

		q->first = q->first->next;

		q->num--;

		//free(temp);

		return;

	}

}

int Peek(Queue * q)

{

	if(IsEmpty(q))

		exit(1);

	else

	{

		return q->first->thread_num;

	}

}

//////declaration end////////////

 

 

Data * data;

Head ** head;

Partitioner global;

int i = 0; // inserted data size!

int state = -1;

pthread_mutex_t * lock;

pthread_mutex_t Queue_lock;

pthread_mutex_t Queue_adder;

pthread_mutex_t Index_lock;

pthread_cond_t cond;

pthread_cond_t cond2;

pthread_t * mapper_thread;

pthread_t * reducer_thread;

th_data * mapper_thread_info;

th_data * reducer_thread_info;

Queue * mapper_queue;

Queue * reducer_queue;

//pthread_cond_t cond;

 

int num_partition;

char* get_next(char * key, int partition_number) 

{

	Data * cur;	

 	cur = head[partition_number]->headP;

	pthread_mutex_lock(&lock[partition_number]);

	//printf("key : %s\n", key);

	if(cur->next == NULL)

	{

		pthread_mutex_unlock(&lock[partition_number]);

        return NULL;

	}

	else

	{

		cur = cur->next;

		

		while(cur != NULL)

		{

			

			if(strcmp(key,cur->key) == 0) // 찾는 값이 있다면

			{

				

				if(cur->value != 0) //0이 아니라면

				{

					 cur->value--;

					 pthread_mutex_unlock(&lock[partition_number]);

					return "a"; // NULL 이 아닌 문자열 반환

				}

				pthread_mutex_unlock(&lock[partition_number]);

				return NULL;

			

			}

			

			

			cur = cur->next;

		}

	}

	pthread_mutex_unlock(&lock[partition_number]);

	return NULL;

}

void* mapper_func(void * dat)

{

    th_data * data = (th_data*)dat;

	data->mapper(data->route);

	data->done = 0;

	data->route = NULL;

	pthread_mutex_lock(&Queue_lock);

	Enqueue(mapper_queue,data);

	//printf("newly inserted after work done, thread : %d\n",data->thread_num);

	pthread_mutex_unlock(&Queue_lock);

	//printf("thread %d  unlock complete\n",data->thread_num);

	pthread_cond_signal(&cond);

	return (void*)data;

}

void* reducer_func(void * dat)

{

	

    th_data * data = (th_data*)dat;

	Data* cur;

	cur = data->cur;

	//printf("I'm in reducer_func thread : %d\n",data->thread_num);

	

	while(cur != NULL)

	{

		data->reducer(cur->key, get_next, global(cur->key, num_partition));			

		cur = cur->next; // 해당 partition 에서 다음 node 로 이동

	}

		

	

	

	pthread_mutex_lock(&Queue_lock);

	data->cur = NULL;

	Enqueue(reducer_queue,data);

	pthread_cond_signal(&cond);

	//state = data->thread_num;

	//printf("state = %d\n",state);

	pthread_mutex_unlock(&Queue_lock);

	

	//printf("end of func\n");

	return (void*)data;

}

 

void MR_Emit(char *key, char *value)

{

    

    // check existence of word

	

    int partition_loc = (int)global(key,num_partition); // 해당하는 key 값에 대한 해쉬값

	//printf("partition selected  %d\n",partition_loc);

	//printf("MR_EMIT KEY :%s partition: %d\n",key,partition_loc);

    Data * cur;

	cur = (head[partition_loc])->headP;

	pthread_mutex_lock(&lock[partition_loc]);

	//printf("in lock : %d @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n",partition_loc);

	

	if(cur->next == NULL)

	{

		

		 Data * newNode = (Data*)malloc(sizeof(Data));	

		

		 newNode->next =NULL;

		 newNode->key = (char*)malloc(strlen(key)+1);

		 

		 strcpy(newNode->key,key);

		 newNode->value = 1;

		 cur->next = newNode;

		 //printf("initial addition\n");

		 pthread_mutex_unlock(&lock[partition_loc]);

		 return;

	}

	else

	{

		cur = cur->next; // 일단 한칸 이동해준다.

		while(cur != NULL)

		{

			//printf("cur is not null\n");

			if(strcmp(key,cur->key) == 0) // 배열에 같은 문자가 있으면!

		    {

				//cur = realloc(cur,strlen(cur->value)+1);

				cur->value++; // 중복되는 배열의 value 값에 1을 더한다!

				//printf("1 added\n");

				pthread_mutex_unlock(&lock[partition_loc]);

				return; // 1을 덧붙인후 종료!

			}

		

			if(cur->next == NULL && strcmp(key,cur->key) != 0) // 다음 노드자리가 비어있고 현재 노드와 값이 다르다면

			{

				Data * newnode = (Data*)malloc(sizeof(Data));

					

					newnode->key = (char*)malloc((int)strlen(key)+1);

					

					strcpy(newnode->key, key);

					newnode->value = 1;

					newnode->next = NULL;

					cur->next = newnode;    // cur 의 다음위치를 newnode 로 연결하고 

					//printf("same but new word added\n");

					pthread_mutex_unlock(&lock[partition_loc]);

					return;  // return 하여 함수 종료!

			}

			cur = cur->next; // 값이 같지도 않고 다음이 비어있지도 않다면 다음으로 이동!

		}

	}

	//printf("before unlock!!!!!!!!!!\n");

	pthread_mutex_unlock(&lock[partition_loc]);

	//printf("insert complete %d ############################\n",partition_loc);

	//printf("return\n");

}

 

void MR_Run(int argc, char *argv[], 

        Mapper map, int num_mappers, 

        Reducer reduce, int num_reducers, 

        Partitioner partition)

{

    

    char *file_name = argv[1]; // input file name

    char * access_name = (char*)malloc(sizeof(file_name)+100);

	num_partition = atoi(argv[2]); // input # of partition 

 ////////******* thread mapper, reducer 를 argument 갯수만큼 정의 ******//////////////////   

	mapper_thread = malloc(sizeof(pthread_t) * num_mappers);  

	reducer_thread = malloc(sizeof(pthread_t) * num_reducers);

	lock = malloc(sizeof(pthread_mutex_t) * num_partition);

	mapper_thread_info = (th_data*)malloc(sizeof(th_data) * num_mappers);

	reducer_thread_info = (th_data*)malloc(sizeof(th_data) * num_reducers);

	pthread_mutex_init(&Queue_lock,NULL);

	pthread_cond_init(&cond,NULL);

	pthread_cond_init(&cond2,NULL);

	pthread_mutex_init(&Queue_adder,NULL);

	

	pthread_mutex_init(&Index_lock,NULL);

	for(int i=0; i< num_partition; i++)

	{

		pthread_mutex_init(&lock[i],NULL);

	}

	for(int i=0; i< num_mappers; i++)

	{

		

		mapper_thread_info[i].thread_num = i;

		mapper_thread_info[i].done = 0;

		mapper_thread_info[i].next = NULL;

		mapper_thread_info[i].mapper = map;

	}

	for(int i=0; i< num_reducers; i++)

	{

		reducer_thread_info[i].thread_num = i;

		reducer_thread_info[i].done = 0;

		reducer_thread_info[i].next = NULL;

		reducer_thread_info[i].reducer =reduce;

		reducer_thread_info[i].cur = NULL;

	} // 뮤텍스 , Thread INFO INITIALIZATION//

/////////////////////////////////////////////////////////////////////////////

   

   

    DIR * dir_info;

    int dir_num = 0;

    char** file_ent;

    struct dirent *dir_entry;

    global = partition;

    strcpy(access_name,file_name);

   

   

    head = (Head**)malloc(sizeof(Head*) * num_partition+1); // 각 partition_loc 을 가리키는 head 포인터

    for(int i=0; i< num_partition+1; i++)

		head[i] = (Head*)malloc(sizeof(Head));

	

    for(int i=0; i< num_partition+1; i++)

   {

	

		Data * dummy = (Data*)malloc(sizeof(Data));

		dummy->next = NULL;

		head[i]->headP = dummy; // i 위치의 각 head에 dummy node 인가

   }

   // INITIALIZATION END

  

	dir_info= opendir(file_name);

	if(NULL != dir_info)

 	{

	    while((dir_entry = readdir(dir_info)) != NULL)

		{	

 			dir_num++; // 해당 디렉토리안에 파일 개수 세기.

		}

            

			closedir(dir_info);

	} 

	dir_info = opendir(file_name);

	file_ent = (char**)malloc(sizeof(char*) * dir_num); // 파일 엔트리

	

	//printf("# of file : %d\n", dir_num);

	if(NULL != dir_info)

	{

		int temp = 0;

		while((dir_entry = readdir(dir_info)) != NULL)

		{

			strcat(access_name,dir_entry->d_name);

    			//map(access_name); // In this function, MR_Emit runs (data is recored sequentialy)

			file_ent[temp] = (char*)malloc(strlen(access_name)+1);

			strcpy(file_ent[temp],access_name);	

			access_name[0] = '\0';

			strcpy(access_name,file_name);

			temp++;

		}

	}

	/*for(int i=0; i< dir_num; i++)

		printf("%d : %s\n",i,file_ent[i]);*/

/////////// multi thread 를 위한 file entry 구성 완료///////////////////////

 

////////	/*multi thread mapping*/        ///////

	

 

 mapper_queue = (Queue*)malloc(sizeof(Queue));

 reducer_queue = (Queue*)malloc(sizeof(Queue)); // mapper 와 reducer 를 위한 Queue 선언 

QueueInit(mapper_queue);

QueueInit(reducer_queue);

 

//mapper thread 모두 Queue 에 삽입(thread pool 생성)//

for(int i=0; i<num_mappers; i++)

{

	Enqueue(mapper_queue,&mapper_thread_info[i]);

}

for(int i=0; i<num_reducers; i++)

{

	Enqueue(reducer_queue,&reducer_thread_info[i]);

}

////////////////////////////////////////////////////

 

char * file;

int th_num;

for(int i=0; i< dir_num; i++)

{

    file = file_ent[i];

	//printf("file name : %s\n",file);

	if(!IsEmpty(mapper_queue))

	{

		th_num = Peek(mapper_queue);

		//printf("here\n");

		Dequeue(mapper_queue);

		//printf("here2\n");

		mapper_thread_info[th_num].done = 1;

		mapper_thread_info[th_num].route = file;

		pthread_create(&mapper_thread[th_num],NULL,(void*)mapper_func,(void*)&mapper_thread_info[th_num]);

		//printf("order : %d if statement\n",i);

	}

	else

	{  

		//printf("before while\n");

		while(IsEmpty(mapper_queue))

		{

			pthread_cond_wait(&cond,&Queue_adder);

		}

		//printf("go to start\n");

		th_num = Peek(mapper_queue);

		Dequeue(mapper_queue);

		mapper_thread_info[th_num].done = 1;

		mapper_thread_info[th_num].route = file;

		pthread_create(&mapper_thread[th_num],NULL,(void*)mapper_func,(void*)&mapper_thread_info[th_num]);

		//printf("else order : %d if statement\n",i);

		continue;

	}

	

}

 

 

 

 

 

for(int i=0; i< num_mappers; i++)

{

    pthread_join(mapper_thread[i],NULL);

	//printf(" thread : %d ends\n",i);

}

 

 

 

 

 

 

 

//////					       ///////

	

	//Reducing 시작

	

	

	Data * cur;

	for(int m =0; m< num_partition; m++)

	{

		cur = head[m]->headP;

		if(cur->next == NULL) // No component

			continue;

        cur = cur->next;

		//printf("key : %s\n",cur->key);

		if(!IsEmpty(reducer_queue))

		{

			//printf("m : %d\n",m);

			pthread_mutex_lock(&Queue_lock);

			th_num = Peek(reducer_queue);

			Dequeue(reducer_queue);

			pthread_mutex_unlock(&Queue_lock);

			reducer_thread_info[th_num].cur = cur;

			pthread_create(&reducer_thread[th_num],NULL,(void*)reducer_func,(void*)&reducer_thread_info[th_num]);

			

		}

		else

		{  

			pthread_mutex_lock(&Queue_lock);

			while(IsEmpty(reducer_queue))

				pthread_cond_wait(&cond,&Queue_lock);

			th_num = Peek(reducer_queue);

			Dequeue(reducer_queue);

			pthread_mutex_unlock(&Queue_lock);

			reducer_thread_info[th_num].cur = cur;

			pthread_create(&reducer_thread[th_num],NULL,(void*)reducer_func,(void*)&reducer_thread_info[th_num]);

			pthread_mutex_unlock(&Queue_lock);

		}

		//printf("m : %d\n",m);

	}

	for(int i=0; i< num_reducers; i++)

{

    pthread_join(reducer_thread[i],NULL);

	//printf(" reduce thread : %d ends\n",i);

}

	

	/*

	

	Data * cur;

	for(int m =0; m< num_partition; m++)

	{

		cur = head[m]->headP;

		if(cur->next == NULL) // NO COMPONENTS!

		    continue;

		else

		{

			//printf("%d\n",m);

			cur = cur->next; // dummy node 다음으로 이동

			while(cur != NULL)

			{

				reduce(cur->key, get_next, partition(cur->key,num_partition));

				//printf("%s\n",cur->key);

				//printf("value: %d\n",cur->value);				

				cur = cur->next; // 해당 partition 에서 다음 node 로 이동

			}

		}

	}

	*/

	return;

}

 
