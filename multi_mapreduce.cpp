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

}Queue; // thread queue ������ ���� Queue data structure declaration

 

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

			

			if(strcmp(key,cur->key) == 0) // ã�� ���� �ִٸ�

			{

				

				if(cur->value != 0) //0�� �ƴ϶��

				{

					 cur->value--;

					 pthread_mutex_unlock(&lock[partition_number]);

					return "a"; // NULL �� �ƴ� ���ڿ� ��ȯ

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

		cur = cur->next; // �ش� partition ���� ���� node �� �̵�

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

	

    int partition_loc = (int)global(key,num_partition); // �ش��ϴ� key ���� ���� �ؽ���

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

		cur = cur->next; // �ϴ� ��ĭ �̵����ش�.

		while(cur != NULL)

		{

			//printf("cur is not null\n");

			if(strcmp(key,cur->key) == 0) // �迭�� ���� ���ڰ� ������!

		    {

				//cur = realloc(cur,strlen(cur->value)+1);

				cur->value++; // �ߺ��Ǵ� �迭�� value ���� 1�� ���Ѵ�!

				//printf("1 added\n");

				pthread_mutex_unlock(&lock[partition_loc]);

				return; // 1�� �������� ����!

			}

		

			if(cur->next == NULL && strcmp(key,cur->key) != 0) // ���� ����ڸ��� ����ְ� ���� ���� ���� �ٸ��ٸ�

			{

				Data * newnode = (Data*)malloc(sizeof(Data));

					

					newnode->key = (char*)malloc((int)strlen(key)+1);

					

					strcpy(newnode->key, key);

					newnode->value = 1;

					newnode->next = NULL;

					cur->next = newnode;    // cur �� ������ġ�� newnode �� �����ϰ� 

					//printf("same but new word added\n");

					pthread_mutex_unlock(&lock[partition_loc]);

					return;  // return �Ͽ� �Լ� ����!

			}

			cur = cur->next; // ���� ������ �ʰ� ������ ��������� �ʴٸ� �������� �̵�!

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

 ////////******* thread mapper, reducer �� argument ������ŭ ���� ******//////////////////   

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

	} // ���ؽ� , Thread INFO INITIALIZATION//

/////////////////////////////////////////////////////////////////////////////

   

   

    DIR * dir_info;

    int dir_num = 0;

    char** file_ent;

    struct dirent *dir_entry;

    global = partition;

    strcpy(access_name,file_name);

   

   

    head = (Head**)malloc(sizeof(Head*) * num_partition+1); // �� partition_loc �� ����Ű�� head ������

    for(int i=0; i< num_partition+1; i++)

		head[i] = (Head*)malloc(sizeof(Head));

	

    for(int i=0; i< num_partition+1; i++)

   {

	

		Data * dummy = (Data*)malloc(sizeof(Data));

		dummy->next = NULL;

		head[i]->headP = dummy; // i ��ġ�� �� head�� dummy node �ΰ�

   }

   // INITIALIZATION END

  

	dir_info= opendir(file_name);

	if(NULL != dir_info)

 	{

	    while((dir_entry = readdir(dir_info)) != NULL)

		{	

 			dir_num++; // �ش� ���丮�ȿ� ���� ���� ����.

		}

            

			closedir(dir_info);

	} 

	dir_info = opendir(file_name);

	file_ent = (char**)malloc(sizeof(char*) * dir_num); // ���� ��Ʈ��

	

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

/////////// multi thread �� ���� file entry ���� �Ϸ�///////////////////////

 

////////	/*multi thread mapping*/        ///////

	

 

 mapper_queue = (Queue*)malloc(sizeof(Queue));

 reducer_queue = (Queue*)malloc(sizeof(Queue)); // mapper �� reducer �� ���� Queue ���� 

QueueInit(mapper_queue);

QueueInit(reducer_queue);

 

//mapper thread ��� Queue �� ����(thread pool ����)//

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

	

	//Reducing ����

	

	

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

			cur = cur->next; // dummy node �������� �̵�

			while(cur != NULL)

			{

				reduce(cur->key, get_next, partition(cur->key,num_partition));

				//printf("%s\n",cur->key);

				//printf("value: %d\n",cur->value);				

				cur = cur->next; // �ش� partition ���� ���� node �� �̵�

			}

		}

	}

	*/

	return;

}

 
