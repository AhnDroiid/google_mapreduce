#include <stdio.h>

#include "mapreduce.h"

#include <stdlib.h>

#include <assert.h>

#include <string.h>

#include <dirent.h>

#include <unistd.h>

 

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

 

Data * data;

Head ** head;

Partitioner global;

int i = 0; // inserted data size!

 

int num_partition;

void MR_Emit(char *key, char *value)

{

    //printf("input key : %s\n", key);

    // check existence of word

    int partition_loc = (int)global(key,num_partition); // 해당하는 key 값에 대한 해쉬값

    Data * cur;

	cur = (head[partition_loc])->headP;

	//printf("partition_loc : %d\n", partition_loc); 

	//printf("cur : %s\n", cur->key);

	if(cur->next == NULL)

	{

		 //printf("only dummy\n");

		 Data * newNode = (Data*)malloc(sizeof(Data));	

		 //printf("%lu\n",sizeof(newNode));

		 //printf("only dummy2\n");

		 newNode->next =NULL;

		 newNode->key = (char*)malloc(strlen(key)+1);

		 //newNode->key = key;

		 //printf("only dummy3s\n");

		 strcpy(newNode->key,key);

		 newNode->value = 1;

		 cur->next = newNode;

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

				return; // 1을 덧붙인후 종료!

			}

		

			if(cur->next == NULL && strcmp(key,cur->key) != 0) // 다음 노드자리가 비어있고 현재 노드와 값이 다르다면

			{

				Data * newnode = (Data*)malloc(sizeof(Data));

					 //printf("another hashhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh\n");

					 //printf("node plus\n");

					//printf("%lu\n",sizeof(newnode));

					newnode->key = (char*)malloc((int)strlen(key)+1);

					//newnode->key = key;

					//printf("node plus2\n");

					strcpy(newnode->key, key);

					newnode->value = 1;

					newnode->next = NULL;

					cur->next = newnode;    // cur 의 다음위치를 newnode 로 연결하고 

					return;  // return 하여 함수 종료!

			}

			cur = cur->next; // 값이 같지도 않고 다음이 비어있지도 않다면 다음으로 이동!

		}

	}

	//printf("return\n");

}

char* get_next(char * key, int partition_number) 

{

	Data * cur;	

 	cur = head[partition_number]->headP;

	//printf("int\n");

	//printf("%s\n",key);

	if(cur->next == NULL)

        return NULL;

	else

	{

		cur = cur->next;

		//printf("here\n");

		while(cur != NULL)

		{

			

			if(strcmp(key,cur->key) == 0) // 찾는 값이 있다면

			{

				//printf("int2\n");

				if(cur->value != 0) //0이 아니라면

				{

					 cur->value--;

					return "a"; // NULL 이 아닌 문자열 반환

				}

				return NULL;

			

			}

			

			

			cur = cur->next;

		}

	}

	

	return NULL;

}

void MR_Run(int argc, char *argv[], 

        Mapper map, int num_mappers, 

        Reducer reduce, int num_reducers, 

        Partitioner partition)

{

    

    char *file_name = argv[1]; // input file name

    char * access_name = (char*)malloc(sizeof(file_name)+100);

    

    DIR * dir_info;

    struct dirent *dir_entry;

    global = partition;

    strcpy(access_name,file_name);

    num_partition = atoi(argv[2]); // input # of partition 

   

    head = (Head**)malloc(sizeof(Head*) * num_partition); // 각 partition_loc 을 가리키는 head 포인터

    for(int i=0; i< num_partition; i++)

		head[i] = (Head*)malloc(sizeof(Head));

	

    for(int i=0; i< num_partition; i++)

   {

	

		//data[i].key = (char*)malloc(sizeof(10));

		//data[i].value = (char*)malloc(sizeof(10));

		//data[i].next = NULL;

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

 			strcat(access_name,dir_entry->d_name);

			//printf("access_name : %s\n",access_name);

    			map(access_name); // In this function, MR_Emit runs (data is recored sequentialy)		

			//printf("complete\n");

			access_name[0] = '\0';

			strcpy(access_name,file_name);

			

			

		}

            

			closedir(dir_info);

	} 

	// Single Mapping complete   // data 배열에 주어진 directory 의 단어들이 모두 포함됨.

	

	//Reducing 시작

	//printf("reduce start\n");

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

	return;

}

 
