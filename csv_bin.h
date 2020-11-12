#include <stdio.h>		// standard IO usage
#include <stdlib.h>		// for dynamic memmory usage
#include <pthread.h>	// for multithreading during parsing of csv
#include <string.h>		// for string manipulation
#include <unistd.h>		// used for working with various files type

//for file handling (open, stats, and macros)
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

typedef struct data_ll {
	int data;
	struct data_ll *next;
} data_link;

typedef struct threaded_args {
	char *file_name;

	unsigned long read_point;
	unsigned long end_point;
	char direction;

	struct data_ll *preparse_buff;
	pthread_mutex_t parsing_mtx;
	
} multi_args;

void clear_list(data_link *head);
void append_cell(data_link *head_ref, int new_data);
unsigned long seek_mid(char *file_name, int *start_point);
data_link *parse_csv(char *file_name);
data_link *stich_lists(data_link *first_list, data_link *second_list);
int write_binfile(char *file_name, data_link *buffer);
data_link *read_binfile(char *file_name);