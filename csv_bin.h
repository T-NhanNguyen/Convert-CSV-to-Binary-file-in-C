#include <stdio.h>		// standard IO usage
#include <stdlib.h>		// for dynamic memmory usage
#include <pthread.h>	// for multithreading during parsing of csv
#include <string.h>		// for string manipulation
#include <unistd.h>		// used for working with various files type

//for file handling (open, stats, and macros)
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

struct parse_structure {
	char *filename;
	unsigned int read_point;
	unsigned int end_point;
	struct csv_data *preparse_buff;
	pthread_mutex_t parsing_mtx;
	int left_side;
};
struct csv_data {
	int data;
	struct csv_data *next;
};
struct read_list {
	float sensor_value;
	struct read_list *next;
};

static void *thread_operation(void *arg);
struct csv_data *parse_csv(char *filename);
unsigned long seek_mid(char *filename, int *start_point);
void add_to_buffer(struct csv_data **head_ref, float new_data);
struct csv_data *stich_lists(struct csv_data *first_list, struct csv_data *second_list);
int write_binfile(char *file_name, struct csv_data *buffer);
struct csv_data *read_binfile(char *file_name);
void clear_list(struct read_list **head, struct csv_data **head2, int type);