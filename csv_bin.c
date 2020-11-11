#include "csv_bin.h"

struct parse_structure *left_read;	// for threaded linked list parsing
struct parse_structure *right_read;	// unfortunately theres no way out of using global


static void *thread_operation(void *arg) {
	
	// dereferencing to make it easier to work with
	struct parse_structure **file_worm_pointer, *file_worm;
	file_worm_pointer = (struct parse_structure **)arg;
	file_worm = *file_worm_pointer;
	
	int fd = -1;
	if((fd = open(file_worm->filename, O_RDONLY)) < 0) {		// each thread MUST have thier own unique file descriptor.
		fprintf(stderr, "Strange, there should'nt be permission erorrs\n");
		_exit(EXIT_FAILURE);
	}

	unsigned long read_point= file_worm->read_point;

	lseek(fd, read_point, SEEK_SET);
	int index = 0;
	char c = '\0';
	char data_read[20] = {'\0'};
	char headers[180] = {'\0'};
	ssize_t numRead = 0;
	ssize_t total_read = 0;	
	if(file_worm->left_side) {			// splitting the operations to parse the left
		while((numRead = read(fd, &c, 1)) != 0) {
			total_read += numRead;
			if(total_read >= file_worm->end_point) {	// read up to the mid point and break out
				break;
			}
			if(c == ',' || c == '\n') {
				if(data_read[0] >= 32) {
					if((data_read[0] == 45) || ((data_read[0] >= 32) && (data_read[0] <= 57))) {	// check for signed integers
						pthread_mutex_lock(&file_worm->parsing_mtx);
						add_to_buffer(&file_worm->preparse_buff, atoi(data_read));
						pthread_mutex_unlock(&file_worm->parsing_mtx);
					}
					else {	// non-numerical data, so we'll treat is as column name
						strncpy(headers, data_read, strlen(data_read));
					}
				}
				memset(data_read, '\0', 20*sizeof(char));
				index = 0;
			}
			else {
				data_read[index++] = c;
			}
		}
	}
	else if(!file_worm->left_side) {	// to parse the right side
		while((numRead = read(fd, &c, 1)) != 0) {
			if(c == ',' || c == '\n') {
				if(data_read[0] >= 32) {
					if((data_read[0] == 45) || ((data_read[0] >= 32) && (data_read[0] <= 57))) {	// check for signed integers
						pthread_mutex_lock(&file_worm->parsing_mtx);
						add_to_buffer(&file_worm->preparse_buff, atoi(data_read));
						pthread_mutex_unlock(&file_worm->parsing_mtx);
					}
					else {	// non-numerical data, so we'll treat is as column name
						strncpy(headers, data_read, strlen(data_read));
					}
				}
				memset(data_read, '\0', 20*sizeof(char));
				index = 0;
			}
			else {
				data_read[index++] = c;
			}
		}
	}

	if ((close(fd) < 0)) {
		fprintf(stderr, "closing fd\n");
		_exit(EXIT_FAILURE);
	}
	return NULL;
}

struct csv_data *parse_csv(char *filename) {

	struct csv_data *list_return = NULL;
	left_read = malloc(sizeof(struct parse_structure));
	left_read->preparse_buff = malloc(sizeof(struct parse_structure));
	right_read = malloc(sizeof(struct parse_structure));
	right_read->preparse_buff = malloc(sizeof(struct parse_structure));

	char errmsg[50] = {'\0'};
	int start_point = 0;
	unsigned long mid_point = 0;

	pthread_t t1, t2;

	// Check if the file exists and we have permission to read it
	if(access(filename, F_OK) == 0) {
		if(access(filename, R_OK) != 0) {
			sprintf(errmsg, "No permission to read %s\n", filename);
			fprintf(stderr, "%s\n", errmsg);
			_exit(EXIT_FAILURE);
		}
	} else {
		sprintf(errmsg, "%s does not exists\n", filename);
		fprintf(stderr, "%s\n", errmsg);
		_exit(EXIT_FAILURE);
	}
	

	mid_point = seek_mid(filename, &start_point);	// returns the mid point of the character count, not line count

	left_read->left_side = 1;				// indicating the left side
	left_read->filename = filename;
	left_read->read_point = start_point;
	left_read->end_point = mid_point;		// read up till this point
	
	right_read->left_side = 0;				// indicating the right side
	right_read->read_point = mid_point;
	right_read->filename = filename;
	right_read->end_point = 0;				// read till end of file
	
	// creating the threads to parse the file
	if(pthread_create(&t1, NULL, (void *)thread_operation, &left_read)) {
		fprintf(stderr, "error creating thread 1\n");
		_exit(EXIT_FAILURE);
	}
	if(pthread_create(&t2, NULL, (void *)thread_operation, &right_read)) {
		fprintf(stderr, "error creating thread 2\n");
		_exit(EXIT_FAILURE);
	}
	printf("Threads established\n");
	if(pthread_join(t1, NULL)){
		fprintf(stderr, "error creating thread 2\n");
		_exit(EXIT_FAILURE);
	}
	if(pthread_join(t2, NULL)){
		fprintf(stderr, "error creating thread 2\n");
		_exit(EXIT_FAILURE);
	}

	list_return = stich_lists(left_read->preparse_buff, right_read->preparse_buff);
	if(list_return == NULL)
		return NULL;
	
	left_read->preparse_buff = NULL;
	right_read->preparse_buff = NULL;
	free(left_read);
	free(right_read);

	return list_return;
}

unsigned long seek_mid(char *filename, int *start_point) {
	
	char c;
	int distance[2] = {-1};
	char cmp_buffer[3] = {'\0'};
	int midway_of_file = 0, fd = -1;

	if((fd = open(filename, O_RDONLY)) < 0) {
		fprintf(stderr, "Strange, there should'nt be permission erorrs\n");
		_exit(EXIT_FAILURE);
	}

	lseek(fd, 0, SEEK_SET);						// checking for invalid characters to find true real point
	while(read(fd, &c, 1) != 0) {
		if(((int)c < 32) || ((int)c > 126)) {	// non-regular numerical and alphabet characters
			(*start_point)++;
		}
		else
			break;
	}

	// this calculates the midpoint of the character count, not line count
	midway_of_file = (lseek(fd, 0, SEEK_END) - lseek(fd, *start_point, SEEK_SET))/2;
	lseek(fd, midway_of_file - 3, SEEK_SET);	// check left side of mid point, 3 characters should be more than half of max digit of the max value number in file
	read(fd, cmp_buffer, 3);					// compare the following characters to numerical characters to check for proper starting loc
	for (int i = 0; i < 3; i++){
		if(cmp_buffer[i] == ',') {
			distance[0] = (3 - (i + 1));		// mark that location of comma. 1 is to offset it
			break;
		}
	}
	lseek(fd, midway_of_file, SEEK_SET);		// check right side of mid point
	read(fd, cmp_buffer, 3);
	for (int i = 0; i < 3; i++){
		if(cmp_buffer[i] == ',') {
			// printf("comma found on %d\n", i);
			distance[1] = (i + 1);
			break;
		}
	}

	lseek(fd, *start_point, SEEK_SET);								// reset cursor back to start
	if((distance[0] < distance[1]) && (distance[0] != -1))			// if left side delimeter is actually closer than right side
		midway_of_file = (midway_of_file - distance[0]);
	else if((distance[0] > distance[1]) && (distance[1] != -1))		// if right side delmiter is closer than left side
		midway_of_file = (midway_of_file + distance[1]);
	else if((distance[0] == -1) && (distance[1] >= 0))				// if left side hasn't detected a delimiter, user right side
		midway_of_file = midway_of_file + distance[1];
	else if((distance[1] == -1) && (distance[0] >= 0))				// if right side hasn't detected a delimeter, use left side
		midway_of_file = midway_of_file - distance[0];
	
	if ((close(fd) < 0)) {
		fprintf(stderr, "closing fd\n");
		_exit(EXIT_FAILURE);
	}
	return midway_of_file;
}

// basic append to linked list function
void add_to_buffer(struct csv_data **head_ref, float new_data) { 
    struct csv_data *new_node = malloc(sizeof(struct read_list));
    struct csv_data *last = *head_ref;  /* used in step 5*/
   
    new_node->data  = new_data; 
  
    new_node->next = NULL; 
  
    if (*head_ref == NULL) 
    { 
       *head_ref = new_node; 
       return; 
    }   
       
    while (last->next != NULL) 
        last = last->next; 
   
    last->next = new_node; 
    return;     
} 

struct csv_data *stich_lists(struct csv_data *first_list, struct csv_data *second_list) {
	if((first_list == NULL) || (second_list == NULL)) {
		fprintf(stderr, "failed stiching lists\n");
		return NULL;
	}
	struct csv_data *head = NULL;
	struct csv_data *pointer = first_list;
	while(pointer->next != NULL) {
		pointer = pointer->next;
	}
	pointer->next = second_list->next;
	head = first_list->next;			// skipping the first node
	free(second_list);
	free(first_list);
	return head;
}

int write_binfile(char *file_name, struct csv_data *buffer) {
    FILE *Write_fptr = NULL;
    int data;
    size_t nElement  = 1;
    if (buffer == NULL) {
    	return -1;
    }
	if ( (Write_fptr = fopen(file_name, "wb")) != NULL ) {
		while(buffer != NULL){
			data = buffer->data;
			if ( fwrite(&data, nElement, sizeof(data), Write_fptr) != sizeof(data) ) {
	            fprintf(stderr,"Error: Writing to file\n");
	            fclose(Write_fptr);
	            return -1;
	        }
	        buffer = buffer->next;
		}
    }
    fclose(Write_fptr);
    return 0;
}

struct csv_data *read_binfile(char *file_name) {
	FILE *file;
   	int data = 0;
   	char errmsg[50];

   	struct csv_data *new_node = malloc(sizeof(struct csv_data));
   	struct csv_data *head_ref = new_node;	// declare our head
    new_node->next = NULL;  

    if(access(file_name, F_OK) == 0) {		// check file for permission
		if(access(file_name, R_OK) != 0) {
			sprintf(errmsg, "No permission to read %s\n", file_name);
			fprintf(stderr, "%s\n", errmsg);
			_exit(EXIT_FAILURE);
		}
	} else {
		sprintf(errmsg, "%s does not exists\n", file_name);
		fprintf(stderr, "%s\n", errmsg);
		_exit(EXIT_FAILURE);
	}

    if((file = fopen(file_name, "rb")) == NULL) {
        fprintf(stderr,"Error: opening file for reading\n");
        return NULL;
    }
    while(fread(&data, 1, sizeof(data), file) == sizeof(data)) {	// while byte is still being read
        new_node->data = data;
        new_node->next = malloc(sizeof(struct csv_data));
        new_node = new_node->next;
    }
    new_node->next = NULL;

    fclose(file);
    return head_ref;
}

void clear_list(struct read_list **head, struct csv_data **head2, int type) {

	// unexpected missing arguments check
	if (((head == NULL) & (head2 == NULL)) || ((head == NULL) && (type == 0)) || ((head2 == NULL) && (type == 1)) || ((type < 0) || (type > 1))) {
        fprintf(stderr, "clear list\n");
        return;
    }
    // if the desired struct was read_list
    if(type == 0) {
    	struct read_list* next_node = NULL;
	    while((*head)->next != NULL) {
	    	next_node = (*head)->next;
	    	free(*head);
	    	*head = next_node;
	    }
	    free(*head);
	    *head = NULL;
    }

	// if the desired struct was read_list
    if(type == 1) {
    	struct csv_data* next_node = NULL;
	    while((*head2)->next != NULL) {
	    	next_node = (*head2)->next;
	    	free(*head2);
	    	*head2 = next_node;
	    }
	    free(*head2);
	    *head2 = NULL;
    } 
}