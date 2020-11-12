#include "csv_bin.h"

multi_args *left_read;	// for threaded linked list parsing
multi_args *right_read;	// unfortunately theres no way out of using global


static void *thread_operation(void *arg) {
	
	// dereferencing to make it easier to work with
	multi_args **file_worm_pointer, *file_worm;
	file_worm_pointer = (multi_args **)arg;
	file_worm = *file_worm_pointer;
	
	int fd = -1;
	if((fd = open(file_worm->file_name, O_RDONLY)) < 0) {		// each thread MUST have their own unique file descriptor.
		fprintf(stderr, "Strange, there shouldn't be permission errors\n");
		_exit(EXIT_FAILURE);
	}
	lseek(fd, file_worm->read_point, SEEK_SET);

	int index = 0;
	char c = '\0';
	char data_read[50] = {'\0'};	// this stores in the digits of the the value in the current cell

	ssize_t numRead = 0;
	ssize_t total_read = 0;	

	if(file_worm->direction) {							// splitting the operations to parse the left
		while((numRead = read(fd, &c, 1)) != 0) {
			total_read += numRead;
			if(total_read >= file_worm->end_point) {	// reading left side is limited up to mid point
				break;
			}
			if(c == ',' || c == '\n') {					// the delimiter indicated the current cell it's in
				if(data_read[0] >= 32) {				// ascii values that are accessible on keyboards
					if((data_read[0] == 45) || ((data_read[0] >= 32) && (data_read[0] <= 57))) {	// check for signed integers
						pthread_mutex_lock(&file_worm->parsing_mtx);
						append_cell(file_worm->preparse_buff, atoi(data_read));
						pthread_mutex_unlock(&file_worm->parsing_mtx);
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
	else if(!file_worm->direction) {					// right side will parse till end of file
		
		// same procedure as above
		while((numRead = read(fd, &c, 1)) != 0) {
			if(c == ',' || c == '\n') {
				if(data_read[0] >= 32) {
					if((data_read[0] == 45) || ((data_read[0] >= 48) && (data_read[0] <= 57))) {
						pthread_mutex_lock(&file_worm->parsing_mtx);
						append_cell(file_worm->preparse_buff, atoi(data_read));
						pthread_mutex_unlock(&file_worm->parsing_mtx);
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
		fprintf(stderr, "error closing the CSV file descriptor\n");
	}
	return NULL;
}

// super simple linked list algorithm to add the value. This does not alter the head, unless head was NULL
void append_cell(data_link *head_ref, int new_data) { 
    data_link *new_node = malloc(sizeof(data_link));
    data_link *last = head_ref;
   
    new_node->data  = new_data; 
    new_node->next = NULL; 
  
    if(head_ref == NULL) { 
       head_ref = new_node; 
       return; 
    }   

    while (last->next != NULL) {
        last = last->next; 
    }
   
    last->next = new_node; 
    return;     
}

void clear_list(data_link *head) {

	// unexpected missing arguments check
	if(head == NULL)
		return;
	
	data_link *next_node = NULL;
    while(head->next != NULL) {
    	next_node = head->next;
    	free(head);
    	head = next_node;
    }
    free(head);
    head = NULL;
}

unsigned long seek_mid(char *file_name, int *start_point) {
	
	int distance[2] = {-1};
	int midway_of_file = 0, fd = -1;
	char c, cmp_buffer[3] = {'\0'};
	
	if((fd = open(file_name, O_RDONLY)) < 0) {
		fprintf(stderr, "error: unexpected permission problems\n");
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

	// this calculates the midpoint of the character count, not line count (rounds down from int)
	midway_of_file = (lseek(fd, 0, SEEK_END) - lseek(fd, *start_point, SEEK_SET))/2;
	lseek(fd, midway_of_file - 3, SEEK_SET);	// check left side of mid point, 3 characters should be more than half of max digit of the max value number in file
	read(fd, cmp_buffer, 3);					// compare the following characters to numerical characters to check for proper starting location
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
			distance[1] = (i + 1);
			break;
		}
	}

	lseek(fd, *start_point, SEEK_SET);								// reset cursor back to start
	if((distance[0] < distance[1]) && (distance[0] != -1))			// if left side delimiter is actually closer than right side
		midway_of_file = (midway_of_file - distance[0]);
	else if((distance[0] > distance[1]) && (distance[1] != -1))		// if right side delimiter is closer than left side
		midway_of_file = (midway_of_file + distance[1]);
	else if((distance[0] == -1) && (distance[1] >= 0))				// if left side hasn't detected a delimiter, user right side
		midway_of_file = midway_of_file + distance[1];
	else if((distance[1] == -1) && (distance[0] >= 0))				// if right side hasn't detected a delimiter, use left side
		midway_of_file = midway_of_file - distance[0];
	
	if ((close(fd) < 0)) {
		fprintf(stderr, "closing fd\n");
		_exit(EXIT_FAILURE);
	}
	return midway_of_file;
}

data_link *parse_csv(char *file_name) {

	char errmsg[50] = {'\0'};
	// Check if the file exists and we have permission to read it
	if(access(file_name, F_OK) == 0) {
		if(access(file_name, R_OK) != 0) {
			sprintf(errmsg, "error: no permission to read %s\n", file_name);
			fprintf(stderr, "%s\n", errmsg);
			return NULL;
		}
	} else {
		sprintf(errmsg, "error: %s does not exists\n", file_name);
		fprintf(stderr, "%s\n", errmsg);
		return NULL;
	}

	data_link *list_return = NULL;
	left_read = malloc(sizeof(multi_args));
	left_read->preparse_buff = malloc(sizeof(multi_args));
	right_read = malloc(sizeof(multi_args));
	right_read->preparse_buff = malloc(sizeof(multi_args));

	int err_status = 0;
	int start_point = 0;
	unsigned long mid_point = 0;

	pthread_t t1, t2;

	mid_point = seek_mid(file_name, &start_point);	// returns the mid point of the character count, not line count

	left_read->direction = 1;				// indicating the left side
	left_read->file_name = file_name;
	left_read->read_point = start_point;
	left_read->end_point = mid_point;		// read up till this point
	
	right_read->direction = 0;				// indicating the right side
	right_read->read_point = mid_point;
	right_read->file_name = file_name;
	right_read->end_point = 0;				// read till end of file
	
	// creating the threads to parse the file
	err_status  = pthread_create(&t1, NULL, (void *)thread_operation, &left_read);
	err_status += pthread_create(&t2, NULL, (void *)thread_operation, &right_read);
	if(err_status != 0) {
		fprintf(stderr, "error in threading process\n");
		_exit(EXIT_FAILURE);
	} err_status = 0;
	err_status += pthread_join(t1, NULL);
	err_status += pthread_join(t2, NULL);
	if(err_status != 0) {
		fprintf(stderr, "error in threading process\n");
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

data_link *stich_lists(data_link *first_list, data_link *second_list) {
	if(first_list == NULL) {
		return second_list;
	}
	if(second_list == NULL) {
		return first_list;
	}

	data_link *head = NULL;
	data_link *pointer = first_list;
	while(pointer->next != NULL) {
		pointer = pointer->next;
	}
	pointer->next = second_list->next;
	head = first_list->next;			// skipping the first node
	free(second_list);
	free(first_list);
	return head;
}

int write_binfile(char *file_name, data_link *buffer) {
    if(buffer == NULL)
    	return -1;
    FILE *file = NULL;
    int data;
	if((file = fopen(file_name, "wb")) == NULL )
		return 0;
    while(buffer != NULL){
		data = buffer->data;
		if(fwrite(&data, 1, sizeof(data), file) != sizeof(data)) {
            fprintf(stderr,"error: Writing to file\n");
            fclose(file);
            return -1;
        }
        buffer = buffer->next;
	}
    fclose(file);
    return 0;
}

data_link *read_binfile(char *file_name) {
	FILE *file;
   	int data = 0;
   	char errmsg[50];

   	data_link *new_node = malloc(sizeof(data_link));
   	data_link *head_ref = new_node;	// declare our head
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
        new_node->next = malloc(sizeof(data_link));
        new_node = new_node->next;
    }
    new_node->next = NULL;

    fclose(file);
    return head_ref;
}