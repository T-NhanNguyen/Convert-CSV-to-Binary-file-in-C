/*	Test with "valgrind --leak-check=full ./main"
	The output from valgrind might be due to accessing global variables
	of during the threading operation	
*/

#include "csv_bin.h"
#include <time.h>	// optional library used for timing.

double timing(struct timespec *start, struct timespec *end, int start_end_flag) {
	double elapsed = 0;
	if(start_end_flag == 0) {	// start the timer
		clock_gettime(CLOCK_MONOTONIC, start);
		return elapsed;
	}
	else if(start_end_flag == 1) {
		clock_gettime(CLOCK_MONOTONIC, end);
		elapsed = (end->tv_sec - start->tv_sec);
		elapsed += (end->tv_nsec - start->tv_nsec) / 1000000000.0;
	}
	return elapsed;
}

int main(int argc, char const *argv[]){
	char *import_csv = "in.csv";
	char *export_bin = "out.bin";

	// optional timing
	struct timespec start, finish;
	timing(&start, &finish, 0);
	

	// main conversion operation
	data_link *parse_buffer = parse_csv(import_csv);
	write_binfile(export_bin, parse_buffer);
	
	// optional timing
	double elapsed = timing(&start, &finish, 1);
	printf("Conversion completed. time elapsed: %lf seconds\n", elapsed);
	
	// quick comparison test
	data_link *bin_buffer = read_binfile(export_bin);
	data_link *ptr1 = parse_buffer;
	data_link *ptr2 = bin_buffer;
	while(ptr1 != NULL) {
		if(ptr1->data != ptr2->data) {
			printf("doesn't match\n");
			break;
		}
		ptr1 = ptr1->next;
		ptr2 = ptr2->next;
	}
	elapsed = timing(&start, &finish, 1) - elapsed;
	printf("Checking completed. time elapsed: %lf seconds\n", elapsed);
	
	// cleaning up
	clear_list(parse_buffer);
	clear_list(bin_buffer);
	ptr1 = NULL;
	ptr2 = NULL;
	return 0;
}