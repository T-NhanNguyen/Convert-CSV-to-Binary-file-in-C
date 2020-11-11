/*	Test with "valgrind --leak-check=full ./main"
	The output from valgrind might be due to accessing global variables
	of during the threading operation	
*/

#include "csv_bin.h"
#include <time.h>	// optional library used for timing.

double timing(struct timespec *start, struct timespec *end, int start_end_flag) {
	double elapsed;
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

	// main function
	struct csv_data *parse_buffer = parse_csv(import_csv);
	write_binfile(export_bin, parse_buffer);
	clear_list(NULL, &parse_buffer, 1);

	// optional timing
	double elapsed = timing(&start, &finish, 1);
	printf("Parsing file completed. time elapsed: %lf seconds\n", elapsed);


	return 0;
}