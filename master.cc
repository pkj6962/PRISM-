
#include "mpitracer.h"

extern int errno; 

using namespace std; 

namespace danzer {

// This module reads layouts of a single file, generates and pushes tasks into corresponding Task_queue. If Task_queue is full, it sends tasks to Reader Process through MPI_SEND command. 
void Dedupe::layout_analysis(filesystem::directory_entry entry, vector<vector<object_task*>> &task_queue){
//void layout_analysis(fs::directory_entry entry){
//int main(){
	int rc[5];
	uint64_t count = 0, size, start, end, idx, offset, interval, file_size;  
	struct stat64 sb; 
	object_task * task;
	int worldSize = this->worldSize; 
	int readerNum = 10; // TODO should be checked during runtime

	//vector <vector <struct object_task*>> task_queue(OST_NUMBER); 
	
	static int file_idx = 0;
	int obj_idx = 0; 

	// Get layouts of the file 
	struct llapi_layout * layout; 
	layout = llapi_layout_get_by_path(entry.path().c_str(), 0); 
	if (layout == NULL){
		cout << "errno:" << errno; 
		return; 
		// continue; 
	}
	
	// Get size of the file. 
	stat64(entry.path().c_str(), &sb);
	file_size = sb.st_size; 

	total_file_size += file_size; 


	// Get first layout of the file 
	rc[0] = llapi_layout_comp_use(layout, 1);
	if (rc[0]){
		cout << "error: layout component iteration failed\n";
		return; 
		//continue; 
	}
	while(1){

		// Get stripe count, stripe size, and range(in terms of the size) of the layout. 
		rc[0] = llapi_layout_stripe_count_get(layout, &count);
		rc[1] = llapi_layout_stripe_size_get(layout, &size); 
		//if (size != 1048576)
		//	this->non_default_pfl ++; 
		rc[2] = llapi_layout_comp_extent_get(layout, &start, &end);
		if (rc[0] || rc[1] || rc[2]) 
		{
			cout << "error: cannot get stripe information\n"; 
			continue; 
		}

		
		interval = count * size; 
		end = (end < file_size)? end : file_size; 
		// For each stripe, Get OST index and create Task that contains the information about what to read that will be sent to each Reader Process 
		for (int i = 0; i < count; i ++)
		{	
			// Get OST index 
			rc[0] = llapi_layout_ost_index_get(layout, i, &idx); 
			// If OST information cannot be taken, it means that we get all the OST idx that stores the object of the file and we can break out of the loop. 
			if (rc[0]){
				//cout << "error: cannot get ost index\n"; 
				// TODO: Change log message 
				goto here_exit; 
			}
			
			// Generate the task and push it into corresponding task_queue (We allocate queue as much as the number of the OST). 
			task = object_task_generate(entry.path().c_str(), idx, start + i * size, end, interval, size);
			this->task_cnt ++; 

			task_queue[task->ost].push_back(task); 
		
			// If task queue is full, then we send the tasks in the queue to corresponding Reader Process. 
			if (task_queue[task->ost].size() == TASK_QUEUE_FULL)
			{
				
				char * Msg = object_task_queue_clear(task_queue[task->ost], NULL); 
				
				// Send Msg to Read Processes(whose rank is OST_NUMBER & Read_Process_Num)
				// MPI_SEND
				//int rc = MPI_SUCCESS; 
				int rc = MPI_Ssend(Msg, sizeof(object_task) * TASK_QUEUE_FULL, MPI_CHAR, task->ost % (worldSize-1) + 1, TASK_QUEUE_FULL, MPI_COMM_WORLD); 
				if (rc != MPI_SUCCESS)
					cout << "MPI Send Failed\n"; 
				
				//free(Msg); 
			//	delete(Msg); 
				delete[] Msg;

			}	
		}
		rc[0] = llapi_layout_comp_use (layout, 3); 
		if (rc[0] == 0)
			continue; 
		if (rc[0] < 0)
			cout << "error: layout component iteration failed\n"; 
		break; 
	}
	here_exit:
		return; 
		//continue;
}

void Dedupe::layout_end_of_process(vector<vector<object_task*>> &task_queue){
	int task_num;
	int worldSize = this->worldSize;
	char termination_task[20];
	strcpy(termination_task, TERMINATION_MSG);

	for (int ost = 0; ost < OST_NUMBER; ost++)
	{
		char * Msg = object_task_queue_clear(task_queue[ost], &task_num); 
		if(Msg == nullptr){
			cout << "Msg is NULL\n";
			continue;
		}	
		int rc = MPI_Ssend(Msg, sizeof(object_task) * TASK_QUEUE_FULL, MPI_CHAR, ost % (worldSize-1) + 1, task_num, MPI_COMM_WORLD); 
		if (rc != MPI_SUCCESS)
			cout << "MPI Send Failed\n";
		
		object_task_buffer_free(Msg); 

	}
	for(int i=1; i < worldSize; i++) {
    		MPI_Send(termination_task, sizeof(termination_task), MPI_CHAR, i, 0, MPI_COMM_WORLD);
		cout << "termination msg sent\n";
	}
}

// Push IDX th BUFFER into Large MSG data structure. BUFFER is at the IDX th position on the Msg 
// BUFFER is the serialized string of the struct object_task. 
void Dedupe::Msg_Push(char * buffer, char * Msg, int idx){
	memcpy(Msg + idx * sizeof(object_task), buffer, sizeof(object_task)); 
}

char * Dedupe::object_task_queue_clear(vector<object_task*> &task_queue, int *task_num){
	char * Msg = new char[sizeof(object_task) * TASK_QUEUE_FULL];
	if (Msg == nullptr){
		cout << "Memory allocation error\n"; 
		return nullptr;
	}

	if (task_num != nullptr)
		*task_num = task_queue.size(); 

	char* buffer = new char[sizeof(object_task)];
	if(buffer == nullptr){
		cout << "Memory allocation error\n";
		return nullptr;
	}	

	int idx = 0; 
	while (!task_queue.empty()){
		object_task * task = task_queue.back(); 
		task_queue.pop_back(); 

		object_task_serialization(task, buffer); 
		Msg_Push(buffer, Msg, idx); 
		idx ++; 
	}
	delete[] buffer;

	return Msg; 
}

void Dedupe::object_task_buffer_free (char * buffer){
	delete[] buffer; 
}

// Serialize struct OBJECT_TASK into string so that it can be sent by MPI_SEND command.
// Each element is delimited by ,(comma) and ENDOF struct is marked by '\n'
void Dedupe:: object_task_serialization(object_task* task, char * buffer)
//string  object_task_serialization(struct object_task* task)
{
	int filepath_len = strlen(task->file_path); 

	int * p = (int *)buffer; 
	*p = task->ost; p++; 
	
	uint64_t *q = (uint64_t*)p; 
	*q = task->start; q++; 
	*q = task->end; q++; 
	*q = task->interval; q++; 
	*q = task->size; q++; 

	char *r = (char*)q; 
	for (int i = 0; i < filepath_len; i ++)
	{
		*r = task->file_path[i]; 
		r ++; 
	}
	*r = 0; 
	
}

object_task * Dedupe::object_task_deserialization(const char* buffer){
	object_task * task = new object_task; 
	//const char * buf = buffer.c_str()d`; 
	
	int *p = (int *)buffer;
	task->ost = *p; p ++; 

	uint64_t *q = (uint64_t*)p; 
	task->start = *q; q++; 
	task->end = *q; q++; 
	task->interval = *q; q++; 
	task->size = *q; q++; 

	memcpy(task->file_path, q, MAX_FILE_PATH_LEN); 
	//string file_path((char*)q);  
	//task->file_path = file_path; 


	//printf("%d %d %d %d \n", task->ost, task->start, task->end, task->interval) ; 
	//cout << task->file_path << "\n" << "hello world!";

	return task; 
}

void Dedupe::object_task_insert(object_task* task, vector<object_task*> queue){
	queue.push_back(task); 	
	return; 
}

object_task* Dedupe::object_task_generate(const char * file_path, int ost, uint64_t start, uint64_t end, uint64_t interval, uint64_t size){
	// task should be deleted after completing the task on Reader process. 
	object_task *task = new object_task; 
	
	//memcpy(task->file_path, file_path, MAX_FILE_PATH_LEN); 
	memcpy(task->file_path, file_path, strlen(file_path)+1); 
	//task->file_path = file_path; 
	//
	task->ost = ost; 
	task->start = start; 
	task->end = end; 
	task->interval = interval; 
	task->size = size; 

	return task; 
}

};
