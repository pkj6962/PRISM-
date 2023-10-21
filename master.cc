
#include "mpitracer.h"

extern int errno; 

using namespace std; 

namespace danzer {

// This module reads layouts of a single file, generates and pushes tasks into corresponding Task_queue. If Task_queue is full, it sends tasks to Reader Process through MPI_SEND command. 
void Dedupe::layout_analysis(filesystem::directory_entry entry, vector<vector<object_task*>> &task_queue){
//void layout_analysis(fs::directory_entry entry){
//int main(){
	
	// static vector<uint64_t> num_tasks_per_ost (OST_NUMBER, 0);
	
	chrono::high_resolution_clock::time_point start_time, end_time; 
	static int traverse_cnt = 0; 
	if (traverse_cnt == 0)
	{
		traverse_cnt ++; 
		cout << "Hello world!\n"; 
		start_time = chrono::high_resolution_clock::now();
	}
	int rc[5];
	uint64_t count = 0, size, start, end, idx, offset, interval, file_size;  
	struct stat64 sb; 
	object_task * task;
	int worldSize = this->worldSize; 
	int worker_number = worldSize - 1; 

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
				goto here_exit; 
			}
			// Generate the task and push it into corresponding task_queue (We allocate queue as much as the number of the OST). 
			
			task = object_task_generate(entry.path().c_str(), idx, start + i * size, end, interval, size);
			this->task_cnt ++; 

			int task_ost = task->ost;

			task_queue[task_ost].push_back(task); 

			//delete task; 
			
			// load balancing code 
			uint64_t object_task_size = (end - start) / count; 
			this->size_per_ost[task_ost] += object_task_size; 
			//this->size_per_rank[task->ost % (worldSize-1) + 1] += object_task_size; 
			
			int dest_rank, num_binded_worker; 
			
			// temp code: should be deleted right away 
			//worker_number = 1;
			
			if (OST_NUMBER >= worker_number)
				dest_rank = task_ost % worker_number + 1; 
			else // Otherwise, more than two worker process is binded to one OST, so tasks should be passed in round robin manner. 
			{	
				int remainder = task->ost < (worker_number % OST_NUMBER)? 1:0;
				num_binded_worker =  worker_number / OST_NUMBER + remainder;
				dest_rank = (num_tasks_per_ost[task_ost] % num_binded_worker) * OST_NUMBER + task_ost;   
				if (dest_rank == 0)
					dest_rank = worker_number; 

			}

			size_per_rank[dest_rank] += object_task_size;
			
			

			// If task queue is full, then we send the tasks in the queue to corresponding Reader Process. 
			
			if (!load_balance && task_queue[task_ost].size() == TASK_QUEUE_FULL)
			{
	
				char * Msg = object_task_queue_clear(task_queue[task_ost], NULL); 
				int dest_rank, num_binded_worker; 
				
				// IF the number of OST exceeds the number of worker process, then Each OST is binded only to one worker process. 
				if (OST_NUMBER >= worker_number)
					dest_rank = task_ost % worker_number + 1; 
				else // Otherwise, more than two worker process is binded to one OST, so tasks should be passed in round robin manner. 
				{	
					int remainder = task_ost < (worker_number % OST_NUMBER)? 1:0;
					num_binded_worker =  worker_number / OST_NUMBER + remainder;
					dest_rank = (num_tasks_per_ost[task_ost] % num_binded_worker) * OST_NUMBER + task_ost;   
					if (dest_rank == 0)
						dest_rank = worker_number; 
					


				}
				num_tasks_per_ost[task_ost] += 1; 
				

				// Send Msg to Read Processes(whose rank is OST_NUMBER & Read_Process_Num)
				// MPI_SEND
				
				// 실제 코드 
				int rc = MPI_Ssend(Msg, sizeof(object_task) * TASK_QUEUE_FULL, MPI_CHAR, dest_rank, TASK_QUEUE_FULL, MPI_COMM_WORLD); 
				
				
				// int rc = MPI_Ssend(Msg, sizeof(object_task) * TASK_QUEUE_FULL, MPI_CHAR, task->ost % (worldSize-1) + 1, TASK_QUEUE_FULL, MPI_COMM_WORLD); 
				if (rc != MPI_SUCCESS)
					cout << "MPI Send Failed\n"; 
			
				//free(Msg); 
			//	delete(Msg); 
				delete[] Msg;

			}	
			//delete task;
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
		
		int dest_rank, num_binded_worker; 
		int worker_number = worldSize - 1; 

		if (OST_NUMBER >= worker_number)
			dest_rank = ost % worker_number + 1; 
		else // Otherwise, more than two worker process is binded to one OST, so tasks should be passed in round robin manner. 
		{	
			int remainder = ost < (worker_number % OST_NUMBER)? 1:0;
			num_binded_worker =  worker_number / OST_NUMBER + remainder;
			dest_rank = (num_tasks_per_ost[ost] % num_binded_worker) * OST_NUMBER + ost;   
			
			if (dest_rank == 0)
				dest_rank = worker_number; 
		}	
		int rc = MPI_Ssend(Msg, sizeof(object_task) * task_num, MPI_CHAR, dest_rank, task_num, MPI_COMM_WORLD); 
		// int rc = MPI_Ssend(Msg, sizeof(object_task) * task_num, MPI_CHAR, ost % (worldSize-1) + 1, task_num, MPI_COMM_WORLD); 
		//int rc = MPI_Ssend(Msg, s	izeof(object_task) * TASK_QUEUE_FULL, MPI_CHAR, ost % (worldSize-1) + 1, task_num, MPI_COMM_WORLD); 
		if (rc != MPI_SUCCESS)
			cout << "MPI Send Failed\n";
		object_task_buffer_free(Msg); 

	}
	for(int i=1; i < worldSize; i++) {
    	MPI_Ssend(termination_task, sizeof(TERMINATION_MSG), MPI_CHAR, i, 0, MPI_COMM_WORLD);
		cout << "termination msg sent\n";
	}

	
	string ost_size_distribution = "ost_size_distribution.eval";  
	ofstream ofs(ost_size_distribution, ios::app); 
	if (!ofs)
	{
		cerr << "Error opening output file\n"; 
		exit(0); 
	}

	ofs << Dataset << '\n'; 
	for (int i = 0; i < OST_NUMBER; i++)
	{
		ofs << size_per_ost[i] << endl; 
	}

}

// Push IDX th BUFFER into Large MSG data structure. BUFFER is at the IDX th position on the Msg 
// BUFFER is the serialized string of the struct object_task. 
void Dedupe::Msg_Push(char * buffer, char * Msg, int idx){
	memcpy(Msg + idx * sizeof(object_task), buffer, sizeof(object_task)); 
}


/*
현재 로드 발란싱의 한계 
1. 워커 프로세스의 개수가 24개 미만(12개 이하)의 경우에 대해서만 유효한 구현 방식 
2. MPI_Send로 큐에 저장된 데이터를  한꺼번에 보내는 방식은 부담. 
*/




bool Dedupe:: taskQueueCompare (const object_task* task1, const object_task * task2)
{
	uint64_t task_size1 = (task1->end - task1->start) / (task1->interval / task1->size); 
	uint64_t task_size2 = (task2->end - task2->start) / (task2->interval / task2->size); 
	
	return task_size1 > task_size2; 
}



void  Dedupe:: object_task_load_balance(vector<vector<object_task*>>& task_queue)
{
	uint64_t ost_size_mean = 0; 
	for (int i = 0; i < OST_NUMBER; i ++)
	{
		ost_size_mean += size_per_ost[i]; 
	}
	ost_size_mean /= OST_NUMBER; 

	
	vector <int> highGroup; 
	auto compareSecondElement =
		[](const std::pair<int, uint64_t>& p1, const std::pair<int, uint64_t>& p2) {
				return p1.second > p2.second; 
				// ">" for ascending order, "<" for descending order
		};

	priority_queue<pair<int, uint64_t>, vector<pair<int, uint64_t>>, decltype(compareSecondElement)>	 lowGroup(compareSecondElement);


	for (int i = 0; i < OST_NUMBER; i ++)
	{
		if (size_per_ost[i] < ost_size_mean) 
		{	
			auto p = make_pair(i, size_per_ost[i]); 
			lowGroup.push(p); 
		}
		else
		{
			highGroup.push_back(i); 
		}
	}




	// 검증
	
	printf("mean: %lld\n", ost_size_mean); 
	/*
	uint64_t stddev = 0; 
	printf("high group:\n"); 
	for (int idx:highGroup)
	{
		printf("%d\t%lld\n", idx, size_per_ost[idx]);  

		stddev += (size_per_ost[idx] - ost_size_mean) * (size_per_ost[idx] - ost_size_mean); 
		
		sort(task_queue[idx].begin(), task_queue[idx].end(), taskQueueCompare); 
		
	}

	printf("low group:\n"); 
	while (!lowGroup.empty())
	{
		auto p = lowGroup.top(); 
		lowGroup.pop(); 

		printf("%d\t%lld\n", p.first, p.second); 
		
		uint64_t dev = p.second - ost_size_mean;
		stddev += dev * dev ;

		//lowGroup.push(p); 
	}

	stddev /= OST_NUMBER; 
	printf("stddev: %lld\n", stddev); 
	*/
	
	int low_idx;
	uint64_t low_ost_size;
	int count = 0; 
	for (int idx:highGroup)
	{
		//while (1)
		sort(task_queue[idx].begin(), task_queue[idx].end(), taskQueueCompare); 
		while (size_per_ost[idx] > ost_size_mean)
		{
			
			if (task_queue[idx].empty())
			{
				puts("task_queue is empty");
				break; 
			}
			auto task = task_queue[idx].back();
			task_queue[idx].pop_back(); 
			

			// this is the false task: contain empty job
			if (task->start > task->end)
			{
				continue; 
			}

			uint64_t object_task_size = (task->end - task->start) / (task->interval / task->size); 
			//printf("%lld %lld %lld %lld %lld\n", object_task_size, task->end, task->start, task->interval, task->size); 


			// 만약 지금 task를 꺼내서 task_queue의 전체 크기가 평균 미만으로 떨어지면 다시 그 task를 넣고 break함. 
			if (size_per_ost[idx] - object_task_size < ost_size_mean)
			{
				task_queue[idx].push_back(task); 
				break; 
			}
		
			auto p = lowGroup.top();
			lowGroup.pop(); 

			low_idx = p.first; 
			low_ost_size = p.second; 

			// 만약 지금 Task를 lowGroup의 task_queue에 넣어서 그 큐의 전체 큐기가 평균 이상으로 올라가면 원래 큐에 넣고 break함.
			if (low_ost_size + object_task_size > ost_size_mean)
			{
				task_queue[idx].push_back(task); 
				lowGroup.push(p); 
				break; 
			}
			
			// comm Thread 구현상 task의 ost를 바꿔주고 넘겨줘야 함.
			task->ost = low_idx; 
			
			task_queue[low_idx].push_back(task); 
			low_ost_size += object_task_size; 
			size_per_ost[idx] -= object_task_size; 
			
	
			p.second = low_ost_size; 
			lowGroup.push(p); 
			count ++; 
			
		}
	} // end of for loop
	
	printf("cnt: %d\n", count); 
	printf("high group:\n"); 


	int64_t stddev=0; 
	int64_t dev, dev_mb; 
	for (int idx: highGroup)
	{
		printf("%d\t %lld\n", idx, size_per_ost[idx]); 
		dev = size_per_ost[idx] - ost_size_mean; 
		dev_mb = dev / 1048576; 
		
		//printf("dev: %lld %lld %lld  %lld\n", size_per_ost[idx], ost_size_mean, dev, dev_mb); 
		
		stddev += dev_mb * dev_mb; 
	}

	printf("low group: %d\n", lowGroup.size()); 
	while (!lowGroup.empty())
	{
		auto p = lowGroup.top(); 
		lowGroup.pop(); 

		dev = p.second - ost_size_mean; 
		dev_mb = dev / 1048576; 
		stddev += dev_mb * dev_mb; 
		
		printf("%d\t%lld\n", p.first, p.second); 
	}

	stddev = stddev / OST_NUMBER; 

	printf("stddev: %lld MB\n", stddev); 

	
	int task_num;
	printf("task count\n"); 
	for (int i = 0 ; i < OST_NUMBER; i++)
	{
		printf("ost\t%d\t%d\n", i, task_queue[i].size()); 
		while (task_queue[i].size() >= TASK_QUEUE_FULL)
		{
			
			char * Msg = object_task_queue_clear (task_queue[i], &task_num); 

			int dest_rank; 
			int worker_number = worldSize - 1; 
			if (OST_NUMBER >= worker_number)
				dest_rank = i % worker_number + 1; 
			int rc = MPI_Ssend(Msg, sizeof(object_task) * task_num, MPI_CHAR, dest_rank, task_num, MPI_COMM_WORLD); 
			if (rc != MPI_SUCCESS)
				cout << "MPI Send Failed\n"; 
			
		   }
		
	}
	
	

	/*
		ddwhile taskqueue is not empty 
			msg = object_task_queue_clear (taskqueue, )

			dest_rank <-  
		
			MPI_Send(msg)

	


		char * Msg = object_task_queue_clear(task_queue[i], &task_num);
		
		printf("task_num: %d\n", task_num); 
		int rc = MPI_Send(Msg, sizeof(object_task) * task_num, MPI_CHAR, rank, task_num, MPI_COMM_WORLD);
		if (rc != MPI_SUCCESS)
			cout << "MPI Send Failed\n";
	*/


	
/*
	






검증: 

for idx in largerThanMean
	print(size_per_ost[idx])
	stddev <- pow(size_per_ost[idx] - mean) 

while !smallerThanMean.empty()
	p = smallerThanMean.pop() 
	print(p.second)
	stddev <- pow(size_per_ost[idx] - mean)

stddev /= OST_NUMBER
for idx in largerThanMean
	while size_per_ost[idx] > mean
		task = task_queue[idx].pop

		object_task_size <- task로부터 size 구하기 
	
		object_task_size = (task.end - task.start) / (task.interval / task.size) 

		size_per_ost[idx] -= object_task_size

		p = smallerThanMean.pop()
		idx2 = p->first
		size = p->second

		task_queue[idx2].push(task)

		size
		object_task_size = task


		size += objct_task_size

		p->second = size

		smallerThanMean.push(p)


	*/ 

	puts("vv"); 
}


/*
구현 알고리즘 
1. mean <- size_per_ost 의 평균을 구함.


2.

vector <int> highGroup; 
auto compareSecondElement =
	[](const std::pair<int, uint64_t>& p1, const std::pair<int, uint64_t>& p2) {
			return p1.second > p2.second; 
			// ">" for ascending order, "<" for descending order
	};

priority_queue<pair<int, uint64_t>, vector<pair<int, uint64_t>>, decltype(compareSecondElement)>	 lowGroup(compareSecondElement);


for (int i = 0; i < OST_NUMBER; i ++)
{
	if (size_per_ost[i] < ost_size_mean) 
	{	
		auto p = make_pair(i, size_per_ost[i]); 
		lowGroup.push(p); 
	}
	else
	{
		highGroup.push(i); 
	}
}



for i = 0 to OST_NUMBER
	if size_per_ost[i] < mean 
		largerThanMean.push(i) <- array
	else
		smallerThanMean.push(i) <- priority Queue


for idx in largerThanMean
	while size_per_ost[idx] > mean
		task = task_queue[idx].pop

		object_task_size <- task로부터 size 구하기 
		size_per_ost[idx] -= object_task_size

		p = smallerThanMean.pop()
		idx2 = p->first
		size = p->second

		task_queue[idx2].push(task)
		size += objct_task_size

		p->second = size

		smallerThanMean.push(p)


	
for each taskQueue 
	MPI_Send to Binded worker process.





검증: 

for idx in largerThanMean
	print(size_per_ost[idx])
	stddev <- pow(size_per_ost[idx] - mean) 

while !smallerThanMean.empty()
	p = smallerThanMean.pop() 
	print(p.second)
	stddev <- pow(size_per_ost[idx] - mean)

stddev /= OST_NUMBER

print(stddev)



Bin Packing Algorithm

while size_per_ost[i] 가 mean에 수렴할 때까지
	가장 큰 OST i 의 작업 task_queue[i]을 빼서 가장 작은 OST j의 작업큐 task_queue[]에 넣음.
	각각의 size_per_ost[i], size_per_ost[j]를 조정함. 


평균보다 높은 OST의 집합: 작업을 오프로딩해야 하는 작업큐   -> 배열 
평균보다 낮은 OST의 집합: 작업을 오프로딩받아야 하는 작업큐  -> 우선순위 큐 







for 평균보다 높은 OST의 배열 
	while 해당 배열이 평균에 수렴할 때까지 
		p = 우선순위큐.pop
		j = p.first

		size_per_ost[i] -= object_task_size; 
		p.second += object_task_size
		
		task = task_queue[i].pop();
		task_queue[j].push(task)
		
		우선순위큐.push(p)  




*/   


/*


// Load Balancing code
void  Dedupe:: object_task_load_balance(vector<vector<object_task*>>& task_queue)
{
	auto compareSecondElement =
			[](const std::pair<int, uint64_t>& p1, const std::pair<int, uint64_t>& p2) {
					        return p1.second > p2.second; 
							// ">" for ascending order, "<" for descending order
			};
	priority_queue<pair<int, uint64_t>, vector<pair<int, uint64_t>>, decltype(compareSecondElement)> total_size_per_rank(compareSecondElement);
	
	for (int i = 1; i < worldSize; i++)
	{
		auto p = make_pair(i, 0);
		total_size_per_rank.push(p);
	}
	// Sort Task bucket per OST in order of size ascending order 
	sort(this->size_per_ost, this->size_per_ost + OST_NUMBER, greater<uint64_t>());
	
	int task_num;
		  
	// Distribute tasks from each OST to the rank whose size is minimal. 
	for (int i = 0; i < OST_NUMBER; i++)
	{
		// Get the rank which contains least size of tasks.
		pair<int, uint64_t> p = total_size_per_rank.top();
		total_size_per_rank.pop();
		int rank = p.first;

		char * Msg = object_task_queue_clear(task_queue[i], &task_num);
		
		printf("task_num: %d\n", task_num); 
		int rc = MPI_Send(Msg, sizeof(object_task) * task_num, MPI_CHAR, rank, task_num, MPI_COMM_WORLD);
		if (rc != MPI_SUCCESS)
			cout << "MPI Send Failed\n";

		p.second += this->size_per_ost[i];
		total_size_per_rank.push(p); 
	}
	
	// Test Code 
	/*
	printf("per ost\n"); 
	for(int i = 0; i < OST_NUMBER; i++)
	{
		printf("ost\t%d\t%lld\n", i, size_per_ost[i]); 
	}


	 printf("Before Load Balance\n");
	 for (int i = 1; i <= this->worldSize-1; i++)
	{
		printf("rank\t%dsize\t%llu\n", i, this->size_per_rank[i]);
	}
	
	 printf("After Load Balance\n"); 
	 for(int i = 0; i < this->worldSize-1; i++)
	 {
		pair<int, uint64_t> p = total_size_per_rank.top();
		total_size_per_rank.pop();
										
		printf("rank\t%d\tsize\t%llu\n", p.first, p.second);
	 }// test purpose code
	*/

/*
}

*/

char * Dedupe::object_task_queue_clear(vector<object_task*> &task_queue, int *task_num){
	
	//char * Msg = new char[sizeof(object_task) * task_queue.size()]; 
	char * Msg = new char[sizeof(object_task) * TASK_QUEUE_FULL];
	if (Msg == nullptr){
		cout << "Memory allocation error\n"; 
		return nullptr;
	}

	if (task_num != nullptr){
		//*task_num = task_queue.size(); 
		
		// test code 
		
		if (task_queue.size() < TASK_QUEUE_FULL)
			*task_num = task_queue.size(); 
		else
			*task_num = TASK_QUEUE_FULL; 

	}
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
		
		// Test code : should be deleted right away 
		if (idx == TASK_QUEUE_FULL) break; 
	
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

