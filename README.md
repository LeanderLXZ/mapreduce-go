# mapreduce
- nReduce  = number of intermediate files
- master map:
    - one list stores rest files
    - one list stores working files(with the worker who working for this file)
    - when a worker request a task, master give a file from the rest lists,and record it into working files
    - when a worker report failed, master find the file in working files, and put it back to rest files. 

    - after worker report failed or finished, master will tell worker to wait, when worker in wait, it will constant request task from master.

    - ::when a task work over 10 second, master cancel the task, put the file back to rest files (should I cancel the worker too? how I count the task time to 10 second?):: use a timer to count
    - when this two lists has been empty, the process of map end.

    - a list stores unoccupied workers(seems don't need a worker list, I can use taskList to record which worker is working right now)
        - after a worker request, put it in
        - after a worker report msg = success/fail, put it in
        

