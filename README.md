@[WenbinZhu](https://github.com/WenbinZhu)
# MIT6.824-labs
## Lab 1: MapReduce
### Tasks
1. Implement one master process and one or more worker processses executing in parallel.
2. The workers will talk to the master via RPC.
3. Each worker will ask the master for a task, read input from files, execute the task and write output to files.
4. The master will schedul the task to another worker when current worker hasn't complete its task in a reasonalbe time.
