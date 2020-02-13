# Product Name

TECHNICAL SKILLS ASSESSMENT 
EQ Works - Data Engineer


## Installation


OS X & Linux:

```sh
pip install -r requirements.txt 

```


## Usage example


#### Pipeline Dependency

Looking to implement a pipeline dependency tool.
The implementation of a solution that must satisfy both necessity and sufficiency -- if a task is not a prerequisite task of goal, or its task is a prerequisite task for starting tasks (already been executed), then it shouldn't be included in the path. 

The path needs to follow a correct topological ordering of the DAG, hence a task needs to be placed behind all its necessary prerequisite tasks in the path.
For example, we have 6 tasks [A, B, C, D, E, F], C depends on A (denoted as A->C), B->C, C->E, E->F. A new job has at least 2 tasks and at most 6 tasks, each task can only appear once.

Examples:

Inputs: starting task: A, goal task: F, output: A,B,C,E,F or B,A,C,E,F.
Input: starting task: A,C, goal task:'F', outputs: C,E,F.





```bash
# Checking start and goal detailed in data/question.txt using relationships from data/relationships.txt 
python pipeline_script.py --relationships data/relations.txt --eval_nodes data/question.txt --tasks data/task_ids.txt

```




## Meta

Bernardo Sandi – bsandi1220@gmail.com


