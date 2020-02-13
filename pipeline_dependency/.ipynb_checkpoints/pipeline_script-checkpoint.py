#!/usr/bin/env python3

import pipeline_functions.pipeline_functions as pipeline
import argparse
import time
import pandas as pd
import numpy as np 
import csv
import pandas.api.types as ptypes

'''
The present script helps to determine the pipeline dependencies 
given a set of tasks and relationships  


Usage Example:

- Get dependencies detailed in eval_nodes txt file associated, using dependencies specified in relationships txt file associated
python pipeline_script.py --relationships data/relations.txt --eval_nodes data/question.txt --tasks data/task_ids.txt


- Get dependencies detailed in eval_nodes txt file associated, using dependencies specified in relationships txt file associated
python pipeline_script.py --relationships data/relations.txt --eval_nodes data/question.txt --tasks data/task_ids.txt --save_csv

'''



if __name__=='__main__':
    
    parser = argparse.ArgumentParser()
    
    # Input arguments 
    parser.add_argument(
        '--relationships',
        help = 'Path to relationships',
        required = True
    )
    parser.add_argument(
        '--tasks',
        help = 'Path to tasks ids',
        required = True
    )
    parser.add_argument(
        '--eval_nodes',
        help = 'Path to start and end nodes',
        required = True
    )

    parser.add_argument(
        '--save_csv',
        help = 'save DataFrame as a csv file',
        action='store_true'
    )


    args = parser.parse_args()
    arguments = args.__dict__

    
    relationships_path = arguments.pop('relationships')    
    tasks_path = arguments.pop('tasks')  
    eval_nodes_path = arguments.pop('eval_nodes')  
    save_csv = arguments.pop('save_csv')




    # Get start and end nodes 
    # If there is more than one initial node please separate them using ','
    request = pd.read_csv(eval_nodes_path,sep=':', header=None, index_col=0, names=['Vertex'])
    
    start = request.loc['starting task','Vertex']
    if not isinstance(start,np.int64):
        start = [int(i) for i in request.loc['starting task','Vertex'].split(',')]
    else:
        start = [int(start)]
    
    goal = int(request.loc['goal task','Vertex'])
    
    
    
    # Get relationships
    relations = pd.read_csv(relationships_path,sep='->', header=None, names=['scr','dest'], engine='python')

    
    # Get list of tasks
    with open('data/task_ids.txt',mode='r') as file:
        tasks = [int(i) for i in file.readline().split(',')]
    
    # Check the datatypes before looking for dependencies
    assert(all(ptypes.is_numeric_dtype(relations[col]) for col in ['scr','dest'])) , 'Please check data types in relationships data'    
    assert(isinstance(goal,int)) , 'Please check goal data type'
    
    for i in start:
        assert(isinstance(i,int)), 'Please check start nodes data type' 
    assert(goal in tasks), 'goal is not in available tasks'
    

    
    # Check for dependencies 
    dependencies = (pipeline.dependency_pipeline(goal,relations,start,[]))

    # Check if it is a valid task
    if len(dependencies) > len(tasks) or len(dependencies) < 2:
        print('Not a valid task, review parameters')
        exit()
        
    # Save as csv
    if save_csv:
        with open('dependencies_to_{}_{}.csv'.format(goal,time.strftime("%Y%m%d-%H:%M:%S")), 'x', newline='') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            wr.writerow(dependencies)
    
    print(dependencies)