

def dependency_pipeline(node, relations, initial=[], l=[], ):
    '''
    Check for direct and indirect dependencies of node according to relations
    until a node has no more dependencies or it is in the inital set of nodes

    Parameters
    ----------
    node: value of node to check for dependencies  
    relations: Dataframe containing the source and destination relationships 
    initial: starting tasks
    l: list to cache the dependencies already checked

    
    Returns
    -------
    list of dependencies ordered from bottom to top 
    
    
    '''
    dependencies = get_dependencies(node, relations)
    missing_dependencies = (set(dependencies)-set(l)) -set(initial)
    
    # Base case descendants == initial or descendants == None 
    if len(missing_dependencies)==0:
        return initial
    
    
    # Recursive function 
    else:
            
        for val in missing_dependencies:

            if val not in l:
                ls = dependency_pipeline(val,relations,initial, l)
                [ls.remove(item) for item in l if item in ls]
                l = l + ls
                l.append(val)

        return l
    
    

def get_dependencies(node, relations):
    '''
    Returns a list with the direct dependencies of node according to the 
    relationships specified in relations

    Parameters
    ----------
    node: value of node to check for dependencies 
    relations: Dataframe containing the source and destination relationships 

    
    Returns
    -------
    list with direct dependencies of node 
    
    
    '''

    return list(relations[relations['dest']==node]['scr'])