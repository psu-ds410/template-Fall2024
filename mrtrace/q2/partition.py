import hashlib

def what_would_partitioner_do(idname, keys, numreducers):
    """ This function tells us what the partitioner would do in our 
    hypothetical setup for tracing through a mapreduce program.

    Suppose your psu id is abc1234, the number of reducers is 4 and you
    want to know which reducers will be assigned to the keys "hello", "world", and 3
    Then you would call this function like this:
    
    what_would_partitioner_do("abc1234", ["hi", "worlds", 5, "5", ["a", 7]], 4)
    
    and it will return you a list like [1, 1, 3, 2, 2] which means "hi" and "worlds" would be sent 
    to reducer 1, the number 5 to reducer 3 and the string "5" to reducer 2 and list ["a", 7] to
    reducer 2. 
    """
    multiple = isinstance(keys, list)
    keylist = keys if multiple else [keys] 
    result = [makehash(f"{idname}|{type(k)}|{k}") % numreducers for k in keylist]
    if multiple:
        return result
    else:
        return result[0]


def makehash(k):
    thehash = int(hashlib.sha1(bytes(str(k),'utf-8')).hexdigest(), 16) 
    return thehash
    
