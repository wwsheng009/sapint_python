'''
Created on 2014-4-19

@author: wangweisheng
'''
import sys
import gc
import psutil
import os
sys.path.append('D:\java\workspace\sapintpy')
import sapint
proc = psutil.Process(os.getpid())
from sapint.tableutil.ReadTable import CReadTable
    

#def startCountMemory():
    ##==============================================
    #gc.collect()
    #mem0 = proc.get_memory_info().rss
    
    ##==============================================
#def endofCountMemory():
    ##==============================================
    #mem1 = proc.get_memory_info().rss

    
    #pd = lambda x2, x1: 100.0 * (x2 - x1) / mem0

    #print("Allocation: %0.2f%%" % pd(mem1, mem0))        
    ##==============================================
def test_increase():
    table = CReadTable("AIP")
    table.TableName = "MAKT"
    table.RowCount = 10
    table.Run()
    result = table.GetResult()
    headers = table.GetFields()
def testCase():
    gc.collect()
    mem0 = proc.get_memory_info().rss    
    
    table = CReadTable("AIP")
    table.TableName = "MAKT"
    table.RowCount = 10
    table.Run()
    result = table.GetResult()
    headers = table.GetFields()
    
    
    mem1 = proc.get_memory_info().rss
    pd = lambda x2, x1: 100.0 * (x2 - x1) / mem0
    
    #table.clearAll()
    return table
    #del table,result,headers
    mem2 = proc.get_memory_info().rss
    gc.collect()
    mem3 = proc.get_memory_info().rss
    
    pd = lambda x2, x1: 100.0 * (x2 - x1) / mem0
    print("Allocation: %0.2f%%" % pd(mem1, mem0))
    print("Unreference: %0.2f%%" % pd(mem2, mem1))
    print("Collect: %0.2f%%" % pd(mem3, mem2))
    print("Overall: %0.2f%%" % pd(mem3, mem0))
    
    
    
    #printMemoryCount()
    
def printMemoryCount():
    result = {} 
    for o in gc.get_objects(): 
        t = type(o)
        #print(t)
        count = result.get(t, 0) 
        result[t] = count + 1 
    
    for k,v in result.items():
        print(k,v)

    return result

if __name__ == "__main__":
    from pympler import tracker
    import objgraph
    #import pdb
    #pdb.set_trace()
    memory_tracker = tracker.SummaryTracker()
    objgraph.show_growth(limit=10)
    #startCountMemory()
    table = testCase()
    memory_tracker.print_diff()
    #testCase()
    
    #testCase()

    #printMemoryCount()
    #objgraph.show_growth(limit=10)
    import inspect, random
    
    #objgraph.show_chain(
        #objgraph.find_backref_chain(
            #random.choice(objgraph.by_type('list')),
            #inspect.ismodule),
        #filename='chain.png')
    
    
    objgraph.show_most_common_types()
    objgraph.show_growth(limit=10)
    objgraph.show_backrefs([table], filename="objgraph.dot")