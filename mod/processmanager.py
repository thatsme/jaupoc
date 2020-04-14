

class Pmanager:
    def __init__(self):
        self.numactive = 0
        self.active_process = {}
        self.process_struct = {}
        self.running_process = []
        self.temp = {}        
    def remove(self, processid):
        pass
    
    def add(self, processstruct):
        self.active_process.update(processstruct)
                
    def remove(self, processid):
        self.active_process.pop(processid, None)
        self.running_process.remove(processid)
        self.numactive = len(self.running_process)
        
    def adddetailed(self, processid, pid=None, cycle=None, timestamp=None ):
        self.process_struct = {}
        self.process_struct["pid"] = pid
        self.process_struct["cycle"] = cycle
        self.process_struct["timestamp"] = timestamp
        self.temp = {}
        self.temp[processid] = self.process_struct
        ### Use update method on dictionary 
        self.add(self.temp)
        self.running_process.append(processid)
        self.numactive = len(self.running_process)
        
    def assignname(self, processid, processname):
        pass
    
    def getprocess(self):
        return self.active_process

    def printprocess(self):
        for k in self.active_process:
            print(f'Key {k} value {self.active_process[k]}')
    