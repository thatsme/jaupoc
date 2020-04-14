from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory
from prompt_toolkit.completion import WordCompleter
import logging
import redis
import json
import pprint
import sys
import time
pp = pprint.PrettyPrinter(indent=2)
MASTER_CHANNEL = "master"
CMD_CHANNEL = "commandline"
message_struct = {}
logname = "client.log"
logging.basicConfig(filename=logname,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)
logger = logging.getLogger('programLog')

def runaction(cmdlist):
    ncommands = len(cmdlist)
    if ncommands < 2:
        return False
    
    maction = cmdlist[1].strip()
    if(maction=="spawn"):
        if(ncommands >=3):
            nprocess = cmdlist[2]
            message_struct["numofprocess"] = nprocess
        else:
            return False
        ## Third value in cmdlist number of polls before exit parameter
        if(ncommands >= 4):
            npolls = cmdlist[3]
        else:
            # Default Infinite, no exit  
            npolls = 0
        message_struct["numofpolls"] = npolls
        ## Forth value in cmdlist polldelay parameter
        if(ncommands >= 5):
            polldelay = cmdlist[4]
        else:
            # Default 1 second
            polldelay = 1
        message_struct["polldelay"] = polldelay
        
    elif(maction=="shutdown"):
        if(ncommands >=3):
            process = cmdlist[2]
            message_struct["processtoshutdown"] = process
        else:
            return False
        
    elif(maction=="listactive"):
        pass
    else:
        return False
    
    message_struct["action"] = maction
    message_struct["sender"] = CMD_CHANNEL        
    try:
        r.publish(mprocess, dict_to_string(message_struct))
    except:
        print("opppss publish error")
    
    ## Just get the first response, we have to modify, like info zone, or just 
    ## avoid this, maybe we try with some like 5 seconds blocking loop 
    t_end = time.time() + 5 
    while time.time() < t_end:
        try:
            message = p.get_message()
        except:
            print("problem in read message")
            
        if message:
            mydata = string_to_dict(message["data"])
            return mydata
            if(mydata["action"]=="response"):
                pp.pprint(mydata)
            elif(mydata["action"]=="activeprocesslist"):
                return mydata                
    return True

def getqueue():
    pass

def dict_to_string(the_dict):
    mstring = json.dumps(the_dict)
    return mstring

def string_to_dict(the_string):
    d = json.loads(the_string)  
    return d

try:
    r = redis.Redis(host='localhost', port=6379, db=0)
    p = r.pubsub(ignore_subscribe_messages=True)
    p.subscribe('commandline')
except:
    print("opssss Redis problem")
    
active_process = []    
PROCESSCompleter = WordCompleter(active_process,
                    ignore_case=True)
while 1:
    user_input = prompt('>',
                        history=FileHistory('history.txt'),
                        completer=PROCESSCompleter,
                       )
    if(user_input==""):
        continue
    command = user_input.split(" ")
    #print(command)
    mprocess = command[0]
    if(mprocess=="master"):
        #maction = command[1]
        print("== Use <info> to check response from long run process ============================================")
        afteraction = runaction(command)
        #print(afteraction)
        if(afteraction==True):
            pp.pprint(afteraction)            
        elif(afteraction["action"]=="activeprocesslist"):
            active_process = list(afteraction["data"].keys())
            PROCESSCompleter = WordCompleter(active_process,
                             ignore_case=True)
            print(active_process)
        else:
            pp.pprint(afteraction)
            
    elif(mprocess=="info"):
        ## Waiting info from master for 10 seconds
        t_end = time.time() + 10 
        while time.time() < t_end:
            try:
                message = p.get_message()
            except:
                print("problem in read message")
                
            if message:
                mydata = string_to_dict(message["data"])
                pp.pprint(mydata)
                ## Clear user_input
                user_input = ""
                
    elif(mprocess=="exit"):
        sys.exit(0)
    elif(mprocess=="?"):
        print("master spawn <numprocess> <numping> <delayping>  ## numping=0 for no exit")
        print("master listactive  ## list of active spawned process by id")
        print("master shutdown <processid>  ## all|<processid>")
        print("master setname <processid> <processname>  ## to be implemented")
        print("exit ## exit from command line")
        print("? ## this help")
        print("\nFor getting process name autocomplete feature, u need to call listactive")
        
    else:          
        print(user_input)
        # Clear user_input
        user_input = ""