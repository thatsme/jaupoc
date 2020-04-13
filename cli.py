from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory
import logging
import redis
import json
import pprint
import sys
pp = pprint.PrettyPrinter(indent=2)

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
    
    maction = cmdlist[1]
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
    message_struct["sender"] = "commandline"        
    try:
        r.publish(mprocess, dict_to_string(message_struct))
    except:
        print("opppss publish error")
    while True:
        try:
            message = p.get_message()
        except:
            print("problem in read message")
            
        if message:
            mydata = string_to_dict(message["data"])
            return mydata
            break


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
    
while 1:
    user_input = prompt('>',
                        history=FileHistory('history.txt'),
                       )
    if(user_input==""):
        continue
    command = user_input.split(" ")
    #print(command)
    mprocess = command[0]
    if(mprocess=="master"):
        #maction = command[1]
        print("==============================================")
        afteraction = runaction(command)
        if(afteraction != False):
            pp.pprint(afteraction)
            
    elif(mprocess=="info"):
        while True:
            try:
                message = p.get_message()
            except:
                print("problem in read message")
                
            if message:
                mydata = string_to_dict(message["data"])
                pp.pprint(mydata)
                ## Clear user_input
                user_input = ""
                break
    elif(mprocess=="exit"):
        sys.exit(0)
    else:          
        print(user_input)
        # Clear user_input
        user_input = ""