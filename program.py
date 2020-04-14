from sys import argv
import subprocess
from subprocess import Popen
import uuid
import sys
import os
import ast
import time 
import logging
import redis
import datetime
from random import randint
from secrets import randbelow
import pprint
import argparse
import signal

pp = pprint.PrettyPrinter(indent=4)

def shutdown_process(plist):
    elist = []
    if(type(plist) is list):
        elist = plist
    else:
        elist.append(plist)
    
    message_struct = {}
    message_struct["action"] = "shutdown"
    message_struct["sender"] = MASTER_CHANNEL

    for sp in elist:
        ##
        print(f"Process to shudtown {sp}")
        try:
            r.publish(sp, ut.dict_to_string(message_struct))
        except:
            logger.debug(f"Error publish to child {sp}")         
            return False
        
    return True


time_start = str(datetime.datetime.now())
immaster = False
virtualenv = False
active_process = []
running_process = {}
process_struct = {}
process_struct["pid"] = ""
process_struct["cycle"] = ""
process_struct["timestamp"] = ""
activation_counter = 0
message_struct = {}
count = 0
MASTER_CHANNEL = "master"
CMD_CHANNEL = "commandline"
PYTHON = "python"

script = argv
pre = script[0]
if(pre[0:2]==".\\"):
    stripped = pre[2:]
else:
    stripped = pre
    
fullname = str(stripped).split(".")    
currentDirectory = os.getcwd()

parser = argparse.ArgumentParser()
parser.add_argument("npolls", nargs='?', default=0)
parser.add_argument("polldelay", nargs='?', default=1)
args = parser.parse_args()

#print(fullname)
name = fullname[0]
extension = fullname[1]
#print(name)
#print(extension)
if "_" in name:
    name, id = name.split("_")
else:   
    immaster = True
    
## OK .. i'm the master, doing some shit     
if(immaster):
    from mod import util as ut
    from mod.processmanager import Pmanager
    from mod.platformmanager import Platform
    logname = name+".log"

## Bulshit relative import, no separate directory for spawn process .. for now
## until I get this madness
else:
    from mod import util as ut
    from mod.processmanager import Pmanager
    from mod.platformmanager import Platform
    logname = name+"_"+id+".log"

#logging.info("Running Urban Planning")


pm = Pmanager()
pt = Platform()
print(pt.getplatform())

logging.basicConfig(filename=pt.logpath+logname,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)

logger = logging.getLogger('programLog')

try:
    r = redis.Redis(host='localhost', port=6379, db=0)
    p = r.pubsub(ignore_subscribe_messages=True)

except:
    logger.debug("Error with Redis server")
            
def signal_handler(sig, frame):
    print('You pressed Ctrl+C!')
    shutdown_process(active_process)
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

        
logger.debug('%s %s %s %s %s', name, "is master :", immaster, " Virtual : ", virtualenv)
logger.debug(currentDirectory)

#time.sleep(1000)
if(immaster):
    try:
        p.subscribe(MASTER_CHANNEL)
        # p.subscribe('commandline')
        logger.debug("Subscribed master")
    except:
        logger.debug("Error subscribing")

    pull_data = []
    
    completed = True            
    while True:
        try:
            message = p.get_message()
        except Exception as e:
            logger.debug("problem in read message")
            logger.debug(e)
            print(e)
            sys.exit()
            
        if message:
            logger.debug("\n\n")
            logger.debug("====================== server receive start ===============")
            logger.debug(f'Channel {message["channel"]}')
            logger.debug(f'Type {message["type"]}')            
            logger.debug(f'Data {message["data"]}')
            mydata = ut.string_to_dict(message["data"])
            logger.debug(f'Decoded data {mydata} Type {type(mydata)}')
            ##pp.pprint(mydata)
            logger.debug("====================== server receive end ==================\n\n")

            if(message["channel"]==b'master'):
                if(mydata["action"]=="ping" and mydata["sender"] in running_process.keys() ):
                    
                    ## Reach the cycle limit, send shutdown to child process
                    if(int(mydata["counter"])>=int(mydata["cycle"]) and int(mydata["cycle"])>0):
                        message_struct["action"] = "shutdown"
                        message_struct["sender"] = MASTER_CHANNEL
                        ##
                        try:
                            r.publish(mydata["sender"], ut.dict_to_string(message_struct))
                        except:
                            logger.debug(f"Error publish to child {mydata['sender']}")         
                    else:
                        message_struct["action"] = "acknowledge"
                        message_struct["sender"] = MASTER_CHANNEL
                        try:
                            r.publish(mydata["sender"], ut.dict_to_string(message_struct))
                        except:
                            logger.debug(f"Error publish to child {mydata['sender']}")         
                        
                        print(f"Pinged {mydata['counter']} {mydata['cycle']} {mydata['sender']}")     
                elif(mydata["action"]=="spawn"):
                    #print(mydata["numofprocess"])
                    #sys.exit()
                    mrange = int(mydata["numofprocess"])
                    for i in range(mrange):
                        id_to_run = str(uuid.uuid4())
                        tofile = currentDirectory+pt.spawndir+pt.slash+name+"_"+id_to_run+"."+extension
                        torun = name+"_"+id_to_run+"."+extension
                        fromfile = currentDirectory+pt.slash+name+"."+extension
                        deletespawnlog = "program_*.log"
                        deletespawnsource = "program_*.py"
                        
                        print(torun)
                        print(deletespawnlog)
                        print(deletespawnsource)
                        
                        logger.debug("=== Span section =======")
                        logger.debug(fromfile)
                        logger.debug(tofile)
                            
                        subprocess.run(["rm",deletespawnlog], cwd = pt.spawndir, shell=True)
                        #subprocess.run(["rm", deletespawnsource], shell=True)
                        
                        subprocess.run([pt.copycommand, fromfile, tofile], shell=True)
                        
                        # Spawn Popen args
                        margs = []
                        margs.append(PYTHON)
                        margs.append(torun)
                        margs.append(str(mydata["numofpolls"]))
                        margs.append(str(mydata["polldelay"]))
                        
                        # Python >= 3.3 has subprocess.DEVNULL
                        try:
                            preturn = Popen(margs, cwd=pt.spawndir, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                            #print(preturn)
                            #sts = os.waitpid(process.pid, 0)
                        except Exception as e:
                            print(e)
                            logger.debug(e)      
                                             
                        ## Acknowledge to command cli                 
                        message_struct["action"] = "response"
                        message_struct["sender"] = MASTER_CHANNEL     
                        message_struct["data"] = f" {id_to_run} spawn done"
                        try:
                            r.publish(mydata["sender"], ut.dict_to_string(message_struct))
                            logger.debug(f"published to {mydata['sender']}")
                        except Exception as e:
                            logger.debug(f"Error publish to {mydata['sender']}")                

                        #time.sleep(10)
                    message = None                       
                elif(mydata["action"]=="started" and completed):
                    activation_counter+=1
                    completed = False
                    pp.pprint(mydata)
                                
                    pm.adddetailed(mydata["sender"], mydata["pid"], mydata["cycle"], mydata["timestamp"])  
                    print(f"new one {mydata['sender']}")
                        
                    ##
                    try:
                        message_struct["action"] = "acknowledge"
                        message_struct["sender"] = MASTER_CHANNEL        
                        r.publish(mydata["sender"], ut.dict_to_string(message_struct))
                        #print (f'published to {mydata["sender"]} run {mydata["cycle"] }')
                        logger.debug(f'published to {mydata["sender"]} run {mydata["cycle"] }')
                    except Exception as e:
                        logger.debug(f"Error publish to child {id_to_run}")                
                                
                    print(f"Exit from {activation_counter} cycle ")
                    #running_process.update(temp)
                    completed = True    
                ## Get list of active process <running_process> list         
                elif(mydata["action"]=="listactive"):
                    message_struct["action"] = "activeprocesslist"
                    message_struct["sender"] = MASTER_CHANNEL       
                    message_struct["data"] = pm.running_process
                    try:
                        r.publish(mydata["sender"], ut.dict_to_string(message_struct))
                        logger.debug(f"published to {mydata['sender']}")
                    except Exception as e:
                        logger.debug(f"Error publish to child {mydata['sender']}")                

                elif(mydata["action"]=="printprocess"):
                    print(f'Process {pm.numactive}')
                    print(f'List {pm.running_process}')
                    pm.printprocess()
                    
                    
                ## Shutdown call from command cli
                elif(mydata["action"]=="shutdown"):
                    print(f"Process to shutdown { mydata['processtoshutdown']}")
                    if(mydata["processtoshutdown"]=="all"):
                        shutdown_process(pm.running_process)
                    else:
                        shutdown_process(mydata["processtoshutdown"])

                    ## Acknowledge to command cli
                    message_struct["action"] = "response"
                    message_struct["sender"] = MASTER_CHANNEL       
                    message_struct["data"] = "shutdown start"
                    try:
                        r.publish(mydata['sender'], ut.dict_to_string(message_struct))
                        logger.debug(f"published to {mydata['sender']}")
                    except Exception as e:
                        logger.debug("Error publish to command cli")                
                
                ## Shutdown initiator call from spawn process
                elif(mydata["action"]=="executeshutdown"):
                    
                    ## Remove process from list of running process
                    
                    pm.remove(mydata["sender"])
                    
                    print(f'{mydata["sender"]} down, last ping {mydata["lastping"]}')
                    
                    ## Acknowledge to command cli
                    message_struct["action"] = "info"
                    message_struct["sender"] = MASTER_CHANNEL       
                    message_struct["data"] = f'{mydata["sender"]} shutdown done'
                    try:
                        r.publish(CMD_CHANNEL, ut.dict_to_string(message_struct))
                        logger.debug("published to command line")
                    except Exception as e:
                        logger.debug("Error publish to command line")                
                    
                elif(mydata["action"]=="response_to_acknowledge"):
                    print(f'good to know, {mydata["sender"]} data: {mydata["data"]}')
            else:
                print(f'Channel {message["channel"]}')
                
                pp.pprint(mydata)
                
        else:
            pass
            #logger.debug("no msg")
        mydata = None
        time.sleep(0.001)  # be nice to the system :)
#################################################################################
### SPAWN SECTION
#################################################################################
else:
    #random_number = str(randbelow(60))
    get_ack = False
    print(f"Process id : {id}")
    logger.debug(f"Process id  {id}")
    try:
        p.subscribe(id)
        logger.debug(f"Spawn process subscription {id}")
    except:
        logger.debug(f"Error subscribing {id}")
        
    message_struct["action"] = "started"
    message_struct["sender"] = id
    message_struct["timestamp"] = time_start
    message_struct["pid"] = str(os.getpid())
    message_struct["data"] = " is running"
    message_struct["cycle"] = args.npolls
    #print(args.npolls)
    #print(args.pollsdelay)

    ##
    try:
        r.publish(MASTER_CHANNEL, ut.dict_to_string(message_struct))
        logger.debug("Published to master")
    except Exception as e:
        logger.debug("Error publish to master ")
        logger.debug(e)
        
    while True:
        try:
            message = p.get_message()
        except Exception as e:
            logger.debug("Error reading message")
            logger.debug(e)
            
        if message:
            logger.debug("\n\n")
            logger.debug("========================= client receive START ===============")
            logger.debug(f'Channel {message["channel"]}')
            logger.debug(f'Type {message["type"]}')
            logger.debug(f'Data {message["data"]} ')
            mydata = ut.string_to_dict(message["data"])
            logger.debug('Decoded data {mydata} and type {type(mydata)}')
            logger.debug("======================== client receive END =================\n\n")
            
            ##
            ## shutdown action 
            ## 
            if(mydata["action"] == "shutdown"):
                message_struct["action"] = "executeshutdown"
                message_struct["sender"] = id
                message_struct["timestamp"] = str(datetime.datetime.now())
                message_struct["pid"] = str(os.getpid())
                message_struct["lastping"] = str(count-1)
                
                message_struct["data"] = " is exiting"
                try:
                    r.publish(MASTER_CHANNEL, ut.dict_to_string(message_struct))
                    logger.debug("Published to master")
                    sys.exit(0)
                except Exception as e:
                    logger.debug("Error publish to master ")
                    logger.debug(e)
                
            elif(mydata["action"] == "stats"):
                pass
            elif(mydata["action"] == "alive"):
                pass
            elif(mydata["action"] == "acknowledge"):
                get_ack = True
                
            
        #time.sleep(0.02)  # be nice to the system :)
        time.sleep(int(args.polldelay))
        
        if(get_ack):
            count+=1
            message_struct["action"] = "ping"
            message_struct["sender"] = id
            message_struct["data"] = "Im still alive"
            message_struct["counter"] = str(count)
            message_struct["cycle"] = args.npolls

            try:
                r.publish(MASTER_CHANNEL, ut.dict_to_string(message_struct))
                logger.debug(f"{id} Ping to master {count} {args.npolls}")
                get_ack = False
            except Exception as e:
                logger.debug("Error publishing to master")
                logger.debug(e)
            
            
    
