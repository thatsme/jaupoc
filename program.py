from sys import argv
import subprocess
from subprocess import Popen
import uuid
import sys
import os
import ast
import json
from sys import platform
import time 
import logging
import redis
import datetime
from random import randint
from secrets import randbelow
import pprint
import argparse

pp = pprint.PrettyPrinter(indent=4)

def dict_to_binary(the_dict):
    mstring = json.dumps(the_dict)
    mbinary = ' '.join(format(ord(letter), 'b') for letter in mstring)
    return mbinary

def binary_to_dict(the_binary):
    jsn = ''.join(chr(int(x, 2)) for x in the_binary.split())
    d = json.loads(jsn)  
    return d

def dict_to_string(the_dict):
    mstring = json.dumps(the_dict)
    return mstring

def string_to_dict(the_string):
    d = json.loads(the_string)  
    return d

time_start = str(datetime.datetime.now())
master_script = "master"
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

script = argv
pre = script[0]
#print(pre)
if(pre[0:2]==".\\"):
    #print(pre[0:2])
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
    
if(immaster):
    logname = name+".log"
else:
    logname = name+"_"+id+".log"

logging.basicConfig(filename=logname,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)

#logging.info("Running Urban Planning")

logger = logging.getLogger('programLog')

try:
    r = redis.Redis(host='localhost', port=6379, db=0)
    p = r.pubsub(ignore_subscribe_messages=True)

except:
    logger.debug("Error with Redis server")
    
if platform == "linux" or platform == "linux2":
    # linux
    copycommand = "cp"
    mdircommand = "md"
elif platform == "darwin":
    # OS X
    copycommand = "cp"
    mdircommand = "md"
elif platform == "win32":
    # Windows
    copycommand = "copy"
    mdircommand = "mkdir"
    
if hasattr(sys, 'real_prefix'):
    virtualenv = True
        
        
logger.debug('%s %s %s %s %s', name, "is master :", immaster, " Virtual : ", virtualenv)
logger.debug(currentDirectory)

#time.sleep(1000)
if(immaster):
    try:
        p.subscribe('master')
        p.subscribe('commandline')
        logger.debug("subscribed master/cli interface")
    except:
        logger.debug("Error subscribing")

    #print("****************************************************************")
    #for key in running_process:
    #    print(key)
    #    print(running_process[key])
    #print("****************************************************************")
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
            logger.debug('%s %s',"channel ", message["channel"])
            logger.debug('%s %s',"type ", message["type"])            
            logger.debug('%s %s',"data ", message["data"])
            mydata = string_to_dict(message["data"])
            logger.debug('Decoded data %s %s', mydata, type(mydata))
            ##pp.pprint(mydata)
            logger.debug("====================== server receive end ==================\n\n")

            if(message["channel"]==b'master'):
                if(mydata["action"]=="ping" and mydata["sender"] in running_process.keys() ):
                    
                    #if(int(mydata["counter"])>=10):
                    #if(int(mydata["counter"])>=int(running_process[mydata["sender"]]["cycle"])):
                    if(int(mydata["counter"])>=int(mydata["cycle"]) and int(mydata["cycle"])>0):
                        message_struct["action"] = "shutdown"
                        message_struct["sender"] = master_script
                        ##
                        try:
                            r.publish(mydata["sender"], dict_to_string(message_struct))
                        except:
                            logger.debug("Error pubblish to child "+mydata["sender"])         
                    else:
                        message_struct["action"] = "acknowledge"
                        message_struct["sender"] = master_script
                        try:
                            r.publish(mydata["sender"], dict_to_string(message_struct))
                        except:
                            logger.debug("Error pubblish to child "+mydata["sender"])         
                        
                        print("Pinged %s %s %s " % (mydata["counter"], mydata["cycle"], mydata["sender"]))     
                elif(mydata["action"]=="spawn"):
                    #print(mydata["numofprocess"])
                    #sys.exit()
                    mrange = int(mydata["numofprocess"])
                    for i in range(mrange):
                        print("==============================================================================================")
                        id_to_run = str(uuid.uuid4())
                        tofile = currentDirectory+"\\"+name+"_"+id_to_run+"."+extension
                        torun = name+"_"+id_to_run+"."+extension
                        fromfile = currentDirectory+"\\"+name+"."+extension
                        logger.debug("=== Span section =======")
                        logger.debug(fromfile)
                        logger.debug(tofile)
                            
                        subprocess.run(["rm", "program_*.log"], shell=True)
                        #subprocess.run(["rm", "program_*.py"], shell=True)
                        
                        subprocess.run([copycommand, fromfile, tofile], shell=True)
                        
                        margs = []
                        margs.append("python")
                        margs.append(torun)
                        margs.append(str(mydata["numofpolls"]))
                        margs.append(str(mydata["polldelay"]))
                        
                        # Python >= 3.3 has subprocess.DEVNULL
                        try:
                            ## process parameters
                            ## mydata["numofpolls"]   <- number of polls befor programmatically exit
                            ## mydata["pollsdelay"]   <- poll delay seconds
                            #Popen(["python", torun, int(mydata["numofpolls"]), int(mydata["polldelay"]) ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                            process = Popen(margs, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                            active_process.append(id_to_run)
                            #sts = os.waitpid(process.pid, 0)
                        except Exception as e:
                            print(e)
                            logger.debug(e)      
                                                              
                        message_struct["action"] = "response"
                        message_struct["sender"] = master_script        
                        message_struct["data"] = "spawn done"
                        try:
                            r.publish(mydata["sender"], dict_to_string(message_struct))
                            logger.debug("published to "+mydata["sender"])
                        except Exception as e:
                            logger.debug("Error pubblish to child "+mydata["sender"])                

                        #time.sleep(10)
                    message = None                       
                elif(mydata["action"]=="started" and completed):
                    activation_counter+=1
                    completed = False
                    pp.pprint(mydata)
                    temp = {}
                    message_struct = {}
                    process_struct["pid"] = mydata["pid"]
                    process_struct["cycle"] = mydata["cycle"]
                    process_struct["timestamp"] = mydata["timestamp"]
                    temp[mydata["sender"]] = process_struct
                    print("new one "+mydata["sender"])
                        
                    ##
                    try:
                        message_struct["action"] = "acknowledge"
                        message_struct["sender"] = master_script        
                        #message_struct["counter"] = mydata["cycle"]
                        r.publish(mydata["sender"], dict_to_string(message_struct))
                        #formatted_str = 'The first name is %s and second name is %s' % (f_name, l_name)
                        print ("published to %s %s " % (mydata["sender"], mydata["cycle"]))

                        logger.debug("published to %s %s ", mydata["sender"], mydata["cycle"])
                    except Exception as e:
                        logger.debug("Error pubblish to child "+id_to_run)                
                                
                    print("Esce dal ciclo "+str(activation_counter))
                    running_process.update(temp)
                    completed = True             
                elif(mydata["action"]=="listactive"):
                    message_struct["action"] = "response"
                    message_struct["sender"] = master_script        
                    message_struct["data"] = running_process
                    try:
                        r.publish(mydata["sender"], dict_to_string(message_struct))
                        logger.debug("published to "+mydata["sender"])
                    except Exception as e:
                        logger.debug("Error pubblish to child "+mydata["sender"])                

                elif(mydata["action"]=="shutdown"):
                    print(f"Process to shutdown { mydata['processtoshutdown']}")
                    message_struct["action"] = "shutdown"
                    message_struct["sender"] = master_script
                    if(mydata["processtoshutdown"]=="all"):
                        for sp in active_process:
                            ##
                            print(f"Process to shudtown {sp}")
                            try:
                                r.publish(sp, dict_to_string(message_struct))
                            except:
                                logger.debug("Error pubblish to child "+mydata["sender"])         
                    else:
                        ##
                        try:
                            r.publish(mydata["processtoshutdown"], dict_to_string(message_struct))
                        except:
                            logger.debug("Error pubblish to child "+mydata["sender"])         

                    message_struct["action"] = "response"
                    message_struct["sender"] = master_script        
                    message_struct["data"] = "shutdown done"
                    try:
                        r.publish(mydata["sender"], dict_to_string(message_struct))
                        logger.debug("published to "+mydata["sender"])
                    except Exception as e:
                        logger.debug("Error pubblish to child "+mydata["sender"])                

                elif(mydata["action"]=="executeshutdown"):
                    del running_process[mydata["sender"]]
                    print(f'{mydata["sender"]} down, last ping {mydata["lastping"]}')
                    message_struct["action"] = "info"
                    message_struct["sender"] = master_script        
                    message_struct["data"] = running_process
                    try:
                        r.publish("client", dict_to_string(message_struct))
                        logger.debug("published to client")
                    except Exception as e:
                        logger.debug("Error pubblish to child client")                
                    
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
    logger.debug(f"Process id  {+id}")
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
        r.publish(master_script, dict_to_string(message_struct))
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
            mydata = string_to_dict(message["data"])
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
                    r.publish(master_script, dict_to_string(message_struct))
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
                r.publish("master", dict_to_string(message_struct))
                logger.debug(f"{id} Ping to master {count} {args.npolls}")
                get_ack = False
            except Exception as e:
                logger.debug("Error publishing to master")
                logger.debug(e)
            
            
    
