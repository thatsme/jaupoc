from sys import platform
import sys

class Platform:
    def __init__(self):
        if platform == "linux" or platform == "linux2":
            # linux
            self.copycommand = "cp"
            self.mdircommand = "md"
            self.spawndir = "."
            self.slash = "/"
            self.logpath = "./logs/"
        elif platform == "darwin":
            # OS X
            self.copycommand = "cp"
            self.mdircommand = "md"
            self.spawndir = "."
            self.slash = "/"
            self.logpath = "./logs/"
        elif platform == "win32":
            # Windows
            self.copycommand = "copy"
            self.mdircommand = "mkdir"
            self.spawndir = "."
            self.slash = "\\"
            self.logpath = ".\\logs\\"
    
        if hasattr(sys, 'real_prefix'):
            self.virtualenv = True
    def getplatform(self):
        return platform
    
