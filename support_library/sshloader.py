import os
import sys
curruser = os.environ.get('USER')
sys.path.insert(0, '/home/{}/python35-libs/lib/python3.5/site-packages/'.format(curruser))
import paramiko
from pathlib import Path
import traceback

class Ssh(object):

    def __init__(self, 
                 username = 'ektov-av', 
                 password = None, 
                 host     = None, 
                 port     = 22,
                 verbosity = 0):
        super(Ssh, self).__init__()
        self.username  = username
        self.password  = password
        self.host      = host
        self.port      = port
        self.verbosity = verbosity
        
    def setup(self):
        '''Setup connection'''
        try:
            # DEBUG
            paramiko.common.logging.basicConfig(level=paramiko.common.ERROR)          
            ssh=paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.host,self.port,self.username,self.password)
            self.ssh = ssh
            
            self.transport = paramiko.Transport((self.host, self.port))
            self.transport.connect(username = self.username, password = self.password)
            self.sftp = paramiko.SFTPClient.from_transport(self.transport)
            if self.verbosity:
                print(self.sftp.sock)
        except Exception as e:
            print(traceback.format_exc())

    def putFiles(self, localpath, remotepath, filename, destFolderName):
        if self.verbosity:
            print('\n# Ssh.putFiles() #')
        try:
            # self.setup()
            destinationFolder = destFolderName
            final_path = Path.joinpath(Path(remotepath),destinationFolder).as_posix()
            try:
                stdin,stdout,stderr=self.ssh.exec_command('mkdir -p {}'.format(final_path))
                outlines=stdout.readlines()
                if self.verbosity:
                    resp=''.join(outlines)
                    print(resp)
                #self.sftp.mkdir(final_path, mode=777)
            except:
                print(traceback.format_exc())
            final_destination = Path.joinpath(Path(final_path), filename).as_posix()
            sourceFilePath    = Path.joinpath(Path(localpath),filename).as_posix()
            if self.verbosity:
                print('\n# Source Path: {}\n# Destination Path: {}\n'.format(sourceFilePath,final_destination))
            self.sftp.put(sourceFilePath, final_destination)
        except Exception as e:
            print(traceback.format_exc())
        return
    
    def getFiles(self, remotepath, localpath, filename):
        print('\n# Ssh.putFiles() #')
        try:
            # self.setup()
            final_destination = Path.joinpath(Path(localpath), filename).as_posix()
            sourceFilePath    = Path.joinpath(Path(remotepath),filename).as_posix()
            if self.verbosity:
                print('\n# Source Path: {}\n# Destination Path: {}\n'.format(sourceFilePath,final_destination))
            self.sftp.get(sourceFilePath, final_destination)
        except Exception as e:
            print(traceback.format_exc())
        return    