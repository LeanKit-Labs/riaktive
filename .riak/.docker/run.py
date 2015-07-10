#!/usr/bin/env python

import subprocess as sp
import os
import sys
import re
import json
import argparse

parser = argparse.ArgumentParser(description='Gentlemen, start your containers!')
parser.add_argument('command', nargs='?', default='run', help='Command to pass to container [run|/bin/bash]')
opts = parser.parse_args()

command = opts.command

#config file
path = os.path.dirname(__file__)
with open( path+'/run.json', 'r') as config: 
    conf = json.load(config)
        
def runCmd (cmd):
    if not isinstance(cmd, list):
        cmd = cmd.split()
    p = sp.Popen(cmd, stdout=sp.PIPE, stdin=sp.PIPE, stderr=sp.STDOUT)
    [out, err] = p.communicate()
    if p.returncode:
        cmd = ' '.join(cmd)
        print('ERROR: {}\n{}'.format(cmd, out))
        sys.exit(1)
    return out

#does container exist?
cmd = ['docker', 'ps', '-a', '-q']
cons = runCmd(cmd).splitlines()
container = dict()
for c in cons:
    cmd = ['docker', 'inspect', c]
    j = json.loads(runCmd(cmd))
    myName = j[0]['Name']
    #remove starting /
    myName = re.sub('^/', '', myName)
    container[myName] = dict()
    container[myName]['running'] = j[0]['State']['Running']

if conf['name'] in container:
    #container exists is it running?
    if container[conf['name']]['running']:
        print("--- {} is already running ---".format(conf['name']))
        sys.exit()
    else:
        print("--- Starting existing {} container ---".format(conf['name']))
        runCmd(['docker', 'start', conf['name']])
else:
    #container doesn't exist
    print("--- Starting new {} container ---".format(conf['name']))
    cmd = [ 'docker', 'run', '-it', '--restart=always' ]
    #daemonize is just run, rm if command
    if command == 'run':
        cmd.extend(['-d'])
    else:
        cmd.extend(['--rm'])
    #add ports to command
    for p in conf['ports']: 
        pts = ':'.join([p, p])
        cmd.extend(['-p', pts])
    #add volumes
    for v in conf['volumes']:
        v = re.sub('\~', os.environ['HOME'], v)
        cmd.extend(['-v', v])
    #add name
    cmd.extend(['--name', conf['name'], conf['name']])
    if command == 'run':
        cId = runCmd(cmd).rstrip()
        print("\tContainer {} started".format(cId))
        print('\n'.join(conf['notes']))
    else:
        cmd.extend([command])
        sp.call(cmd)


