"""
A RESTful API for Ceph
"""

# Global instance to share
instance = None

import json
import errno
import traceback

import api

from uuid import uuid4
from flask import Flask, request
from flask_restful import Api
from multiprocessing import Process

#from request import CommandsRequest
from mgr_module import MgrModule, CommandResult



class CommandsRequest(object):
    """
    This class handles parallel as well as sequential execution of
    commands. The class accept a list of iterables that should be
    executed sequentially. Each iterable can contain several commands
    that can be executed in parallel.
    """


    @staticmethod
    def run(commands, uuid = str(uuid4())):
        """
        A static method that will execute the given list of commands in
        parallel and will return the list of command results.
        """
        results = []
        instance.log.warn("RUNNING..")
        for index in range(len(commands)):
            tag = '%s:%d' % (str(uuid), index)
            instance.log.warn("RUNNING: " + str(tag))

            # Store the result
            result = CommandResult(tag)
            instance.log.warn("RUNNING: " + str(result))
            result.command = commands[index]
            instance.log.warn("RUNNING: " + str(result.command))
            results.append(result)
            instance.log.warn("RUNNING: " + str(results))

            # Run the command
            instance.send_command(result, json.dumps(commands[index]),tag)
            instance.log.warn("RUNNING: send_command")

        return results


    def __init__(self, commands_arrays):
        self.uuid = str(id(self))

        self.running = []
        self.waiting = commands_arrays[1:]
        self.finished = []

        if not len(commands_arrays):
            # Nothing to run
            return

        # Process first iteration of commands_arrays in parallel
        instance.log.warn("RUNNING: " + str(self.running))
        try:
            results = self.run(commands_arrays[0], self.uuid)
        except:
            instance.log.error(str(traceback.format_exc()))
            #instance.log.warn("RUNNING: " + str(self.running))
        self.running.extend(results)
        instance.log.warn("RUNNING: " + str(self.running))


    def next(self):
        if not self.waiting:
            # Nothing to run
            return

        # Run a next iteration of commands
        commands = self.waiting[0]
        self.waiting = self.waiting[1:]

        self.running.extend(self.run(commands), self.uuid)


    def finish(self, tag):
        for index in range(len(self.running)):
            if self.running[index].tag == tag:
                self.finished.append(self.running.pop(index))
                return True

        # No such tag found
        return False


    def is_running(self, tag):
        for result in self.running:
            if result.tag == tag:
                return True
        return False


    def is_ready(self):
        return not self.running and self.waiting


    def is_waiting(self):
        return bool(self.waiting)


    def is_finished(self):
        return not self.running and not self.waiting



class Module(MgrModule):
    COMMANDS = [
            {
                "cmd": "enable_auth "
                       "name=val,type=CephChoices,strings=true|false",
                "desc": "Set whether to authenticate API access by key",
                "perm": "rw"
            },
            {
                "cmd": "auth_key_create "
                       "name=key_name,type=CephString",
                "desc": "Create an API key with this name",
                "perm": "rw"
            },
            {
                "cmd": "auth_key_delete "
                       "name=key_name,type=CephString",
                "desc": "Delete an API key with this name",
                "perm": "rw"
            },
            {
                "cmd": "auth_key_list",
                "desc": "List all API keys",
                "perm": "rw"
            },
    ]


    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        global instance
        instance = self

        self.requests = []

        self.keys = {}
        self.enable_auth = True
        self.app = None
        self.api = None


    def notify(self, notify_type, tag):
        try:
            self._notify(notify_type, tag)
        except:
            self.log.error(str(traceback.format_exc()))


    def _notify(self, notify_type, tag):
        if notify_type == "command":
            self.log.warn("Processing request '%s'" % str(tag))
            request = filter(
                lambda x: x.is_running(tag),
                self.requests)
            self.log.warn("Requests '%s'" % str(request))
            if len(request) != 1:
                self.log.warn("Unknown request '%s'" % str(tag))
                return

            request = request[0]
            request.finish(tag)
            self.log.warn("RUNNING Requests '%s'" % str(request.running))
            self.log.warn("WAITING Requests '%s'" % str(request.waiting))
            self.log.warn("FINISHED Requests '%s'" % str(request.finished))
            if request.is_ready():
                request.next()
        else:
            self.log.debug("Unhandled notification type '%s'" % notify_type)
    #    elif notify_type in ['osd_map', 'mon_map', 'pg_summary']:
    #        self.requests.on_map(notify_type, self.get(notify_type))


    def serve(self):
        try:
            self._serve()
        except:
            self.log.error(str(traceback.format_exc()))


    def _serve(self):
        #self.keys = self._load_keys()
        self.enable_auth = self.get_config_json("enable_auth")
        if self.enable_auth is None:
            self.enable_auth = True

        self.app = Flask('ceph-mgr')
        self.app.config['RESTFUL_JSON'] = {
            'sort_keys': True,
            'indent': 4,
            'separators': (',', ': '),
        }
        self.api = Api(self.app)

        # Add the resources as defined in api module
        for _obj in dir(api):
            obj = getattr(api, _obj)
            # We need this try statement because some objects (request)
            # throw exception on any out of context object access
            try:
                _endpoint = getattr(obj, '_neverest_endpoint', None)
            except:
                _endpoint = None
            if _endpoint:
                self.api.add_resource(obj, _endpoint)

        self.log.warn('RUNNING THE SERVER')
        self.app.run(host = '0.0.0.0', port = 8002)
        self.log.warn('FINISHED RUNNING THE SERVER')


    def get_mons(self):
        mon_map_mons = self.get('mon_map')['mons']
        mon_status = json.loads(self.get('mon_status')['json'])

        # Add more information
        for mon in mon_map_mons:
            mon['in_quorum'] = mon['rank'] in mon_status['quorum']
            mon['server'] = self.get_metadata("mon", mon['name'])['hostname']
            mon['leader'] = mon['rank'] == mon_status['quorum'][0]

        return mon_map_mons


    def submit_request(self, request):
        self.log.warn("REQUEST: " + str(request))
        self.requests.append(CommandsRequest(request))
        self.log.warn("AFTER REQUEST: " + str(request))


    def handle_command(self, cmd):
        self.log.info("handle_command: {0}".format(json.dumps(cmd, indent=2)))
        prefix = cmd['prefix']
        if prefix == "enable_auth":
            enable = cmd['val'] == "true"
            self.set_config_json("enable_auth", enable)
            self.enable_auth = enable
            return 0, "", ""
        elif prefix == "auth_key_create":
            if cmd['key_name'] in self.keys:
                return 0, self.keys[cmd['key_name']], ""
            else:
                self.keys[cmd['key_name']] = self._generate_key()
                self._save_keys()

            return 0, self.keys[cmd['key_name']], ""
        elif prefix == "auth_key_delete":
            if cmd['key_name'] in self.keys:
                del self.keys[cmd['key_name']]
                self._save_keys()

            return 0, "", ""
        elif prefix == "auth_key_list":
            return 0, json.dumps(self._load_keys(), indent=2), ""
        else:
            return -errno.EINVAL, "", "Command not found '{0}'".format(prefix)
