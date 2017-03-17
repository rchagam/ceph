from flask import request
from flask_restful import Resource

import json
import traceback

## We need this to access the instance of the module
#
# We can't use 'from module import instance' because
# the instance is not ready, yet (would be None)
import module

# List of valid osd flags
OSD_FLAGS = ('pause', 'noup', 'nodown', 'noout', 'noin', 'nobackfill', 'norecover', 'noscrub', 'nodeep-scrub')


# Helper function to catch and log the exceptions
def catch(f):
    def catcher(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except:
            module.instance.log.error(str(traceback.format_exc()))
    return catcher



class Index(Resource):
    _neverest_endpoint = '/'

    @catch
    def get(self):
        return {
            'api_version': 1,
            'info': "Ceph Manager RESTful API server"
        }



class ClusterConfig(Resource):
    _neverest_endpoint = '/cluster/config'

    @catch
    def get(self):
        return module.instance.get("config")



class ClusterConfigKey(Resource):
    _neverest_endpoint = '/cluster/config/<string:key>'

    @catch
    def get(self, key):
        return module.instance.get("config").get(key, None)



class ClusterServer(Resource):
    _neverest_endpoint = '/cluster/server'

    @catch
    def get(self):
        return module.instance.list_servers()



class ClusterServerFqdn(Resource):
    _neverest_endpoint = '/cluster/server/<string:fqdn>'

    @catch
    def get(self, fqdn):
        return module.instance.get_server(fqdn)



class ClusterMon(Resource):
    _neverest_endpoint = '/cluster/mon'

    @catch
    def get(self):
        return module.instance.get_mons()



class ClusterMonName(Resource):
    _neverest_endpoint = '/cluster/mon/<string:name>'

    @catch
    def get(self, name):
        mons = filter(lambda x: x['name'] == name, module.instance.get_mons())
        if len(mons) != 1:
                return None
        return mons[0]



class ClusterOsd(Resource):
    _neverest_endpoint = '/cluster/osd'



class ClusterOsdConfig(Resource):
    _neverest_endpoint = '/cluster/osd/config'

    @catch
    def get(self):
        module.instance.log.error("TESTING...")
        return module.instance.get("osd_map")['flags'].split(',')


    @catch
    def patch(self):
        attributes = json.loads(request.data)

        commands = []

        module.instance.log.error("TESTING...")
        valid_flags = set(attributes.keys()) & set(OSD_FLAGS)
        module.instance.log.error(str(valid_flags))
        invalid_flags = list(set(attributes.keys()) - valid_flags)
        module.instance.log.error(str(invalid_flags))
        if invalid_flags:
            module.instance.log.warn("%s not valid to set/unset" % invalid_flags)

        for flag in list(valid_flags):
            if attributes[flag]:
                mode = 'set'
            else:
                mode = 'unset'

            commands.append({
                'prefix': 'osd ' + mode,
                'key': flag,
            })

        module.instance.log.warn(str(commands))
        module.instance.submit_request([commands])
        #module.instance.requests[]
        return commands



class Request(Resource):
    _neverest_endpoint = '/request'

    @catch
    def get(self):
        return map(
            lambda x: x.uuid,
            module.instance.requests
        )


class RequestUuid(Resource):
    _neverest_endpoint = '/request/<string:uuid>'

    @catch
    def get(self, uuid):
        request = filter(
            lambda x: x.uuid == uuid,
            module.instance.requests
        )

        if len(request) != 1:
            module.instance.log.warn("Unknown request UUID '%s'" % str(uuid))
            return {}

        request = request[0]
        return {
            'uuid': request.uuid,
            'running': len(request.running),
            'finished': len(request.finished),
            'waiting': len(request.waiting),
            'is_waiting': request.is_waiting(),
            'is_finished': request.is_finished(),
        }

    @catch
    def post(self, uuid):
        for index in range(len(module.instance.requests)):
            if module.instance.requests[index].uuid == uuid:
                module.instance.requests.pop(index)
                return True

        # Failed to find the job to cancel
        return False
