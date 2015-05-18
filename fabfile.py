from __future__ import with_statement
import json
from fabric.api import *
from fabric.colors import red, green
# from fabric.contrib import files
from functools import wraps


def excludehosts(func):
    def closuref(*args, **kwargs):
        exhosts = json.loads(env.exhosts)
        if exhosts:
            print(green("Verifying host %s") % (env.host))
            if any(env.host in s for s in exhosts):
                print(green("Excluding host %s" % (env.host)))
                return
        return func(*args, **kwargs)
    # This is necessary so that custom decorator is interpreted as fabric decorator
    # Fabric fix: https://github.com/mvk/fabric/commit/68601ae817c5c26f4937f0d04cb56e2ba8ca1e04
    # is also necessary.
    closuref.func_dict['wrapped'] = func
    return wraps(func)(closuref)


# @excludehosts
@task
def dashaction(screen_name, script, script_params=None):
    command = actionscript(script, script_params, True)
    # print("Running %s") % (command)
    dashcommand(command, screen_name, True)
