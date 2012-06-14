#!/usr/bin/evn python
# vim: set expandtab ts=4 sw=4:

'''
Statsd Python client helpers

Adaptation of Steve Ivy's python_example.py script into a 
configurable module.

Statsd server configuration is a module level global property 
called `addr`.  The default server address is localhost:8125. 
Specify a different server by configuring the module with 
`statsd.set_server("example.com", 8125)`. 

To send some stats to a different statsd server without changing 
the global config, you can use the Statsd class, which takes a 
host and port at instantiation. 

Example:

    import statsd
    statsd.set_server("example.com")

    statsd.increment("my.counter")

    custom_statsd = statsd.Statsd("other.example.com", 2222)
    custom_statsd.increment("my.other.counter")

'''

# adapted from python_example.py, by Steve Ivy <steveivy@gmail.com> http://monkinetic.com
__author__ = 'Steve Ivy <steveivy@gmail.com>, Christopher H. Casebeer'

import logging
logging.basicConfig(level=logging.DEBUG)

import functools
import random
import socket
import pprint
import sys

addr = "localhost", 8125

def set_server(host, port=8125):
    '''
    Configure statsd logging server. Defaults to localhost:8125.
    '''
    global addr
    addr = (host, port)

def timing(stat, time, sample_rate=1):
    """
    Log timing information.
    >>> import statsd
    >>> statsd.timing('some.time', 500)
    """
    stats = {}
    stats[stat] = "%d|ms" % time
    send(stats, sample_rate)
    
def gauge(stat, value, sample_rate=1):
    """
    Log gauge information.
    >>> import statsd
    >>> statsd.gauge('some.gauge', 99)
    """
    stats = {}
    stats[stat] = "%d|g" % value
    send(stats, sample_rate)

def increment(stats, sample_rate=1):
    """
    Increments one or more stats counters.
    >>> statsd.increment('some.int')
    >>> statsd.increment('some.int',0.5)
    """
    update_stats(stats, 1, sample_rate)

def decrement(stats, sample_rate=1):
    """
    Decrements one or more stats counters.
    >>> statsd.decrement('some.int')
    """
    update_stats(stats, -1, sample_rate)
    
def update_stats(stats, delta=1, sampleRate=1):
    """
    Updates one or more stats counters by arbitrary amounts.
    >>> statsd.update_stats('some.int',10)
    """
    if (type(stats) is not list):
        stats = [stats]
    data = {}
    for stat in stats:
        data[stat] = "%s|c" % delta

    send(data, sampleRate)
    
def send(data, sample_rate=1):
    """
    Squirt the metrics over UDP
    """
    sampled_data = {}

    if(sample_rate < 1):
        if random.random() <= sample_rate:
            for stat in data.keys():
                value = sampled_data[stat]
                sampled_data[stat] = "%s|@%s" %(value, sample_rate)
    else:
        sampled_data=data
    
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        for stat in sampled_data.keys():
            value = data[stat]
            send_data = "%s:%s" % (stat, value)
            udp_sock.sendto(send_data, addr)
    except:
        logging.warn("Unexpected error: %s" % pprint.pformat(sys.exc_info()))

class ServerContext(object):
    '''
    Context manager to change and reset the `addr` module global.
    '''
    def __init__(self, addr):
        self.addr = addr
    def __enter__(self):
        global addr
        self.saved = addr
        addr = self.addr
    def __exit__(self, type, value, tb):
        global addr
        addr = self.saved

class Statsd(object):
    '''
    Wrapper class permitting non-global statsd server configs. 
    Use if you want to send some but not all stats to a different
    statsd server. 
    
    The methods have the same signatures as their corresponding
    module level functions. 
    '''
    def __init__(self, host, port=8125):
        self.addr = host,port
    def _wrap(f):
        @functools.wraps(f)
        def wrapped(self, *args, **kwargs):
            with ServerContext(self.addr):
                f(*args, **kwargs)
        #wrapped.__doc__ = f.__doc__
        return wrapped
    timing = _wrap(timing)
    increment = _wrap(increment)
    decrement = _wrap(decrement)
    update_stats = _wrap(update_stats)
    send = _wrap(send)

