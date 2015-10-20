from engines.communicator import ComputeEngineCommunicator, TaskMonitor
from tasks import worker_manager
from mpi4py import MPI
from modules.utilities import *
from collections import deque
import cPickle, time

class MpiTaskMonitor(TaskMonitor):

    def __init__( self, rid, **args ):
        TaskMonitor. __init__( self, rid, **args )
        self.comm = args.get( 'comm', None )
        self.nworkers = args.get( 'nworkers', 1 )
        self.stats = {}
        self._status = "NONE"
        self.responses = deque()

    def genericize(self):
        stat = dict(self.stats)
        stat['rid'] = self._request_id
        return stat

    def __str__(self):
        return "%s: %s" % ( TaskMonitor.__str__(self), str(self.stats) )

    def push_response(self,response):
        self.responses.appendleft( response )

    def status(self):
        return self._status

    def empty(self):
        return ( len( self.responses ) == 0 )

    def full(self):
        return ( len( self.responses ) == self.nworkers )

    def ready(self):
        self.flush_incoming()
        return not self.empty()

    def flush_incoming(self):
        status = MPI.Status()
        while self.comm.Iprobe( MPI.ANY_SOURCE, MPI.ANY_TAG, status ):
            rid = status.Get_tag()
            response = self.comm.recv( source=status.Get_source(), tag=rid )
            if rid == self._request_id:
                self._status = "READY"
                self.push_response( response )
            else:
                task_monitor = self.get_monitor( rid )
                if task_monitor is not None:
                    task_monitor.push_response( response )

    def response(self, **args):
        self.addStats( **args )
        while not self.full():
            self.flush_incoming()
        return self.responses if self.nworkers > 1 else self.responses.pop()

    def result( self, **args ):
        response = self.response( **args )
        results = response['results']
        if len( self.stats ):
            for result in results: result.update( self.stats )
        return results

    def taskName(self):
        return self.rid

    def addStats(self,**args):
        self.stats.update( args )

class MpiCommunicator( ComputeEngineCommunicator ):

    RequestIndex = 1

    @classmethod
    def new_request_id(cls):
        cls.RequestIndex = cls.RequestIndex + 1
        return cls.RequestIndex

    def __init__( self ):
        from modules import configuration
        ComputeEngineCommunicator.__init__( self )

    def close(self):
        worker_manager.close()

    def submitTaskImpl( self, task_request, worker ):
        rid = self.new_request_id()
        task_request.setRequestId(rid)
        if worker == "*":
            comm, nworkers = worker_manager.broadcast( task_request.task, rid )
        elif isinstance( worker, list ):
            comm, nworkers  = worker_manager.broadcast( task_request.task, rid, worker  )
        else:
            comm, nworkers  =  worker_manager.send( task_request.task, rid, worker )
        return MpiTaskMonitor( rid, comm=comm, nworkers=nworkers )

    def initWorkerStats(self):
       return worker_manager.getProcessStats()



