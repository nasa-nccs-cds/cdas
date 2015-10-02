from modules.utilities import wpsLog
import celeryconfig
import celery
from billiard import current_process
from kernels.manager import KernelManager

app = celery.Celery( 'tasks', broker=celeryconfig.BROKER_URL, backend=celeryconfig.CELERY_RESULT_BACKEND )
app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['json','pickle'],  # Ignore other content
    CELERY_RESULT_SERIALIZER='pickle',
)

class DomainBasedTask(celery.Task):
    abstract = True
    kernelMgr = None

    def __init__(self):
        celery.Task.__init__(self)

    @classmethod
    def getKernelMgr(cls, wname ):
        if cls.kernelMgr is None:
            cls.kernelMgr = KernelManager( wname )
        return cls.kernelMgr


 #    def on_success(self, retval, task_id, args, kwargs):
 #        logger.debug( " !!!!!!!!! Task %s SUCCESSSSSSSSS, rv: %s " % ( task_id, str(retval) ) )
 # #       self.processPendingTask( task_id, retval )

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        wpsLog.error( " Task %s failure, Error: %s " % ( task_id, str(einfo) ) )



from request.manager import TaskRequest

@app.task(base=DomainBasedTask)
def getWorkerName():
    p = current_process()
    return p.initargs[1]

@app.task(base=DomainBasedTask,name='tasks.execute')
def execute( task_request_args ):
    wpsLog.debug( " Executing Task request: %s " % str(task_request_args) )
    kernelMgr = DomainBasedTask.getKernelMgr( getWorkerName() )
    response = kernelMgr.run( TaskRequest(task=task_request_args) )
    return response

@app.task(base=DomainBasedTask,name='tasks.executeAll')
def executeAll( task_request_args ):
    wpsLog.debug( " Executing Task request: %s " % str(task_request_args) )
    kernelMgr = DomainBasedTask.getKernelMgr( getWorkerName() )
    response = kernelMgr.run( TaskRequest(task=task_request_args) )
    return response



from engines.manager import ComputeEngine
from communicator import CeleryCommunicator

class CeleryEngine( ComputeEngine ):

    def getCommunicator( self ):
        return  CeleryCommunicator()


