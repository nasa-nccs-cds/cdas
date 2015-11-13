from request.manager import TaskRequest
from engines import engineRegistry
from datacache.domains import Domain, Region
from modules.configuration import MERRA_TEST_VARIABLES, CDAS_COMPUTE_ENGINE
from modules.utilities import *

class PerformanceTests:

    def __init__(self):
        self.local_data = True
        self.time_range = ( '2011-01-01T00:00:00','2011-12-31T00:00:00' )
        self.engine = engineRegistry.getInstance( CDAS_COMPUTE_ENGINE + "Engine" )
        self.cache_region_indexed =  Region( { 'id':"r0", "lev":{"start":10,"end":11,"system":"indices"}, "time":self.time_range } )
        self.cache_region =  Region( { 'id':"r0", "lev":850.0 } )    # , "time":self.time_range } )

    def getData(self):
        return self.getLocalData() if self.local_data else self.getRemoteData()

    def getRemoteData(self, vars=[0]):
        var_list = ','.join( [ ( '{"dset":"%s","id":"v%d:%s","domain":"r0"}' % ( MERRA_TEST_VARIABLES["collection"], ivar, MERRA_TEST_VARIABLES["vars"][ivar] ) ) for ivar in vars ] )
        data = '[%s]' % ( var_list )
        return data

    def getLocalData(self):
        data = '{"dset":"MERRA/mon/atmos/td","id":"v0:T","domain":"r0"}'
        return data

    def getResultData( self, results, index=0 ):
        if isinstance( results, Exception ):
            raise results
        if isinstance( results, list ): rdata = results[index].get( 'data', None )
        else:                           rdata = results.get( 'data', None )
        return None if (rdata is None) else ( [ float(rd) for rd in rdata ] if hasattr( rdata, '__iter__' ) else float(rdata) )

    def getResultStats( self, results, index=0 ):
        if isinstance( results, Exception ): print str(results)
        return ExecutionRecord( results[index]['exerec'] )

    def test010_cache(self):
        t0 = time.time()
        result = self.engine.execute( TaskRequest( request={ 'domain': self.cache_region, 'variable': self.getData(), 'async': False, 'embedded': False, 'operation': [ "CDTime.departures(v0,slice:t)" ] } ) )
#        print "departures result = %s" % str(result)
        t1 = time.time()
        print "Operation completed, time: %.2f" % ( t1-t0 )

if __name__ == '__main__':
    ptests = PerformanceTests()
    ptests.test010_cache()



