import unittest, json, sys, logging
from request.manager import TaskRequest
from manager import KernelManager
from modules.configuration import MERRA_TEST_VARIABLES
from modules.utilities import wpsLog
from datacache.domains import Region
verbose = False

if verbose:
    wpsLog.addHandler( logging.StreamHandler(sys.stdout) )
    wpsLog.setLevel(logging.DEBUG)

kernelMgr = KernelManager('W-1')

class KernelTests(unittest.TestCase):

    def setUp(self):
        self.test_point = [ -137.0, 35.0, 85000.0 ]
        self.test_time = '2010-01-16T12:00:00'
        self.operations = [ "CDTime.departures(v0,slice:t)", "CDTime.climatology(v0,slice:t,bounds:annualcycle)", "CDTime.value(v0)" ]
        self.ave_operations = [ "CWT.average(*,axis:z)", "CWT.average(*,axis:ze)" ]
        self.def_task_args =  { 'domain': self.getRegion(), 'variable': self.getData() }

    def tearDown(self):
        pass

    def getRegion(self, ipt=0 ):
        return '[{"id":"r0","longitude": {"value":%.2f,"system":"values"}, "latitude": %.2f, "level": %.2f, "time":"%s" }]' % (self.test_point[0]+5*ipt,self.test_point[1]-5*ipt,self.test_point[2],self.test_time)

    def getData(self, vars=[0]):
        var_list = ','.join( [ ( '{"dset":"%s","id":"v%d:%s","domain":"r0"}' % ( MERRA_TEST_VARIABLES["collection"], ivar, MERRA_TEST_VARIABLES["vars"][ivar] ) ) for ivar in vars ] )
        data = '[%s]' % ( var_list )
        return data

    def getEnsembleData(self,var="ta"):
        collections = [ "MERRA/mon/atmos", "CFSR/mon/atmos" ]
        var_list = ','.join( [ ( '{"dset":"%s","id":"v%d:%s","domain":"r0"}' % ( collections[ivar], ivar, var ) ) for ivar in range(len(collections)) ] )
        data = '[%s]' % ( var_list )
        return data

    def getLocalData(self):
        data = '{"dset":"MERRA/mon/atmos/hur","id":"v0:hur","domain":"r0"}'
        return data

    def getOp(self, op_index ):
        return [ self.operations[ op_index ] ]

    def getResults( self, response ):
        error = response.get( 'error', None )
        if error: raise Exception( error )
        return response['results']

    def getResultData( self, response, rindex=0 ):
        results = self.getResults( response )
        if isinstance( results, list ): rdata = results[rindex].get('data',[])
        else:                           rdata = results.get('data',[])
        return ( [ float(rd) for rd in rdata ] if hasattr( rdata, '__iter__' ) else float(rdata) )


    def getTaskArgs(self, op, ipt=0 ):
        task_args = { 'domain': self.getRegion(ipt), 'variable': self.getData(), 'embedded': True }
        task_args['operation'] = op
        return task_args

    def test01_cache(self):
        cache_level = 85000.0
        request_region = Region( { "lev": {"config":{},"bounds":[cache_level]}, "id":"r0" } )
        results = self.getResults( kernelMgr.run( TaskRequest( request={ 'domain': [ {"id":"r0", "level": cache_level } ], 'data': self.getData() } ) ) )
        region_spec = results['domain_spec'].region_spec
        self.assertEqual( Region(region_spec) , request_region )

    def test02_departures(self):
        test_result = [-3.29693603515625, -4.198516845703125, -4.247314453125, -3.934478759765625, 0.02935791015625, 1.18524169921875, 3.093414306640625, 3.30181884765625,
                       2.87091064453125, 1.8109130859375, -0.725311279296875, -0.78521728515625, -0.73663330078125]
        task_args = self.getTaskArgs( op=self.getOp( 0 ) )
        result_spec = kernelMgr.run( TaskRequest( request=task_args ) )
        result_data = self.getResultData( result_spec )
    #    print "result_data: ", str(result_data)
        self.assertEqual( test_result, result_data[0:len(test_result)] )


    def test03_annual_cycle(self):
        test_result = [283.121826171875, 282.06532118055554, 282.0176595052083, 282.59760199652777, 284.8202853732639, 287.36102973090277,
                       288.89800347222223, 288.7062174479167, 287.56987847222223, 286.255126953125, 284.5141872829861, 283.2602267795139]
        task_args = self.getTaskArgs( self.getOp( 1 ), 1 )
        kernelMgr.persist()
        result_data = self.getResultData( kernelMgr.run( TaskRequest( request=task_args ) ) )
    #    print "result_data: ", str(result_data)
        self.assertEqual( test_result, result_data[0:len(test_result)] )

    def test04_value_retreval(self):
        test_result = 285.666259765625
        task_args = self.getTaskArgs( self.getOp( 2 ), 2 )
        result_data =  self.getResultData( kernelMgr.run( TaskRequest( request=task_args ) ) )
        self.assertEqual( test_result, result_data )

    def test05_multitask(self):
        test_results = [ [ -3.29693603515625, -4.198516845703125, -4.247314453125 ], [280.96861436631946, 279.7786458333333, 279.4698079427083], 280.681884765625 ]
        task_args = self.getTaskArgs( op=self.operations )
        results = kernelMgr.run( TaskRequest( request=task_args ) )
        for ir, test_result in enumerate( test_results ):
            result = self.getResultData( results, ir )
            if hasattr( test_result, '__iter__' ):  self.assertEqual( test_result, result[0:len(test_result)] )
            else:                                   self.assertEqual( test_result, result )

    def test06_average(self):
        test_result = [ 280.8800610838381, 280.9892993869274, 281.6702337237978, 282.263175115314, 283.2421639579057, 283.97856582467875, 284.1801476209755 ]
        op_domain = Region( { "lev": {"config":{},"bounds":[85000.0]}, "id":"r0" } )
        task_args = { 'domain': op_domain, 'variable': self.getData(), 'operation' : [ "CWT.average(*,axis:xy)" ],'embedded': True }
        result_data = self.getResultData( kernelMgr.run( TaskRequest( request=task_args ) ) )
    #    print "result_data: ", str(result_data)
        self.assertEqual( test_result, result_data[0:len(test_result)] )

    def test07_ensemble_average(self):
        test_result = [ 280.5660821022122, 280.6036305230575, 281.2833028260845, 281.93823648983914, 282.8928702717118, 283.6747343132441, 283.8997834538976, 283.72630654497095 ]
        op_domain = Region( { "lev": {"config":{},"bounds":[85000.0]}, "id":"r0" } )
        task_args = { 'domain': op_domain, 'variable': self.getEnsembleData(), 'operation' : [ "CWT.average(*,axis:xye)" ],'embedded': True }
        result_data = self.getResultData( kernelMgr.run( TaskRequest( request=task_args ) ) )
     #   print "result_data: ", str(result_data)
        self.assertEqual( test_result, result_data[0:len(test_result)] )


if __name__ == '__main__':
    test_runner = unittest.TextTestRunner(verbosity=2)
    suite = unittest.defaultTestLoader.loadTestsFromTestCase( KernelTests )
    test_runner.run( suite )


