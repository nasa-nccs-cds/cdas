import json, sys
from request.manager import TaskRequest
from engines import engineRegistry
from datacache.domains import Domain, Region
from modules.configuration import MERRA_TEST_VARIABLES, CDAS_COMPUTE_ENGINE

class EngineTests:

    def setUp(self):
        self.test_point = [ -137.0, 35.0, 85000 ]
        self.test_time = '2010-01-16T12:00:00'
        self.def_task_args =  { 'region': self.getRegion(), 'data': self.getData() }
        self.engine = engineRegistry.getInstance( CDAS_COMPUTE_ENGINE + "Engine"  )
        self.cache_region = { "level": 85000 }

    def tearDown(self):
        pass

    def fail(self,msg):
        print>>sys.stderr, msg

    def getRegion(self):
        return '{"longitude": %.2f, "latitude": %.2f, "level": %.2f, "time":"%s" }' % (self.test_point[0],self.test_point[1],self.test_point[2],self.test_time)

    def getData(self, vars=[0]):
        var_list = ','.join( [ ( '"v%d:%s"' % ( ivar, MERRA_TEST_VARIABLES["vars"][ivar] ) ) for ivar in vars ] )
        data = '{"%s":[%s]}' % ( MERRA_TEST_VARIABLES["collection"], var_list )
        return data

    def getOp(self, kernel, type, bounds=None, slice=None ):
        op_spec = { "kernel": kernel, "type": type, }
        if bounds: op_spec[ "bounds" ] = bounds
        if slice: op_spec[ "slice" ] = slice
        return json.dumps( op_spec )

    def getOps(self, ops_args_list ):
        return [ self.getOp( *op_args ) for op_args in ops_args_list ]

    def getTaskArgs(self, op ):
        task_args = dict( self.def_task_args )
        task_args['operation'] = op
        return task_args

    def getResultData( self, results, index=0 ):
        if isinstance( results, Exception ):
            self.fail(str(results))
        return results[index]['data']

    def getResultStats( self, results, index=0 ):
        from modules.utilities import ExecutionRecord
        if isinstance( results, Exception ):
            self.fail(str(results))
        return ExecutionRecord( results[index]['exerec'] )

    def assertStatusEquals(self, result, **kwargs ):
        status = self.getResultStats( result )
        for item in kwargs.iteritems():
            self.assertEqual( status[item[0]], item[1] )

    def test01_cache(self):
        result = self.engine.execute( TaskRequest( task={ 'region': self.cache_region, 'data': self.getData() } ) )
        self.assertStatusEquals( result, cache_add=self.cache_region )

    def test02_departures(self):
        test_result = [-1.405364990234375, -1.258880615234375, 0.840728759765625, 2.891510009765625, -18.592864990234375, -11.854583740234375, -3.212005615234375, -5.311614990234375, 5.332916259765625, -1.698333740234375]
        task_args = self.getTaskArgs( op=self.getOp( "time", "departures", "np", 't' ) )
        result = self.engine.execute( TaskRequest( task=task_args ) )
        result_data = self.getResultData( result )
        self.assertAlmostEqual( test_result, result_data[0:len(test_result)] )
        self.assertStatusEquals( result, cache_found=Domain.COMPLETE, cache_found_domain=self.cache_region,  designated=True )

    def test03_annual_cycle(self):
        test_result = [48.07984754774306, 49.218166775173614, 49.36114501953125, 46.40715196397569, 46.3406982421875, 44.37486775716146, 46.54383680555556, 48.780619303385414, 46.378028021918404, 46.693325466579864, 48.840003119574654, 46.627953423394096]
        task_args = self.getTaskArgs( op=self.getOp( "time", "climatology", "annualcycle", 't' ) )
        result = self.engine.execute( TaskRequest( task=task_args ) )
        result_data = self.getResultData( result )
        self.assertAlmostEqual( test_result, result_data[0:len(test_result)] )

    def test04_value_retreval(self):
        test_result = 59.765625
        task_args = self.getTaskArgs( op=self.getOp( "time", "value" ) )
        result = self.engine.execute( TaskRequest( task=task_args ) )
        result_data = self.getResultData( result )
        self.assertAlmostEqual( test_result, result_data )

    def test05_multitask(self):
        test_results = [ [-1.405364990234375, -1.258880615234375, 0.840728759765625 ], [48.07984754774306, 49.218166775173614, 49.36114501953125], 59.765625 ]
        op_list = self.getOps( [ ( "time", "departures", "np", 't' ) , ( "time", "climatology", "annualcycle", 't' ), ( "time", "value" ) ] )
        task_args = self.getTaskArgs( op=op_list )
        results = self.engine.execute( TaskRequest( task=task_args ) )
        for ir, result in enumerate(results):
            result_data = self.getResultData( results, ir )
            test_result = test_results[ir]
            if hasattr( test_result, '__iter__' ):  self.assertAlmostEqual( test_result, result_data[0:len(test_result)] )
            else:                                   self.assertAlmostEqual( test_result, result_data )


if __name__ == '__main__':
    engineTests = EngineTests()
    engineTests.setUp()
    engineTests.test01_cache()


