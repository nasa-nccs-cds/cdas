from modules import configuration
from modules.utilities import *
from collections import OrderedDict
from datacache.domains import Region, CDAxis
from decomposition.manager import StrategyManager, RegionReductionStrategy


class CacheDims:
    def __init__( self, dims ):
        self.grid = {}
        for key,value in dims.items():
            self.grid[key] = [value] if isinstance( value, int ) else value

    def size( self ):
        size = 1
        for bnds in self.grid.values():
            dim_size = bnds[0] if (len(bnds) == 1) else bnds[1] - bnds[0]
            size *= dim_size
        return size

    def axes(self):
        return self.grid.keys()

    def __getitem__(self, key):
        return self.grid.get(key,None)

    def ordered_axes( self, ordered_axis_list="tzyx" ):
        grid_axes = ''
        for axis in ordered_axis_list:
            if axis in self.axes():
               grid_axes += axis
        return grid_axes

class Decomposition:

    def __init__( self, cache_grid, **args ):
        self.chunks = []
        self.grid = cache_grid

    def add(self, chunk ):
        self.chunks.append( chunk )

    def __str__(self):
        return "Decomp[\n\t%s\n\t\t]" % '\n\t'.join( [str(chunk) for chunk in self.chunks] )

    def mergeSlices(self,slices):
        if isinstance( slices, str ):
            merged_slices = slices
        else:
            merged_slices = ""
            for slice in slices:
                for s in slice:
                    if s not in merged_slices:
                        merged_slices += s
        return merged_slices

    def getAxis( self, slices ):
        merged_slices = self.mergeSlices( slices )
        decomp_axis = 't'
        for axis in self.grid.ordered_axes():
            if axis not in merged_slices:
                decomp_axis = axis
                break
        return decomp_axis, self.grid[decomp_axis]

class DecompositionChunk:

    def __init__( self, axis_intervals=[] ):
        if not isinstance( axis_intervals, (list,tuple) ): axis_intervals = [ axis_intervals ]
        self.axis_intervals = axis_intervals

    def __str__(self):
         return "DC[ %s ]" % ', '.join( [str(ai) for ai in self.axis_intervals] )

    def append( self, axis_interval ):
        self.axis_intervals.append( axis_interval )

    def getSubregion( self, region ):
        subregion = Region( region )
        for axis_interval in self.axis_intervals:
            subregion.addAxisBounds( *axis_interval.toCDAxis() )
        return subregion

    def __len__(self):
        return len(self.axis_intervals)

    def __getitem__(self, index):
        return self.axis_intervals[ index ]

    def __iter__(self):
        return self.axis_intervals.__iter__()

class AxisInterval:

    def __init__( self, axis_id, interval_range, coord_system="indexed" ):
        self.axis = axis_id
        self.interval = interval_range
        self.system = coord_system

    def __str__(self):
        return '{%s:%s}' % ( self.axis, str(self.interval) )

    def toCDAxis(self):
        axis_name = CDAxis.AXIS_LIST[ self.axis ]
        axis_spec = { 'start':self.interval[0], 'end':self.interval[1], 'system': self.system }
        return axis_name, CDAxis.getInstance( axis_name, axis_spec )

class DecompositionStrategy(RegionReductionStrategy):

    def __init__( self, **args ):
        RegionReductionStrategy.__init__( self, **args )
        self.max_chunk_size = configuration.CDAS_MAX_CHUNK_SIZE

class DecimationStrategy(RegionReductionStrategy):
    pass


class SpaceStrategy( DecompositionStrategy ):

    ID = 'space.lon'

    def __init__( self, **args ):
        DecompositionStrategy.__init__( self, **args )

    def getReducedRegion( self, global_region, **args ):
        inode=args.get('node',0)
        num_nodes=args.get('node_count',configuration.CDAS_DEFAULT_NUM_NODES )
        return global_region
        if global_region == None: return None
        node_region = global_region
        for dim_name, range_val in global_region.items():
           if dim_name.startswith('lon'):
               if ( range_val[0] == range_val[1] ) or ( num_nodes <= 1 ):
                   return global_region if inode == 0 else None
               else:
                   dx = ( range_val[1] - range_val[0] ) / num_nodes
                   r0 = range_val[0] + dx * inode
                   r1 = r0 + dx
                   node_region[ dim_name ] = ( r0, r1 )
                   return node_region

    def getDecomposition( self, cache_grid, slices, **args  ):
        ncores= args.get( 'ncores', configuration.CDAS_NUM_WORKERS )
        global_size = cache_grid.size()
        nchunks_suggestion = ( global_size / self.max_chunk_size ) + 1
        decomp = Decomposition( cache_grid )
        if nchunks_suggestion > 1:
            decomp_axis, axis_bounds = decomp.getAxis( slices )
            axis_size = axis_bounds[1] - axis_bounds[0]
            if axis_size < ncores:
                for iChunk in range(axis_bounds[0],axis_bounds[1]):
                    decomp.add( DecompositionChunk( AxisInterval( decomp_axis, [iChunk,iChunk+1] ) ) )
            else:
                chunk_multiplicity = axis_size / float(ncores)
                chunk_start_location = axis_bounds[0]
                for iChunk in range(1,ncores+1):
                    chunk_end_location = int( round( axis_bounds[0] + iChunk*chunk_multiplicity ) )
                    decomp.add( DecompositionChunk( AxisInterval( decomp_axis, [ chunk_start_location, chunk_end_location ] )  ) )
                    chunk_start_location = chunk_end_location
        return decomp


class TimeSubsetStrategy( DecimationStrategy ):

    ID = 'time.subset'

    def __init__( self, **args ):
        RegionReductionStrategy.__init__( self, **args )
        self.max_size = args.get( 'max_size', 1000 )

    def getTimeAxis(self, **args):
        axes = args.get( 'axes', None )
        for axis in axes:
            if axis.isTime():
                return axis

    def getReducedRegion( self, region, **args ):
        time_axis = self.getTimeAxis( **args )
        if time_axis:
            region_size = region.getIndexedAxisSize( CDAxis.AXIS_LIST['t'], time_axis )
            step_size = int(region_size/self.max_size) + 1
            region.setStepSize( 't', step_size, time_axis )
        return region


class DecompositionManager(StrategyManager):
    StrategyClass = DecompositionStrategy

    def getDecomposition( self, cache_grid, slices, **args ):
        strategy = self.getStrategy()
        assert strategy is not None, "Error, undefined decomposition strategy."
        return strategy.getDecomposition( cache_grid, slices, **args  )

decompositionManager = DecompositionManager()

class DecimationManager(StrategyManager):
    StrategyClass = DecimationStrategy

    def getReducedRegion( self, region, **args ):
        strategy = self.getStrategy( region, **args  )
        assert strategy is not None, "Error, undefined decomposition strategy."
        return strategy.getReducedRegion( region, **args  )

decimationManager = DecimationManager()


if __name__ == '__main__':

    grid = CacheDims( 'xyzt', [ (0,365), (0,180), (0,40), (0,500) ] )
    ss = SpaceStrategy()
    ncores = 3
    slices = ['t']
    rrs = ss.getReducedRegions( grid, slices, ncores )
    print rrs







