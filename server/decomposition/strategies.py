from modules import configuration
from modules.utilities import *
from collections import OrderedDict
from datacache.domains import Region, CDAxis
from decomposition.manager import StrategyManager, RegionReductionStrategy


class CacheGrid:
    def __init__( self, index_bounds, axes ):
        self.grid = OrderedDict()
        for bnds, a in zip( index_bounds, axes ):
           self.grid[a] = bnds

    def size( self ):
        size = 1
        for bnds in self.grid.values(): size *= bnds[1] - bnds[0]
        return size

    def axes(self):
        return self.grid.keys()

    def ordered_axes( self, ordered_axis_list="tzyx" ):
        grid_axes = ''
        for axis in ordered_axis_list:
            if axis in self.grid.axes():
               grid_axes += axis
        return grid_axes

class Decomposition:

    def __init__( self, cache_grid, **args ):
        self.chunks = []
        self.grid = cache_grid

    def add(self, chunk ):
        self.chunks.append( chunk )

    def getAxis( self, slices ):
        merged_slices = ""
        for slice in slices:
            for s in slice:
                if s not in merged_slices:
                    merged_slices += s
        decomp_axis = 't'
        for axis in self.grid.ordered_axes():
            if axis not in merged_slices:
                decomp_axis = axis
                break
        return decomp_axis, self.grid[decomp_axis]

class DecompositionChunk:

    def __init__( self, axis_intervals=[] ):
        self.axis_intervals = axis_intervals

    def append( self, axis_interval ):
        self.axis_intervals.append( axis_interval )

    def __len__(self):
        return len(self.axis_intervals)

    def __getitem__(self, index):
        return self.axis_intervals[ index ]

    def __iter__(self):
        return self.axis_intervals.__iter__()

class AxisInterval:

    def __init__( self, axis_index, interval_range, coord_system="indexed" ):
        self.axis = axis_index
        self.interval = interval_range
        self.system = coord_system

class DecompositionStrategy(RegionReductionStrategy):

    def __init__( self, **args ):
        RegionReductionStrategy.__init__( self, **args )
        self.max_chunk_size = configuration.CDAS_MAX_CHUNK_SIZE



class DecimationStrategy(RegionReductionStrategy):
    pass


class SpaceStrategy( DecompositionStrategy ):

    ID = 'space.lon'

    def __init__( self, **args ):
        RegionReductionStrategy.__init__( self, **args )

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

    def getReducedRegions( self, cache_grid, slices, ncores, **args  ):
        global_size = cache_grid.get_size()
        nchunks_suggestion = ( global_size / self.max_chunk_size ) + 1
        decomp = Decomposition( cache_grid )
        if nchunks_suggestion > 1:
            decomp_axis, axis_bounds = decomp.getAxis( slices )
            axis_size = axis_bounds[1] - axis_bounds[0]
            if axis_size < ncores:
                for iChunk in range(axis_bounds[0],axis_bounds[1]):
                    decomp.add( DecompositionChunk( AxisInterval( decomp_axis, [iChunk,iChunk+1] ) ) )
            else:
                base_chunk_multiplicity = axis_size / ncores
                value_shift = axis_size - base_chunk_multiplicity * ncores
                chunk_start_location = axis_bounds[0]
                for iChunk in range(ncores):
                    chunk_multiplicity = base_chunk_multiplicity if (iChunk > value_shift) else base_chunk_multiplicity+1
                    chunk_end_location = chunk_start_location + chunk_multiplicity
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

decompositionManager = DecompositionManager()

class DecimationManager(StrategyManager):
    StrategyClass = DecimationStrategy

decimationManager = DecimationManager()







