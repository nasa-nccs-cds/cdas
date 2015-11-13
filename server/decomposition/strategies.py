from modules import configuration
from modules.utilities import *
from datacache.domains import Region, CDAxis
from decomposition.manager import StrategyManager, RegionReductionStrategy

def get_global_size( shape ):
    size = 1
    for sval in shape: size *= sval
    return size

class Decomposition:

    def __init__( self, chunks, global_region, **args ):
        self.chunks = chunks
        self.region = global_region

    @staticmethod
    def getAxis( grid, slices ):
        merged_slices = ""
        for slice in slices:
            for s in slice:
                if s not in merged_slices:
                    merged_slices += s
        decomp_axis = 't'
        grid_axes = ''
        for axis in "tzyx":
            if axis in grid:
               grid_axes += axis
        for axis in grid_axes:
            if axis not in merged_slices:
                decomp_axis = axis
                break
        return decomp_axis

class DecompositionChunk:

    def __init__( self, shape, subregion ):
        self.region = subregion
        self.shape = shape

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

    def getReducedRegions( self, global_shape, global_region, grid, slices, ncores, **args  ):
        global_size = get_global_size( global_shape )
        nchunks_suggestion = ( global_size / self.max_chunk_size ) + 1
        if nchunks_suggestion > 1:
            decomp_axis = Decomposition.getAxis(grid,slices)
            axis_index = grid.find( decomp_axis )
            axis_size = global_shape[axis_index]
            if axis_size < ncores:
                chunk_shape = list( global_shape )
                chunk_shape[axis_index] = 1
                subregions = global_region.subdevide( decomp_axis, axis_size )
                chunks = [ DecompositionChunk( chunk_shape, sr ) for sr in subregions ]
            else:
                base_chunk_multiplicity = axis_size / ncores
                remainder = axis_size - base_chunk_multiplicity * ncores
                chunks = []
                chunk_start_location = 0
                for iChunk in range(ncores):
                    chunk_shape = list( global_shape )
                    chunk_multiplicity = base_chunk_multiplicity if (iChunk > remainder) else base_chunk_multiplicity+1
                    chunk_shape[axis_index] = chunk_multiplicity
                    chunk_end_location = chunk_start_location + chunk_multiplicity
                    sr = global_region.get_subdivision( decomp_axis, [ chunk_start_location/float(axis_size), chunk_end_location/float(axis_size) ] )
                    chunks.append( DecompositionChunk( chunk_shape, sr ) )
                    chunk_start_location = chunk_end_location

            decomp = Decomposition( chunks, global_region, global_shape )




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







