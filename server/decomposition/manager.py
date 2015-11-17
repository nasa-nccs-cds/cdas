from modules import configuration
from modules.utilities import *
import inspect

class RegionReductionStrategy:

    def __init__( self, **args ):
        pass


class StrategyManager:

    def __init__( self ):
        self.strategies = {}
        self.strategy_spec = configuration.CDAS_REDUCTION_STRATEGY[ self.StrategyClass.__name__ ]
        self.load()

    def getStrategy( self ):
        return self.strategies[ self.strategy_spec['id'] ]

    def load(self):
        import strategies
        class_map = strategies.__dict__
        for name, cls in class_map.items():
            if inspect.isclass(cls) and issubclass( cls, self.StrategyClass ) and cls <> self.StrategyClass:
                try:
                    self.strategies[ cls.ID ] = cls( **self.strategy_spec )
                except Exception, err:
                    wpsLog.error( "StrategyManager load error: %s " % str( err ) )
