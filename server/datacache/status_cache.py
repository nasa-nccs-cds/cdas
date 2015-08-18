import os, pickle
from modules.utilities import *

class StatusCacheManager:

    def __init__( self, **args ):
        self._cache_map = {}
        self.cacheDir = args.get( 'cache_dir', os.path.expanduser( "~/.cdas_cache") )
        if not os.path.exists( self.cacheDir ):
            try:
                os.makedirs( self.cacheDir )
            except OSError, err:
                if not os.path.exists( self.cacheDir ):
                    wpsLog.error( "Failed to create cache dir: %s ", str( err ) )
                    self.cacheDir = None

    def getCacheFilePath( self, cache_key ):
        return os.path.join( self.cacheDir, cache_key + ".pkl" ) if self.cacheDir else "UNDEF"

    def restore(self, cache_key):
        try:
            cacheFile = self.getCacheFilePath( cache_key )
            with open( cacheFile ) as cache_file:
                return pickle.load( cache_file )
        except IOError, err:
            wpsLog.error( " Error reading cache file '%s': %s" % ( cacheFile, str(err) ) )
        except EOFError:
            wpsLog.warning( " Empty cache file '%s'" % ( cacheFile  ) )

    def cache( self, cache_key, status_data ):
        try:
            cacheFile = self.getCacheFilePath( cache_key )
            with open( cacheFile, 'w' ) as cache_file:
                pickle.dump( status_data, cache_file )
        except IOError, err:
            wpsLog.error( " Error writing to cache file '%s': %s" % ( cacheFile, str(err) ) )
