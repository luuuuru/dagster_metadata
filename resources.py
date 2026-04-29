import dagster as dg
import ibis

class ImpalaResource(dg.ConfigurableResource):
    """
    Resource Manager for Connections.
        """
def get_connection(self):
        """
        Establishes and returns an Ibis Impala connection.
        
        """
