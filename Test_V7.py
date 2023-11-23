import logging
from neo4j import GraphDatabase, Transaction

# Configure logging
logging.basicConfig(level=logging.INFO)

# We can also create a new class NozzleRemoval for this feature
class knodex:
    """def __init__():
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
"""
    # To execute a Cypher query on the Neo4j database
    #_execute_query doesn’t depend on the state of the class
    @staticmethod
    def _execute_query(tx: Transaction, query: str, parameters=None):
        try:
            result = tx.run(query, parameters)
            return result
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            return None

    # To modify the graph in the Neo4j database
    def remove_nozzle_and_update_flow(self, nozzle_id: str, component_class_n: str, component_class_m: str):
        logging.info(f"remove_nozzle_and_update_flow called with nozzle_id={nozzle_id}, component_class_n={component_class_n}, component_class_m={component_class_m}")
        
        # Nozzle removal and update Relationships
        cypher_query = """
        MATCH (o)-[hn:has_nozzle]->(m)-[mf:material_flow]->(n)
        WHERE n.ComponentClass=$component_class_n 
        AND m.ComponentClass=$component_class_m AND m.dexpi_id = $nozzle_id
        CREATE (n)-[m1:material_flow]->(o) 
        CREATE (o)-[m2:material_flow]->(n)
        DETACH DELETE m
        RETURN m,o,n
        """
        parameters = {"nozzle_id": nozzle_id, "component_class_n": component_class_n, "component_class_m": component_class_m}
        with self.driver.session() as session:
            result = session.write_transaction(self._execute_query, cypher_query, parameters)

        if result is None:
            logging.warning(f"Nozzle with id {nozzle_id} not found or could not be removed.")
            return

        logging.info("Nozzle removed and material flow relationships updated successfully.")

        # Define'o' and 'n' are our actual nodes
        triple = dict(subject='o', predicate="material_flow", object='n')
        self.publish_relation(triple)
        self.publish_node({'name': 'o'})
        self.publish_node({'name': 'n'})



    # Defined implementation from knodex.py
    def publish_relation(self, triple):
        
        pass

    def publish_node(self, properties):

        pass
