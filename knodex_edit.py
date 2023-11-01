#import xml.etree.ElementTree as ET
import lxml.etree as ET
from neo4j import GraphDatabase
from string import Template
import json
import pdb as pdb
from os import path, remove
from rdflib import Graph, Literal, RDF, URIRef

class knodex:
    def __init__(
        self,
        path,
        graph_type="Neo4j",
        uri="bolt://localhost:7687",
        user="neo4j",
        password="comoscomos",
    ):
        """Initialize the knowledge graph.

        A knowlegde graph object can be initialized by setting the path to the XML file, set the knowledge graph type and initializing the knowledge graph.

        Parameters
        ----------
        path : str
            directory of the dexpi file
        graph_type : str, optional (default='GraphML')
            The type of the knowledge graph that should be generated 'GraphML', 'Neo4j', 'RDF'
        uri : str, optional (default='bolt://localhost:7687')
            (Only needed for the case of neo4j) The uri for the server of the graph structure
        user: str, optional (default='neo4j')
            (Only needed for the case of neo4j) The username for accessing the server of the graph structure
        password: str, optional (default='comoscomos')
            (Only needed for the case of neo4j) The password for accessing the server of the graph structure
        """
        self._path = path
        self._graph_type = graph_type
        print(graph_type)
        if self._graph_type == "Neo4j":
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
        elif self._graph_type == "RDF":
            
            self.driver = Graph()
        elif self._graph_type == "GraphML":
            #initialize a graphml
            ""


    def close(self):
        self.driver.close()

    def add_rdfnode(self, properties):
        string_temp = "http://acplt.de"
        element_instance = URIRef(string_temp + '#' + properties["dexpi_id"])
        element_class = URIRef(string_temp + '/' + properties["ComponentClassURI"])
        self.driver.add((element_instance, RDF.type, element_class))

    def augment_rdfnode(self, properties):
        s = Template(
            "MATCH (a {dexpi_id:'$dexpi_id'}) "
            "SET a.$attr_name = '$value' "
            "RETURN id(a)"
        )
        string_temp = "http://acplt.de"
        predicate = URIRef(string_temp + '/' + properties["attr_name"].replace(" ", ""))
        subject = URIRef(string_temp + '#' + properties["dexpi_id"].replace(" ", ""))
        object = Literal(properties["value"])
        self.driver.add((subject, predicate, object))
    
    def augment_rdf_relation(self, properties):
        string_temp = "http://acplt.de"
        predicate = URIRef(string_temp + '/' + properties["predicate"].replace(" ", ""))
        subject = URIRef(string_temp + '#' + properties["subject"].replace(" ", ""))
        object = URIRef(string_temp + '#' + properties["object"].replace(" ", ""))
        self.driver.add((subject, predicate, object))

    def add_graphml_node(self, properties):
        """Adds a graphml node

        
        Parameters
        ----------
        properties : dictionary object that contains the name of the node and additional attributes
                e.g. properties = dict(name='PlateHeatExchanger1', ComponentClass= "PlateHeatExchanger", ComponentClassURI= "http://sandbox.dexpi.org/rdl/PlateHeatExchanger", dexpi_id= "PlateHeatExchanger-1")
        """

    def augment_graphml_node(self, properties):
        """ Augments an existing node with additional properties.

            Parameters
            ----------
            properties : dictionary object that contains the name of the node and additional attributes
                e.g. properties = dict(name='Pump', ComponentClass= "Centrifugal")

            """
    
    def augment_graphml_relation(self, properties):
        """
            Adds a relation between 2 nodes.

            Parameters
            ----------
            properties : dictionary object that contains the ID of the source node (subject), relationship type (predicate) & the ID of the target node (Object)
                e.g. properties = dict(subject= DEXPI_node_ID_1, predicate= "material_flow" , object= DEXPI_node_ID_2)

            """

    def publish_node(self, properties):
            """Neo4J only:

            Sends a write request to create a node to the Neo4j server.
            
            Parameters
            ----------
            properties : dictionary object that contains the name of the node and additional attributes
                e.g. properties = dict(name='Pump', type= "Centrifugal", Type_id= "Centrifugal")

            """
            if self._graph_type == "Neo4j":
                result = self.driver.session().write_transaction(self._create_nodes, properties)
                #print(result)
            elif self._graph_type == "RDF":
                result = self.add_rdfnode(properties)
            elif self._graph_type == "GraphML":
                result = self.add_graphml_node(properties)

    def publish_relation(self, triple):
            """Neo4J only:

            Sends a write request to create a relation to the Neo4j server.

            Parameters
            ----------
            triple : dictionary object that contains the ID of the source node (subject), relationship type (predicate) & the ID of the target node (Object)
                e.g. triple = dict(subject= child3.attrib.get("FromID"), predicate= "material_flow" , object= child3.attrib.get("ToID"))

            """
            if self._graph_type == "Neo4j":
                self.driver.session().write_transaction(self._create_relations, triple)
            elif self._graph_type == "RDF":
                result = self.augment_rdf_relation(triple)
            elif self._graph_type == "GraphML":
                result = self.augment_graphml_relation(triple)



    def augment_node(self, properties):
        
            """
            Sends a write request to add attributes to an existing node to the Neo4j server.

            Parameters
            ----------
            properties : dictionary object that contains the name of the node and additional attributes
                e.g. properties = dict(name='Pump', ComponentClass= "Centrifugal")

            """
            if self._graph_type == "Neo4j":
                result = self.driver.session().write_transaction(
                self._augment_nodes, properties
            )
            elif self._graph_type == "RDF":
                result = self.augment_rdfnode(properties)
            elif self._graph_type == "GraphML":
                result = self.augment_graphml_node(properties)
                
                
            #print(result)
    
    @staticmethod
    def _create_nodes(tx, properties):
        """Neo4J only:

            Writes a CYPTHER command to create a node

            Parameters
            ----------
            properties : dictionary object that contains the name of the node and additional attributes
                e.g. properties = dict(name='PlateHeatExchanger1', ComponentClass= "PlateHeatExchanger", ComponentClassURI= "http://sandbox.dexpi.org/rdl/PlateHeatExchanger", dexpi_id= "PlateHeatExchanger-1")

            """
        s = Template(
            "CREATE (a:$name) "
            "SET a.Name = '$name' "
            "SET a.ComponentClass = '$ComponentClass' "
            "SET a.ComponentClassURI = '$ComponentClassURI' "
            "SET a.dexpi_id = '$dexpi_id' "
            "RETURN id(a)"
        )
        result = tx.run(s.substitute(properties), properties=properties)
        return result.single()[0]

    @staticmethod
    def _create_relations(tx, triple):
        """Neo4J only:

            Writes a CYPTHER command to create a relation
            
            Parameters
            ----------
            properties : dictionary object that contains the name of the node and additional attributes
                e.g. properties = dict(name='Pump', ComponentClass= "Centrifugal")

            """
        s = Template(
            "MATCH (subject),(object) WHERE subject.dexpi_id = '$subject' AND object.dexpi_id = '$object' "
            "CREATE (subject)-[r:$predicate]->(object)  "
            "RETURN type(r)"
        )
        result = tx.run(s.substitute(triple), triple=triple)
        return result.single()[0]

    @staticmethod
    def _augment_nodes(tx, properties):
        """Neo4J only:

            Writes a CYPTHER command to augment an existing node with new attributes
            
            Parameters
            ----------
            properties : dictionary object that contains the dexpi_id of the target node, the attribute's name and its value
                e.g. properties = dict(dexpi_id = equip.attrib["ID"], attr_name=attrib.attrib["Name"].replace("(", '_').replace(")", '_'), value= attrib.attrib["Value"])

            """
        s = Template(
            "MATCH (a {dexpi_id:'$dexpi_id'}) "
            "SET a.$attr_name = '$value' "
            "RETURN id(a)"
        )
        result = tx.run(s.substitute(properties), properties=properties)
        #print(result)
        return result.single()[0]

    def extract_equipment(self):
        """Neo4J only:

            Deserializes the XML file, extracts all modelled equipments and nozzles together with all their attributes. 
            The extracted elements are used to create nodes on the Neo4j server
            
            Parameters
            ----------
            None

            """
        tree = ET.parse(path)
        root = tree.getroot()
        root= self._delete_shape_drawings(root)                           
        #for equip in root.iter("Equipment"):
        equipment = [x for x in root.getchildren() if x.tag == 'Equipment']
        for equip in equipment:
            properties = dict(name=equip.attrib["ID"].replace("-", ''), ComponentClass= equip.attrib["ComponentClass"], ComponentClassURI = equip.attrib["ComponentClassURI"], dexpi_id = equip.attrib["ID"])
            equip_node_id=self.publish_node(properties)
            attribs_all = [x for x in equip.getchildren() if x.tag == 'GenericAttributes']
            for attribs in attribs_all:
                if 'Set' in attribs.attrib and attribs.attrib["Set"] == "DexpiAttributes":
                    #print(child.attrib["ID"], child2.attrib)
                    for attrib in attribs.iter("GenericAttribute"):
                        properties = dict(dexpi_id = equip.attrib["ID"], attr_name=attrib.attrib["Name"].replace("(", '_').replace(")", '_'), value= attrib.attrib["Value"])
                        equip_node_id=self.augment_node(properties)
            equipment2 = [x for x in equip.getchildren() if x.tag == 'Equipment']
            for equip2 in equipment2:
                properties = dict(name=equip2.attrib["ID"].replace("-", ''), ComponentClass= equip2.attrib["ComponentClass"], ComponentClassURI = equip2.attrib["ComponentClassURI"], dexpi_id = equip2.attrib["ID"])
                equip_node_id=self.publish_node(properties)
                attribs_all = [x for x in equip2.getchildren() if x.tag == 'GenericAttributes']
                for attribs in attribs_all:
                    if 'Set' in attribs.attrib and attribs.attrib["Set"] == "DexpiAttributes":
                        #print(child.attrib["ID"], child2.attrib)
                        for attrib in attribs.iter("GenericAttribute"):
                            properties = dict(dexpi_id = equip2.attrib["ID"], attr_name=attrib.attrib["Name"].replace("(", '_').replace(")", '_'), value= attrib.attrib["Value"])
                            equip_node_id=self.augment_node(properties)
                triple = dict(subject= equip.attrib["ID"], predicate= "has_component" , object= equip2.attrib["ID"])
                self.publish_relation(triple)
            nozzle_all = [x for x in equip.getchildren() if x.tag == 'Nozzle']
            for nozzle in nozzle_all:
                properties = dict(name=nozzle.attrib["ID"].replace("-", ''), ComponentClass= nozzle.attrib["ComponentClass"], ComponentClassURI = nozzle.attrib["ComponentClassURI"], dexpi_id = nozzle.attrib["ID"])
                nozzle_node=self.publish_node(properties)
                for attribs in nozzle.iter("GenericAttributes"):
                    if 'Set' in attribs.attrib and attribs.attrib["Set"] == "DexpiAttributes":
                        for attrib in attribs.iter("GenericAttribute"):
                            properties = dict(dexpi_id = nozzle.attrib["ID"], attr_name=attrib.attrib["Name"], value= attrib.attrib["Value"])
                            equip_node_id=self.augment_node(properties)
                triple = dict(subject= equip.attrib["ID"], predicate= "has_nozzle" , object= nozzle.attrib["ID"])
                self.publish_relation(triple)
                #print(child3.attrib["ID"])
            #greeter.print_component("Pump", "Centrifugal")
        #self.driver.close()

    def extract_pipes(self):
        """Neo4J only:

            Deserializes the XML file, extracts all modelled pipes and together with pipe components. 
            The extracted elements are used to create nodes on the Neo4j server
            
            Parameters
            ----------
            None
            
            """
        tree = ET.parse(path)
        root = tree.getroot()
        root= self._delete_shape_drawings(root)
        for pipingsys in root.iter("PipingNetworkSystem"):
            properties = dict(name=pipingsys.attrib["ID"].replace("-", ''), ComponentClass= pipingsys.attrib["ComponentClass"], ComponentClassURI = pipingsys.attrib["ComponentClassURI"], dexpi_id = pipingsys.attrib["ID"])
            equip_node_id=self.publish_node(properties)
            attribs_all = [x for x in pipingsys.getchildren() if x.tag == 'GenericAttributes']
            for attribs in attribs_all:
                #print(child.attrib["ID"], child2.attrib)
                if 'Set' in attribs.attrib and attribs.attrib["Set"] == "DexpiAttributes":
                    for attrib in attribs.iter("GenericAttribute"):
                        properties = dict(dexpi_id = pipingsys.attrib["ID"], attr_name=attrib.attrib["Name"], value= attrib.attrib["Value"])
                        equip_node_id=self.augment_node(properties)
            for pipingseg in pipingsys.iter("PipingNetworkSegment"):
                properties = dict(name=pipingseg.attrib["ID"].replace("-", ''), ComponentClass= pipingseg.attrib["ComponentClass"], ComponentClassURI = pipingseg.attrib["ComponentClassURI"], dexpi_id = pipingseg.attrib["ID"])
                pipseg_node=self.publish_node(properties)
                attribs_all = [x for x in pipingseg.getchildren() if x.tag == 'GenericAttributes']
                for attribs in attribs_all:
                    if 'Set' in attribs.attrib and attribs.attrib["Set"] == "DexpiAttributes":
                        for attrib in attribs.iter("GenericAttribute"):
                            properties = dict(dexpi_id = pipingseg.attrib["ID"], attr_name=attrib.attrib["Name"], value= attrib.attrib["Value"])
                            equip_node_id=self.augment_node(properties)
                triple = dict(subject= pipingsys.attrib["ID"], predicate= "has_seg" , object= pipingseg.attrib["ID"])
                self.publish_relation(triple)
                for pipeoff in pipingseg.iter("PipeOffPageConnector"):
                    properties = dict(name=pipeoff.attrib["ID"].replace("-", ''), ComponentClass= pipeoff.attrib["ComponentClass"], ComponentClassURI = pipeoff.attrib["ComponentClassURI"], dexpi_id = pipeoff.attrib["ID"])
                    pipseg_node=self.publish_node(properties)
                for pipingcomp in pipingseg.iter("PipingComponent"):
                    properties = dict(name=pipingcomp.attrib["ID"].replace("-", ''), ComponentClass= pipingcomp.attrib["ComponentClass"], ComponentClassURI = pipingcomp.attrib["ComponentClassURI"], dexpi_id = pipingcomp.attrib["ID"])
                    pipseg_node=self.publish_node(properties)
                    for attribs in pipingcomp.iter("GenericAttributes"):
                        if 'Set' in attribs.attrib and attribs.attrib["Set"] == "DexpiAttributes":
                            for attrib in attribs.iter("GenericAttribute"):
                                properties = dict(dexpi_id = pipingcomp.attrib["ID"], attr_name=attrib.attrib["Name"], value= attrib.attrib["Value"])
                                equip_node_id=self.augment_node(properties)
                    triple = dict(subject= pipingseg.attrib["ID"], predicate= "has_component" , object = pipingcomp.attrib["ID"])
                    self.publish_relation(triple)                
        #self.driver.close()

    def connect_pipes(self):
        """Neo4J only:

            Deserializes the XML file, extracts all the connections between piping systems and piping segments 
            The extracted elements are used to create relations on the Neo4j server that model the material flow.
            
            Parameters
            ----------
            None
            
            """
        tree = ET.parse(path)
        root = tree.getroot()
        root= self._delete_shape_drawings(root)
        pipingsys_all = [x for x in root.getchildren() if x.tag == 'PipingNetworkSystem']
        for pipingsys in pipingsys_all:
            for pipingseg in pipingsys.iter("PipingNetworkSegment"):
                connection_all = [x for x in pipingseg.getchildren() if x.tag == 'Connection']
                for connection in connection_all:
                    if ("FromID" in connection.attrib):
                        triple = dict(subject= connection.attrib.get("FromID"), predicate= "material_flow" , object= pipingseg.attrib["ID"])
                        self.publish_relation(triple)
                        triple = dict(subject= pipingseg.attrib["ID"], predicate= "material_flow" , object= connection.attrib.get("FromID"))
                        self.publish_relation(triple)
                    if ("ToID" in connection.attrib):
                        triple = dict(subject= pipingseg.attrib["ID"], predicate= "material_flow" , object= connection.attrib.get("ToID"))
                        self.publish_relation(triple)
                        triple = dict(subject= connection.attrib.get("ToID"), predicate= "material_flow" , object= pipingseg.attrib["ID"])
                        self.publish_relation(triple)
                         
    @staticmethod
    def _delete_shape_drawings(root):
        """Neo4J only:

            Deletes the 'Drawing' & 'ShapeCatalogue' objects from the lxml structure.
            The function can sometimes be helpful but it is not necessary to use.

            Parameters
            ----------
            None
            
            """
        for elem in root.findall("Drawing"):
            root.remove(elem)
        for elem in root.findall("ShapeCatalogue"):
            root.remove(elem)
        return root

    def extract_actuators(self):
        """Neo4J only:

            Deserializes the XML file, extracts all modelled actuator systems as well as actuator components. 
            The extracted elements are used to create nodes on the Neo4j server
            
            Parameters
            ----------
            None
            
        """
        tree = ET.parse(path)
        root = tree.getroot()
        root= self._delete_shape_drawings(root)                           
        #for equip in root.iter("Equipment"):
        actsys_all = [x for x in root.getchildren() if x.tag == 'ActuatingSystem']
        for actsys in actsys_all:
            properties = dict(name=actsys.attrib["ID"].replace("-", ''), ComponentClass= actsys.attrib["ComponentClass"], ComponentClassURI = actsys.attrib["ComponentClassURI"], dexpi_id = actsys.attrib["ID"])
            equip_node_id=self.publish_node(properties)
            attribs_all = [x for x in actsys.getchildren() if x.tag == 'GenericAttributes']
            for attribs in attribs_all:
                if 'Set' in attribs.attrib and attribs.attrib["Set"] == "DexpiAttributes":
                    #print(child.attrib["ID"], child2.attrib)
                    for attrib in attribs.iter("GenericAttribute"):
                        properties = dict(dexpi_id = actsys.attrib["ID"], attr_name=attrib.attrib["Name"].replace("(", '_').replace(")", '_'), value= attrib.attrib["Value"])
                        equip_node_id=self.augment_node(properties)
            for actcomp in actsys.iter("ActuatingSystemComponent"):
                properties = dict(name=actcomp.attrib["ID"].replace("-", ''), ComponentClass= actcomp.attrib["ComponentClass"], ComponentClassURI = actcomp.attrib["ComponentClassURI"], dexpi_id = actcomp.attrib["ID"])
                actcomp_node=self.publish_node(properties)
                for attribs in actcomp.iter("GenericAttributes"):
                    if 'Set' in attribs.attrib and attribs.attrib["Set"] == "DexpiAttributes":
                        for attrib in attribs.iter("GenericAttribute"):
                            properties = dict(dexpi_id = actcomp.attrib["ID"], attr_name=attrib.attrib["Name"], value= attrib.attrib["Value"])
                            equip_node_id=self.augment_node(properties)
                triple = dict(subject= actsys.attrib["ID"], predicate= "has_component" , object= actcomp.attrib["ID"])
                self.publish_relation(triple)
        #self.driver.close()

    def extract_instrumentation_functions(self):
        """Neo4J only:

            Deserializes the XML file, and extracts the instrumentation objects (Sensors). 
            The extracted elements are used to create nodes on the Neo4j server
            
            Parameters
            ----------
            None
            
        """
        tree = ET.parse(path)
        root = tree.getroot()
        root= self._delete_shape_drawings(root)                           
        #for equip in root.iter("Equipment"):
        instfcn_all = [x for x in root.getchildren() if x.tag == 'ProcessInstrumentationFunction']
        for instfcn in instfcn_all:
            properties = dict(name=instfcn.attrib["ID"].replace("-", ''), ComponentClass= instfcn.attrib["ComponentClass"], ComponentClassURI = instfcn.attrib["ComponentClassURI"], dexpi_id = instfcn.attrib["ID"])
            equip_node_id=self.publish_node(properties)
            attribs_all = [x for x in instfcn.getchildren() if x.tag == 'GenericAttributes']
            for attribs in attribs_all:
                if 'Set' in attribs.attrib and attribs.attrib["Set"] == "DexpiAttributes":
                    #print(child.attrib["ID"], child2.attrib)
                    for attrib in attribs.iter("GenericAttribute"):
                        properties = dict(dexpi_id = instfcn.attrib["ID"], attr_name=attrib.attrib["Name"].replace("(", '_').replace(")", '_'), value= attrib.attrib["Value"])
                        equip_node_id=self.augment_node(properties)
            for sgngen in instfcn.iter("ProcessSignalGeneratingFunction"):
                properties = dict(name=sgngen.attrib["ID"].replace("-", ''), ComponentClass= sgngen.attrib["ComponentClass"], ComponentClassURI = sgngen.attrib["ComponentClassURI"], dexpi_id = sgngen.attrib["ID"])
                actcomp_node=self.publish_node(properties)
                for attribs in sgngen.iter("GenericAttributes"):
                    if 'Set' in attribs.attrib and attribs.attrib["Set"] == "DexpiAttributes":
                        for attrib in attribs.iter("GenericAttribute"):
                            properties = dict(dexpi_id = sgngen.attrib["ID"], attr_name=attrib.attrib["Name"], value= attrib.attrib["Value"])
                            equip_node_id=self.augment_node(properties)
                #triple = dict(subject= sgngen.attrib["ID"], predicate= "has_component" , object= sgngen.attrib["ID"])
                #self.publish_relation(triple)
            for inflo in instfcn.iter("InformationFlow"):
                properties = dict(name=inflo.attrib["ID"].replace("-", ''), ComponentClass= inflo.attrib["ComponentClass"], ComponentClassURI = inflo.attrib["ComponentClassURI"], dexpi_id = inflo.attrib["ID"])
                actcomp_node=self.publish_node(properties)
                for attribs in inflo.iter("GenericAttributes"):
                    if 'Set' in attribs.attrib and attribs.attrib["Set"] == "DexpiAttributes":
                        for attrib in attribs.iter("GenericAttribute"):
                            properties = dict(dexpi_id = inflo.attrib["ID"], attr_name=attrib.attrib["Name"], value= attrib.attrib["Value"])
                            equip_node_id=self.augment_node(properties)
                #triple = dict(subject= sgngen.attrib["ID"], predicate= "has_component" , object= sgngen.attrib["ID"])
                #self.publish_relation(triple)
            for actfnc in instfcn.iter("ActuatingFunction"):
                properties = dict(name=actfnc.attrib["ID"].replace("-", ''), ComponentClass= actfnc.attrib["ComponentClass"], ComponentClassURI = actfnc.attrib["ComponentClassURI"], dexpi_id = actfnc.attrib["ID"])
                actcomp_node=self.publish_node(properties)
                for attribs in actfnc.iter("GenericAttributes"):
                    if 'Set' in attribs.attrib and attribs.attrib["Set"] == "DexpiAttributes":
                        for attrib in attribs.iter("GenericAttribute"):
                            properties = dict(dexpi_id = actfnc.attrib["ID"], attr_name=attrib.attrib["Name"], value= attrib.attrib["Value"])
                            equip_node_id=self.augment_node(properties)
                #triple = dict(subject= sgngen.attrib["ID"], predicate= "has_component" , object= sgngen.attrib["ID"])
                #self.publish_relation(triple)
        #self.driver.close()

    def extract_instrumentation_loops(self):
        """Neo4J only:

            Deserializes the XML file, and extracts the control loop objects. 
            The extracted elements are used to create nodes on the Neo4j server
            
            Parameters
            ----------
            None
            
        """
        tree = ET.parse(path)
        root = tree.getroot()
        root= self._delete_shape_drawings(root)                           
        #for equip in root.iter("Equipment"):
        instfcn_all = [x for x in root.getchildren() if x.tag == 'InstrumentationLoopFunction']
        for instfcn in instfcn_all:
            properties = dict(name=instfcn.attrib["ID"].replace("-", ''), ComponentClass= instfcn.attrib["ComponentClass"], ComponentClassURI = instfcn.attrib["ComponentClassURI"], dexpi_id = instfcn.attrib["ID"])
            equip_node_id=self.publish_node(properties)
            attribs_all = [x for x in instfcn.getchildren() if x.tag == 'GenericAttributes']
            for attribs in attribs_all:
                if 'Set' in attribs.attrib and attribs.attrib["Set"] == "DexpiAttributes":
                    #print(child.attrib["ID"], child2.attrib)
                    for attrib in attribs.iter("GenericAttribute"):
                        properties = dict(dexpi_id = instfcn.attrib["ID"], attr_name=attrib.attrib["Name"].replace("(", '_').replace(")", '_'), value= attrib.attrib["Value"])
                        equip_node_id=self.augment_node(properties)
            for ass in instfcn.iter("Association"):
                triple = dict(subject= instfcn.attrib["ID"], predicate= ass.attrib["Type"].replace(" ", '_') , object= ass.attrib["ItemID"])
                self.publish_relation(triple)

    def connect_instrumentation_functions(self):
        """Neo4J only:

            Deserializes the XML file, and extracts the connections (associations) between the sensors and control loops. 
            The extracted elements are used to create relations on the Neo4j server
            
            Parameters
            ----------
            None
            
        """
        tree = ET.parse(path)
        root = tree.getroot()
        root= self._delete_shape_drawings(root)                           
        #for equip in root.iter("Equipment"):
        instfcn_all = [x for x in root.getchildren() if x.tag == 'ProcessInstrumentationFunction']
        for instfcn in instfcn_all:
            ass_all = [x for x in instfcn.getchildren() if x.tag == 'Association']
            for ass in ass_all:
                triple = dict(subject= instfcn.attrib["ID"], predicate= ass.attrib["Type"].replace(" ", '_') , object= ass.attrib["ItemID"])
                self.publish_relation(triple)
            for sgngen in instfcn.iter("ProcessSignalGeneratingFunction"):
                ass_all = [x for x in sgngen.getchildren() if x.tag == 'Association']
                for ass in ass_all:
                    triple = dict(subject= sgngen.attrib["ID"], predicate= ass.attrib["Type"].replace(" ", '_') , object= ass.attrib["ItemID"])
                    self.publish_relation(triple)
            for inflo in instfcn.iter("InformationFlow"):
                ass_all = [x for x in inflo.getchildren() if x.tag == 'Association']
                for ass in ass_all:
                    triple = dict(subject= inflo.attrib["ID"], predicate= ass.attrib["Type"].replace(" ", '_') , object= ass.attrib["ItemID"])
                    self.publish_relation(triple)
            for actfnc in instfcn.iter("ActuatingFunction"):
                ass_all = [x for x in instfcn.getchildren() if x.tag == 'Association']
                for ass in ass_all:
                    triple = dict(subject= actfnc.attrib["ID"], predicate= ass.attrib["Type"].replace(" ", '_') , object= ass.attrib["ItemID"])
                    self.publish_relation(triple)
            
    def connect_actuators_instrumentation_equipment(self):
        """Neo4J only:

            Deserializes the XML file, and extracts the connections (associations) between the actuators and the control loops. 
            The extracted elements are used to create relations on the Neo4j server
            
            Parameters
            ----------
            None
            
        """
        tree = ET.parse(path)
        root = tree.getroot()
        root= self._delete_shape_drawings(root)                           
        #for equip in root.iter("Equipment"):
        actsys_all = [x for x in root.getchildren() if x.tag == 'ActuatingSystem']
        for actsys in actsys_all:
            ass_all = [x for x in actsys.getchildren() if x.tag == 'Association']
            for ass in ass_all:
                    triple = dict(subject= actsys.attrib["ID"], predicate= ass.attrib["Type"].replace(" ", '_') , object= ass.attrib["ItemID"])
                    self.publish_relation(triple)
            for actcomp in actsys.iter("ActuatingSystemComponent"):
                ass_all = [x for x in actcomp.getchildren() if x.tag == 'Association']
                for ass in ass_all:
                    triple = dict(subject= actcomp.attrib["ID"], predicate= ass.attrib["Type"].replace(" ", '_') , object= ass.attrib["ItemID"])
                    self.publish_relation(triple)
        equipment = [x for x in root.getchildren() if x.tag == 'Equipment']
        for equip in equipment:
            ass_all = [x for x in equip.getchildren() if x.tag == 'Association']
            for ass in ass_all:
                triple = dict(subject= equip.attrib["ID"], predicate= ass.attrib["Type"].replace(" ", '_') , object= ass.attrib["ItemID"])
                self.publish_relation(triple)
            for nozzle in equip.iter("Nozzle"):
                ass_all = [x for x in nozzle.getchildren() if x.tag == 'Association']
                for ass in ass_all:
                    triple = dict(subject= nozzle.attrib["ID"], predicate= ass.attrib["Type"].replace(" ", '_') , object= ass.attrib["ItemID"])
                    self.publish_relation(triple)
            equipment2 = [x for x in equip.getchildren() if x.tag == 'Equipment']
            for equip2 in equipment2:
                ass_all = [x for x in equip2.getchildren() if x.tag == 'Association']
                for ass in ass_all:
                    triple = dict(subject= equip2.attrib["ID"], predicate= ass.attrib["Type"].replace(" ", '_') , object= ass.attrib["ItemID"])
                    self.publish_relation(triple)
                for nozzle in equip2.iter("Nozzle"):
                    ass_all = [x for x in nozzle.getchildren() if x.tag == 'Association']
                    for ass in ass_all:
                        triple = dict(subject= nozzle.attrib["ID"], predicate= ass.attrib["Type"].replace(" ", '_') , object= ass.attrib["ItemID"])
                    self.publish_relation(triple)
            
        #self.driver.close()

    def build_tree_neo4j(self):
        self.extract_equipment()
        self.extract_pipes()
        self.connect_pipes()
        self.extract_instrumentation_functions()
        self.extract_actuators() 
        self.extract_instrumentation_loops()
        self.connect_instrumentation_functions()
        self.connect_actuators_instrumentation_equipment()
        #self.driver.close()

if __name__ == "__main__":
    #path= 'C:/Users/Ramy/Documents/Research/Promotion Blueprint/Implementation/Simple_example/P01V01-VER.EX01.xml'
    path= 'C:/Users/Ramy/Documents/Research/Promotion Blueprint/Implementation/DEXPI Parser/Reference_example/C01V04-VER.EX01.xml'
    #path= 'C:/Users/Ramy/Documents/Research/Promotion Blueprint/Implementation/Reference_example/C02V03-VER.EX02.xml'
    #path= 'C:/Users/Ramy/Documents/Research/Promotion Blueprint/Implementation/Reference_example/C03V04-VER.EX02.xml'
    neo = knodex(path=path, graph_type= "Neo4j", uri="bolt://localhost:7687", user="neo4j", password="comoscomos")
    neo.build_tree_neo4j()
    print(neo.driver.serialize(destination="dexpi_demo.xml",format='xml'))