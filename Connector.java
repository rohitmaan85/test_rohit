/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package elfy.Connectors;

import elfy.Exceptions.ElfyConnectorException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;

/**
 * This is an abstract class which declares the methods and variables used by Connector in Elfy integrations.
 * @author jherbault
 */
public abstract class Connector implements ConnectorIntegrationInterface  {
    private final static HashMap<String, Connector> ConnectorSingletonMap = new HashMap<> ();
    
    /**
     * Constant for database Connector
     */
    public  final static String TYPE_DATABASE    = "DatabaseConnector";

    /**
     * Constant for data table Connector
     */
    public  final static String TYPE_DATATABLE   = "DataTableConnector";

    /**
     * Constant for sentinel Connector
     */
    public  final static String TYPE_SENTINEL    = "SentinelConnector";

    /**
     * Constant for tracked object Connector
     */
    public  final static String TYPE_TRKOBJECT   = "TrackedObjectConnector";

    /**
     * Constant for Json reader Connector
     */
    public  final static String TYPE_JSONREADER  = "JsonReaderConnector";

    /**
     * Constant for excel reader Connector
     */
    public  final static String TYPE_EXCELREADER = "ExcelReaderConnector";
    
    /**
     * This method registers Elfy Connector type and name.
     * @param ConnectorType type of Elfy Connector
     * @param ConnectorName name of Elfy Connector
     * @return instance of Connector class
     * @throws ElfyConnectorException if unknown Elfy Connector is specified
     */
    public final static Connector getConnector(String ConnectorType, String ConnectorName) throws ElfyConnectorException {
        if (ConnectorSingletonMap.containsKey(ConnectorName)) {
            return ConnectorSingletonMap.get(ConnectorName);
        }

        try {
            Connector newConnector = (Connector) Class.forName("elfy.Connectors." + ConnectorType).getConstructor(String.class).newInstance(ConnectorName);
            Connector.ConnectorSingletonMap.put(ConnectorName, newConnector);
            return newConnector;
        } catch (NoSuchMethodException | SecurityException | ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            throw new ElfyConnectorException("Unknow connector : '" + "elfy.Connectors." + ConnectorType + "'", ex);
        }
    }
    
    /**
     * This method is to get new instance of Elfy Connector.
     * @param ConnectorType type of Elfy Connector
     * @param ConnectorName name of Elfy Connector
     * @return instance of Connector class
     * @throws ElfyConnectorException if unknown Elfy Connector is specified
     */
    public final static Connector getUnattachedConnector(String ConnectorType, String ConnectorName) throws ElfyConnectorException {
        try {
            return (Connector) Class.forName("elfy.Connectors." + ConnectorType).getConstructor(String.class).newInstance(ConnectorName);
        } catch (NoSuchMethodException | SecurityException | ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            throw new ElfyConnectorException("Unknow connector : '" + "elfy.Connectors." + ConnectorType + "'", ex);
        }
    } 
    
    /**
     * This method is to initialize connection to target unit.
     * @throws ElfyConnectorException if unknown Elfy Connector is specified
     */
    public void    connector_targetUnit_init()         throws ElfyConnectorException { this.connector_targetUnit_init(""); }

    /**
     * This method is to connect to next target unit.
     * @throws ElfyConnectorException if unknown Elfy Connector is specified
     */
    public void    connector_targetUnit_next()         throws ElfyConnectorException { this.connector_targetUnit_next(""); }

    /**
     * This method is to send data to target unit.
     * @throws ElfyConnectorException if unknown Elfy Connector is specified
     */
    public void    connector_targetUnit_send()         throws ElfyConnectorException { this.connector_targetUnit_send(""); }

    /**
     * This method is to commit data in target unit.
     * @throws ElfyConnectorException if unknown Elfy Connector is specified
     */
    public void    connector_targetUnit_done()         throws ElfyConnectorException { this.connector_targetUnit_done(""); }

    /**
     * This method is to initialize connection to source unit.
     * @throws ElfyConnectorException if unknown Elfy Connector is specified
     */
    public void    connector_sourceUnit_init()         throws ElfyConnectorException { this.connector_sourceUnit_init(""); }

    /**
     * This method is to select next data from source.
     * @return true if next record is selected else return false
     * @throws ElfyConnectorException if unknown Elfy Connector is specified
     */
    public Boolean connector_sourceUnit_selectNext()   throws ElfyConnectorException { return this.connector_sourceUnit_selectNext(""); }

    /**
     * This method is used when connection to source is done.
     * @throws ElfyConnectorException  if unknown Elfy Connector is specified
     */
    public void    connector_sourceUnit_done()         throws ElfyConnectorException { this.connector_sourceUnit_done(""); }
    
    
}
