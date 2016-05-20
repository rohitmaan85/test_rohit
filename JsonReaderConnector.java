/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package elfy.Connectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeParser;
import elfy.Properties.JsonReaderProperties;
import elfy.Exceptions.ElfyConnectorException;
import elfy.Exceptions.ElfyFieldNotFoundException;
import elfy.Exceptions.ElfyJsonReaderException;
import elfy.Exceptions.ElfyPropertiesException;
import elfy.JsonReader.JsonReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class is to define Json Reader Connectors used in Elfy integrations.
 * @author jherbault
 */
public class JsonReaderConnector extends Connector implements ConnectorIntegrationInterface {

    /**
     * Constant for CGExport_V2 json reader Connector
     */
    public static final String    CNTOR_NAMES_CGEXPORTV2 = "CGExport_V2";
    
    private String                connectorName;
    private JsonReaderProperties  properties;
    private JsonReader            rootReader;
    private JsonReader            currentReader;
    
    private JsonReader            rootWriter;
    private JsonReader            currentWriter;
    
     /**
     * This method is to get Json Reader Connector type and name.
     * @param ConnectorName name of Json Reader Connector
     * @return instance of JsonReaderConnector class
     * @throws ElfyConnectorException if failed to get a valid Json Reader connector
     */
    public final static JsonReaderConnector getConnector(String ConnectorName) throws ElfyConnectorException {
        return (JsonReaderConnector) Connector.getConnector(Connector.TYPE_JSONREADER, ConnectorName);
    }

    /**
     * This method is to get new instance of Json Reader Connector.
     * @param ConnectorName name of Json Reader Connector
     * @return instance of JsonReaderConnector class
     * @throws ElfyConnectorException if failed to get a valid sentinel configuration
     */
    public final static JsonReaderConnector getUnattachedConnector(String ConnectorName) throws ElfyConnectorException {
        return (JsonReaderConnector) Connector.getUnattachedConnector(Connector.TYPE_JSONREADER, ConnectorName);
    }

    /**
     * Constructor to initialize Json Reader Connector configuration.
     * @param connectorName name of Json Reader Connector
     * @throws ElfyConnectorException if failed to get a valid sentinel configuration 
     */
    public JsonReaderConnector(String connectorName) throws ElfyConnectorException {
        try {
            this.connectorName      = connectorName;
            this.properties         = JsonReaderProperties.getProperties(connectorName);

        } catch (ElfyPropertiesException ex) {
            throw new ElfyConnectorException("Failed to get a valid sentinel configuration for '" + connectorName + "'", ex);
        } 
    }

    /**
     * This method is to initialize the Json reader connector in write mode.
     * @param initParameter attribute name
     * @throws ElfyConnectorException if failed to initialize JsonReader connector in write mode
     */
    @Override
    public void connector_targetUnit_init(String initParameter) throws ElfyConnectorException {
        try {
            this.rootWriter = JsonReader.getJsonReader(properties.readerClass);
            this.currentWriter = this.rootWriter;
            System.out.println("Connector Info : JsonReader : '" + connectorName + "' ready in Write mode.");
        } catch (ElfyJsonReaderException ex) {
            throw new ElfyConnectorException("Failed to initialize JsonReader '" + this.properties.readerClass + "' in Writer mode.", ex);
        }
    }

    /**
     * This method is to select value from attribute path in Json file.
     * @param nextParameter attribute name
     * @throws ElfyConnectorException if failed to select next parameter on JsonReader
     */
    @Override
    public void connector_targetUnit_next(String nextParameter) throws ElfyConnectorException {
        try {
            this.rootWriter.setAttributeByPath(nextParameter, null);
            this.currentWriter = (JsonReader) this.rootWriter.getAttributeByPath(nextParameter);
        } catch (ElfyJsonReaderException | ElfyFieldNotFoundException ex) {
            throw new ElfyConnectorException("Failed to Select next on JsonReader '" + this.properties.readerClass + "' for '" + nextParameter + "'in Writer mode.", ex);
        }
    }

    /**
     * This method is to store value for attributes in JsonReader in Writer mode
     * @param addParameter attribute path
     * @param objectToAdd attribute value
     * @throws ElfyConnectorException
     */
    @Override
    public void connector_targetUnit_addAttribute(String addParameter, Object objectToAdd) throws ElfyConnectorException {
        try {
            this.currentWriter.setAttributeByPath(addParameter, objectToAdd);
        } catch (ElfyJsonReaderException ex) {
            throw new ElfyConnectorException("Failed to add Attribute '" + addParameter + "' in JsonReader '" + this.properties.readerClass + " in Writer mode.", ex);
        }
    }

    /**
     * This method is to write Json file in the output.
     * @param sendParameter attribute path of json file
     * @throws ElfyConnectorException if failed to write generated Json to output file
     */
    @Override
    public void connector_targetUnit_send(String sendParameter) throws ElfyConnectorException {
        try {
            ObjectMapper obj = new ObjectMapper();
            
            FileWriter      ouputFile = new FileWriter(sendParameter, false);
            BufferedWriter  output    = new BufferedWriter(ouputFile);
            obj.writerWithDefaultPrettyPrinter().writeValue(output, this.rootWriter);

            System.out.println("Connector Info : JsonReader : JsonObject written to '" + sendParameter + "'");
        } catch (JsonProcessingException ex) {
            throw new ElfyConnectorException("Failed to generate output for JsonReader '" + this.properties.readerClass + " in Writer mode.", ex);
        } catch (IOException ex) {
            throw new ElfyConnectorException("Failed to write generated Json to output file '" + sendParameter + "'.", ex);
        }
    }

    /**
     * This method is to initialize the json reader for input file.
     * @param initParameter attribute path name
     * @throws ElfyConnectorException if failed to initialize JsonReader for input file
     */
    @Override
    public void connector_sourceUnit_init(String initParameter) throws ElfyConnectorException {
        ObjectMapper mapper      = new ObjectMapper();
        try {
            TypeParser t = new TypeParser(mapper.getTypeFactory());
            this.rootReader = mapper.readValue(new File(initParameter), t.parse(this.properties.readerClass));
        } catch (IOException ex) {
            throw new ElfyConnectorException("Failed to initialize JsonReader '" + this.properties.readerClass + "' for input file '" + initParameter + "'.", ex);
        }
        
    }

    /**
     * This method is to select next item or parameter from the attribute path in json file.
     * @param nextParameter attribute name
     * @return true if next parameter found else false
     * @throws ElfyConnectorException if failed to select JSONReader next item
     */
    @Override
    public Boolean connector_sourceUnit_selectNext(String nextParameter) throws ElfyConnectorException {
        try {
            this.currentReader = (JsonReader) this.rootReader.getAttributeByPath(nextParameter);
        } catch (ElfyJsonReaderException ex) {
            throw new ElfyConnectorException("Failed to select JSONReader next item '" + nextParameter + "'.", ex);
        } catch (ElfyFieldNotFoundException ex) {
           return false;
        }
        return true;
    }

    /**
     * This method is to get the attribute name from attribute path in json file.
     * @param getParameter attribute name in attribute path
     * @return attribute name
     * @throws ElfyConnectorException if Failed to get JSONReader Attribute
     */
    @Override
    public Object connector_sourceUnit_getAttribute(String getParameter) throws ElfyConnectorException {
        try {
            return this.currentReader.getAttributeByPath(getParameter);
        } catch (ElfyJsonReaderException | ElfyFieldNotFoundException ex) {
            throw new ElfyConnectorException("Failed to get JSONReader Attribute for '" + getParameter + "'.", ex);
        }
    }
    /**
     * This method is to commit data in target unit.
     * @param sendParameter attribute name
     * @throws ElfyConnectorException if unsupported operation exception occurs
     */
    @Override
    public void connector_targetUnit_done(String sendParameter) throws ElfyConnectorException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    /**
     * This method is used when connection to source is done.
     * @throws ElfyConnectorException if unsupported operation exception occurs
     */  
    
    /**
     * This method is used when connection to source is done.
     * @param sendParameter attribute name
     * @throws ElfyConnectorException if unsupported operation exception occurs
     */
    @Override
    public void connector_sourceUnit_done(String sendParameter) throws ElfyConnectorException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }


    
}
    
    
