/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package elfy.Connectors;

import com.axway.trkapiua.TrkApiUA;
import com.axway.trkapiua.TrkMessageUAEvent;
import elfy.Exceptions.ElfyConnectorException;
import elfy.Exceptions.ElfyPropertiesException;
import elfy.Exceptions.UniversalAgentException;
import elfy.Properties.SentinelProperties;

/**
 * This class is to define Sentinel Connectors used in Elfy integrations.
 * @author jherbault
 */
public class SentinelConnector extends Connector {
    private String              connectorName;
    private SentinelProperties  properties;

    /**
     * Class to process and send messages to universal agent which is further forwarded to Sentinel server.
     */
    public  TrkApiUA            univervalAgent;
    
     /**
     * This method registers Sentinel Connector type and name.
     * @param ConnectorName name of Sentinel Connector
     * @return Sentinel Connector
     * @throws ElfyConnectorException if failed to get a valid Sentinel connector
     */
    public final static SentinelConnector getConnector(String ConnectorName) throws ElfyConnectorException {
        return (SentinelConnector) Connector.getConnector(Connector.TYPE_SENTINEL, ConnectorName);
    }
    
    /**
     * This method is to get new instance of Sentinel Connector.
     * @param ConnectorName name of Sentinel Connector
     * @return new instance of Sentinel Connector
     * @throws ElfyConnectorException if failed to get a valid sentinel configuration
     */
    public final static SentinelConnector getUnattachedConnector(String ConnectorName) throws ElfyConnectorException {
        return (SentinelConnector) Connector.getUnattachedConnector(Connector.TYPE_SENTINEL, ConnectorName);
    }    

    /**
     * Constructor to initialize Sentinel Connector configuration.
     * @param connectorName name of Sentinel Connector
     * @throws ElfyConnectorException if failed to get a valid sentinel configuration 
     */
    public SentinelConnector(String connectorName) throws ElfyConnectorException {
        try {
            this.connectorName      = connectorName;
            this.properties         = SentinelProperties.getProperties(connectorName);
            this.univervalAgent     = new TrkApiUA(properties.host, properties.port);

        } catch (ElfyPropertiesException ex) {
            throw new ElfyConnectorException("Failed to get a valid sentinel configuration for '" + connectorName + "'", ex);
        } 
    }
    
    /**
     * This method is to send message to universal agent.
     * @param eventMessage containing data for Tracked Events
     * @throws ElfyConnectorException if failed to send a message to universal agent 
     */
    public void sendMessage(TrkMessageUAEvent eventMessage) throws ElfyConnectorException {
        int errCode = this.univervalAgent.sendMessage(eventMessage);
        
        if (errCode != 0) {
            throw new ElfyConnectorException(
                "failed to send a message to '" + this.connectorName + "' Sentinel instance.",
                new UniversalAgentException(errCode)
            );
        }
    }

     /**
     * This method is to initialize connection to target unit.
     * @param initParameter attribute name
     * @throws ElfyConnectorException if unsupported operation exception occurs
     */
    @Override
    public void connector_targetUnit_init(String initParameter) throws ElfyConnectorException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    /**
     * This method is to connect to next target unit.
     * @param nextParameter attribute name
     * @throws ElfyConnectorException if unsupported operation exception occurs
     */
    @Override
    public void connector_targetUnit_next(String nextParameter) throws ElfyConnectorException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    /**
     * This method is to add attribute to target unit.
     * @throws ElfyConnectorException if unsupported operation exception occurs
     */

    /**
     * This method is to add attribute to target unit.
     * @param addParameter attribute name
     * @param objectToAdd instance of Object class
     * @throws ElfyConnectorException if unsupported operation exception occurs
     */
    @Override
    public void connector_targetUnit_addAttribute(String addParameter, Object objectToAdd) throws ElfyConnectorException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    /**
     * This method is to send data to target unit.
     * @throws ElfyConnectorException if unsupported operation exception occurs
     */

    /**
     * This method is to send data to target unit.
     * @param sendParameter attribute name
     * @throws ElfyConnectorException if unsupported operation exception occurs
     */
    @Override
    public void connector_targetUnit_send(String sendParameter) throws ElfyConnectorException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * This method is to initialize connection to source unit.
     * @param initParameter attribute name
     * @throws ElfyConnectorException if unsupported operation exception occurs
     */ 
    @Override
    public void connector_sourceUnit_init(String initParameter) throws ElfyConnectorException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * This method is to select next data from source.
     * @param nextParameter attribute name
     * @return true if next record is selected else return false
     * @throws ElfyConnectorException if unsupported operation exception occurs
     */
    @Override
    public Boolean connector_sourceUnit_selectNext(String nextParameter) throws ElfyConnectorException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * This method is used to get attribute from source unit.
     * @param getParameter attribute name
     * @return attribute name
     * @throws ElfyConnectorException if unsupported operation exception occurs
     */
    @Override
    public Object connector_sourceUnit_getAttribute(String getParameter) throws ElfyConnectorException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
     * @param sendParameter attribute name
     * @throws ElfyConnectorException if unsupported operation exception occurs
     */  
    @Override
    public void connector_sourceUnit_done(String sendParameter) throws ElfyConnectorException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
    
    
