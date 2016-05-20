/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package elfy.Connectors;

import com.axway.trkapiua.TrkMessageUAEvent;
import elfy.Exceptions.ElfyConnectorException;
import elfy.Exceptions.ElfyPropertiesException;
import elfy.Properties.TrackedObjectProperties;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class is to define Tracked Object Connectors used in Elfy integrations.
 * @author jherbault
 */
public class TrackedObjectConnector extends Connector {

    /**
     * Tracked Object Connector for Enriched_Flows_V2
     */
    public static final String TRKOBJ_ENRICHED_FLOWS_V2 = "Enriched_Flows_V2";

    /**
     * Tracked Object Connector for Enriched_Applications_V2
     */
    public static final String TRKOBJ_ENRICHED_APPLICATIONS_V2 = "Enriched_Applications_V2";

    /**
     * Tracked Object Connector for Template_Flows_V2
     */
    public static final String TRKOBJ_TEMPALTE_FLOWS_V2 = "Template_Flows_V2";
    
    private String                              connectorName;
    private TrackedObjectProperties             properties;
    private SentinelConnector                   sentinelConnector;
    private HashMap<Integer, TrkMessageUAEvent> messageMap;
    
    private Boolean                             isDebugMode;
    
    private Integer                             uniqId;  
    
    private Integer                             currentConnectorMessageId = 0;
    
    /**
     * This method registers Tracked Object Connector type and name.
     * @param ConnectorName name of Tracked Object Connector
     * @return Tracked Object Connector
     * @throws ElfyConnectorException if failed to get a valid Tracked Object connector
     */
    public final static TrackedObjectConnector getConnector(String ConnectorName) throws ElfyConnectorException {
        return (TrackedObjectConnector) Connector.getConnector(Connector.TYPE_TRKOBJECT, ConnectorName);
    }
    
     /**
     * This method is to get new instance of Tracked Object Connector.
     * @param ConnectorName name of Tracked Object Connector
     * @return new instance of Tracked Object Connector
     * @throws ElfyConnectorException if failed to get a valid Tracked Object connector
     */
    public final static TrackedObjectConnector getUnattachedConnector(String ConnectorName) throws ElfyConnectorException {
        return (TrackedObjectConnector) Connector.getUnattachedConnector(Connector.TYPE_TRKOBJECT, ConnectorName);
    }    

    /**
     * Constructor to initialize Tracked Object Connector configuration.
     * @param connectorName name of Tracked Object Connector
     * @throws ElfyConnectorException if failed to get a valid Tracked Object connector
     */
    public TrackedObjectConnector(String connectorName) throws ElfyConnectorException {
        try {
            this.connectorName      = connectorName;
            this.properties         = TrackedObjectProperties.getProperties(connectorName);
            this.sentinelConnector  = SentinelConnector.getConnector(this.properties.instance);
            this.uniqId             = 0;
            this.isDebugMode        = false;
            this.messageMap         = new HashMap<>();

        } catch (ElfyPropertiesException ex) {
            throw new ElfyConnectorException("Failed to get a valid sentinel configuration for '" + connectorName + "'", ex);
        } 
    }
    /**
     * This is to get unique message id for Tracked object.
     * @return unique id
     */
    private Integer getUniqId() {
        return this.uniqId++;
    }
    
    /**
     * This method creates message with Tracked object properties.
     * @return message id
     * @throws ElfyConnectorException if failed to get a valid Tracked Object connector
     */
    public Integer createMessage() throws ElfyConnectorException {
        Integer messageId = getUniqId();
        
        messageMap.put(messageId, new TrkMessageUAEvent(this.sentinelConnector.univervalAgent, this.properties.publicName, this.properties.version));        
        return messageId;
    }
    
    /**
     * This method add attribute to Sentinel Message Event.
     * @param messageId unique message id
     * @param AttributeName name of attribute
     * @param AttributeValue value of attribute
     * @throws ElfyConnectorException if Sentinel Message Event to send does not exist or already sent
     */
    public void addAttributeToMessage(Integer messageId, String AttributeName, String AttributeValue) throws ElfyConnectorException {
        if (! messageMap.containsKey(messageId)) {
            throw new ElfyConnectorException("Sentinel Message Event to send does not exist or already sent.");
        }
        
        messageMap.get(messageId).addValue(AttributeName, AttributeValue);
    }
    
    /**
     * This method is to get Event-type messages which contains data for Tracked Events.
     * @param messageId unique message id
     * @return message id of the Event-type message
     * @throws ElfyConnectorException if Sentinel Message Event to send does not exist or already sent.
     */
    public TrkMessageUAEvent getMessage(Integer messageId) throws ElfyConnectorException {
        if (! messageMap.containsKey(messageId)) {
            throw new ElfyConnectorException("Sentinel Message Event to get does not exist or already sent.");
        }
        
        return messageMap.get(messageId);
    }    
    
    /**
     * This method is to send the event type messages to Sentinel.
     * @param messageId unique message id
     * @throws ElfyConnectorException if Sentinel Message Event to send does not exist or already sent
     */
    public void sendMessage(Integer messageId) throws ElfyConnectorException {
        if (! messageMap.containsKey(messageId)) {
            throw new ElfyConnectorException("Sentinel Message Event to send does not exist or already sent.");
        }
        
        this.sentinelConnector.sendMessage(messageMap.get(messageId));
    }
    
    /**
     * This method is to get the CycleId of the message event.
     * @param messageId unique message id
     * @return CycleId of the message event
     * @throws ElfyConnectorException if Sentinel Message Event to send does not exist or already sent
     */
    public String getMessageCycleId(Integer messageId) throws ElfyConnectorException {
        if (! messageMap.containsKey(messageId)) {
            throw new ElfyConnectorException("Sentinel Message Event to send does not exist or already sent.");
        }
        
        return messageMap.get(messageId).getValue("CycleId");
    }
    
    /**
     * This method is to describe the message parameters like  message formated size, Payload size.
     * @param messageId unique message id
     * @return message formated size and Payload size
     * @throws ElfyConnectorException if Sentinel Message Event to send does not exist or already sent
     */
    public String describeMessage(Integer messageId) throws ElfyConnectorException {
        
        String  result          = "";
        String  formatedMessage = "";
        Integer totalPayload    = 0;
       
        TrkMessageUAEvent message = this.getMessage(messageId);
        
        Map<String, String> attributeMap = message.getAttributeMap();
        
        for (Map.Entry<String, String> entrySet : attributeMap.entrySet()) {
            String key = entrySet.getKey();
            String value = entrySet.getValue();
            
            result += "   - " + key + " : " + value + "(" + value.length() + ")\n";
            
            formatedMessage += "|" + key.length() + "|" + key + "|" + value.length() + "|" + value;
            totalPayload    += value.length();
        }
        
        formatedMessage += "|";
        
        result += " === Total Message formated size is : " + formatedMessage.length() + "===\n";
        result += " === Total Payload size is          : " + totalPayload.toString()  + "===\n";
        return result;
    }
    
    /**
     * This method is to initialize connection to target unit.
     * @param initParameter attribute name
     * @throws ElfyConnectorException if unsupported operation exception occurs
     */
    @Override
    public void connector_targetUnit_init(String initParameter) throws ElfyConnectorException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    /**
     * This method creates message at the Target with the unique message id.
     * @param nextParameter attribute name
     * @throws ElfyConnectorException if Sentinel Message Event to send does not exist or already sent
     */
    @Override
    public void connector_targetUnit_next(String nextParameter) throws ElfyConnectorException {
        this.currentConnectorMessageId = this.createMessage();
    }
    /**
     * This method is to add attributes to the target.
     * @param addParameterName attribute name at target
     * @param objectToAdd attribute value
     * @throws ElfyConnectorException if Sentinel Message Event to send does not exist or already sent
     */
    @Override
    public void connector_targetUnit_addAttribute(String addParameterName, Object objectToAdd) throws ElfyConnectorException {
        String addParameterValue = "";
        
        if (Objects.isNull(objectToAdd)) {
            return;
        }
        
        switch (objectToAdd.getClass().getName()) {
            case "java.lang.String": 
                addParameterValue = (String) objectToAdd;
                break;
            case "java.lang.Integer":
                addParameterValue = ((Integer) objectToAdd).toString();
                break;                
            case "java.lang.Long":
                addParameterValue = ((Long) objectToAdd).toString();
                break;
            case "java.lang.Boolean":
                addParameterValue = ((Boolean) objectToAdd).toString();
                break;
            default:
                throw new ElfyConnectorException("Invalid Attribute Type '" + objectToAdd.getClass().getName() + "' for AddAttribute operation for '" + addParameterName + "'.");
        }
        
        this.addAttributeToMessage(this.currentConnectorMessageId, addParameterName, addParameterValue);
    }
    /**
     * This method is to send the message event to target.
     * @param sendParameter attribute name
     * @throws ElfyConnectorException if Sentinel Message Event to send does not exist or already sent
     */
    @Override
    public void connector_targetUnit_send(String sendParameter) throws ElfyConnectorException {
        if (this.isDebugMode) {
            System.out.println("Connector Info : TrackedObject : \n" + this.describeMessage(this.currentConnectorMessageId) );
        }
        this.sendMessage(this.currentConnectorMessageId);
        System.out.println("Connector Info : TrackedObject : Event Sent to " + this.properties.publicName + "(CycleId:" + this.getMessageCycleId(this.currentConnectorMessageId) + ").");
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
     * @return 
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
    
    
