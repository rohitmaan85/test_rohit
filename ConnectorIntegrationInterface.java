/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package elfy.Connectors;

import elfy.Exceptions.ElfyConnectorException;

/**
 * Interface for Connector used in Elfy integrations,it declare method that a Elfy Connector defines.
 * @author jherbault
 */
public interface ConnectorIntegrationInterface {
    
    /**
     * This is abstract method to initialize connection to target unit.
     * @param initParameter attribute name
     * @throws ElfyConnectorException if unknown Elfy Connector is specified
     */
    public abstract void connector_targetUnit_init            (String initParameter)                      throws ElfyConnectorException ;

    /**
     * This is abstract method to connect to next target unit.
     * @param nextParameter attribute name
     * @throws ElfyConnectorException if unknown Elfy Connector is specified
     */
    public abstract void connector_targetUnit_next            (String nextParameter)                      throws ElfyConnectorException ;

    /**
     * This is abstract method to add attribute to target.
     * @param addParameter attribute name
     * @param objectToAdd attribute value
     * @throws ElfyConnectorException if unknown Elfy Connector is specified
     */
    public abstract void connector_targetUnit_addAttribute    (String addParameter, Object objectToAdd)   throws ElfyConnectorException ;

    /**
     * This is abstract method to send data to target.
     * @param sendParameter attribute name
     * @throws ElfyConnectorException if unknown Elfy Connector is specified
     */ 
    public abstract void connector_targetUnit_send            (String sendParameter)                      throws ElfyConnectorException ;

    /**
     * This is abstract method to commit data in target.
     * @param doneParameter attribute name
     * @throws ElfyConnectorException if unknown Elfy Connector is specified
     */
    public abstract void connector_targetUnit_done            (String doneParameter)                      throws ElfyConnectorException ;
    
    /**
     * This is abstract method to initialize connection to source unit.
     * @param initParameter attribute name
     * @throws ElfyConnectorException if unknown Elfy Connector is specified
     */
    public abstract void   connector_sourceUnit_init          (String initParameter)                      throws ElfyConnectorException ;

    /**
     * This is abstract method to select next data from source.
     * @param nextParameter attribute name
     * @return
     * @throws ElfyConnectorException if unknown Elfy Connector is specified
     */
    public abstract Boolean connector_sourceUnit_selectNext (String nextParameter)                      throws ElfyConnectorException ;

    /**
     * This is abstract method to get attribute from source.
     * @param getParameter attribute name
     * @return source attribute
     * @throws ElfyConnectorException if unknown Elfy Connector is specified
     */
    public abstract Object connector_sourceUnit_getAttribute  (String getParameter)                       throws ElfyConnectorException ;

    /**
     * This is abstract method used when connection to source unit is done.
     * @param doneParameter attribute name
     * @throws ElfyConnectorException if unknown Elfy Connector is specified
     */
    public abstract void connector_sourceUnit_done   (String doneParameter)                      throws ElfyConnectorException ;
}
