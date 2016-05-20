/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package elfy.Connectors;

import elfy.Exceptions.ElfyConnectorException;
import elfy.Exceptions.ElfyPropertiesException;
import elfy.Properties.Properties;
import elfy.Properties.DatabaseProperties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

/**
 * This class is to define Data Base Connectors used in Elfy integrations.
 */
public class DatabaseConnector extends Connector {

    /**
     * variable for DATALOADER_DB data base Connector
     */
    public static String        DATALOADER_DB_CONNECTOR_NAME = "elfy.db.dataloader";
    
    private DatabaseProperties  properties;
    private Connection          databaseConnection;

    /**
     * This method is to get data base Connector type and name. 
     * @param ConnectorName name of Data Base Connector
     * @return instance of DatabaseConnector class
     * @throws ElfyConnectorException if failed to get a valid data base connector
     */
    public final static DatabaseConnector getConnector(String ConnectorName) throws ElfyConnectorException {
        return (DatabaseConnector) Connector.getConnector(Connector.TYPE_DATABASE, ConnectorName);
    } 
    
    /**
     * This method is to get new instance of data base Connector.
     * @param ConnectorName name of Data Base Connector
     * @return instance of DatabaseConnector class
     * @throws ElfyConnectorException if failed to get a valid data base connector
     */
    public final static DatabaseConnector getUnattachedConnector(String ConnectorName) throws ElfyConnectorException {
        return (DatabaseConnector) Connector.getUnattachedConnector(Connector.TYPE_DATABASE, ConnectorName);
    }       
    
    /**
     * Constructor to initialize data base Connector configuration.
     * @param connectorName name of Data Base Connector
     * @throws ElfyConnectorException if failed to get a valid data Base connector 
     */
    public DatabaseConnector(String connectorName) throws ElfyConnectorException {
        try {
            this.properties = DatabaseProperties.getProperties(connectorName);
        } catch (ElfyPropertiesException ex) {
            throw new ElfyConnectorException("Failed to get a valid database configuration for '" + connectorName + "'", ex);
        }
    }
    
    /**
     * Method to initialize data base connection.
     * @return Database connection
     * @throws SQLException if SQL exception occurs
     */
    public Connection getDatabaseConnection() throws SQLException {
        if (! Objects.isNull(this.databaseConnection)) {
            return this.databaseConnection;
        }
        
        return initializeDatabase();
    }
    
    /**
     * Method to execute SQL statement.
     * @param statementRequest SQL statement to be executed
     * @return result of SQL statement
     * @throws SQLException if SQL exception occurs
     */
    public PreparedStatement prepareStatement(String statementRequest) throws SQLException {
        this.getDatabaseConnection();
        return this.databaseConnection.prepareStatement(statementRequest); 
    }
    
    /**
     * This method is to commit to database.
     * @throws SQLException
     */
    public void commit() throws SQLException {
        this.databaseConnection.commit();
    }
    /**
     * This method is to initialize data base connection.
     * @return connection to database
     * @throws SQLException if SQL exception occurs
     */
    private Connection initializeDatabase() throws SQLException {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            this.databaseConnection = DriverManager.getConnection(this.properties.url, this.properties.username, this.properties.password);
            this.databaseConnection.setAutoCommit(false);
            System.out.println("Plugin Info  : Database Connection OK !");
            System.out.println("   - url      : " + this.properties.url);
            System.out.println("   - username : " + this.properties.username);
            return this.databaseConnection;
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            throw new SQLException("Plugin Error : Failed to load JDBC Driver. \n", ex);
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
