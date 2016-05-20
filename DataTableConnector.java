/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package elfy.Connectors;

import elfy.Exceptions.ElfyComitFailedException;
import elfy.Exceptions.ElfyConnectorException;
import elfy.Exceptions.ElfyFieldNotFoundException;
import elfy.Exceptions.ElfyPropertiesException;
import elfy.Properties.DataTableProperties;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.StringUtils;

/**
 * This class is to define Data Table Connectors used in Elfy integrations.
 * @author jherbault
 */
public class DataTableConnector extends Connector {
    
    /**
     * Class variable for EXCEL_AG2RLM Data Table Connector 
     */
    public static String        EXCEL_AG2RLM_DATAIMPORT_V1            = "elfy.datatable.excel.ag2rlm_dataimport_v1";
    
    /**
     * Class variable for CFT2CG_CFTCATAL Data Table Connector
     */
    public static String        CFT2CG_CFTCATAL_DATATABLE_NAME        = "elfy.datatable.cft2cg.cftcatal";

    /**
     * Class variable for CFT2CG_CFTSEND Data Table Connector
     */
    public static String        CFT2CG_CFTSEND_DATATABLE_NAME         = "elfy.datatable.cft2cg.cftsend";

    /**
     * Class variable for CFT2CG_CFTRECV Data Table Connector
     */
    public static String        CFT2CG_CFTRECV_DATATABLE_NAME         = "elfy.datatable.cft2cg.cftrecv";

    /**
     * Class variable for CFT2CG_CFTACCNT Data Table Connector
     */
    public static String        CFT2CG_CFTACCNT_DATATABLE_NAME        = "elfy.datatable.cft2cg.cftaccnt";
    
    /**
     * Class variable for CD2CG_UNIXLOG Data Table Connector
     */
    public static String        CD2CG_UNIXLOG_DATATABLE_NAME          = "elfy.datatable.cd2cg.unix.log";
    
    /**
     * Class variable for PEL2CG_SITE_EXPORT Data Table Connector
     */
    public static String        PEL2CG_SITE_EXPORT_DATATABLE_NAME     = "elfy.datatable.pel2cg.export.site";

    /**
     * Class variable for PEL2CG_LSITE_EXPORT Data Table Connector
     */
    public static String        PEL2CG_LSITE_EXPORT_DATATABLE_NAME    = "elfy.datatable.pel2cg.export.lsite";

    /**
     * Class variable for PEL2CG_APPLI_EXPORT Data Table Connector
     */
    public static String        PEL2CG_APPLI_EXPORT_DATATABLE_NAME    = "elfy.datatable.pel2cg.export.appli";

    /**
     * Class variable for PEL2CG_MODEL_EXPORT Data Table Connector
     */
    public static String        PEL2CG_MODEL_EXPORT_DATATABLE_NAME    = "elfy.datatable.pel2cg.export.model";

    /**
     * Class variable for PEL2CG_LIST_EXPORT Data Table Connector
     */
    public static String        PEL2CG_LIST_EXPORT_DATATABLE_NAME     = "elfy.datatable.pel2cg.export.list";

    /**
     * Class variable for PEL2CG_USER_EXPORT Data Table Connector
     */
    public static String        PEL2CG_USER_EXPORT_DATATABLE_NAME     = "elfy.datatable.pel2cg.export.user";

    /**
     * Class variable for PEL2CG_PROFILE_EXPORT Data Table Connector
     */
    public static String        PEL2CG_PROFILE_EXPORT_DATATABLE_NAME  = "elfy.datatable.pel2cg.export.profile";
    
    /**
     * Class variable for PELMVS2CG_STATISTICS Data Table Connector
     */
    public static String        PELMVS2CG_STATISTICS_DATATABLE_NAME   = "elfy.datatable.pelmvs2cg.statistics";
    
    /**
     * Class variable for TRKOBJECT_TEMPLATE_FLOW_V2_CURRENT Data Table Connector
     */
    public static String        TRKOBJECT_TEMPLATE_FLOW_V2_CURRENT    = "elfy.datatable.trackedObject.template_flows_v2.current";

    /**
     * Class variable for TRKOBJECT_ENRICHED_FLOW_V2_CURRENT Data Table Connector
     */
    public static String        TRKOBJECT_ENRICHED_FLOW_V2_CURRENT    = "elfy.datatable.trackedObject.enriched_flows_v2.current";

    /**
     * Class variable for TRKOBJECT_ENRICHED_APPLI_V2_CURRENT Data Table Connector
     */
    public static String        TRKOBJECT_ENRICHED_APPLI_V2_CURRENT   = "elfy.datatable.trackedObject.enriched_applications_v2.current";

    /**
     * Class variable for ELFYVIEW_TEMPLATE_FLOW_MAPPING_V2 Data Table Connector
     */
    public static String        ELFYVIEW_TEMPLATE_FLOW_MAPPING_V2     = "elfy.datatable.view.template_toflow_mapping_view";
    
    
    // Class variables for XFB2CG plugin.
    
     /**
     * Class variable for PEL2CG_SITE_EXPORT Data Table Connector
     */
    public static String        XFB2CG_APPLI_EXPORT_DATATABLE_NAME     = "elfy.datatable.xfb2cg.conf.appli";

    /**
     * Class variable for PEL2CG_LSITE_EXPORT Data Table Connector
     */
    public static String        XFB2CG_CERT_EXPORT_DATATABLE_NAME    = "elfy.datatable.xfb2cg.conf.cert";

    /**
     * Class variable for PEL2CG_APPLI_EXPORT Data Table Connector
     */
    public static String        XFB2CG_CGATE_EXPORT_DATATABLE_NAME    = "elfy.datatable.xfb2cg.conf.cgate";

    /**
     * Class variable for PEL2CG_MODEL_EXPORT Data Table Connector
     */
    public static String        XFB2CG_CGATEGROUP_EXPORT_DATATABLE_NAME    = "elfy.datatable.xfb2cg.conf.cgategroup";

    /**
     * Class variable for PEL2CG_LIST_EXPORT Data Table Connector
     */
    public static String        XFB2CG_KEY_EXPORT_DATATABLE_NAME     = "elfy.datatable.xfb2cg.conf.key";

    /**
     * Class variable for PEL2CG_USER_EXPORT Data Table Connector
     */
    public static String        XFB2CG_LIST_EXPORT_DATATABLE_NAME     = "elfy.datatable.xfb2cg.conf.list";

    /**
     * Class variable for PEL2CG_USER_EXPORT Data Table Connector
     */
    public static String        XFB2CG_LSITE_EXPORT_DATATABLE_NAME     = "elfy.datatable.xfb2cg.conf.lsite";

    /**
     * Class variable for PEL2CG_USER_EXPORT Data Table Connector
     */
    public static String        XFB2CG_MODEL_EXPORT_DATATABLE_NAME     = "elfy.datatable.xfb2cg.conf.model";


    /**
     * Class variable for PEL2CG_USER_EXPORT Data Table Connector
     */
    public static String        XFB2CG_NPROF_EXPORT_DATATABLE_NAME     = "elfy.datatable.xfb2cg.conf.nprof";

  /**
     * Class variable for PEL2CG_USER_EXPORT Data Table Connector
     */
    public static String        XFB2CG_PROFILE_EXPORT_DATATABLE_NAME     = "elfy.datatable.xfb2cg.conf.profile";

    /**
     * Class variable for PEL2CG_USER_EXPORT Data Table Connector
     */
    public static String        XFB2CG_PROXY_EXPORT_DATATABLE_NAME     = "elfy.datatable.xfb2cg.conf.proxy";


    /**
     * Class variable for PEL2CG_USER_EXPORT Data Table Connector
     */
    public static String        XFB2CG_PURGE_EXPORT_DATATABLE_NAME     = "elfy.datatable.xfb2cg.conf.purge";
    
    
    /**
     * Class variable for PEL2CG_PROFILE_EXPORT Data Table Connector
     */
    public static String        PEL2CG_RULEDEC_EXPORT_DATATABLE_NAME  = "elfy.datatable.xfb2cg.conf.ruledec";

    /**
     * Class variable for PEL2CG_USER_EXPORT Data Table Connector
     */
    public static String        XFB2CG_RULETAB_EXPORT_DATATABLE_NAME     = "elfy.datatable.xfb2cg.conf.ruletab";

    /**
     * Class variable for PEL2CG_USER_EXPORT Data Table Connector
     */
    public static String        XFB2CG_SITE_EXPORT_DATATABLE_NAME     = "elfy.datatable.xfb2cg.conf.site";

    /**
     * Class variable for PEL2CG_USER_EXPORT Data Table Connector
     */
    public static String        XFB2CG_SSHPROF_EXPORT_DATATABLE_NAME     = "elfy.datatable.xfb2cg.conf.sshprof";
    
    /**
     * Class variable for PEL2CG_USER_EXPORT Data Table Connector
     */
    public static String        XFB2CG_TRADEPART_EXPORT_DATATABLE_NAME     = "elfy.datatable.xfb2cg.conf.tradepart";

    
    /**
     * Class variable for PEL2CG_USER_EXPORT Data Table Connector
     */
    public static String        XFB2CG_USER_EXPORT_DATATABLE_NAME     = "elfy.datatable.xfb2cg.conf.user";

    /**
     * Class variable for PEL2CG_USER_EXPORT Data Table Connector
     */
    public static String        XFB2CG_XMSCTOR_EXPORT_DATATABLE_NAME     = "elfy.datatable.xfb2cg.conf.xmsctor";

    
    private DataTableProperties  properties;
    private DatabaseConnector    databaseConnector;
    private PreparedStatement    databaseInsertStatement;
    private PreparedStatement    databaseSelectStatement;
    private Integer              databaseSelectStatementArgumentCount;
    
    private Integer              InBatchCount;
    
    
    private ResultSet            currentSelectResult;
    private Map<Object, Integer> currentSelectResultMap;
    
    private Map<String, String>  currentInsertMap;
    
    /**
     * This method is to get Data Table Connector type and name.
     * @param ConnectorName name of Data Table Connector
     * @return instance of DataTableConnector class
     * @throws ElfyConnectorException if failed to get a valid data table connector
     */
    public final static DataTableConnector getConnector(String ConnectorName) throws ElfyConnectorException {
        return (DataTableConnector) Connector.getConnector(Connector.TYPE_DATATABLE, ConnectorName);
    }
    
    /**
     * This method is to get new instance of Data Table Connector.
     * @param ConnectorName name of Data Table Connector
     * @return instance of DataTableConnector class
     * @throws ElfyConnectorException if failed to get a valid data table connector
     */
    public final static DataTableConnector getUnattachedConnector(String ConnectorName) throws ElfyConnectorException {
        return (DataTableConnector) Connector.getUnattachedConnector(Connector.TYPE_DATATABLE, ConnectorName);
    }    

    /**
     * Constructor to initialize Data Table Connector configuration.
     * @param connectorName name of Data Table Connector
     * @throws ElfyConnectorException if failed to get a valid data table connector 
     */
    public DataTableConnector(String connectorName) throws ElfyConnectorException {
        try {
            this.InBatchCount       = 0;
            this.properties         = DataTableProperties.getProperties(connectorName);
            this.databaseConnector  = DatabaseConnector.getConnector(this.properties.databaseConnectorName);
            this.statementIntialisation();
        } catch (ElfyPropertiesException ex) {
            throw new ElfyConnectorException("Failed to get a valid data table configuration for '" + connectorName + "'", ex);
        } catch (SQLException ex) {
            throw new ElfyConnectorException("Failed to prepare data table SQL Statement for '" + connectorName + "'", ex);
        }
    }
    /**
     * Method to call insert and select SQL query.
     * @throws SQLException if SQL exception occurs
     */
    private void statementIntialisation() throws SQLException {
            this.buildInsertStatement();
            this.buildSelectStatement();        
    }
    
    /**
     * This method is to insert values in Data Table.
     * @param insertData data to be inserted in Data Table columns
     * @return number of rows inserted in Data Table
     * @throws SQLException if SQL exception occurs
     * @throws ElfyComitFailedException if failed to commit to the database
     */
    public Integer addOneStatementToInsertBatch(Map<String, String> insertData) throws SQLException, ElfyComitFailedException {
        int statementParameterIndex = 0;
        
        for (String columnName : this.properties.tableDefinitionColumnsNames) {
            Integer ColumnType = this.properties.tableDefinition.get(columnName);
            
            statementParameterIndex++;
            
            if (! insertData.containsKey(columnName)) {
                this.databaseInsertStatement.setNull(statementParameterIndex, ColumnType);
            } else {
                this.databaseInsertStatement.setObject(statementParameterIndex, insertData.get(columnName), ColumnType);
            }
        }
        this.databaseInsertStatement.addBatch();
        this.InBatchCount++;
        
        if (this.InBatchCount >= this.properties.maxBatchSize) {
            this.InBatchCount = 0;
            return this.commitInsertBatch();
        }
        
        return 0;
    }
    
    /**
     * Method to commit the records inserted in Data Table.
     * @return number of records committed
     * @throws ElfyComitFailedException if failed to commit to the database
     */
    public Integer commitInsertBatch() throws ElfyComitFailedException  {
        Integer changedRowCount = 0;
        
        try {
            int results[] = this.databaseInsertStatement.executeBatch();
            for (int result : results) {
                if (result > 0) changedRowCount += result; 
            }
            this.databaseConnector.commit();
            //System.out.println(changedRowCount + " new Commited records for " + this.properties.targetDatabaseName + "." + this.properties.targetTableName );
            return changedRowCount;
        } catch (SQLException ex) {
            throw new ElfyComitFailedException("Failed to commit to the database.", ex);
        } 
    }
    /**
     * Method to build SQL insert statement to insert data into Data Table.
     * @throws SQLException if SQL exception occurs
     */
    private void buildInsertStatement() throws SQLException {
        String statementArgumentList = "?" + new String(new char[this.properties.tableDefinitionColumnsNames.size() - 1]).replace("\0", ",?");
        String statementRequest = "INSERT INTO `" + this.properties.targetDatabaseName + "`." + this.properties.targetTableName  + " (" + this.join(this.properties.tableDefinitionColumnsNames, ",") + ") VALUE (" + statementArgumentList + ");";

        this.databaseInsertStatement = this.databaseConnector.prepareStatement(statementRequest);                
    }
    
    /*
     * Select  
     **************************************************************************************/
    /**
     * Method to build SQL select statement to select data from Data Table.
     * @throws SQLException if SQL exception occurs
     */
    private void buildSelectStatement() throws SQLException {
        this.buildSelectStatement("");
    }
    /**
     * Method to build SQL select statement to select data from Data Table.
     * @throws SQLException if SQL exception occurs
     */
    private void buildSelectStatement(String whereClase) throws SQLException {
        String statementRequest = "SELECT * FROM " + this.properties.targetDatabaseName + "." + this.properties.targetTableName;
        
        if (!"".equals(whereClase)) {
            this.databaseSelectStatementArgumentCount = StringUtils.countMatches(whereClase, "?");
            statementRequest                          += "WHERE " + whereClase;
        } else {
            this.databaseSelectStatementArgumentCount = 0;
        }
        
        this.databaseSelectStatement = this.databaseConnector.prepareStatement(statementRequest);
    }
    /**
     * This method is to execute database Select Statement.
     * @param indexColumnName index of column name 
     * @throws SQLException if SQL exception occurs 
     */
    private void select_doInit(String indexColumnName) throws SQLException {
        // if indexColumnName is empty we go for the 'no index' mode.
        if ("".equals(indexColumnName)) {
            this.select_doInit();
            return;
        }
        
        // Otherwise we process normally
        this.currentSelectResult    = this.databaseSelectStatement.executeQuery();
        this.currentSelectResultMap = new HashMap<>();
        
        this.currentSelectResult.beforeFirst();
        while (this.currentSelectResult.next()) {
            this.currentSelectResultMap.put(this.currentSelectResult.getObject(indexColumnName),this.currentSelectResult.getRow());
        }
        this.currentSelectResult.beforeFirst();
        System.out.println("Connector Info : DataTable : '" + this.properties.targetTableName + "' loaded.");
    }
    /**
     * This method is to execute database Select Statement.
     * @throws SQLException if SQL exception occurs
     */
    private void select_doInit() throws SQLException {
        
        this.currentSelectResult    = this.databaseSelectStatement.executeQuery();
        this.currentSelectResult.beforeFirst();
        
        this.currentSelectResultMap = null;
        System.out.println("Connector Info : DataTable : '" + this.properties.targetTableName + "' loaded.");
    }    
    /**
     * This method is to execute select query in Data Table.
     * @throws SQLException if SQL exception occurs
     * @throws ElfyFieldNotFoundException if no more results from SQL query
     */
    private void select_doNext() throws SQLException, ElfyFieldNotFoundException {
        if (! this.currentSelectResult.next()) {
            throw new ElfyFieldNotFoundException("No more results in " + this.properties.targetTableName + ".");
        }
        
    }
    /**
     * This method is to execute select rows from Data Table. 
     * @param indexValueLookup value of index
     * @throws SQLException if SQL exception occurs
     * @throws ElfyFieldNotFoundException if no more results from SQL query
     * @throws ElfyConnectorException if Data Table is not initialized with an index
     */
    private void select_doNext(String indexValueLookup) throws SQLException, ElfyFieldNotFoundException, ElfyConnectorException {
        if (Objects.isNull(this.currentSelectResultMap)) {
            throw new ElfyConnectorException("DataTable " + this.properties.targetTableName + " not initialized with an index.");
        }
        
        if (this.currentSelectResultMap.containsKey(indexValueLookup)) {
            Integer index = this.currentSelectResultMap.get(indexValueLookup);
            this.select_doNext(index);
        } else {
            throw new ElfyFieldNotFoundException("Index for '" + indexValueLookup + "' not found in " + this.properties.targetTableName + ".");
        }
    }
    /**
     * This method points to a row given by its index.
     * @param indexValueLookup index of row to be selected
     * @throws SQLException if SQL exception occurs
     * @throws ElfyFieldNotFoundException if Data Table is not initialized with an index
     */
    private void select_doNext(Integer indexValueLookup ) throws SQLException, ElfyFieldNotFoundException {
        if (! this.currentSelectResult.absolute(indexValueLookup)) {
            throw new ElfyFieldNotFoundException("Index '" + indexValueLookup + "' not found in " + this.properties.targetTableName + ".");
        }
        
    }
    /**
     * This method is to return value of column in data table.
     * @param columnName column in Data Table
     * @return value of Data Table column 
     * @throws SQLException if SQL exception occurs
     */
    private Object select_doGet(String columnName) throws SQLException {
        return this.currentSelectResult.getObject(columnName);
    }
    
    /*
     * Tools 
     **************************************************************************************/
    /**
     * This method returns column names separated by a separator.
     * @param set list of columns in Data Table
     * @param sep separator value
     * @return column names separated by a separator 
     */
    private String join(Set<String> set, String sep) {
        String result = null;
        if(set != null) {
            StringBuilder sb = new StringBuilder();
            Iterator<String> it = set.iterator();
            if(it.hasNext()) {
                sb.append(it.next());
            }
            while(it.hasNext()) {
                sb.append(sep).append(it.next());
            }
            result = sb.toString();
        }
        return result;
    }    

    /*
     * Connector's Interface. 
     **************************************************************************************/    
      
    @Override
    public void connector_targetUnit_init(String initParameter) throws ElfyConnectorException {
    }

    /**
     * This method initialize hash map currentInsertMap.
     * @param nextParameter next attribute of Data Table
     * @throws ElfyConnectorException
     */
    @Override
    public void connector_targetUnit_next(String nextParameter) throws ElfyConnectorException {
        this.currentInsertMap = new HashMap<>();
    }

    /**
     * This method is to store value for attributes in data table.
     * @param addParameter attribute name 
     * @param objectToAdd data to be added
     * @throws ElfyConnectorException
     */
    @Override
    public void connector_targetUnit_addAttribute(String addParameter, Object objectToAdd) throws ElfyConnectorException {
        if (! this.properties.tableDefinition.containsKey(addParameter)) {
            throw new ElfyConnectorException("Failed to add attribute " + addParameter + " for '" + this.properties.targetTableName + "' : Unknown column.");
        }
        
        if (!Objects.isNull(objectToAdd) && !objectToAdd.toString().trim().equals("")) {
            this.currentInsertMap.put(addParameter, objectToAdd.toString());
        }
        
    }

    /**
     * This method is to add and commit record through the SQL Batch to the Database.
     * @param sendParameter attribute name
     * @throws ElfyConnectorException if failed to add or commit record to database
     */
    @Override
    public void connector_targetUnit_send(String sendParameter) throws ElfyConnectorException {
        try {
            this.addOneStatementToInsertBatch(this.currentInsertMap);
        } catch (SQLException ex) {
            throw new ElfyConnectorException("Failed to add record to the current SQL Batch.", ex);
        } catch (ElfyComitFailedException ex) {
            throw new ElfyConnectorException("Failed to Commit SQL Batch to the Database.", ex);
        } 
    }

    /**
     * This method commit the records in database.
     * @param sendParameter attribute name
     * @throws ElfyConnectorException if failed to commit records to database
     */
    @Override
    public void connector_targetUnit_done(String sendParameter) throws ElfyConnectorException {
        try {
            this.commitInsertBatch();
        } catch (ElfyComitFailedException ex) {
            
        }
    }

    /**
     * This method is to select value of the requested column from data table.
     * @param initParameter column name
     * @throws ElfyConnectorException if failed to request DataTable for column
     */
    @Override
    public void connector_sourceUnit_init(String initParameter) throws ElfyConnectorException {
        try {
            this.select_doInit(initParameter);
        } catch (SQLException ex) {   
            throw new ElfyConnectorException("Failed to request DataTable for '" + initParameter + "'.", ex);
        }
    }

    /**
     * This method is to select attribute name in next row of data table.
     * @param nextParameter attribute name
     * @return true if attribute found in next row else return false 
     * @throws ElfyConnectorException if failed to get DataTable next row for attribute
     */
    @Override
    public Boolean connector_sourceUnit_selectNext(String nextParameter) throws ElfyConnectorException {
        try {
            if ("".equals(nextParameter)) {
                this.select_doNext();
            } else {
               this.select_doNext(nextParameter);
            }
        } catch (SQLException ex) {   
            throw new ElfyConnectorException("Failed to get DataTable next row for '" + nextParameter + "'.", ex);
        } catch (ElfyFieldNotFoundException ex) {
            return false;
        }
        return true;
    }

    /**
     * This method is to return value of column in data table.
     * @param getParameter attribute name
     * @return  value of column in data table
     * @throws ElfyConnectorException if failed to get DataTable attribute
     */
    @Override
    public Object connector_sourceUnit_getAttribute(String getParameter) throws ElfyConnectorException {
        try {
            return this.select_doGet(getParameter);
        } catch (SQLException ex) {   
            throw new ElfyConnectorException("Failed to get DataTable Attribute for '" + getParameter + "'.", ex);
        }
    }

    /**
     * This method is to initialize connection to source unit.
     * @param sendParameter attribute name
     * @throws ElfyConnectorException if unsupported operation exception occurs
     */
    @Override
    public void connector_sourceUnit_done(String sendParameter) throws ElfyConnectorException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
      
}
