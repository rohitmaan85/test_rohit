/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package elfy.Connectors;

import elfy.Exceptions.ElfyConnectorException;
import elfy.Exceptions.ElfyPropertiesException;
import elfy.Properties.ExcelReaderProperties;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

/**
 * This class is to define Excel Reader Connectors used in Elfy integrations.
 * @author jherbault
 */
public class ExcelReaderConnector extends Connector {

    /**
     * Variable for AG2RLM_V1 excel reader Connector
     */
    public static String            AG2RLM_V1_EXCELREADER_CONNECTOR_NAME = "excel.AG2RLM_DataImport_V1";

    private String                  connectorName;
    private String                  connectorType;
    private ExcelReaderProperties   properties;

    private Workbook                workbook;
    private Sheet                   sheet;

    private Integer                 startRowIndex;
    private Integer                 currentRowIndex;
    private Integer                 endRowIndex;

    private Integer                 startColumnIndex;
    private Integer                 currentColumnIndex;
    private Integer                 endColumnIndex;

    /**
     * This method is to get Excel Reader Connector type and name.
     * @param ConnectorName name of Excel Reader Connector
     * @return instance of ExcelReaderConnector class
     * @throws ElfyConnectorException if failed to get a valid Excel Reader connector
     */
    public final static ExcelReaderConnector getConnector(String ConnectorName) throws ElfyConnectorException {
        return (ExcelReaderConnector) Connector.getConnector(Connector.TYPE_EXCELREADER, ConnectorName);
    }

    /**
     * This method is to get new instance of Excel Reader Connector.
     * @param ConnectorName name of Excel Reader Connector
     * @return instance of ExcelReaderConnector class
     * @throws ElfyConnectorException if failed to get a valid Excel Reader connector
     */
    public final static ExcelReaderConnector getUnattachedConnector(String ConnectorName) throws ElfyConnectorException {
        return (ExcelReaderConnector) Connector.getUnattachedConnector(Connector.TYPE_EXCELREADER, ConnectorName);
    }

    /**
     * Constructor to initialize Excel Reader Connector configuration.
     * @param connectorName name of Excel Reader Connector
     * @throws ElfyConnectorException if failed to get a valid Excel Reader connector
     */
    public ExcelReaderConnector(String connectorName) throws ElfyConnectorException {
        try {
            this.connectorType  = ExcelReaderConnector.AG2RLM_V1_EXCELREADER_CONNECTOR_NAME;
            this.connectorName  = connectorName;
            this.properties     = ExcelReaderProperties.getProperties(connectorName);
        } catch (ElfyPropertiesException ex) {
            throw new ElfyConnectorException("Failed to get a valid ExcelReader configuration for '" + connectorName + "'", ex);
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
     * This method is to commit data in target unit.
     * @param sendParameter attribute name
     * @throws ElfyConnectorException if unsupported operation exception occurs
     */
    @Override
    public void connector_targetUnit_done(String sendParameter) throws ElfyConnectorException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * This method is to read source excel file from start tag to end tag.
     * @param initParameter source excel file
     * @throws ElfyConnectorException if failed to open or initialize ExcelReader
     */
    @Override
    public void connector_sourceUnit_init(String initParameter) throws ElfyConnectorException {
        try {

            FileInputStream file = new FileInputStream(new File(initParameter));

            //Get the workbook instance for XLS file
            this.workbook = WorkbookFactory.create(file);
            this.sheet    = this.workbook.getSheetAt(0);


            Boolean hasStart = false;
            Boolean hasEnd   = false;

            if (this.properties.dataLookupMethod.equals("Tags")) {
                for (Row currentRow: sheet) {
                    if (hasStart && hasEnd) break;
                    for (Cell currentCell: currentRow) {
                        if (currentCell.getCellType() == Cell.CELL_TYPE_STRING) {
                            String currentCellValue = currentCell.getRichStringCellValue().getString().trim();
                            if (currentCellValue.equals(this.properties.startTag)) {
                                this.startRowIndex      = currentCell.getRowIndex();
                                this.currentRowIndex  = this.startRowIndex;

                                this.startColumnIndex   = currentCell.getColumnIndex();
                                this.currentColumnIndex = currentCell.getColumnIndex();
                                this.endColumnIndex     = this.currentColumnIndex + this.properties.recordLength + 2;

                                hasStart = true;
                                break;
                            } else if (currentCellValue.equals(this.properties.endTag)) {
                                this.endRowIndex = currentCell.getRowIndex();
                                hasEnd = true;
                                break;
                            }
                        }
                    }
                }
            } else {
                // In this case we are in 'Coordinate mode' we look for start Cell based on postion and End Cell base on position or first non empty row in the first column

                if (this.properties.startCoordinateX < 0 || this.properties.startCoordinateY < 0) {
                    throw new ElfyConnectorException("Invalid Start Coordinates in 'Coordinate Mode'. Please check " + this.properties.getEntityName() + " in " + this.properties.getEntityType());
                }

                // minus 1 to match Tag position. Coordinate may be -1 but we should never look for tag directly.
                this.startRowIndex      = this.properties.startCoordinateY - 1;
                this.currentRowIndex    = this.startRowIndex;

                this.startColumnIndex   = this.properties.startCoordinateX - 1;
                this.currentColumnIndex = this.startColumnIndex;

                hasStart = true;

                if (this.properties.stopCoordinateX < 0 && this.properties.stopCoordinateY < 0) {
                    // In this case we need to look up where the end tag should be. We lookup the first null row in the first column.
                    int rowShift = 0;
                    Cell currentCell = null;
                    Boolean lastRowFound = false;
                    Boolean dataFoundInRow = false;

                    while (++rowShift < 65536 ) {
                        dataFoundInRow = false;

                        if (Objects.isNull(sheet.getRow(startRowIndex + rowShift))) {
                            // end of the file.
                            lastRowFound = true;
                            break;
                        }

                        for (Integer columnShift = 1; columnShift <= this.properties.recordLength; columnShift++) {
                            // We try to Select the Cell.
                            if (Objects.isNull(currentCell = sheet.getRow(startRowIndex + rowShift).getCell(startColumnIndex + columnShift))) {
                                throw new ElfyConnectorException("Error while searching for data range. Invalid Cell at location row: " + (startRowIndex + rowShift) + "column : " + (startColumnIndex + columnShift));
                            }

                            // Is tehre data in the Cell
                            if (currentCell.getCellType() != Cell.CELL_TYPE_BLANK && !(currentCell.getCellType() == Cell.CELL_TYPE_STRING && currentCell.getRichStringCellValue().getString().trim().equals(""))) {
                                dataFoundInRow = true;
                                break;
                            }

                        }

                        if (dataFoundInRow == false) {
                            lastRowFound = true;
                            break;
                        }
                    }

                    if (! lastRowFound) {
                        throw new ElfyConnectorException("Error while searching for data range. More than 65536 rows ? ");
                    }

                    this.endRowIndex    = this.startRowIndex + rowShift;
                    this.endColumnIndex = this.startColumnIndex;

                    hasEnd = true;

                } else if (this.properties.stopCoordinateX < 0 && this.properties.stopCoordinateY < 0) {
                    throw new ElfyConnectorException("Invalid Stop Coordinates in 'Coordinate Mode'. Please check " + this.properties.getEntityName() + " in " + this.properties.getEntityType());
                } else {
                    this.endRowIndex        = this.properties.stopCoordinateY + 1; // end tag should be one row bellow
                    this.endColumnIndex     = this.properties.stopCoordinateX - 1; // end tag should be on column before
                    hasEnd = true;
                }


            }

            if (!hasStart || !hasEnd) {

                throw new ElfyConnectorException("Invalid File Format : Failed to find Start and End Tags (hasStart : " + hasStart + ", hasEnd : " + hasEnd + ")");
            }

            System.out.println("Connector Info : ExcelReader : '" + this.connectorName + "' ready in Read mode.");

        } catch (IOException | InvalidFormatException | EncryptedDocumentException ex ) {
            throw new ElfyConnectorException("Failed to open ExcelReader '" + this.connectorType + "' file in Writer mode (" + initParameter + ") .", ex);
        } catch (ElfyConnectorException ex) {
            throw new ElfyConnectorException("Failed to initialize ExcelReader '" + this.connectorType + "'(" + initParameter + ").", ex);
        }
        /* catch (ElfyJsonReaderException ex) {
        throw new ElfyConnectorException("Failed to initialize JsonReader '" + this.connectorType + "' in Writer mode.", ex);
         */

            /* catch (ElfyJsonReaderException ex) {
            throw new ElfyConnectorException("Failed to initialize JsonReader '" + this.connectorType + "' in Writer mode.", ex);
        */
    }

    /**
     * Method to check if end row is reached or not.
     * @param nextParameter attribute name
     * @return true if end row not reached else return false
     * @throws ElfyConnectorException
     */
    @Override
    public Boolean connector_sourceUnit_selectNext(String nextParameter) throws ElfyConnectorException {
        this.currentRowIndex++;
        return this.currentRowIndex < this.endRowIndex;
    }

    /**
     * This method is to check cell type of excel file and return its string value.
     * @param getParameter attribute name
     * @return string value of cell in excel file
     * @throws ElfyConnectorException if trying to get data out of bound from the Excel file or Invalid parameter for getAttribute
     */
    @Override
    public Object connector_sourceUnit_getAttribute(String getParameter) throws ElfyConnectorException {
        Integer targetIndex;

        if (this.currentRowIndex >= this.endRowIndex) {
            throw new ElfyConnectorException("Trying to get data out of bound from the Excel file : line n° " + this.currentRowIndex);
        }

        try {
            targetIndex = Integer.parseInt(getParameter);
        } catch (NumberFormatException ex) {
            throw new ElfyConnectorException("Invalid parameter for getAttribute : an string representing an integer is expected (getParameter=" + getParameter +")");
        }

        if (targetIndex <= 0 || targetIndex > this.properties.recordLength) {
           throw new ElfyConnectorException("Invalid parameter for getAttribute : value expected between 1 and " + this.properties.recordLength + ".");
        }


        Cell currentCell = this.sheet.getRow(this.currentRowIndex).getCell(this.startColumnIndex + targetIndex);

        switch(currentCell.getCellType()) {
            case Cell.CELL_TYPE_STRING:
                return currentCell.getRichStringCellValue().getString().trim();
            case Cell.CELL_TYPE_BLANK:
                return null;
            case Cell.CELL_TYPE_BOOLEAN:
                return currentCell.getBooleanCellValue();
            case Cell.CELL_TYPE_ERROR:
                return null;
            case Cell.CELL_TYPE_FORMULA:
                return currentCell.getRichStringCellValue().getString().trim();
            case Cell.CELL_TYPE_NUMERIC:
                Double  dblVlaue = currentCell.getNumericCellValue();
                Integer intValue = dblVlaue.intValue();
                return intValue.toString();
            default:
                throw new ElfyConnectorException("Unknow Type for Cell at row:" + currentCell.getRowIndex() + " col:" + currentCell.getColumnIndex());
        }


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
        try {
            this.workbook.close();
        } catch (IOException ex) {
            throw new ElfyConnectorException("Error encountered while trying to close Connector.", ex);
        }
    }
}
