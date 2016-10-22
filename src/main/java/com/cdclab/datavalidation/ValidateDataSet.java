/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cdclab.datavalidation;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Weli
 */
public class ValidateDataSet {
     private static final Log LOG
            = LogFactory.getLog(ValidateDataSet.class);

    private final ResultSet resultSet;
    private final ResultSetMetaData metaData;
    private final int colCount;

    public ValidateDataSet(final ResultSet rset) throws SQLException {
        resultSet = rset;
        metaData = rset.getMetaData();
        colCount = metaData.getColumnCount();
    }

    public int getColumnCount() {
        return colCount;
    }
    
    public int getPrimaryKey()throws SQLException{
        for (int i = 1; i <= colCount;i++) {
            if(getMetaDataColumnType(i) ==4){
                return getResultSetIntData(i);
            }
        }
        return 0;
    }

    public ResultSetMetaData getMetaData() {
        return metaData;
    }

    public String getMetaDataColumnName(int index) throws SQLException {
        return metaData.getColumnName(index).toUpperCase();
    }
    
    public int getMetaDataColumnType(int index) throws SQLException {
        return metaData.getColumnType(index);
    }
    public boolean getResultSetNext()throws SQLException{
        return resultSet.next();
    }
    
    public int getResultSetIntData(int index)throws SQLException {
        return resultSet.getInt(getMetaDataColumnName(index));
    }
    
    public String getResultSetStringData(int index)throws SQLException {
        String A = resultSet.getString(getMetaDataColumnName(index));
        if(A == null) A="";
        return A.toUpperCase();
    }
    
     public Timestamp getResultSetTime(int index)throws SQLException {
        return resultSet.getTimestamp(index);
    }
        
    public String getValue( int index)throws SQLException {
        switch (getMetaDataColumnType(index)) {
            case 4:
                return String.valueOf(getResultSetIntData(index));
            case 12:
                return getResultSetStringData(index);
            case 93:
                return String.valueOf(getResultSetTime(index));
            default:
                LOG.warn("no Data type " + getMetaDataColumnType(index));
                return "NULL";
        }
    }

}
