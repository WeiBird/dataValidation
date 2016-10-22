/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cdclab.datavalidation;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Weli
 */
public class ValidateMethod {

    private static final Log LOG
            = LogFactory.getLog(ValidateMethod.class);
  
    List<DiffCol> inConsistentList = new ArrayList<DiffCol>();

    public ValidateMethod() {
    }


    public boolean isColNameAndTypeSame(final ValidateDataSet souceDataSet, final ValidateDataSet targetDataSet)throws SQLException{
       for (int i = 1; i <= souceDataSet.getColumnCount(); i++) {
            if (souceDataSet.getMetaDataColumnName(i).compareTo(targetDataSet.getMetaDataColumnName(i)) != 0
                    || souceDataSet.getMetaDataColumnType(i) != (targetDataSet.getMetaDataColumnType(i))) {
                return false;
            }
        }
        return true;
        
    }



    public void validateData(final ValidateDataSet souceDataSet, final ValidateDataSet targetDataSet) throws SQLException {
        while (souceDataSet.getResultSetNext() && targetDataSet.getResultSetNext()) {      
            for (int i = 1; i <= souceDataSet.getColumnCount(); i++) {
                if (isInconsisitent(souceDataSet, i, targetDataSet)) {
                    inConsistentList.add(new DiffCol(targetDataSet.getMetaDataColumnName(i), targetDataSet.getValue(i), souceDataSet.getPrimaryKey()));
                }
            }
        }
    }
    
    public static boolean isInconsisitent(final ValidateDataSet sourceRs, final int index, final ValidateDataSet targetRs) throws SQLException {
        switch (sourceRs.getMetaDataColumnType(index)) {
            case 4://int
                return sourceRs.getResultSetIntData(index) != (targetRs.getResultSetIntData(index));
            case 12://varchar
                String A=sourceRs.getResultSetStringData(index);
                String B = targetRs.getResultSetStringData(index);
                if(A==null) A="";
                if(B==null) B="";
                return A.compareTo(B) != 0;
            case 1://char
                String C=sourceRs.getResultSetStringData(index);
                String D = targetRs.getResultSetStringData(index);
                if(C==null) C="";
                if(D==null) D="";
                return C.compareTo(D) != 0;
            case 6://float
                return sourceRs.getResultSetIntData(index) != (targetRs.getResultSetIntData(index));
            case 8://double 
                 return sourceRs.getResultSetIntData(index) != (targetRs.getResultSetIntData(index));
            case 93://timestamp
                return sourceRs.getValue(index).compareTo(targetRs.getValue(index)) !=0;
                
            default:
                LOG.warn("no support type" + sourceRs.getMetaDataColumnType(index));
                return false;
        }

    }
    

    public void ShowValidateResult() {
        
        if (inConsistentList.isEmpty()) {
            LOG.info( "Result : Data Consistent" );
        } else if (!inConsistentList.isEmpty()) {
            LOG.info( "Result : Data Inconsistent" );
            LOG.info("==========InConsistent Data List==========");
            for (int i = 0; i < inConsistentList.size(); i++) {
                LOG.info(inConsistentList.get(i).toString());
            }
        }
    }

    public List<DiffCol> getInconsistentList(){
        return inConsistentList;
    }

}
