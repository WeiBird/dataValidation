/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cdclab.datavalidation;
import java.lang.Math;
/**
 *
 * @author Weli
 */
public class SqlQuery {
    
     public SqlQuery(){
         
     }
     public String selectAll(final String tableName,final String primaryKey) {
//         int end = start+500;
        return new StringBuilder()
                .append("SELECT * FROM ")
                .append(tableName)
//                .append(" WHERE ")
//                .append(primaryKey)
//                .append(" BETWEEN ")
//                .append(String.valueOf(start))
//                .append(" AND ")
//                .append(String.valueOf(end))
                .append(" ORDER BY ")
                .append(primaryKey)
                .toString();
    }

    public String selectCol(final String tableName, final String colName) {
        
            return new StringBuilder()
                    .append("SELECT * FROM ")
                    .append(tableName)
                    
                    .toString();
       
    }
    
    public String splitSql(final String tableName, final String colName,final int startNum,final int endNum){
        return new StringBuilder()
                .append("SELECT * FROM ")
                .append(tableName)
                .append(" WHERE ")
                .append(colName)
                .append(" > ")
                .append(startNum)
                .append(" AND ")
                .append(colName)
                .append(" <= ")
                .append(endNum)
                .append(" ORDER BY ")
                .append(colName)
                .toString();
        
    
    }
    
}
