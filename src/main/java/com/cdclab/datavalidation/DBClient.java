    /*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cdclab.datavalidation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Weli
 */
public class DBClient {
    
    private static final Log LOG
            = LogFactory.getLog(DBClient.class);

    public static DBClient create(final String url) throws ClassNotFoundException, SQLException {
        String lowerCaseUrl;
        if(url == null)lowerCaseUrl="";
        else lowerCaseUrl = url.toLowerCase();
        
        if (lowerCaseUrl.startsWith("jdbc:sqlserver")) {
            return new DBClient(url, "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } else if (lowerCaseUrl.startsWith("jdbc:phoenix")) {
            return new DBClient(url, "org.apache.phoenix.jdbc.PhoenixDriver");
        } else if (lowerCaseUrl.startsWith("jdbc:mysql")) {
            return new DBClient(url, "com.mysql.jdbc.Driver");
        } else {
            throw new RuntimeException("not supported DBClient");
        }
    }
    private final Statement stat;
    private final Connection connection;
    

    public DBClient(final String url, final String driverClass) throws ClassNotFoundException, SQLException {
        if (driverClass != null) {
            Class.forName(driverClass);
        }
        connection = DriverManager.getConnection(url);
        connection.setAutoCommit(false);
        stat = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stat.setFetchSize(1000);
        stat.setFetchDirection(ResultSet.FETCH_FORWARD);
    }

    public ResultSet executeSql(final String sql) throws SQLException {
        return stat.executeQuery(sql);
    }

    public int getRowCount(final String table, final String col) throws SQLException {
        int count = 0;
        String sql = new StringBuilder()
                .append("SELECT COUNT(")
                .append(col.toUpperCase())
                .append(") FROM ")
                .append(table)
                .toString();
        try (ResultSet rset = executeSql(sql)) {
            rset.next();
            count = rset.getInt(1);
            return count;
        }
    }

    public void close() throws SQLException {
        stat.close();
        connection.close();
    }

}
