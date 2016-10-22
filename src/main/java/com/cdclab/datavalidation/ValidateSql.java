/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cdclab.datavalidation;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 * @author Weli
 */
public class ValidateSql {

    private static final Log LOG
            = LogFactory.getLog(ValidateSql.class);
    private static boolean sameRowCount = true;
    private static final Path OUTPUT_PATH2 = new Path("/tmp/validateResult2");

    public static void main(final String[] args) throws
            Exception {
        Configuration conf = HBaseConfiguration.create();
        GenericOptionsParser optionParser
                = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();

        if (remainingArgs.length != 2) {
            LOG.warn("Usage: <local/mr> <config path>...");
            System.exit(0);
        }
        String mode = remainingArgs[0].toUpperCase();

        List<String> configPathes = new LinkedList();
        for (int i = 1; i != remainingArgs.length; ++i) {
            configPathes.add(remainingArgs[i]);
        }
        while (!configPathes.isEmpty()) {
            Configuration cloneConfig = new Configuration(conf);
            String configPath = configPathes.remove(0);
            try (SimpleLock lock = new SimpleLock(configPath)) {
                load(cloneConfig, configPath, mode);
            }
        }

    }

    public static void load(final Configuration conf,
            final String configPath, final String mode) throws Exception {

        Path path = new Path(configPath);
        Configs validationConf = new Configs(conf, path);

        final long initTime = System.currentTimeMillis();
        DBClient sourceDB = DBClient.create(validationConf.getInputURL());
        DBClient targetDB = DBClient.create(validationConf.getOutputURL());

        int sRowCount = sourceDB.getRowCount(validationConf.getInputTable(), validationConf.getPrimaryKey());
        int tRowCount = targetDB.getRowCount(validationConf.getOutputTable(), validationConf.getPrimaryKey());

        if (sRowCount != tRowCount) {
            sameRowCount = false;
        }

        if (mode.compareTo("LOCAL") == 0) {
            LOG.info("Run LOCAL validation");

            String sourceSQL = new SqlQuery().selectAll(validationConf.getInputTable(), validationConf.getPrimaryKey());
            String targetSQL = new SqlQuery().selectAll(validationConf.getOutputTable(), validationConf.getPrimaryKey());
            LOG.info("source query: " + sourceSQL);
            LOG.info("target query: " + targetSQL);
            ResultSet sourceRS = sourceDB.executeSql(sourceSQL);
            ResultSet targetRS = targetDB.executeSql(targetSQL);

            ValidateDataSet souceDataSet = new ValidateDataSet(sourceRS);
            ValidateDataSet targetDataSet = new ValidateDataSet(targetRS);
//        for (int i = 1; i <= souceDataSet.getColumnCount(); i++) {
//        LOG.info(souceDataSet.getMetaDataColumnType(i));
//        LOG.info(targetDataSet.getMetaDataColumnType(i));
//        }
            LOG.info("Validating.......");
            ValidateMethod validateMethod = new ValidateMethod();

            if (validateMethod.isColNameAndTypeSame(souceDataSet, targetDataSet)) {
                LOG.info("Column Name And Type Consistent");
                if (sameRowCount) {

                    validateMethod.validateData(souceDataSet, targetDataSet);
                    validateMethod.ShowValidateResult();
                } else {
                    LOG.info("Row Count Inconsistent");
                }
            } else {
                LOG.info("Column Name Or Type Inconsistent");
            }
            final long endReturnTime = System.currentTimeMillis();
            LOG.info("validating takes " + (endReturnTime - initTime) + " ms");
            //close connect
            if (sourceRS != null) {
                sourceRS.close();
            }
            if (targetRS != null) {
                targetRS.close();
            }
            targetDB.close();
            sourceDB.close();
            LOG.info("DBclose");
        } else if (mode.compareTo("MR") == 0) {
            LOG.info("Run MR validation");
            if (sameRowCount) {
                MRD mr = new MRD(validationConf);
                mr.createJob();
                HDFSFile.ReadFile();
                HDFSFile.checkAndDelete(OUTPUT_PATH2, validationConf.getConf());
            } else {
                LOG.info("Row Count Inconsistent");
            }
            final long endReturnTime = System.currentTimeMillis();
            LOG.info("validating takes " + (endReturnTime - initTime) + " ms");
        } else {
            LOG.warn("error run type,please ensure the type:<local/mr>");
        }
    }

    private static class SimpleLock implements Closeable {

        /**
         * The configuration path.
         */
        private final String path;
        /**
         * The lock file for the configuration file. It is the configuration
         * file with a extra name.
         */
        private final File lockFile;

        /**
         * Constructs a simple lock for the specified configuration.
         *
         * @param p configuration path.
         * @throws IOException If failed to create lock file.
         */
        SimpleLock(final String p) throws IOException {
            path = p;
            lockFile = new File(p + ".lock");
            if (!lockFile.createNewFile()) {
                throw new IOException("The " + path
                        + " cannot be used by more than one prcoess");
            }
        }

        /**
         * Deletes the file or directory denoted by config pathname.
         */
        void deleteOriginFile() {
            new File(path).delete();
        }

        /**
         * @return File path
         */
        String getStringPath() {
            return path;
        }

        /**
         * @return The path of configuration file.
         */
        Path getPath() {
            return new Path(path);
        }

        @Override
        public void close() throws IOException {
            lockFile.delete();
        }
    }

    private static void Configs(Configuration cloneConfig, Path path) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * Locks the specified configuration file for preventing the duplicate
     * loader job.
     */
}
