/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cdclab.datavalidation;

import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author Weli
 */
public class Configs {

    
    private final Configuration config;
    
    //source and target info
    public static final String DATABASE_SOURCE_URL = "validation.database.source.url";
    public static final String DATABASE_TARGET_URL = "validation.database.target.url";
    public static final String DATABASE_OUTPUT_TABLE = "validation.output.table.name";
    public static final String DATABASE_INPUT_TABLE ="validation.input.table.name";
    public static final String DATABASE_PRIMARY_KEY="validation.database.primarykey";
    
    //the number of splited file for map to run
    public static final String MR_PROCESS_NUMBER="validation.mr.process.number";
    public static final int MR_PROCESS_NUMBER_DEFAULT=5;

    public Configs(final Configuration configuration) {
        this(configuration, null);
    }

    public Configs(final Configuration configuration, final Path source) {
        config = new Configuration(configuration);
        if (source != null) {
            config.addResource(source);
        }
    }
    public Configuration getConf(){
        return config;
    }
    
    public String getInputURL() {
        return config.get(DATABASE_SOURCE_URL);
    }
    public String getOutputURL() {
        return config.get(DATABASE_TARGET_URL);
    }
    
    public String getOutputTable(){
        return config.get(DATABASE_OUTPUT_TABLE);
    }
    
      public String getInputTable() {
        return config.get(DATABASE_INPUT_TABLE);
    }
      
      public String getPrimaryKey(){
          return config.get(DATABASE_PRIMARY_KEY);
      }
      
      public int getMRProcessNumber(){
          return Math.max(config.getInt(MR_PROCESS_NUMBER,
                MR_PROCESS_NUMBER_DEFAULT), MR_PROCESS_NUMBER_DEFAULT);
      }


}
