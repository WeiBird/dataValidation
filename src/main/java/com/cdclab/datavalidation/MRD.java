/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cdclab.datavalidation;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 *
 * @author Weli
 */
public class MRD {

    private static final Log LOG
            = LogFactory.getLog(MRD.class);

    private static final Path INPUT_PATH = new Path("/tmp/validateInput");
    private static final Path OUTPUT_PATH = new Path("/tmp/validate");
    private static final Path OUTPUT_PATH2 = new Path("/tmp/validateResult2");
//    private static final short TMP_REPLICATION_NUMBER = 1;

    private final FileSystem fs;
    private final Configs config;
    private final int rowCount;
    private static List<DiffCol> inConsistentList = new ArrayList<DiffCol>();

    public MRD(Configs conf) throws IOException, SQLException, ClassNotFoundException {
        this.rowCount = getRowCount(conf);
        this.config = conf;
        fs = FileSystem.get(conf.getConf());
        if (!fs.exists(INPUT_PATH)) {
            fs.mkdirs(INPUT_PATH);
        }       
    }

    public int getRowCount(Configs conf) throws SQLException, ClassNotFoundException {
        DBClient sourceDB = DBClient.create(conf.getInputURL());
        int sRowCount = sourceDB.getRowCount(conf.getInputTable(), conf.getPrimaryKey());
        return sRowCount;
    }

    public void createJob() throws Exception {

        List<Path> iPaths = HDFSFile.WriteFile(fs, rowCount, config);

        final Job job = Job.getInstance(config.getConf());
        job.setJarByClass(MRD.class);
        job.setInputFormatClass(InputFormatImpl.class);
        job.setMapperClass(ValidateMapper.class);
//        job.setCombinerClass(ValidateReducer.class);
//        job.setReducerClass(ValidateReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);
        

        FileInputFormat.setInputPaths(job,
                iPaths.toArray(new Path[iPaths.size()]));
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);
        
        job.waitForCompletion(true);

        LOG.info("Delete the output dir : " + OUTPUT_PATH);
        HDFSFile.checkAndDelete(OUTPUT_PATH, config.getConf());
        for (Path path : iPaths) {
            LOG.info("Delete the input file : " + path);
            fs.delete(path, true);
        }
    }

    private static class InputFormatImpl
            extends FileInputFormat<Text, Text> {

        @Override
        protected boolean isSplitable(final JobContext context,
                final Path filename) {
            return false;
        }

        @Override
        public RecordReader<Text, Text> createRecordReader(
                final InputSplit split, final TaskAttemptContext context)
                throws IOException, InterruptedException {
            return new RecordReader() {
                private Path path;
                private Text key;
                private Text value;
                private Configuration config;
                private int length;

                @Override
                public void initialize(final InputSplit genericSplit,
                        final TaskAttemptContext context)
                        throws IOException, InterruptedException {
                    FileSplit split = (FileSplit) genericSplit;
                    if (split.getStart() != 0) {
                        throw new RuntimeException(
                                "The position of start must be 0");
                    }
                    config = context.getConfiguration();
                    path = split.getPath();
                    length = (int) split.getLength();
                }

                @Override
                public boolean nextKeyValue()
                        throws IOException, InterruptedException {
                    if (path == null) {
                        return false;
                    } else {
                        key = new Text(path.toString());
                        try (FSDataInputStream input
                                = FileSystem.get(config).open(path)) {
                            byte[] buf = new byte[length];
                            input.read(buf, 0, length);
                            value = new Text(new String(buf, "UTF-8"));
                            path = null;
                            return true;
                        }
                    }
                }

                @Override
                public Object getCurrentKey()
                        throws IOException, InterruptedException {
                    return key;
                }

                @Override
                public Object getCurrentValue()
                        throws IOException, InterruptedException {
                    return value;
                }

                @Override
                public float getProgress()
                        throws IOException, InterruptedException {
                    if (path == null) {
                        return 0.0f;
                    } else {
                        return 1.0f;
                    }
                }

                @Override
                public void close() throws IOException {
                }
            };
        }
    }

    private static class ValidateMapper extends Mapper<Text, Text, Text, Text> {

        private Configs config;

        @Override
        protected void setup(final Mapper.Context context)
                throws IOException, InterruptedException {
            config = new Configs(context.getConfiguration());
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            try {
                DBClient sourceDB = DBClient.create(config.getInputURL());
                DBClient targetDB = DBClient.create(config.getOutputURL());

                //Split value from input file
                String[] sql = value.toString().split(",");
                LOG.info(sql[0]);
                LOG.info(sql[1]);

                ResultSet sourceRS = sourceDB.executeSql(sql[0]);
                ResultSet targetRS = targetDB.executeSql(sql[1]);

                ValidateDataSet souceDataSet = new ValidateDataSet(sourceRS);
                ValidateDataSet targetDataSet = new ValidateDataSet(targetRS);

                ValidateMethod validateMethod = new ValidateMethod();
                if (validateMethod.isColNameAndTypeSame(souceDataSet, targetDataSet)) {
                    validateMethod.validateData(souceDataSet, targetDataSet);
                    inConsistentList.addAll(validateMethod.getInconsistentList());
                    for (DiffCol info : inConsistentList) {
                        LOG.info("key "+key+"map "+info);
//                       context.write(key, new Text(info.toString()));
                    }
                } else {
                    
                }

                if (sourceRS != null) {
                    sourceRS.close();
                }
                if (targetRS != null) {
                    targetRS.close();
                }
                targetDB.close();
                sourceDB.close();
            } catch (ClassNotFoundException ex) {
                LOG.warn("No supported driver class");
            } catch (SQLException ex) {
                Logger.getLogger(MRD.class.getName()).log(Level.SEVERE, null, ex);
            }

        }

        protected void cleanup(final Mapper.Context context)
                throws IOException, InterruptedException {
            if (inConsistentList.isEmpty()) {
                LOG.info("Result : Data Consistent");
                
            } else if (!inConsistentList.isEmpty()) {
                HDFSFile.WriteFile(inConsistentList);
                LOG.info("Result : Data Inconsistent");
                LOG.info("==========InConsistent Data List==========");
                for (int i = 0; i < inConsistentList.size(); i++) {
                    LOG.info(inConsistentList.get(i).toString());
                }
            }
        }
    }
/*
    public static class ValidateReducer
            extends
            Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text str : values) {
                String info =str.toString();
                LOG.info("add "+info);
                result.set(info+",");
                context.write(key, result);
            }
            context.write(key, result);
        }
    }*/

    private static Path getUniqueFile(final Path dir, final FileSystem fs,
            final String extension)
            throws IOException {
        if (!fs.getFileStatus(dir).isDirectory()) {
            throw new IOException("Expecting " + dir.toString()
                    + " to be a directory");
        }
        if (extension == null) {
            return new Path(dir, UUID.randomUUID()
                    .toString()
                    .replaceAll("-", ""));
        } else {
            return new Path(dir, UUID.randomUUID()
                    .toString()
                    .replaceAll("-", "") + extension);
        }

    }

    public List<DiffCol> getInconsistentList() {
        return inConsistentList;
    }

}
