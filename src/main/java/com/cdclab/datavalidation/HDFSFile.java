/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cdclab.datavalidation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 *
 * @author Weli
 */
public class HDFSFile {

    private static final Log LOG
            = LogFactory.getLog(HDFSFile.class);

    private static final Path INPUT_PATH = new Path("/tmp/validateInput");
    private static final Path OUTPUT_PATH2 = new Path("/tmp/validateResult2");
    private static List<Path> iPaths = new LinkedList();

    static boolean putToHdfs(String src, String dst, Configuration conf) {
        Path dstPath = new Path(dst);
        try {
            FileSystem hdfs = dstPath.getFileSystem(conf);
            hdfs.copyFromLocalFile(false, new Path(src), new Path(dst));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    static boolean getFromHdfs(String src, String dst, Configuration conf) {
        Path dstPath = new Path(dst);
        try {
            FileSystem hdfs = dstPath.getFileSystem(conf);
            hdfs.copyToLocalFile(false, new Path(src), new Path(dst));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static void ReadFile() throws IOException {
        LOG.info("READ FILE");
//        Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(conf);
//            try (FSDataInputStream input = fs.open(OUTPUT_PATH2)) {
//                String value =input.readUTF();
//                LOG.info("內容:"+value);
//            }
//        LOG.info("READ FILE");
//        Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(conf);
//
//        for(Path p:iPaths){
//        LOG.info(p.toString());
//        FSDataInputStream fsStream = fs.open(p);
//        byte[] buf = new byte[100];
//                            fsStream.read(buf);
//                            Text result = new Text(new String(buf, "UTF-8"));
////        BufferedReader br
////                = new BufferedReader(new InputStreamReader(fsStream, "UTF-8"));
//
//        String value = result.toString();
//        LOG.info(value);
//        String[] info = value.toString().split("/");
//        for (int i = 0; i < info.length; i++) {
//            LOG.info(info[i]);
//        }

//        br.close();
//        fsStream.close();
//        }
//        
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(OUTPUT_PATH2);
            for (int i = 0; i < status.length; i++) {

                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                String line;
                line = br.readLine();
                if (line != null) {
                    LOG.info("Result : Data Inconsistent");
                    LOG.info("==========InConsistent Data List==========");

                    String[] info = line.split("/");
                    for (int count = 0; count < info.length; count++) {
                        LOG.info(info[count]);
                    }
                    LOG.info("========================================");
                } 
            }
        } catch (Exception e) {
            LOG.info("Column Name And Type Consistent");
            LOG.info("Result : Data Consistent");
        }

    }

    public static void WriteFile(List<DiffCol> list) throws IOException {
        LOG.info("WRITE RESULT");
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        if (!fs.exists(OUTPUT_PATH2)) {
            fs.mkdirs(OUTPUT_PATH2);
        }
        Path p = getUniqueFile(OUTPUT_PATH2, fs, null);
        LOG.info(p.toString());
        try (FSDataOutputStream output = fs.create(p)) {
            for (DiffCol info : list) {
                if (info == null) {
                    output.writeBytes("");
                } else {
                    LOG.info(info.toString());
                    output.writeBytes(info.toString());
                    output.writeBytes("/");
                    iPaths.add(p);
                }
            }
        }
    }

    public static List<Path> WriteFile(final FileSystem fs, final int rowCount, final Configs conf) throws IOException {

        final int incr = (int) Math.ceil(
                (double) rowCount / (double) conf.getMRProcessNumber());

        List<Path> iPaths = new LinkedList();
        for (int offset = 0; offset < rowCount; offset += incr) {
            if (offset + incr >= rowCount) {
                int remaining = rowCount % incr;
                if (remaining == 0) {
                    remaining = incr;
                }
                String sourceSql = new SqlQuery().splitSql(conf.getInputTable(), conf.getPrimaryKey(), offset, offset + remaining);
                String targetSql = new SqlQuery().splitSql(conf.getOutputTable(), conf.getPrimaryKey(), offset, offset + remaining);
                LOG.info("create input file");
                Path p = getUniqueFile(INPUT_PATH, fs, null);
                try (FSDataOutputStream output = fs.create(p)) {
                    output.writeBytes(sourceSql);
                    output.writeBytes(",");
                    output.writeBytes(targetSql);
                    iPaths.add(p);
                }
                break;
            } else {
                String sourceSql = new SqlQuery().splitSql(conf.getInputTable(), conf.getPrimaryKey(), offset, offset + incr);
                String targetSql = new SqlQuery().splitSql(conf.getOutputTable(), conf.getPrimaryKey(), offset, offset + incr);
                Path p = getUniqueFile(INPUT_PATH, fs, null);
                try (FSDataOutputStream output = fs.create(p)) {
                    output.writeBytes(sourceSql);
                    output.writeBytes(",");
                    output.writeBytes(targetSql);
                    iPaths.add(p);
                }
            }
        }
        return iPaths;
    }

    static boolean checkAndDelete(final Path dstpath, Configuration conf) {
        try {
            FileSystem hdfs = dstpath.getFileSystem(conf);
            if (hdfs.exists(dstpath)) {
                hdfs.delete(dstpath, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

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

}
