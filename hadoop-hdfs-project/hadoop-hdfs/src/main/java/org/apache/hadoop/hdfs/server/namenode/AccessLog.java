package org.apache.hadoop.hdfs.server.namenode;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

public class AccessLog extends Configured implements Runnable {
  public static final Log LOG = LogFactory.getLog(AccessLog.class); 

  public static int upperBound = 20;
  public static String nameNodeIp;
  public static String logFilePath;
  public static String hdfsFilePath;
  public static int lowerBound = 8;
  public static float interval = (float) .5;
  public static HashSet<String> hotFiles = new HashSet<String>();
  public static HashSet<String> coldFiles = new HashSet<String>();
  public static ArrayList<String> fileList = new ArrayList<String>();

  public AccessLog(Configuration conf) {
    super(conf);
    try {
        nameNodeIp = new URI(conf.get("fs.default.name")).getHost();
    } catch (URISyntaxException uri) {
        nameNodeIp = "localhost";
    }
    logFilePath = conf.get("eafr.access-log", "hdfs://" + nameNodeIp + "/logs/hdfs-audit.log");
    hdfsFilePath = "hdfs://" + nameNodeIp + "/";
  }

  public static void increaseReplication(FileSystem dfs, String hdfsFilePath) throws IOException {
    short replicationFactor = (short) (dfs.getDefaultReplication(new Path(hdfsFilePath)) + 2);
    dfs.setReplication(new Path(hdfsFilePath), replicationFactor);
  }

  public static void decreaseReplication(FileSystem dfs, String hdfsFilePath) throws IOException {
    short replicationFactor = (short) (dfs.getDefaultReplication(new Path(hdfsFilePath)) - 1);
    dfs.setReplication(new Path(hdfsFilePath), replicationFactor);
  }

  public static ArrayList<String> getAllFilePathInHDFS(FileSystem dfs, String hdfsFilePath)
      throws FileNotFoundException, IOException {
    ArrayList<String> fileList = new ArrayList<String>();

    FileStatus[] fileStatus = dfs.listStatus((new Path(hdfsFilePath)));
    for (FileStatus fileStat : fileStatus) {
      if (fileStat.isDirectory()) {
        fileList.addAll(getAllFilePathInHDFS(dfs, fileStat.getPath().toString().substring(27)));
      } else {
        fileList.add(fileStat.getPath().toString().substring(27));
      }
    }

    return fileList;
  }

  public static ArrayList<Pattern> getRegexPattern(List<String> fileList) {
    ArrayList<Pattern> patterns = new ArrayList<Pattern>();
    // System.out.println("getting file path patterns..\n\n");
    for (String i : fileList) {
      String lim = "src=" + i;
      patterns.add(Pattern.compile(lim));
    }
    return patterns;
  }

  @Override
  public void run() {
    for (;;) {
      try {
        runScan();
      } catch (URISyntaxException | IOException | ParseException e) {
        LOG.error("Exception while running access log scan", e);
      }
      Thread.sleep(interval * 3600 * 1000);
    }
  }
  public void runScan() throws URISyntaxException, IOException, MalformedURLException, ParseException {

    Configuration conf = new Configuration();
    FileSystem dfs1 = null;
    dfs1 = FileSystem.get(new URI(hdfsFilePath), conf);

    URL log = null;
    log = new URL(logFilePath);
    BufferedReader br = null;
    br = new BufferedReader(new InputStreamReader(log.openStream()));
    String line;
    long index = 0;
    fileList = getAllFilePathInHDFS(dfs1, hdfsFilePath.toString());
    // System.out.println("Total number of files stored in HDFS: "+fileList.size());
    ArrayList<Pattern> patterns = getRegexPattern(fileList);
    // System.out.println(patterns+"\n\n");

    HashMap<String, Integer> fileCountMap = new HashMap<String, Integer>();
    int fileCount;

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long currentTime = getCurrentTime();
    // System.out.println(currentTime);
    long timeSpan = (long) (interval * 3600 * 1000);
    long thresholdTime = getThresholdTime(currentTime, timeSpan);
    // System.out.println(thresholdTime);

    Pattern datePatterns = Pattern.compile(getRegexPatternForTime());
    for (int i = 0; i < (patterns.size()); i++) {
      Pattern a = patterns.get(i);
      fileCountMap.put(a.toString(), 0);
    }
    // System.out.println(fileCountMap);
    // System.out.println("parsing the log file to get file count..\n\n");
    while ((line = br.readLine()) != null) {
      index++;
      Matcher dateTime = datePatterns.matcher(line);
      int count = 0;
      for (int j = 0; j < (patterns.size()); j++) {
        Pattern element = patterns.get(j);
        Matcher fileLocation = element.matcher(line);
        while (fileLocation.find() && dateTime.find()) {

          String date = dateTime.group(0);
          long fileAccessTime = (sdf.parse(date)).getTime();
          if (fileAccessTime >= thresholdTime) {
            Date date2 = new Date(fileAccessTime);
            String elem = new String(element.toString());
            fileCountMap.put(elem, fileCountMap.get(elem) + 1);
          }
        }
      }
      // System.out.println(count);

    }
    // System.out.println("List of files with each individual counts within last "+interval+"
    // hours:\n\n");
    // System.out.println(fileCountMap);
    for (int k = 0; k < (patterns.size()); k++) {
      Pattern element = patterns.get(k);
      String elem = new String(element.toString());
      fileCount = fileCountMap.get(elem);
      // System.out.println("x_0");
      if (fileCount >= upperBound) {
        hotFiles.add(elem);
        try {
          increaseReplication(dfs1, elem.substring(4));
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        // System.out.println("x");
      }
      if (fileCount <= lowerBound) {
        coldFiles.add(elem);
        try {
          decreaseReplication(dfs1, elem.substring(4));
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      if ((fileCount < upperBound) && (fileCount > lowerBound)) {
        @SuppressWarnings("deprecation")
        short getrep = 0;
        getrep = dfs1.getReplication(new Path(elem.substring(4)));
        short defrep = dfs1.getDefaultReplication(new Path(elem.substring(4)));
        if (defrep != getrep) {
          dfs1.setReplication(new Path(elem.substring(4)), defrep);
        }
      }
    }
    
    filePopularity(fileCountMap, interval);
  }

  public static HashMap<String, Float> filePopularity(
      HashMap<String, Integer> fileCountMap, float interval) {
    HashMap<String, Float> filePop = new HashMap<String, Float>();
    for (String file : fileCountMap.keySet()) {
      float countperinterval = fileCountMap.get(file) / interval;
      filePop.put(file, countperinterval);
    }
    System.out.println(
        "File popularity for all the files wihtin last " + interval + " hours: \n\n");
    System.out.println(filePop);
    return filePop;
  }

  public static String getRegexPatternForTime() {
    String format =
        "\\d{4}\\-(0?[1-9]|1[012])\\-(0?[1-9]|[12][0-9]|3[0-1])*\\s(([0-1]?[0-9]|2[0-3]):([0-5][0-9])(:[0-5][0-9]))";
    return format;
  }

  public static long getCurrentTime() {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-DD HH:mm:ss");
    Date date = new Date();
    long currentTime = date.getTime();
    return currentTime;
  }

  public static long getThresholdTime(long currentTime, long timeSpan) {
    long thresholdTime = currentTime - timeSpan;
    return thresholdTime;
  }

  public static boolean isHotFile(String filePath) {
    return hotFiles.contains(filePath);
  }

  public static boolean isColdFile(String filePath) {
    return coldFiles.contains(filePath);
  }
}

