package org.apache.hadoop.fs.azurenative;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;

/**
 * An fsck tool implementation for WASB that does various admin/cleanup/recovery
 * tasks on the WASB file system.
 */
public class WasbFsck extends Configured implements Tool {
  private FileSystem mockFileSystemForTesting = null;
  private static final String lostAndFound = "/lost+found";
  private boolean pathNameWarning = false;

  public WasbFsck(Configuration conf) {
    super(conf);
  }

  /**
   * For testing purposes, set the file system to use here instead of
   * relying on getting it from the FileSystem class based on the URI.
   * @param fileSystem The file system to use.
   */
  public void setMockFileSystemForTesting(FileSystem fileSystem) {
    this.mockFileSystemForTesting = fileSystem;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (doPrintUsage(Arrays.asList(args))) {
      printUsage();
      return -1;
    }
    Path pathToCheck = null;
    boolean doRecover = false;
    boolean doDelete = false;
    for (String arg : args) {
      if (!arg.startsWith("-")) {
        if (pathToCheck != null) {
          System.err.println(
              "Can't specify multiple paths to check on the command-line");
          return 1;
        }
        pathToCheck = new Path(arg);
      } else if (arg.equals("-move")) {
        doRecover = true;
      } else if (arg.equals("-delete")) {
        doDelete = true;
      }
    }
    if (doRecover && doDelete) {
      System.err.println(
          "Conflicting options: can't specify both -move and -delete.");
      return 1;
    }
    if (pathToCheck == null) {
      pathToCheck = new Path("/"); // Check everything.
    }
    FileSystem fs;
    if (mockFileSystemForTesting == null) {
      fs = FileSystem.get(pathToCheck.toUri(), getConf());
    } else {
      fs = mockFileSystemForTesting;
    }
    
    if (recursiveCheckChildPathName(fs, fs.makeQualified(pathToCheck)) == false) {
      pathNameWarning = true;      
    }    
    
    if (!(fs instanceof NativeAzureFileSystem)) {
      System.err.println("Can only check WASB file system. Instead I'm asked to" +
          " check: " + fs.getUri());
      return 2;
    }
    NativeAzureFileSystem wasbFs = (NativeAzureFileSystem)fs;
    if (doRecover) {
      System.out.println("Recovering files with dangling data under: "
          + pathToCheck);
      wasbFs.recoverFilesWithDanglingTempData(pathToCheck, new Path(lostAndFound));
    } else if (doDelete) {
      System.out.println("Deleting temp files with dangling data under: "
          + pathToCheck);
      wasbFs.deleteFilesWithDanglingTempData(pathToCheck);
    } else {
      System.out.println("Please specify -move or -delete");
    }
    return 0;
  }

  public boolean getPathNameWarning() {
    return pathNameWarning;
  }
  
  /**
   * Recursively check if a given path and its child paths have colons in their names. 
   * It returns true if none of them has a colon or this path does not exist, 
   * and false otherwise.
   */
  private boolean recursiveCheckChildPathName(FileSystem fs, Path p) 
      throws IOException {
    if (p == null) {
      return true;     
    }
    if (!fs.exists(p)) {
      System.out.println("Path " + p + " does not exist!");
      return true;
    }    
    
    if (fs.isFile(p)) {
      if (containsColon(p)) {
        System.out.println(
            "Warning: file " + p + " has a colon in its name.");
        return false;    
      } else {
        return true;
      }
    } else {     
      boolean flag;      
      if (containsColon(p)) {
        System.out.println(
            "Warning: directory " + p + " has a colon in its name.");
        flag = false;    
      } else {
        flag = true;
      }
      FileStatus [] listed = fs.listStatus(p);
      for (FileStatus l : listed) {
        if (!recursiveCheckChildPathName(fs, l.getPath())) {
          flag = false;
        }
      }
      return flag;
    }
  }
  
  private boolean containsColon(Path p) {
    return p.toUri().getPath().toString().contains(":");
  }
  private static void printUsage() {
    System.out.println("Usage: WasbFSck [<path>] [-move | -delete]");
    System.out.println("\t<path>\tstart checking from this path");
    System.out.println("\t-move\tmove any files whose upload was interrupted" +
    		" mid-stream to " + lostAndFound);
    System.out.println("\t-delete\tdelete any files whose upload was interrupted" +
        " mid-stream");
    ToolRunner.printGenericCommandUsage(System.out);
  }

  private boolean doPrintUsage(List<String> args) {
    return args.contains("-H");
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new WasbFsck(new Configuration()), args);
    System.exit(res);
  }
}
