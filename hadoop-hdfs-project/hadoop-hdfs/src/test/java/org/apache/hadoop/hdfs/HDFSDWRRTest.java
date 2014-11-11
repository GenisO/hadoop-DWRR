package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.util.Daemon;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by DEIM on 30/10/14.
 */
public class HDFSDWRRTest {

  private String hdfsUrl = "10.30.240.109:2100";
  private String sourceFilename;
  private String destinationFilename;

  public String getSourceFilename() {
    return sourceFilename;
  }

  public void setSourceFilename(String sourceFilename) {
    this.sourceFilename = sourceFilename;
  }

  public String getDestinationFilename() {
    return destinationFilename;
  }

  public void setDestinationFilename(String destinationFilename) {
    this.destinationFilename = destinationFilename;
  }

  public void uploadFile() {
    Configuration conf = new HdfsConfiguration();
    conf.set("fs.default.name", this.hdfsUrl);
    DFSClient client = null;
    try {
      client = new DFSClient(new URI(this.hdfsUrl), conf);

      OutputStream out = null;
      InputStream in = null;
      try {
        if (client.exists(destinationFilename)) {
          System.out.println("File already exists in hdfs: " + destinationFilename);
          return;
        }
        out = new BufferedOutputStream(client.create(destinationFilename, false));
        in = new BufferedInputStream(new FileInputStream(sourceFilename));
        byte[] buffer = new byte[1024];

        int len = 0;
        while ((len = in.read(buffer)) > 0) {
          out.write(buffer, 0, len);
        }
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        if (client != null) {
          client.close();
        }
        if (in != null) {
          in.close();
        }
        if (out != null) {
          out.close();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
  }

  public void downloadFile() {
    try {
      Configuration conf = new Configuration();
      conf.set("fs.defaultFS", this.hdfsUrl);
      final DFSClient client = new DFSClient(new URI(this.hdfsUrl), conf);
      long startOffset = 0;

      try {
        if (client.exists(sourceFilename)) {
          LocatedBlocks locatedBlocks = client.getLocatedBlocks(sourceFilename, startOffset);

          for (final LocatedBlock localBlock : locatedBlocks.getLocatedBlocks()) {
            Daemon threadRead = new Daemon(new ThreadGroup("DWRR Thread"),
              new Runnable() {
                @Override
                public void run() {
                  int offset = (int) localBlock.getStartOffset();
                  int len = (int) localBlock.getBlockSize();
                  try {
                    InputStream in = new BufferedInputStream(client.open(sourceFilename));
                    byte[] buffer = new byte[(int) client.getDefaultBlockSize()];

                    in.read(buffer, offset, len);

                    System.out.println("El bloc tal sha acabat de llegir");

                  } catch (IOException e) {
                    e.printStackTrace();
                  }

                }
              });
            threadRead.start();
          }

        } else {
          System.out.println("File does not exist!");
        }
      } finally {
        if (client != null) {
          client.close();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}