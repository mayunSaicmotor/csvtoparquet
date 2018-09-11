package parquet.compat.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public class ConvertUtils {

  private static final Logger LOG = LogManager.getLogger(ConvertUtils.class);
  
 // private static final Log LOG = Log.getLog(ConvertUtils.class);

  public static final String CSV_DELIMITER= "|";

  private static String readFile(String path) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(path));
    StringBuilder stringBuilder = new StringBuilder();

    try {
      String line = null;
      String ls = System.getProperty("line.separator");

      while ((line = reader.readLine()) != null ) {
        stringBuilder.append(line);
        stringBuilder.append(ls);
      }
    } finally {
      Utils.closeQuietly(reader);
    }

    return stringBuilder.toString();
  }

  public static String getSchema(File csvFile) throws IOException {
    String fileName = csvFile.getName().substring(
        0, csvFile.getName().length() - ".csv".length()) + ".schema";
    File schemaFile = new File(csvFile.getParentFile(), fileName);
    return readFile(schemaFile.getAbsolutePath());
  }
  
  public static void convertCsvToParquet(File csvFile, File outputParquetFile) throws IOException {
    convertCsvToParquet(csvFile, outputParquetFile, false);
  }

  public static void convertCsvToParquet(File csvFile, File outputParquetFile, boolean enableDictionary) throws IOException {
    LOG.info("Converting " + csvFile.getName() + " to " + outputParquetFile.getName());
    String rawSchema = getSchema(csvFile);
    if(outputParquetFile.exists()) {
      outputParquetFile.delete();
//      throw new IOException("Output file " + outputParquetFile.getAbsolutePath() + 
//          " already exists");
    }

//    List<Set<String>> setList = new ArrayList<Set<String>>();
//    for(int i = 0;i <16;i++){
//      setList.add(new HashSet<String>());
//    }
    
    Path path = new Path(outputParquetFile.toURI());

    MessageType schema = MessageTypeParser.parseMessageType(rawSchema);
    CsvParquetWriter writer = new CsvParquetWriter(path, schema, CompressionCodecName.SNAPPY, enableDictionary);

    BufferedReader br = new BufferedReader(new FileReader(csvFile));
    String line;
    int lineNumber = 0;
    try {
      while ((line = br.readLine()) != null) {
        String[] fields = line.split(Pattern.quote(CSV_DELIMITER));
//        for(int i = 0;i <fields.length;i++){
//          setList.get(i).add(fields[i]);
//        }
        writer.write(Arrays.asList(fields));
        ++lineNumber;
      }

      writer.close();
    } finally {
      // System.out.println("setList: "+setList);
//      for(Set set : setList){
//        System.out.println("set size: "+set.size());
//      }
      
      System.out.println("Number of lines: " + lineNumber);
      LOG.info("Number of lines: " + lineNumber);
      Utils.closeQuietly(br);
    } 
  }

  public static void convertParquetToCSV(File parquetFile, File csvOutputFile) throws IOException {
    Preconditions.checkArgument(parquetFile.getName().endsWith(".parquet"), 
        "parquet file should have .parquet extension");
    Preconditions.checkArgument(csvOutputFile.getName().endsWith(".csv"), 
        "csv file should have .csv extension");
    Preconditions.checkArgument(!csvOutputFile.exists(), 
        "Output file " + csvOutputFile.getAbsolutePath() + " already exists");

    LOG.info("Converting " + parquetFile.getName() + " to " + csvOutputFile.getName());


    Path parquetFilePath = new Path(parquetFile.toURI());

    Configuration configuration = new Configuration(false);
//    configuration.set("fs.hdfs.impl", 
//        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
//    );
    configuration.set("fs.file.impl",
        org.apache.hadoop.fs.LocalFileSystem.class.getName()
    );
    
    //configuration.set(ReadSupport.PARQUET_READ_SCHEMA, "message test {required int64 L_ORDERKEY;}");
    GroupReadSupport readSupport = new GroupReadSupport();
    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, parquetFilePath);
    MessageType schema = readFooter.getFileMetaData().getSchema();
    //MessageType schema1 =  new MessageType("test", schema.getFields().subList(0, 1));
    //configuration.set(ReadSupport.PARQUET_READ_SCHEMA, "message test {required int64 L_ORDERKEY;}");
    readSupport.init(configuration, null, schema);
    BufferedWriter w = new BufferedWriter(new FileWriter(csvOutputFile));
    ParquetReader<Group> reader = new ParquetReader<Group>(parquetFilePath, readSupport);
    try{
      Group g = null;
      while( (g = reader.read())!= null) {
        // writeGroup(w, g, new MessageType("message test {required int64 L_ORDERKEY;}"));
        writeGroup(w, g, schema);
      }
      reader.close();
    }
    finally {
      Utils.closeQuietly(w);
    }
  }

  private static void writeGroup(BufferedWriter w, Group g, MessageType schema) 
      throws IOException{
    for (int j = 0; j < schema.getFieldCount(); j++) {
      if (j > 0) {
        w.write(CSV_DELIMITER);
      }
      String valueToString = g.getValueToString(j, 0);
      w.write(valueToString);
    }
    w.write('\n');
  }

  @Deprecated
  public static void convertParquetToCSVEx(File parquetFile, File csvOutputFile) throws IOException {
    Preconditions.checkArgument(parquetFile.getName().endsWith(".parquet"), 
        "parquet file should have .parquet extension");
    Preconditions.checkArgument(csvOutputFile.getName().endsWith(".csv"), 
        "csv file should have .csv extension");
    Preconditions.checkArgument(!csvOutputFile.exists(), 
        "Output file " + csvOutputFile.getAbsolutePath() + " already exists");

    LOG.info("Converting " + parquetFile.getName() + " to " + csvOutputFile.getName());

    Path parquetFilePath = new Path(parquetFile.toURI());

    Configuration configuration = new Configuration(true);

    // TODO Following can be changed by using ParquetReader instead of ParquetFileReader
    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, parquetFilePath);
    MessageType schema = readFooter.getFileMetaData().getSchema();
    ParquetFileReader parquetFileReader = new ParquetFileReader(
        configuration, parquetFilePath, readFooter.getBlocks(), schema.getColumns());
    BufferedWriter w = new BufferedWriter(new FileWriter(csvOutputFile));
    PageReadStore pages = null;
    try {
      while (null != (pages = parquetFileReader.readNextRowGroup())) {
        final long rows = pages.getRowCount();
        LOG.info("Number of rows: " + rows);

        final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
        final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
        for (int i = 0; i < rows; i++) {
          final Group g = recordReader.read();
          writeGroup(w, g, schema);
        }
      } 
    } finally {
      Utils.closeQuietly(parquetFileReader);
      Utils.closeQuietly(w);
    }
  }

  public static void main(String[] args) throws IOException {
//  ConvertUtils.convertCsvToParquet(new File("/Users/mayun/git/parquet-compatibility/parquet-compat-1.2.0/src/test/java/parquet/compat/test/data.csv"), 
//  new File("/Users/mayun/git/parquet-compatibility/parquet-compat-1.2.0/src/test/java/parquet/compat/test/data512-1.8-new.parquet"));

     ConvertUtils.convertParquetToCSV(new File("/Users/mayun/git/parquet-compatibility/parquet-compat-1.2.0/src/test/java/parquet/compat/test/data512-1.8.parquet"),
        new File("/Users/mayun/git/parquet-compatibility/parquet-compat-1.2.0/src/test/java/parquet/compat/test/data.csv"));
  
  }
}
