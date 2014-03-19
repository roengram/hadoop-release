package org.apache.hadoop.fs.azurenative;

import java.net.URI;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.UUID;

import junit.framework.Assert;

import org.apache.commons.configuration.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.metrics2.sink.WindowsAzureTableSink;
import org.junit.Assume;
import org.junit.Test;
import com.google.common.collect.Iterables;
import com.microsoft.windowsazure.storage.*;
import com.microsoft.windowsazure.storage.table.*;

/*
 * Test the approach used by WindowsAzureTableSink setup code (which got broken by Azure Storage SDK (Java) v0.5.0
 */
public class TestWindowsAzureTableSinkSetup {

  // Basic Azure Table access using full keys and SAS keys.
  @Test
  public void testBasicTableAndSASbehavior() throws Exception {
    // -- start fiddler: uncomment the following to enable fiddler tracing
    //System.setProperty("http.proxyHost", "localhost");
    //System.setProperty("http.proxyPort", "8888");
    // -- end fiddler
    
    // create cloudTableClient based on a full key.
    // use it to create a table and make a SAS key for that table.
    CloudStorageAccount testAccount = AzureBlobStorageTestAccount.createTestAccount();
    Assume.assumeNotNull(testAccount);
    CloudTableClient tableClient = testAccount.createCloudTableClient();
    CloudTable table = tableClient.getTableReference("testsaskeysetup"); // NOTE: sdk 0.5.0 cannot handle uppercase in names. (known issue)
    table.createIfNotExists();
    
    // ensure there is a row of data. 
    TableServiceEntity entityToInsert = new TableServiceEntity();
    entityToInsert.setPartitionKey("a");
    entityToInsert.setRowKey("a");
    TableOperation insertOperation = TableOperation.insertOrReplace(entityToInsert);
    tableClient.execute(table.getName(), insertOperation);
    
    // retrieve row directly and via enumeration to ensure everything looks good via full-key. 
    TableQuery<TableServiceEntity> tq =
        TableQuery.from(table.getName(), TableServiceEntity.class);
    Iterable<TableServiceEntity> allRowsIterable = tableClient.execute(tq);
    TableServiceEntity[] allRowsArray = Iterables.toArray(allRowsIterable, TableServiceEntity.class);
    
    TableOperation op = TableOperation.retrieve("a", "a", TableServiceEntity.class);
    TableResult singleRowResultSet = tableClient.execute(table.getName(), op);
    TableServiceEntity singleRowResult = (TableServiceEntity) singleRowResultSet.getResult();

    Assert.assertTrue(allRowsArray.length == 1);
    Assert.assertNotNull(singleRowResult);
    
    //
    // SAS testing.
    //
    // create a SAS-based tableClient and perform the same retrieval operations. 
    
    String sasKey = generateTableSAS(table);
    StorageCredentials sasCredentials = new StorageCredentialsSharedAccessSignature(sasKey);
    CloudTableClient sasTableClient = new CloudTableClient(tableClient.getStorageUri(), sasCredentials);
    
    Iterable<TableServiceEntity> allRowsIterable2 = sasTableClient.execute(tq);
    TableServiceEntity[] allRowsArray2 = Iterables.toArray(allRowsIterable2, TableServiceEntity.class);
    
    TableResult singleRowResultSet2 = sasTableClient.execute(table.getName(), op);
    TableServiceEntity singleRowResult2 = (TableServiceEntity) singleRowResultSet2.getResult();
    
 
    Assert.assertTrue(allRowsArray2.length == 1);
    Assert.assertNotNull(singleRowResult2);
  }
  
  @Test
  public void testWindowsAzureTableSinkFullKey() throws Exception {
    WindowsAzureTableSink sink = new WindowsAzureTableSink();
    
    //http://commons.apache.org/proper/commons-configuration/userguide/overview.html#Using_Configuration
    //http://commons.apache.org/proper/commons-configuration/userguide/howto_properties.html#Properties_files
    
    CloudStorageAccount testAccount = AzureBlobStorageTestAccount.createTestAccount();
    Assume.assumeNotNull(testAccount);
    String accountName = testAccount.getCredentials().getAccountName();
    Configuration conf = AzureBlobStorageTestAccount.createTestConfiguration(null);
    String fullKey = AzureNativeFileSystemStore.getAccountKeyFromConfiguration(accountName + ".blob.core.windows.net", conf);
    
    Assume.assumeNotNull(testAccount);
    CloudTableClient tableClient = testAccount.createCloudTableClient();
    CloudTable table = tableClient.getTableReference("azuretablesinktest"); // NOTE: sdk 0.5.0 cannot handle uppercase in names. (known issue)
    table.createIfNotExists();
    
    CompositeConfiguration config = new CompositeConfiguration();
    config.addProperty("azure-file-system.sink.azurefs.class", "org.apache.hadoop.metrics2.sink.WindowsAzureTableSink");
    config.addProperty("azure-file-system.sink.azurefs.context","azureFileSystem");
    config.addProperty("azure-file-system.sink.azurefs.accountname",testAccount.getCredentials().getAccountName());
    config.addProperty("azure-file-system.sink.azurefs.accesskey", fullKey);
    
    config.addProperty("azure-file-system.sink.azurefs.record.filter.include","azureFileSystem");
    config.addProperty("azure-file-system.sink.azurefs.azureTable",table.getName());
    config.addProperty("azure-file-system.sink.azurefs.azureDeploymentId","dummyDeploymentId");
    config.addProperty("azure-file-system.sink.azurefs.azureRole","dummy-IsotopeHeadNode");
    config.addProperty("azure-file-system.sink.azurefs.azureRoleInstance","dummy-IsotopeHeadNode_IN_0");
    config.addProperty("azure-file-system.sink.azurefs.partitionKeyTimeFormat","yyyyMMddHHmm");
    
    SubsetConfiguration subConfig = new SubsetConfiguration(config, "azure-file-system.sink.azurefs.");
    sink.init(subConfig);
    sink.putMetrics(new MyMetricRecord());
  }
  
  // NOTE: this test validates that WindowsAzureTableSink works correctly with a SAS
  // key that is generated by current version of SDK. It does not test behavior with
  // SAS key generated by other methods (older/newer SDK & REST calls).
  // The use of an old SAS key is a known trouble point.  To test that, adjust this  
  // test to use a known set of {account,table,SAS}.
  // A commented-out test below shows this in its simplest form.
  @Test
  public void testWindowsAzureTableSinkSAS() throws Exception {
    WindowsAzureTableSink sink = new WindowsAzureTableSink();
    
    //http://commons.apache.org/proper/commons-configuration/userguide/overview.html#Using_Configuration
    //http://commons.apache.org/proper/commons-configuration/userguide/howto_properties.html#Properties_files
    
    CloudStorageAccount testAccount = AzureBlobStorageTestAccount.createTestAccount();
    Assume.assumeNotNull(testAccount);
    CloudTableClient tableClient = testAccount.createCloudTableClient();
    CloudTable table = tableClient.getTableReference("azuretablesinktest"); // NOTE: sdk 0.5.0 cannot handle uppercase in names. (known issue)
    table.createIfNotExists();
    String sasKey = generateTableSAS(table);
    
    CompositeConfiguration config = new CompositeConfiguration();
    config.addProperty("azure-file-system.sink.azurefs.class", "org.apache.hadoop.metrics2.sink.WindowsAzureTableSink");
    config.addProperty("azure-file-system.sink.azurefs.context","azureFileSystem");
    config.addProperty("azure-file-system.sink.azurefs.accountname",testAccount.getCredentials().getAccountName());
    config.addProperty("azure-file-system.sink.azurefs.sas",sasKey);
    config.addProperty("azure-file-system.sink.azurefs.record.filter.include","azureFileSystem");
    config.addProperty("azure-file-system.sink.azurefs.azureTable",table.getName());
    config.addProperty("azure-file-system.sink.azurefs.azureDeploymentId","dummyDeploymentId");
    config.addProperty("azure-file-system.sink.azurefs.azureRole","dummy-IsotopeHeadNode");
    config.addProperty("azure-file-system.sink.azurefs.azureRoleInstance","dummy-IsotopeHeadNode_IN_0");
    config.addProperty("azure-file-system.sink.azurefs.partitionKeyTimeFormat","yyyyMMddHHmm");
    
    SubsetConfiguration subConfig = new SubsetConfiguration(config, "azure-file-system.sink.azurefs.");
    sink.init(subConfig);
    sink.putMetrics(new MyMetricRecord());
  }

// This manual test demonstrates the core table-access logic used by WindowsAzureTableSink
// The interesting usage of this test is for:
// 1. Azure storage sdk v0.5.0  
// 2. that SAS is sv=2012 and 
// 3. TableOperation.insert(entity, echoContent). echoContent->true is required for success.  
// -> UPDATE the URI/SAS/tableName as necessary.
 @Test
 public void testSASVersionMismatch() throws Exception {
   Boolean testActive = false;
   if(!testActive){
     System.out.println("testSASVersionMismatch() inactive: requires custom azure info/credentials.");
   }
   Assume.assumeTrue(testActive);

   URI tableBaseUri = new URI("https://##ACCOUNT##.table.core.windows.net");
   
   StorageCredentials sasCredentials =new StorageCredentialsSharedAccessSignature(
           "##SAS_KEY##" // eg be ?sv=2012-02-12...
       );

   CloudTableClient tableClient = new CloudTableClient(tableBaseUri, sasCredentials);
   String tableName = "##TABLE_NAME##";

   
   
   
   TableServiceEntity entity = new TableServiceEntity();
   String pk = UUID.randomUUID().toString();
   String rk = UUID.randomUUID().toString();
   entity.setPartitionKey(pk);
   entity.setRowKey(rk);

   TableOperation insertMetricOperation = TableOperation.insert(entity,false); // forcing echoContent=true is necessary with sdk 0.5.0
   TableRequestOptions tro = new TableRequestOptions();
   tro.setTablePayloadFormat(TablePayloadFormat.AtomPub);
   OperationContext context = new OperationContext();
   tableClient.execute(tableName, insertMetricOperation, tro, context);
 }
  
 // Generate a SAS for a table.
 // code based on AzureBlobStorageTestAccount.
 // NOTE: this only generates one flavor of SAS based on the Azure SDK being used.
 // TODO: Find a way to generate SAS in different flavors (ie different ?sv=xxxx-xx-xx versions). 
 private String generateTableSAS(CloudTable table) throws Exception {
   SharedAccessTablePolicy sasPolicy = new SharedAccessTablePolicy();

   GregorianCalendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
   calendar.setTime(new Date());
   sasPolicy.setSharedAccessStartTime(calendar.getTime()); // not strictly necessary. default=now.
   calendar.add(Calendar.HOUR, 1);
   sasPolicy.setSharedAccessExpiryTime(calendar.getTime());

   sasPolicy.setPermissions(EnumSet.of(
       SharedAccessTablePermissions.ADD,
       SharedAccessTablePermissions.UPDATE,
       SharedAccessTablePermissions.QUERY,
       SharedAccessTablePermissions.DELETE)
       );

   String sas = table.generateSharedAccessSignature(sasPolicy, null, null, null,null,null);
   Thread.sleep(1500); // short sleep necessay else subsequent SAS usage can fail 
   return sas;
 }
 
  
  private class MyMetricInfo implements MetricsInfo{

    @Override
    public String name() {
        return "dummyMetricInfoName";
    }

    @Override
    public String description() {
      return "dummyMetricInfoDescription";
    }
  }

  private class MyMetric extends AbstractMetric {
    private final int value;
    
    protected MyMetric(MetricsInfo info, int value) {
      super(info);
      this.value = value;
    }

    @Override
    public Integer value() {
      return value;
    }

    @Override
    public MetricType type() {
      return MetricType.COUNTER;
    }

    @Override
    public void visit(MetricsVisitor visitor) {
      visitor.counter(this, value);
    }
  
  }
  
  private class MyMetricRecord implements MetricsRecord{

    @Override
    public long timestamp() {
      return 0;
    }

    @Override
    public String name() {
      return null;
    }

    @Override
    public String description() {
      return null;
    }

    @Override
    public String context() {
      return null;
    }

    @Override
    public Collection<MetricsTag> tags() {
      ArrayList<MetricsTag> list = new ArrayList<MetricsTag>();
      list.add(new MetricsTag(new MyMetricInfo(), "dummyMetricValue"));
      
      return list;
    }

    
    @Override
    public Iterable<AbstractMetric> metrics() {
      ArrayList<AbstractMetric> list = new ArrayList<AbstractMetric>();
      list.add(new MyMetric(new MyMetricInfo(), 42));
      return list;
    }
  }
}
