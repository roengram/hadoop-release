/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.metrics2.sink;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.*;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;


import com.microsoft.windowsazure.storage.*;
import com.microsoft.windowsazure.storage.table.*;

/**
 * This sink writes the metrics to a Windows Azure Table store. 
 * This depends on the Windows Azure SDK for Java version 0.2.2
 * http://go.microsoft.com/fwlink/?LinkID=236226&clcid=0x409
 * 
 * The jar file also has the following dependencies:
 * http://www.windowsazure.com/en-us/develop/java/java-home/download-the-windows-azure-sdk-for-java/
 *    commons-lang3-3.1.jar
 *    commons-logging-1.1.1.jar
 *    jackson-core-asl-1.8.3.jar
 *    jackson-jaxrs-1.8.3.jar
 *    jackson-mapper-asl-1.8.3.jar
 *    jackson-xc-1.8.3.jar
 *    javax.inject-1.jar
 *    jaxb-impl-2.2.3-1.jar
 *    jersey-client-1.10-b02.jar
 *    jersey-core-1.10-b02.jar
 *    jersey-json-1.10-b02.jar
 *    jettison-1.1.jar
 *    stax-api-1.0.1.jar
 *    javax.mail.jar
 * 
 * Table Schema:
 * tables are named using the format "CONTEXTrecordname".
 * For example:
 * DFSnamenode, DFSdatanode, DFSFSnamesystem, JVMmetrics, MAPREDjobtracker, MAPREDtasktracker, RPCrpc
 * 
 * If the *.accesskey property is populated, the table will be created if it does not exist. 
 * 
 * If *.sas property is used then table of the form CONTEXTrecordname is expected to already exist.
 * 
 * The partitionKeyTimeFormat must be a valid datetime format and is used as a "suffix" of the partition key 
 * which is of the format <azureDeploymentId>-<azureRole>-<suffix>
 * 
 * Hadoop-metrics2.properties:
 * The windows azure storage account name and the access key must be passed in through the properties file.
 * 
 * Here is a sample configuration in the hadoop-metrics2.properties file.
 * 		*.sink.table.class=org.apache.hadoop.metrics2.sink.WindowsAzureTableSink
 * 		
 * 		namenode.sink.dfsinstance.class=${*.sink.table.class}
 * 		namenode.sink.dfsinstance.context=dfs
 * 		namenode.sink.dfsinstance.accountname=azure_storage_account_name_here
 * 		namenode.sink.dfsinstance.accesskey=azure_storage_account_key_here
 * 		namenode.sink.dfsinstance.sas=azure_storage_account_sharedaccesskey_here
 *      namenode.sink.dfsnamenode.azureTable=dfsnamenode_table
 *      namenode.sink.dfsnamenode.azureDeploymentId=azure_deployment_id
 *      namenode.sink.dfsnamenode.azureRole=azure_role
 *      namenode.sink.dfsnamenode.azureRoleInstance=azure_role_instance
 *      namenode.sink.dfsnamenode.partitionKeyTimeFormat=yyyyMMddHHmm
 */
public class WindowsAzureTableSink implements MetricsSink {

	private static final String STORAGE_ACCOUNT_KEY = "accountname";
	private static final String STORAGE_ACCESSKEY_KEY = "accesskey";
	private static final String STORAGE_SAS_KEY = "sas";
	private static final String AZURE_TABLENAME_KEY = "azureTable";
	private static final String AZURE_DEPLOYMENTID_KEY = "azureDeploymentId";
	private static final String AZURE_ROLENAME_KEY = "azureRole";
	private static final String AZURE_ROLEINSTANCENAME_KEY = "azureRoleInstance";
	private static final String STORAGE_PARTITON_KEY_TIMEFORMAT_KEY = "partitionKeyTimeFormat";
	
	private static Log logger = LogFactory.getLog(WindowsAzureTableSink.class);
	
	/*
	 * Contains a list of tables that are created on the Azure table store.
	 * The values are populated when the first metrics arrive.
	 */
	private HashMap<String, CloudTableClient> existingTables = new HashMap<String, CloudTableClient>();
	
	private String deploymentId;
	private String roleName;
	private String roleInstanceName;
	private Boolean logDeploymentIdWithMetrics = false;
	private Boolean createMetricsTables = false;
	private Boolean useSas = false;
	private String storageAccountName;
	private String tableName;
	private String partitionKeyTimeFormat;
	private String storageAccountKey;
	private String storageAccountSas;
	
	@Override
	public void init(SubsetConfiguration conf) {
		logger.info("Entering init");
		
		storageAccountName = conf.getString(STORAGE_ACCOUNT_KEY);
		deploymentId = conf.getString(AZURE_DEPLOYMENTID_KEY);
		roleName = conf.getString(AZURE_ROLENAME_KEY);
		roleInstanceName = conf.getString(AZURE_ROLEINSTANCENAME_KEY);
		tableName = conf.getString(AZURE_TABLENAME_KEY);
		partitionKeyTimeFormat = conf.getString(STORAGE_PARTITON_KEY_TIMEFORMAT_KEY, "yyyyMMddHHmm");
		storageAccountKey = conf.getString(STORAGE_ACCESSKEY_KEY);
		storageAccountSas = conf.getString(STORAGE_SAS_KEY);
		
		// If we have the storage key, then use that. 
		// We can now also create metrics tables if they don't exist
		if (storageAccountKey != null && !StringUtils.isEmpty(storageAccountKey)) {
			createMetricsTables = true;
			logger.info("Using full storageAccessKey. Will create tables if missing");
		} else if (storageAccountSas == null || StringUtils.isEmpty(storageAccountSas)) {
			logger.error("accesskey or sas missing in the metrics2 properties file");
		} else {
			// Use shared access signatures to upload data
			useSas = true;
			createMetricsTables = false;
			
			logger.info("Using SAS. Will not create tables");
		}
		
		logDeploymentIdWithMetrics = (deploymentId != null && !deploymentId.isEmpty() && 
										roleName != null && !roleName.isEmpty() && 
										roleInstanceName != null && !roleInstanceName.isEmpty());
 	}

	@Override
	public void putMetrics(MetricsRecord record) {
		SimpleDateFormat formatter = new SimpleDateFormat(partitionKeyTimeFormat);  
		String partitionKeySuffix = formatter.format( new java.util.Date() ); 
		
		if (StringUtils.isEmpty(tableName)) {
			// Note: Azure Tables can only contain alpha-numeric values
			// assume table name is context + record if it is not specified in the property
			tableName = record.context().toUpperCase() + record.name();
		}
		
		String partitionKey;
		
		HashMap<String, String> metrics2KeyValuePairs = new HashMap<String, String>();
		
		metrics2KeyValuePairs.put("MetricTimestamp", String.valueOf(record.timestamp()));
		metrics2KeyValuePairs.put("Context", record.context());
		metrics2KeyValuePairs.put("Name", record.name());
		metrics2KeyValuePairs.put("IPAddress", getLocalNodeIPAddress());
		
		if (logDeploymentIdWithMetrics) {
			metrics2KeyValuePairs.put("DeploymentId", deploymentId);
			metrics2KeyValuePairs.put("Role", roleName);
			metrics2KeyValuePairs.put("RoleInstance", roleInstanceName);
			
			partitionKey = deploymentId + "-" + roleName + "-" + partitionKeySuffix;
		}
		else {
			partitionKey = getLocalNodeName();
		}
		
		for (MetricsTag tag : record.tags()) {
			metrics2KeyValuePairs.put(tag.name(), String.valueOf(tag.value()));
		}
		
		for (AbstractMetric metric : record.metrics()) {
			metrics2KeyValuePairs.put(metric.name(), metric.value().toString());
		}
		
		// Using a guid as the rowkey to guarantee uniqueness
		AzureTableMetrics2Entity metrics2Entity = 
				new AzureTableMetrics2Entity(partitionKey, UUID.randomUUID().toString());
		
		metrics2Entity.setMetrics2KeyValuePairs(metrics2KeyValuePairs);
		
		CloudTableClient tableClient;
		
		try {
			tableClient = getTableClient(tableName);
		} catch (StorageException storageException) {
			logger.error(String.format("getTableClient failed. Details: %s, %s", 
					storageException.getMessage(), storageException));
			
			return;
		} catch (URISyntaxException syntaxException) {
			logger.error(String.format("getTableClient failed. Details: %s, %s", 
					syntaxException.getMessage(), syntaxException));
			
			return;
		}
		
		TableOperation insertMetricOperation = TableOperation.insert(metrics2Entity, true);
		
		try {
			TableRequestOptions requestOptions = new TableRequestOptions();
			requestOptions.setTablePayloadFormat(TablePayloadFormat.AtomPub);
			tableClient.execute(tableName, insertMetricOperation, requestOptions, null);
		} catch (StorageException storageException) {
			logger.error(String.format("tableClient.execute failed. Details: %s, %s", 
					storageException.getMessage(), storageException));
			
			return;
		}
	}

	@Override
	public void flush() {
		// Nothing to do here
	}
	
	/*
	 * Create a windows azure table if one does not already exist.
	 */
	private CloudTableClient getTableClient(String tableName)
			throws StorageException, URISyntaxException {
		if (existingTables.containsKey(tableName)) {
			return existingTables.get(tableName);
		}
		
		// Create the table client.
		CloudTableClient tableClient;
		
		if (!useSas) {
		  StorageCredentials credentials = new StorageCredentialsAccountAndKey(storageAccountName, storageAccountKey);
		  CloudStorageAccount storageAccount;
		  storageAccount = new CloudStorageAccount(credentials);
			tableClient = storageAccount.createCloudTableClient();
			logger.debug(String.format("tableClient via Fullkey. Endpoint = %s. Table = %s",tableClient.getStorageUri(), tableName));
		} 
		else 
		{
			// If we use SAS, then we will have to create the CloudTableClient object
			// manually using the table endpoint baseUri. 
			URI tableBaseUri = new URI(
					String.format("https://%s.table.core.windows.net",  
					storageAccountName));
			
			StorageCredentials sasCredentials = new StorageCredentialsSharedAccessSignature(storageAccountSas);
			tableClient = new CloudTableClient(tableBaseUri, sasCredentials);
			
			logger.debug(String.format("tableClient via SASkey. Endpoint = %s. Table = %s",tableBaseUri, tableName));
		}
		
		CloudTable table = tableClient.getTableReference(tableName);
		
		if (createMetricsTables) {
			// Create the table if it doesn't exist.
			boolean created = table.createIfNotExists();
			
			if (created) {
				logger.info(String.format("Created table '%s'", tableName));
			} else {
				logger.info(String.format("Table '%s' already exists", tableName));
			}
		}
		
		existingTables.put(tableName, tableClient);
		
		return tableClient;
	}

	private String getLocalNodeIPAddress() {
		String nodeIPAddress; 
	   
		try {
	        nodeIPAddress = InetAddress.getLocalHost().getHostAddress();
		} catch (Exception e) {
			nodeIPAddress = "Unknown";
		}
	    
	    return nodeIPAddress;
	}
	
	public String getLocalNodeName() {
		String nodeName;
		  try {
		    nodeName = InetAddress.getLocalHost().getCanonicalHostName();
		  } catch (Exception e) {
			  nodeName = "Unknown";
		  }
		  return nodeName;
	}
	
	/*
	 * Table store entity for the metrics2 data.
	 */
	private class AzureTableMetrics2Entity extends TableServiceEntity {
		private HashMap<String, String> metrics2KeyValuePairs;
		
		public AzureTableMetrics2Entity(String partitionKey, String rowKey) {
	        this.partitionKey = partitionKey;
	        this.rowKey = rowKey;
	    }
		
		public void setMetrics2KeyValuePairs(HashMap<String, String> metrics2KeyValuePairs) {
			this.metrics2KeyValuePairs = metrics2KeyValuePairs;
		}
		
		@Override    
		public HashMap<String, EntityProperty> writeEntity(final OperationContext opContext) {   
			final HashMap<String, EntityProperty> retVal = new HashMap<String, EntityProperty>();
			
			if (metrics2KeyValuePairs != null) {
				for (Entry<String, String> keyValuePair : metrics2KeyValuePairs.entrySet()) {
					
					String key = keyValuePair.getKey();
					String newkey = sanitizeKey(key);
					retVal.put(newkey, new EntityProperty(keyValuePair.getValue()));
				}
			}
			
			return retVal;
		}
		
		private String sanitizeKey(String originalKey) {
			if (originalKey.contains(" ")) {
				logger.debug("spaces found in metric key " + originalKey + ". Removing them.");
				return originalKey.replaceAll(" ", "");
			}
			return originalKey;
		}
	}
}
