### Licensed to the Apache Software Foundation (ASF) under one or more
### contributor license agreements.  See the NOTICE file distributed with
### this work for additional information regarding copyright ownership.
### The ASF licenses this file to You under the Apache License, Version 2.0
### (the "License"); you may not use this file except in compliance with
### the License.  You may obtain a copy of the License at
###
###     http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.

###
### Install script that can be used to install Hadoop as a Single-Node cluster.
### To invoke the scipt, run the following command from PowerShell:
###   install.ps1 -username <username> -password <password>
###
### where:
###   <username> and <password> represent account credentials used to run
###   Hadoop services as Windows services.
###
### Account must have the following two privileges, otherwise
### installation/runtime will fail.
###   SeServiceLogonRight
###   SeCreateSymbolicLinkPrivilege
###
### By default, Hadoop is installed to "C:\Hadoop". To change this set
### HADOOP_NODE_INSTALL_ROOT environment variable to a location were
### you'd like Hadoop installed.
###
### Script pre-requisites:
###   JAVA_HOME must be set to point to a valid Java location.
###
### To uninstall previously installed Single-Node cluster run:
###   uninstall.ps1
###
### NOTE: Notice @version@ strings throughout the file. First compile
### winpkg with "ant winpkg", that will replace the version string.
### To install, use:
###   build\hadoop-@version@.winpkg.zip#scripts\install.ps1
###

param(
    [String]
    [Parameter( ParameterSetName='UsernamePassword', Position=0, Mandatory=$true )]
    [Parameter( ParameterSetName='UsernamePasswordBase64', Position=0, Mandatory=$true )]
    $username,
    [String]
    [Parameter( ParameterSetName='UsernamePassword', Position=1, Mandatory=$true )]
    $password,
    [String]
    [Parameter( ParameterSetName='UsernamePasswordBase64', Position=1, Mandatory=$true )]
    $passwordBase64,
    [Parameter( ParameterSetName='CredentialFilePath', Mandatory=$true )]
    $credentialFilePath,
    [String]
    $hdfsRoles = "namenode datanode secondarynamenode",
    [String]
    $yarnRoles = "nodemanager resourcemanager historyserver",
    [String]
    $mapredRoles = "jobhistoryserver",
    [Switch]
    $skipNamenodeFormat = $false
    )

function Main( $scriptDir )
{
    if ( -not (Test-Path ENV:WINPKG_LOG))
    {
        $ENV:WINPKG_LOG = "hadoop.core.winpkg.log"
    }

    $HadoopCoreVersion = "@version@"
    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "hadoop-$hadoopCoreVersion.winpkg.log"
    Test-JavaHome

    ### $hadoopInstallDir: the directory that contains the appliation, after unzipping
    $hadoopInstallDir = Join-Path "$ENV:HADOOP_NODE_INSTALL_ROOT" "hadoop-$hadoopCoreVersion"
    $hadoopInstallToBin = Join-Path "$ENV:HADOOP_NODE_INSTALL_ROOT" "hadoop-$hadoopCoreVersion\bin"
    $nodeInstallRoot = "$ENV:HADOOP_NODE_INSTALL_ROOT"

    Write-Log "nodeInstallRoot: $nodeInstallRoot"
    Write-Log "hadoopInstallToBin: $hadoopInstallToBin"

    ###
    ### Create the Credential object from the given username and password or the provided credentials file
    ###
    $serviceCredential = Get-HadoopUserCredentials -credentialsHash @{"username" = $username; "password" = $password; `
        "passwordBase64" = $passwordBase64; "credentialFilePath" = $credentialFilePath}
    $username = $serviceCredential.UserName
    Write-Log "Username: $username"
    Write-Log "CredentialFilePath: $credentialFilePath"

    ###
    ### Initialize root directory used for Core, HDFS and MapRed local folders
    ###
    if( -not (Test-Path ENV:HDFS_DATA_DIR))
    {
        $ENV:HDFS_DATA_DIR = Join-Path "$ENV:HADOOP_NODE_INSTALL_ROOT" "HDFS"
    }

    ###
    ### Stop all services before proceeding with the install step, otherwise
    ### files will be in-use and installation can fail
    ###
    Write-Log "Stopping MapRed services if already running before proceeding with install"
    StopService "mapreduce" "jobhistoryserver"

    Write-Log "Stoppping Yarn services if already running before proceeding with Install"
    StopService "yarn" "resourcemanager nodemanager historyserver"

    Write-Log "Stopping HDFS services if already running before proceeding with install"
    StopService "hdfs" "namenode datanode secondarynamenode"

    ###Map Hadoop Roles to Components

    $hadooproles="NAMENODE SECONDARYNAMENODE RESOURCEMANAGER SLAVE NN_HA_STANDBY_NAMENODE NN_HA_JOURNALNODE RM_HA_STANDBY_RESOURCEMANAGER"
    $hdfsRoles=""
    $mapredRoles=""
    $yarnRoles=""
    foreach ( $role in $hadooproles.split(" ") ) {
        if ((iex `$'ENV:IS_'$role) -eq ("yes"))
        {
            if ($role -eq "RESOURCEMANAGER" ) {
                $mapredRoles = $mapredRoles+" "+"jobhistoryserver"
                $yarnRoles = $yarnRoles+" "+"resourcemanager"
                $yarnRoles = $yarnRoles+" "+"historyserver"
            }
            if ($role -eq "SLAVE" ) {
                $yarnRoles = $yarnRoles+" "+"nodemanager"
                $hdfsroles = $hdfsroles+" "+"datanode"
            }
            if (($role -eq "NAMENODE") -or ($role -eq "NN_HA_STANDBY_NAMENODE")) {
                $hdfsroles = $hdfsroles+" "+"namenode"
                if ($ENV:HA -ieq "yes") {
                    $hdfsroles = $hdfsroles+" "+"zkfc"
                }
            }
            if ($role -eq "NN_HA_JOURNALNODE" ) {
                $hdfsroles = $hdfsroles+" "+"journalnode"
            }
            if ($role -eq "SECONDARYNAMENODE" ) {
                $hdfsroles = $hdfsroles+" "+"secondarynamenode"
            }
            if (($role -eq "RM_HA_STANDBY_RESOURCEMANAGER" ) -and ($ENV:HA -ieq "yes")) {
                $hdfsroles = $hdfsroles+" "+"zkfc"
                $yarnRoles = $yarnRoles+" "+"resourcemanager"
                $yarnRoles = $yarnRoles+" "+"historyserver"
            }
        }
    }
    $hdfsRoles = $hdfsRoles.Trim()
    $mapredRoles = $mapredRoles.Trim()
    $yarnRoles = $yarnRoles.Trim()
    # We need atleast a ' ' for powershell to acceept an empty string
    if ( -not $hdfsRoles)
    {
        $hdfsRoles = " "
    }
    if ( -not $mapredRoles)
    {
        $mapredRoles = " "
    }

    if ( -not $yarnRoles)
    {
        $yarnRoles = " "
    }

    ###
    ### Install and Configure Core
    ###
    # strip out domain/machinename if it exists. will not work with domain users.
    $shortUsername = $username
    if($username.IndexOf('\') -ge 0)
    {
        $shortUsername = $username.SubString($username.IndexOf('\') + 1)
    }

    Install "Core" $NodeInstallRoot $serviceCredential ""
    ### Configure Hadoop Core Configurations
    Write-Log "Configuring the Hadoop Core configurations"
    ### Prepare the proxyuser hosts
    $proxyHostsList = @()
    $proxyHostsList += GetIPAddress $ENV:HIVE_SERVER_HOST
    $proxyHostsList += GetIPAddress $ENV:OOZIE_SERVER_HOST
    $proxyHostsList += GetIPAddress $ENV:WEBHCAT_HOST
    $proxyHostsList = $proxyHostsList | Select -Uniq
    $proxyHosts = ($proxyHostsList -Join ',')

    $coreConfigs = @{
        "hadoop.proxyuser.$shortUsername.hosts" = "$proxyHosts";
        "hadoop.tmp.dir" = Join-Path $ENV:HADOOP_NODE_INSTALL_ROOT "temp\hadoop"
        "hadoop.proxyuser.$shortUsername.groups" = "HadoopUsers"}

    if ((Test-Path ENV:ENABLE_LZO) -and ($ENV:ENABLE_LZO -ieq "yes")){
        $coreConfigs["io.compression.codec.lzo.class"] = "com.hadoop.compression.lzo.LzoCodec"
    }

    if ($ENV:DEFAULT_FS -eq "ASV"){
        Write-Log "ASV usage detected. Configuring HDP to use ASV as default filesystem"

        Write-Log "Stripping double quotes from Containername, storageaccount storageaccountkey"
        if (Test-Path ENV:ASV_CONTAINERNAME) {
           ${ENV:ASV_CONTAINERNAME} = ${ENV:ASV_CONTAINERNAME}.Replace("`"","")
        }
        if (Test-Path ENV:ASV_STORAGEACCOUNT) {
            ${ENV:ASV_STORAGEACCOUNT} = ${ENV:ASV_STORAGEACCOUNT}.Replace("`"","")
        }
        if (Test-Path ENV:ASV_STORAGEACCOUNT_KEY) {
            ${ENV:ASV_STORAGEACCOUNT_KEY} = ${ENV:ASV_STORAGEACCOUNT_KEY}.Replace("`"","")
        }

        $coreConfigs["fs.defaultFS"] = "asv://${ENV:ASV_CONTAINERNAME}@${ENV:ASV_STORAGEACCOUNT}"
        $coreConfigs["fs.azure.account.key.${ENV:ASV_STORAGEACCOUNT}"]="${ENV:ASV_STORAGEACCOUNT_KEY}"
        $coreConfigs["dfs.namenode.rpcaddress"] = "hdfs://${ENV:NAMENODE_HOST}:8020"
    }
    elseif (($ENV:DEFAULT_FS -eq $null) -or ($ENV:DEFAULT_FS -ieq "hdfs")){
           Write-Log "HDFS usage detected. Configuring HDP to use HDFS as default filesystem"
           $coreConfigs["fs.defaultFS"] = "hdfs://${ENV:NAMENODE_HOST}:8020"

       if ($ENV:HA -ieq "yes") {
          $coreConfigs["fs.defaultFS"] = "hdfs://${ENV:NN_HA_CLUSTER_NAME}"
          $zookeeperNodes = $zookeeperNodes = $env:ZOOKEEPER_HOSTS.Replace(",",":2181,")
          $zookeeperNodes = $zookeeperNodes + ":2181"
          $coreConfigs["ha.zookeeper.quorum"] = $zookeeperNodes
       }

           }
    else{
           throw "Incorrect filesystem settings in clusterproperties file. Please check these"
    }


    Configure "Core" $NodeInstallRoot $serviceCredential $coreConfigs

    if ((Test-Path ENV:ENABLE_LZO) -and ($ENV:ENABLE_LZO -ieq "yes")){
            $coresiteFile = Join-Path $ENV:HADOOP_HOME "etc/hadoop/core-site.xml"
            $sourceXml = New-Object System.Xml.XmlDocument
            $sourceXml.PreserveWhitespace = $true
            $sourceXml.Load($coresiteFile)
            $sourcexml.ReleasePath

            $lzoKey = "io.compression.codecs"
            $lzoValue = "com.hadoop.compression.lzo.LzoCodec"

            foreach($property in $sourceXml.SelectNodes('/configuration/property'))
            {
                [string] $name = $property.name
                [string] $value = $property.value

                if ($name -notlike $lzoKey) {
                    continue;
                }

                if (! $value) {
                    $value = $lzoValue
                }

                $codecs = $value.Split(',')
                if ($codecs -notcontains $lzoValue){
                    $value += "," + $lzoValue
                }

                $finalValues = @{$lzoKey = $value}
                UpdateXmlConfig $coresiteFile  $finalValues
            }
    }

    ###
    ### Install and Configure HDFS
    ###

    Install "Hdfs" $NodeInstallRoot $serviceCredential $hdfsRoles

    $replicationfactor = if ($ENV:SLAVE_HOSTS.Split(",").Length -gt 2) { "3" } else { "1" }

    $NMAndMRLocalDir = Join-Path (${ENV:HDP_DATA_DIR}.Split(",") | Select -first 1).Trim() "$shortUsername/local"
    $NMAndMRLogDir = Join-Path (${ENV:HDP_DATA_DIR}.Split(",") | Select -first 1).Trim() "$shortUsername/logs"

    $hdfsConfigs = @{
        "dfs.namenode.checkpoint.dir" = ConvertToFileURI(Get-AppendedPath $ENV:HDFS_DATA_DIR "snn");
        "dfs.namenode.checkpoint.edits.dir" = ConvertToFileURI(Get-AppendedPath $ENV:HDFS_DATA_DIR "snn");
        "dfs.namenode.name.dir" = ConvertToFileURI(Get-AppendedPath $ENV:HDFS_DATA_DIR "nn");
        "dfs.datanode.data.dir" = ConvertToFileURI(Get-AppendedPath $ENV:HDFS_DATA_DIR "dn");
        "dfs.replication" = "$replicationfactor";
        "dfs.hosts" = "${hadoopInstallDir}\etc\hadoop\dfs.include";
        "dfs.hosts.exclude" = "${hadoopInstallDir}\etc\hadoop\dfs.exclude";
        "dfs.support.append" = "true"}
      if ($ENV:HA -ieq "yes") {
          $hdfsConfigs["dfs.nameservices"] = "${ENV:NN_HA_CLUSTER_NAME}"
          $hdfsConfigs["dfs.ha.namenodes.${ENV:NN_HA_CLUSTER_NAME}"] = "nn1,nn2"
          $hdfsConfigs["dfs.namenode.rpc-address.${ENV:NN_HA_CLUSTER_NAME}.nn1"] = "${ENV:NAMENODE_HOST}:8020"
          $hdfsConfigs["dfs.namenode.rpc-address.${ENV:NN_HA_CLUSTER_NAME}.nn2"] = "${ENV:NN_HA_STANDBY_NAMENODE_HOST}:8020"
          $hdfsConfigs["dfs.namenode.http-address.${ENV:NN_HA_CLUSTER_NAME}.nn1"] = "${ENV:NAMENODE_HOST}:50070"
          $hdfsConfigs["dfs.namenode.http-address.${ENV:NN_HA_CLUSTER_NAME}.nn2"] = "${ENV:NN_HA_STANDBY_NAMENODE_HOST}:50070"
          $hdfsConfigs["dfs.namenode.https-address.${ENV:NN_HA_CLUSTER_NAME}.nn1"] = "${ENV:NAMENODE_HOST}:50701"
          $hdfsConfigs["dfs.namenode.https-address.${ENV:NN_HA_CLUSTER_NAME}.nn2"] = "${ENV:NN_HA_STANDBY_NAMENODE_HOST}:50701"
          $hdfsConfigs["dfs.client.failover.proxy.provider.${ENV:NN_HA_CLUSTER_NAME}"] = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
          $hdfsConfigs["dfs.journalnode.edits.dir"] = "${ENV:NN_HA_JOURNALNODE_EDITS_DIR}"
          $hdfsConfigs["dfs.ha.automatic-failover.enabled"] = "true"
          $hdfsConfigs["dfs.ha.fencing.methods"] = "shell(exit 0)"

          $journalNodes = ($ENV:NN_HA_JOURNALNODE_HOSTS.Split(",") | foreach {$_.Trim() + ":8485"})
          $journalNodes = $journalNodes = $env:NN_HA_JOURNALNODE_HOSTS.Replace(",",":8485;")
          $journalNodes = $journalNodes + ":8485"

          $hdfsConfigs["dfs.namenode.shared.edits.dir"] = "qjournal://${journalNodes}/${ENV:NN_HA_CLUSTER_NAME}"

      }
      else {
          $hdfsConfigs["dfs.namenode.http-address"] = "${ENV:NAMENODE_HOST}:50070"
          $hdfsConfigs["dfs.namenode.https-address"] = "${ENV:NAMENODE_HOST}:50701"
          $hdfsConfigs["dfs.namenode.secondary.http-address"] = "${ENV:SECONDARY_NAMENODE_HOST}:50090"
      }

    Configure "Hdfs" $NodeInstallRoot $serviceCredential $hdfsConfigs

    #Adding Slaves information to the slaves file
    $slavesFile = Join-Path $hadoopInstallDir  "etc\hadoop\slaves"
    $slaveHosts = ($ENV:SLAVE_HOSTS.Split(",") | foreach { $_.Trim() })
    Write-Log "Adding slaves list $slaveHosts to $slavesFile"
    if(Test-Path -Path $slavesFile){
        Remove-item -Force $slavesFile
    }
    new-item -Path $slavesFile -Force -itemtype file
    foreach ($shost in $slaveHosts){
        Add-Content -Force -Path $slavesFile -Value $shost
    }

    $throwerror = ( (Test-Path ENV:DESTROY_DATA) -and ($ENV:DESTROY_DATA -eq "undefined") )
    $destroyData = $false
    if ( (Test-Path ENV:DESTROY_DATA) -and ($ENV:DESTROY_DATA -eq "yes") ){
        $destroyData = $true
    }

    $skipNamenodeFormat = (CheckDataDirectories $NodeInstallRoot $destroyData $throwerror)

    if ( ($skipNamenodeFormat -ne $true) -and ($ENV:IS_NAMENODE -eq "yes") )
    {
        ###
        ### Format the namenode
        ###
        FormatNamenode $false
    }
    else
    {
        Write-Log "Skipping Namenode format"
    }

    ###
    ### Install and Configure Yarn
    ###
    Install "Yarn" $NodeInstallRoot $serviceCredential $yarnRoles

    $yarnConfigs =@{
        "yarn.resourcemanager.hostname" = "${ENV:RESOURCEMANAGER_HOST}".ToLower();
        "yarn.resourcemanager.webapp.address" = "${ENV:RESOURCEMANAGER_HOST}:8088".ToLower();
        "yarn.resourcemanager.webapp.https.address" = "${ENV:RESOURCEMANAGER_HOST}:8088".ToLower();
        "yarn.log.server.url" = "http://${ENV:RESOURCEMANAGER_HOST}:19888/jobhistory/logs".ToLower();
        "yarn.nodemanager.log-dirs" = "$NMAndMRLogDir" ;
        "yarn.nodemanager.local-dirs" = "$NMAndMRLocalDir" ;
        "yarn.timeline-service.hostname" = "${ENV:RESOURCEMANAGER_HOST}".ToLower() }
    if ($ENV:HA -ieq "yes") {
        $yarnConfigs += @{
        "yarn.resourcemanager.ha.enabled" = "true";
        "yarn.resourcemanager.ha.rm-ids" = "rm1,rm2";
        "yarn.resourcemanager.recovery.enabled" = "true";
        "yarn.resourcemanager.store.class" = "org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore";
        "yarn.client.failover-proxy-provider" = "org.apache.hadoop.yarn.client.ConfiguredRMFailoverProxyProvider";
        "yarn.resourcemanager.ha.automatic-failover.zk-base-path" = "/yarn-leader-election";
        "yarn.resourcemanager.cluster-id" = "$ENV:RM_HA_CLUSTER_NAME".ToLower();
        "yarn.nodemanager.localizer.address" = "$ENV:COMPUTERNAME".ToLower();
        "yarn.web-proxy.address" = "$ENV:COMPUTERNAME".ToLower();
        "yarn.resourcemanager.ha.automatic-failover.enabled" = "true";
        "yarn.resourcemanager.ha.automatic-failover.embedded" = "true"
        "yarn.resourcemanager.am.max-attempts" = "20";
        "yarn.resourcemanager.hostname.rm2" = "$ENV:RM_HA_STANDBY_RESOURCEMANAGER_HOST".ToLower();
        "yarn.resourcemanager.hostname.rm1" = "$ENV:RESOURCEMANAGER_HOST".ToLower()
        }

        $zookeeperNodes = $env:ZOOKEEPER_HOSTS.Replace(",",":2181,")
        $zookeeperNodes = $zookeeperNodes + ":2181"
        $yarnConfigs["yarn.resourcemanager.zk-address"] = $zookeeperNodes
    }
    Configure "Yarn" $NodeInstallRoot $serviceCredential $yarnConfigs
    ###
    ### Install and Configure MapRed
    ###
    Write-Log "Calling Install "MapReduce" nodeinstallRoot=$NodeInstallRoot serviceCredential=$serviceCredential mapredRoles=$mapredRoles="
    Install "MapReduce" $NodeInstallRoot $serviceCredential $mapredRoles

    $mapredConfigs = @{
        "mapred.local.dir" = Get-AppendedPath $ENV:HDFS_DATA_DIR "mapred\local";
        "mapred.job.tracker.history.completed.location" = "/mapred/history/done"
        "mapred.child.tmp" = Join-Path $ENV:HADOOP_NODE_INSTALL_ROOT "temp\hadoop"
        "mapreduce.jobhistory.address" = "${ENV:RESOURCEMANAGER_HOST}:10020";
        "mapreduce.jobhistory.webapp.address" = "${ENV:RESOURCEMANAGER_HOST}:19888";
        "mapreduce.jobhistory.webapp.https.address" = "${ENV:RESOURCEMANAGER_HOST}:19888";
        "mapreduce.reduce.java.opts" = "-Xmx756m";
        "mapreduce.map.java.opts" = "-Xmx756m";
        "mapreduce.cluster.local.dir" = "$NMAndMRLocalDir" }

    if ((Test-Path ENV:HA) -and ($ENV:HA -ieq "yes")) {
        $mapredConfigs["mapreduce.am.max-attempts"] = "20"
    }

    Configure "mapreduce" $NodeInstallRoot $serviceCredential $mapredConfigs
    Write-Log "Install of Hadoop Core, HDFS, MapRed completed successfully"
}

try
{
    $scriptDir = Resolve-Path (Split-Path $MyInvocation.MyCommand.Path)
    $utilsModule = Import-Module -Name "$scriptDir\..\resources\Winpkg.Utils.psm1" -ArgumentList ("HADOOP") -PassThru
    $apiModule = Import-Module -Name "$scriptDir\InstallApi.psm1" -PassThru
    Main $scriptDir
}
catch
{
    Write-Log $_.Exception.Message "Failure" $_
    exit 1
}
finally
{
    if( $apiModule -ne $null )
    {
        Remove-Module $apiModule
    }
    if( $utilsModule -ne $null )
    {
        Remove-Module $utilsModule
    }
}
