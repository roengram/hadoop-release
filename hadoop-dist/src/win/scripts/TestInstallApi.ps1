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
### Global test variables
###

$ScriptDir = Resolve-Path (Split-Path $MyInvocation.MyCommand.Path)

### Templates
$Username = "@test.wininstaller.username@"
$Password = "@test.wininstaller.password@"
$HadoopCoreVersion = "@version@"

###
### Uncomment and update below section for testing from sources
###
#$Username = "hadoop"
#$Password = "TestUser123"
#$ENV:HADOOP_NODE_INSTALL_ROOT = "C:\Hadoop\test"
#$ENV:WINPKG_LOG = "winpkg_core_install.log"
#$HadoopCoreVersion = "2.1.2-SNAPSHOT"
###
### End of testing section
###

$NodeInstallRoot = "$ENV:HADOOP_NODE_INSTALL_ROOT"
$SecurePassword = ConvertTo-SecureString $Password -AsPlainText -Force
$ServiceCredential = New-Object System.Management.Automation.PSCredential ("$ENV:COMPUTERNAME\$Username", $SecurePassword)
if ($ServiceCredential -eq $null)
{
    throw "Failed to create PSCredential object, please check your username/password parameters"
}

function Assert(
    [String]
    [parameter( Position=0 )]
    $message,
    [bool]
    [parameter( Position=1 )]
    $condition = $false
    )
{
    if ( -not $condition )
    {
        throw $message
    }
}

function CoreInstallTestBasic()
{
    Install "Core" $NodeInstallRoot $ServiceCredential ""
    Assert "ENV:HADOOP_HOME must be set" (Test-Path ENV:HADOOP_HOME)
    Assert "ENV:HADOOP_HOME folder must exist" (Test-Path $ENV:HADOOP_HOME)
    Uninstall "Core" $NodeInstallRoot
}

function CoreInstallTestIdempotent()
{
    Install "Core" $NodeInstallRoot $ServiceCredential ""
    Install "Core" $NodeInstallRoot $ServiceCredential ""
    Uninstall "Core" $NodeInstallRoot
}

function HdfsInstallTestBasic()
{
    Install "core" $NodeInstallRoot $ServiceCredential ""
    Install "hdfs" $NodeInstallRoot $ServiceCredential "namenode datanode"
    Uninstall "hdfs" $NodeInstallRoot
    Uninstall "core" $NodeInstallRoot
}

function HdfsInstallTestNoCore()
{
    $testFailed = $true

    try
    {
        Install "Hdfs" $NodeInstallRoot $ServiceCredential "namenode"
    }
    catch [Exception]
    {
        $testFailed = $false
    } finally
    {
        ### Cleanup
        Uninstall "Hdfs" $NodeInstallRoot
    }

    if ( $testFailed )
    {
        throw "InstallHdfs should fail if InstallCore was not called before"
    }
}

function HdfsInstallTestRoleNoSupported()
{
    $testFailed = $true

    try
    {
        Install "Core" $NodeInstallRoot $ServiceCredential ""
        Install "Hdfs" $NodeInstallRoot $ServiceCredential "namenode UNKNOWN"
    }
    catch [Exception]
    {
        $testFailed = $false
    } finally
    {
        ### Cleanup
        Uninstall "Hdfs" $NodeInstallRoot
    }

    if ( $testFailed )
    {
        throw "InstallHdfs should fail if the given role is not supported"
    }
}

function HdfsInstallTestInvalidUser()
{
    $testFailed = $true

    try
    {
        Install "Core" $NodeInstallRoot $ServiceCredential ""

        $invalidCredential = New-Object System.Management.Automation.PSCredential ("$ENV:COMPUTERNAME\INVALIDUSER", $SecurePassword)

        Install "Hdfs" $NodeInstallRoot $invalidCredential "namenode"
    }
    catch [Exception]
    {
        $testFailed = $false
    } finally
    {
        ### Cleanup
        Uninstall "Core" $NodeInstallRoot
        Uninstall "Hdfs" $NodeInstallRoot
    }

    if ( $testFailed )
    {
        throw "InstallHdfs should fail if username is invalid"
    }
}

function MapRedInstallTestBasic()
{
    Install "Core" $NodeInstallRoot $ServiceCredential ""
    Install "MapReduce" $NodeInstallRoot $ServiceCredential "historyserver"
    Uninstall "MapReduce" $NodeInstallRoot
    Uninstall "Core" $NodeInstallRoot
}

function MapRedInstallTestNoCore()
{
    $testFailed = $true

    try
    {
        Install "MapReduce" $NodeInstallRoot $ServiceCredential "historyserver"
    }
    catch [Exception]
    {
        $testFailed = $false
    } finally
    {
        ### Cleanup
        Uninstall "MapReduce" $NodeInstallRoot
    }

    if ( $testFailed )
    {
        throw "InstallMapRed should fail if InstallCore was not called before"
    }
}

function MapRedInstallTestInvalidUser()
{
    $testFailed = $true

    try
    {
        Install "Core" $NodeInstallRoot $ServiceCredential ""

        $invalidCredential = New-Object System.Management.Automation.PSCredential ("$ENV:COMPUTERNAME\INVALIDUSER", $SecurePassword)

        Install "MapReduce" $NodeInstallRoot $invalidCredential "historyserver"
    }
    catch [Exception]
    {
        $testFailed = $false
    } finally
    {
        ### Cleanup
        Uninstall "Core" $NodeInstallRoot
        Uninstall "MapReduce" $NodeInstallRoot
    }

    if ( $testFailed )
    {
        throw "InstallMapRed should fail if username is invalid"
    }
}

function MapRedInstallTestRoleNoSupported()
{
    $testFailed = $true

    try
    {
        Install "Core" $NodeInstallRoot $ServiceCredential ""
        Install "MapReduce" $NodeInstallRoot $ServiceCredential "historyserver INVALIDROLE"
    }
    catch [Exception]
    {
        $testFailed = $false
    } finally
    {
        ### Cleanup
        Uninstall "MapReduce" $NodeInstallRoot
    }

    if ( $testFailed )
    {
        throw "InstallMapRed should fail if the given role is not supported"
    }
}

function InstallAllTestBasic()
{
    Install "Core" $NodeInstallRoot $ServiceCredential ""
    Install "Hdfs" $NodeInstallRoot $ServiceCredential "namenode datanode secondarynamenode"
    Install "MapReduce" $NodeInstallRoot $ServiceCredential "historyserver"

    # Cleanup
    Uninstall "MapReduce" $NodeInstallRoot
    Uninstall "Hdfs" $NodeInstallRoot
    Uninstall "Core" $NodeInstallRoot
}

function InstallAllTestIdempotent()
{
    Install "Core" $NodeInstallRoot $ServiceCredential ""
    Install "Hdfs" $NodeInstallRoot $ServiceCredential "namenode datanode secondarynamenode"
    Install "MapReduce" $NodeInstallRoot $ServiceCredential "historyserver"

    # Install all services again
    Install "Core" $NodeInstallRoot $ServiceCredential ""
    Install "Hdfs" $NodeInstallRoot $ServiceCredential "namenode datanode secondarynamenode"
    Install "MapReduce" $NodeInstallRoot $ServiceCredential "historyserver"

    # Cleanup
    Uninstall "MapReduce" $NodeInstallRoot
    Uninstall "Hdfs" $NodeInstallRoot
    Uninstall "Core" $NodeInstallRoot
}

function ValidateXmlConfigValue($xmlFileName, $key, $expectedValue)
{
    $xml = [xml](gc $xmlFileName)
    $result = $xml.SelectNodes('/configuration/property') | ? { $_.name -eq $key }
    if ( ($result -eq $null) -or (-not ( $result.value -eq $expectedValue ) ) )
    {
        throw "TEST FAILED: Key/Value $key/$expectedValue not found in the configuration file"
    }
}

function TestUpdateXmlConfig()
{
    $xmlTemplate = "<?xml version=`"1.0`"?>`
    <configuration>`
    </configuration>"
    $testFile = Join-Path $ScriptDir "testFile.xml"
    write-output $xmlTemplate | out-file -encoding ascii $testFile

    ### Create empty configuration xml
    UpdateXmlConfig $testFile

    ### Add two properties to it
    UpdateXmlConfig $testFile @{"key1" = "value1";"key2" = "value2"}

    ### Verify that properties are present
    ValidateXmlConfigValue $testFile "key1" "value1"
    ValidateXmlConfigValue $testFile "key2" "value2"

    ### Update key1 property value and add key3 property
    UpdateXmlConfig $testFile @{"key1" = "value1Updated";"key3" = "value3"}

    ### Verify updated values
    ValidateXmlConfigValue $testFile "key1" "value1Updated"
    ValidateXmlConfigValue $testFile "key2" "value2"
    ValidateXmlConfigValue $testFile "key3" "value3"
}

function CoreConfigureTestBasic()
{
    Install "Core" $NodeInstallRoot $ServiceCredential ""
    Configure "Core" $NodeInstallRoot $ServiceCredential

    $coreSiteXml = Join-Path $NodeInstallRoot "hadoop-$HadoopCoreVersion\etc\hadoop\core-site.xml"

    ### Change Hadoop core configuration
    Configure "Core" $NodeInstallRoot $ServiceCredential @{
        "fs.defaultFs" = "asv://host:8000"}

    ### Verify that the update took place
    ValidateXmlConfigValue $coreSiteXml "fs.defaultFs" "asv://host:8000"

    Uninstall "Core" $NodeInstallRoot
}

function CoreConfigureWithFileTestBasic()
{
    Install "Core" $NodeInstallRoot $ServiceCredential ""

    $coreSiteXml = Join-Path $NodeInstallRoot "hadoop-$HadoopCoreVersion\etc\hadoop\core-site.xml"
    $coreSiteXmlTmp = Join-Path $NodeInstallRoot "hadoop-$HadoopCoreVersion\core-site.xml"

    Copy-Item $coreSiteXml (Join-Path $NodeInstallRoot "hadoop-$HadoopCoreVersion")
    UpdateXmlConfig $coreSiteXmlTmp @{"fs.defaultFs" = "asv://host:8000"}

    ### Configure Core with a new file
    ConfigureWithFile "Core" $NodeInstallRoot $ServiceCredential $coreSiteXmlTmp

    ### Verify that new config took place
    ValidateXmlConfigValue $coreSiteXml "fs.defaultFs" "asv://host:8000"

    Uninstall "Core" $NodeInstallRoot
}

function InstallAndConfigAllTestBasic()
{
    Install "core" $NodeInstallRoot $ServiceCredential ""
    Configure "core" $NodeInstallRoot $ServiceCredential @{
        "fs.defaultFs" = "asv://host:8000";}

    $hdfsSiteXml = Join-Path $NodeInstallRoot "hadoop-$HadoopCoreVersion\etc\hadoop\hdfs-site.xml"
    $mapRedSiteXml = Join-Path $NodeInstallRoot "hadoop-$HadoopCoreVersion\etc\hadoop\mapred-site.xml"

    Install "hdfs" $NodeInstallRoot $ServiceCredential "namenode datanode secondarynamenode"
    Configure "hdfs" $NodeInstallRoot $ServiceCredential @{
        "dfs.namenode.checkpoint.dir" = "$NodeInstallRoot\hdfs\2nn";
        "dfs.namenode.checkpoint.edits.dir" = "$NodeInstallRoot\hdfs\2nn"
        "dfs.namenode.name.dir" = "$NodeInstallRoot\hdfs\nn2";
        "dfs.datanode.data.dir" = "$NodeInstallRoot\hdfs\dn2";
        "dfs.webhdfs.enabled" = "false"}

    ### Verify that the update took place
    ValidateXmlConfigValue $hdfsSiteXml "dfs.namenode.checkpoint.dir" "$NodeInstallRoot\hdfs\2nn"
    ValidateXmlConfigValue $hdfsSiteXml "dfs.namenode.checkpoint.edits.dir" "$NodeInstallRoot\hdfs\2nn"
    ValidateXmlConfigValue $hdfsSiteXml "dfs.namenode.name.dir" "$NodeInstallRoot\hdfs\nn2"
    ValidateXmlConfigValue $hdfsSiteXml "dfs.datanode.data.dir" "$NodeInstallRoot\hdfs\dn2"
    ValidateXmlConfigValue $hdfsSiteXml "dfs.webhdfs.enabled" "false"

    Install "mapreduce" $NodeInstallRoot $ServiceCredential "historyserver"
    Configure "mapreduce" $NodeInstallRoot $ServiceCredential @{
        "mapred.job.tracker" = "host:port";
        "mapred.local.dir" = "$NodeInstallRoot\hdfs\mapred\local2"}

    ### Verify that the update took place
    ValidateXmlConfigValue $mapRedSiteXml "mapred.job.tracker" "host:port"
    ValidateXmlConfigValue $mapRedSiteXml "mapred.local.dir" "$NodeInstallRoot\hdfs\mapred\local2"

    # Cleanup
    Uninstall "mapreduce" $NodeInstallRoot
    Uninstall "hdfs" $NodeInstallRoot
    Uninstall "core" $NodeInstallRoot
}

function TestStartStopServiceRoleNoSupported()
{
    $testFailed = $true
    ### Test starting services with invalid roles
    try
    {
        StartService "hdfs" "namenode INVALIDROLE"
    }
    catch [Exception]
    {
        $testFailed = $false
    }

    if ( $testFailed )
    {
        throw "StartService should fail if the given role is not supported"
    }

    try
    {
        StartService "mapreduce" "historyserver INVALIDROLE"
    }
    catch [Exception]
    {
        $testFailed = $false
    }

    if ( $testFailed )
    {
        throw "StartService should fail if the given role is not supported"
    }

    ### Test stopping services with invalid roles
    try
    {
        StopService "hdfs" "namenode INVALIDROLE"
    }
    catch [Exception]
    {
        $testFailed = $false
    }

    if ( $testFailed )
    {
        throw "StartService should fail if the given role is not supported"
    }

    try
    {
        StopService "mapreduce" "historyserver INVALIDROLE"
    }
    catch [Exception]
    {
        $testFailed = $false
    }

    if ( $testFailed )
    {
        throw "StartService should fail if the given role is not supported"
    }
}

function TestProcessAliasConfigOptions()
{
    $result = ProcessAliasConfigOptions "core" @{
        "dfs.datanode.data.dir" = "$NodeInstallRoot\hdfs\nn2";
        "hdfs_namenode_host" = "machine1"}

    Assert "TestProcessAliasConfigOptions: hdfs_namenode_host not resolved correctly" ( $result["fs.defaultFs"] -eq "hdfs://machine1:8020" )
    Assert "TestProcessAliasConfigOptions: dfs.datanode.data.dir not resolved correctly" ( $result["dfs.datanode.data.dir"] -eq "$NodeInstallRoot\hdfs\nn2" )

    $result = ProcessAliasConfigOptions "hdfs" @{
        "hdfs_secondary_namenode_host" = "machine1";
        "hdfs_namenode_host" = "machine2"}
    Assert "TestProcessAliasConfigOptions: hdfs_secondary_namenode_host not resolved correctly" ( $result["dfs.namenode.secondary.http-address"] -eq "machine1:50090" )
    Assert "TestProcessAliasConfigOptions: hdfs_namenode_host not resolved correctly" ( $result["dfs.namenode.http-address"] -eq "machine2:50070" )
    Assert "TestProcessAliasConfigOptions: hdfs_namenode_host not resolved correctly" ( $result["dfs.namenode.https-address"] -eq "machine2:50470" )

    $result = ProcessAliasConfigOptions "mapreduce" @{
        "mapreduce_historyserver_host" = "machine1" }
    Assert "TestProcessAliasConfigOptions: mapreduce_historyserver_host not resolved correctly" ( $result["mapreduce.jobhistory.address"] -eq "machine1:10020" )
    Assert "TestProcessAliasConfigOptions: mapreduce_historyserver_host not resolved correctly" ( $result["mapreduce.jobhistory.webapp.address"] -eq "machine1:19888" )
}

function CoreConfigureWithAliasesTest()
{
    Install "Core" $NodeInstallRoot $ServiceCredential ""
    Configure "Core" $NodeInstallRoot $ServiceCredential

    $coreSiteXml = Join-Path $NodeInstallRoot "hadoop-$HadoopCoreVersion\etc\hadoop\core-site.xml"

    ### Change Hadoop core configuration
    Configure "Core" $NodeInstallRoot $ServiceCredential @{
        "hdfs_namenode_host" = "machine1"}

    ### Verify that the update took place
    ValidateXmlConfigValue $coreSiteXml "fs.defaultFs" "hdfs://machine1:8020"

    Uninstall "Core" $NodeInstallRoot
}

function CoreConfigureCapacitySchedulerTest()
{
    Install "core" $NodeInstallRoot $ServiceCredential ""

    $yarnSiteXml = Join-Path $NodeInstallRoot "hadoop-$HadoopCoreVersion\etc\hadoop\yarn-site.xml"
    $capSchedulerSiteXml = Join-Path $NodeInstallRoot "hadoop-$HadoopCoreVersion\etc\hadoop\capacity-scheduler.xml"

    Install "yarn" $NodeInstallRoot $ServiceCredential "resourcemanager"
    Configure "yarn" $NodeInstallRoot $ServiceCredential @{
        "yarn.resourcemanager.hostname" = "host:port";
        "yarn.nodemanager.local-dirs" = "$NodeInstallRoot\hdfs\nm\local2";
        "yarn.capacity-scheduler.prop" = "testcapacity";
        "yarn.capacity-scheduler.init-worker-threads" = "10"}

    ### Verify that the update took place
    ValidateXmlConfigValue $yarnSiteXml "yarn.resourcemanager.hostname" "host:port"
    ValidateXmlConfigValue $yarnSiteXml "yarn.nodemanager.local-dirs" "$NodeInstallRoot\hdfs\nm\local2"
    ValidateXmlConfigValue $capSchedulerSiteXml "yarn.capacity-scheduler.prop" "testcapacity"
    ValidateXmlConfigValue $capSchedulerSiteXml "yarn.capacity-scheduler.init-worker-threads" "10"

    ### Scenario where we only have capacity-scheduler configs
    Configure "yarn" $NodeInstallRoot $ServiceCredential @{
        "yarn.capacity-scheduler.prop" = "testcapacity2";
        "yarn.capacity-scheduler.init-worker-threads" = "20"}

    ValidateXmlConfigValue $yarnSiteXml "yarn.resourcemanager.hostname" "host:port"
    ValidateXmlConfigValue $yarnSiteXml "yarn.nodemanager.local-dirs" "$NodeInstallRoot\hdfs\nm\local2"
    ValidateXmlConfigValue $capSchedulerSiteXml "yarn.capacity-scheduler.prop" "testcapacity2"
    ValidateXmlConfigValue $capSchedulerSiteXml "yarn.capacity-scheduler.init-worker-threads" "20"

    # Cleanup
    Uninstall "yarn" $NodeInstallRoot
    Uninstall "core" $NodeInstallRoot
}

try
{
    ###
    ### Import dependencies
    ###
    $utilsModule = Import-Module -Name "$ScriptDir\..\resources\Winpkg.Utils.psm1" -ArgumentList ("HADOOP") -PassThru
    $apiModule = Import-Module -Name "$ScriptDir\InstallApi.psm1" -PassThru

    ###
    ### Test methods
    ###
    CoreInstallTestBasic
    CoreInstallTestIdempotent
    HdfsInstallTestBasic
    HdfsInstallTestNoCore
    HdfsInstallTestInvalidUser
    HdfsInstallTestRoleNoSupported
    MapRedInstallTestBasic
    MapRedInstallTestNoCore
    MapRedInstallTestInvalidUser
    MapRedInstallTestRoleNoSupported
    InstallAllTestBasic
    InstallAllTestIdempotent
    TestUpdateXmlConfig
    CoreConfigureTestBasic
    CoreConfigureWithFileTestBasic
    InstallAndConfigAllTestBasic
    TestStartStopServiceRoleNoSupported
    TestProcessAliasConfigOptions
    CoreConfigureWithAliasesTest
    CoreConfigureCapacitySchedulerTest

    # Start/StopService should be tested E2E as it requires all Hadoop binaries
    # to be installed (this test only installs a small subset so that it runs
    # faster).

    Write-Host "TEST COMPLETED SUCCESSFULLY"
}
finally
{
	if( $utilsModule -ne $null )
	{
		Remove-Module $apiModule
        Remove-Module $utilsModule
	}
}
