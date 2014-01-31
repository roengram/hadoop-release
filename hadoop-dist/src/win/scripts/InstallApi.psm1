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
### A set of basic PowerShell routines that can be used to install and
### manage Hadoop services on a single node. For use-case see install.ps1.
###

###
### Global variables
###
$ScriptDir = Resolve-Path (Split-Path $MyInvocation.MyCommand.Path)
$HadoopCoreVersion = "@version@"

###
### Uncomment and update below section for testing from sources
###
#$HadoopCoreVersion = "2.2.0-SNAPSHOT"
###
### End of testing section
###

### core-site.xml properties for folders that should be ACLed and deleted on
### uninstall
$CorePropertyFolderList = @()
$CorePropertyTempFolderList = @("hadoop.tmp.dir")

### hdfs-site.xml properties for folders
$HdfsPropertyFolderList = @("dfs.namenode.name.dir", "dfs.datanode.data.dir", "dfs.namenode.checkpoint.dir", "dfs.namenode.checkpoint.edits.dir")

### yarn-site.xml properties for folders
$YarnPropertyFolderList = @("yarn.nodemanager.local-dirs")

### mapred-site.xml properties for folders
$MapRedPropertyFolderList = @("mapreduce.cluster.local.dir")

### Returns the value of the given propertyName from the given xml file.
###
### Arguments:
###     xmlFileName: Xml file full path
###     propertyName: Name of the property to retrieve
function FindXmlPropertyValue(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $xmlFileName,
    [string]
    [parameter( Position=1, Mandatory=$true )]
    $propertyName)
{
    $value = $null

    if ( Test-Path $xmlFileName )
    {
        $xml = [xml] (Get-Content $xmlFileName)
        $xml.SelectNodes('/configuration/property') | ? { $_.name -eq $propertyName } | % { $value = $_.value }
        $xml.ReleasePath
    }

    $value
}

### Helper routing that converts a $null object to nothing. Otherwise, iterating over
### a $null object with foreach results in a loop with one $null element.
function empty-null($obj)
{
   if ($obj -ne $null) { $obj }
}

### Gives full permissions on the folder to the given user
function GiveFullPermissions(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $folder,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $username,
    [bool]
    [Parameter( Position=2, Mandatory=$false )]
    $recursive = $false)
{
    Write-Log "Giving user/group `"$username`" full permissions to `"$folder`""
    $cmd = "icacls `"$folder`" /grant ${username}:(OI)(CI)F"
    if ($recursive) {
        $cmd += " /T"
    }
    Invoke-CmdChk $cmd
}

### Checks if the given space separated roles are in the given array of
### supported roles.
function CheckRole(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    [AllowEmptyString()]
    $roles,
    [array]
    [parameter( Position=1, Mandatory=$true )]
    $supportedRoles
    )
{
    $roles = $roles.Trim()
    if (-not $roles) {
        return
    }
    foreach ( $role in $roles -Split("\s+") )
    {
        Write-Log "Checking for role $role in supported roles. $supportedRoles"
        if ( -not ( $supportedRoles -contains $role ) )
        {
            throw "CheckRole: Passed in role `"$role`" is outside of the supported set `"$supportedRoles`""
        }
    }
}

### List the non-empty folders defined in property value
function ListNonEmptyFoldersListedInPropertyValue(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $configFile,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $propertyName
    )
{
    $nonEmptyFolderList = @()
    [String]$folders = FindXmlPropertyValue $configFile $propertyName
    foreach ($folder in $folders.Split(","))
    {
        $folder = $folder.Trim()
        if ( ( $folder -ne $null ) -and ( Test-Path "$folder\*" ) )
        {
            $nonEmptyFolderList = $nonEmptyFolderList + $folder
        }
    }
    return $nonEmptyFolderList
}

### Function to delete data directories conditionally
function DataDirectoriesDelete-Chk(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $configFile,
    [array]
    [Parameter( Position=1, Mandatory=$true )]
    [AllowEmptyCollection()]
    $propertyList,
    [bool]
    [Parameter( Position=2, Mandatory=$false )]
    $destroyData = $false,
    [bool]
    [Parameter( Position=3, Mandatory=$false )]
    $throwerror = $true)
{
    $dataexists = $false
    foreach ($property in $propertyList)
    {
        $nonEmptyFolderList = @(ListNonEmptyFoldersListedInPropertyValue $configFile $property)
        if ($destroyData -eq $true)
        {
            foreach ($folder in $nonEmptyFolderList)
            {
                Write-Log "Removing $property -> $folder"
                $cmd = "rd /s /q `"$folder`""
                Invoke-Cmd $cmd > $null
            }
        }
        else
        {
            $dataexists = ($nonEmptyFolderList.Length -gt 0)
            if ( ($throwerror) -and ($dataexists) )
            {
                throw "Non-empty $property folders: $($nonEmptyFolderList -Join ',')"
            }
        }
    }
    return $dataexists
}

### Function to append a sub-path to a list of paths
function Get-AppendedPath(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $pathList,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $subPath,
    [String]
    [Parameter( Position=2, Mandatory=$false )]
    $delimiter = ",")
{
    $newPath = @()
    foreach ($path in $pathList -Split($delimiter))
    {
        $path = $path.Trim()
        if ($path -ne $null)
        {
            $apath = Join-Path $path $subPath
            $newPath = $newPath + $apath
        }
    }
    return ($newPath -Join $delimiter)
}
###############################################################################
###
### If no data is to be preserved clean the data directories
###
###############################################################################
function CheckDataDirectories(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [bool]
    [Parameter( Position=1, Mandatory=$false )]
    $destroyData = $false,
    [bool]
    [Parameter( Position=2, Mandatory=$false )]
    $throwerror = $false)
{
    ### Check the data directories in core-site.xml
    $xmlFile = Join-Path $nodeInstallRoot "etc\hadoop\core-site.xml"
    $csdataexists = (DataDirectoriesDelete-Chk $xmlFile $CorePropertyFolderList $destroyData $throwerror)

    ### Check the data directories in hdfs-site.xml
    $xmlFile = Join-Path $nodeInstallRoot "etc\hadoop\hdfs-site.xml"
    $hsdataexists = (DataDirectoriesDelete-Chk $xmlFile $HdfsPropertyFolderList $destroyData $throwerror)

    ### Check the data directories in mapred-site.xml
    $xmlFile = Join-Path $nodeInstallRoot "etc\hadoop\mapred-site.xml"
    $msdataexists = (DataDirectoriesDelete-Chk $xmlFile $MapRedPropertyFolderList $destroyData $throwerror)

    return ($csdataexists -or $hsdataexists -or $msdataexists)
}


###############################################################################
###
### Installs Hadoop Core component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###
###############################################################################
function InstallCore(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$true )]
    $serviceCredential
    )
{
    $username = $serviceCredential.UserName

    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-@version@.winpkg.log"
    Test-JavaHome

    ### $hadoopInstallToDir: the directory that contains the application, after unzipping
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"
    $hadoopInstallToBin = Join-Path "$hadoopInstallToDir" "bin"
    $hadoopConfDir = Join-Path "$hadoopInstallToDir" "etc\hadoop"

    Write-Log "hadoopInstallToDir: $hadoopInstallToDir"
    Write-Log "hadoopInstallToBin: $hadoopInstallToBin"
    Write-Log "hadoopConfDir: $hadoopConfDir"
    Write-Log "Username: $username"

    ###
    ### Set environment variables
    ### Also making the environment variables available in the current powershell session
    ###
    Write-Log "Setting the HADOOP_HOME environment variable at machine scope to `"$hadoopInstallToDir`""
    [Environment]::SetEnvironmentVariable("HADOOP_HOME", "$hadoopInstallToDir", [EnvironmentVariableTarget]::Machine)
    $ENV:HADOOP_HOME = "$hadoopInstallToDir"

    Write-Log "Setting the HADOOP_CONF_DIR environment variable at machine scope to `"$hadoopConfDir`""
    [Environment]::SetEnvironmentVariable("HADOOP_CONF_DIR", "$hadoopConfDir", [EnvironmentVariableTarget]::Machine)
    $ENV:HADOOP_CONF_DIR = "$hadoopConfDir"

    Write-Log "Setting the HADOOP_COMMON_HOME environment variable at machine scope to `"$hadoopInstallToDir`""
    [Environment]::SetEnvironmentVariable("HADOOP_COMMON_HOME", "$hadoopInstallToDir", [EnvironmentVariableTarget]::Machine)
    $ENV:HADOOP_COMMON_HOME = "$hadoopInstallToDir"

    Write-Log "Setting the HADOOP_HDFS_HOME environment variable at machine scope to `"$hadoopInstallToDir`""
    [Environment]::SetEnvironmentVariable("HADOOP_HDFS_HOME", "$hadoopInstallToDir", [EnvironmentVariableTarget]::Machine)
    $ENV:HADOOP_HDFS_HOME = "$hadoopInstallToDir"

    Write-Log "Setting the HADOOP_MAPRED_HOME environment variable at machine scope to `"$hadoopInstallToDir`""
    [Environment]::SetEnvironmentVariable("HADOOP_MAPRED_HOME", "$hadoopInstallToDir", [EnvironmentVariableTarget]::Machine)
    $ENV:HADOOP_MAPRED_HOME = "$hadoopInstallToDir"

    Write-Log "Setting the HADOOP_YARN_HOME environment variable at machine scope to `"$hadoopInstallToDir`""
    [Environment]::SetEnvironmentVariable("HADOOP_YARN_HOME", "$hadoopInstallToDir", [EnvironmentVariableTarget]::Machine)
    $ENV:HADOOP_MAPRED_HOME = "$hadoopInstallToDir"


    ### HDP 2.0 TODO This is a temporary workaround for MAPREDUCE-5451
    Write-Log "Add HADOOP_COMMOM_HOME\bin to PATH"
    [Environment]::SetEnvironmentVariable("PATH", "$hadoopInstallToDir\bin;$env:PATH", [EnvironmentVariableTarget]::Machine)
    $ENV:PATH = "$hadoopInstallToDir\bin;$env:PATH"

    ###
    ### Begin install
    ###
    Write-Log "Installing Apache Hadoop Core hadoop-$HadoopCoreVersion to $nodeInstallRoot"

    ### Create Node Install Root directory
    if( -not (Test-Path "$nodeInstallRoot"))
    {
        Write-Log "Creating Node Install Root directory: `"$nodeInstallRoot`""
        $cmd = "mkdir `"$nodeInstallRoot`""
        Invoke-CmdChk $cmd
    }

    ###
    ###  Xcopy Hadoop distribution from winpkg to install folder
    ###
    Write-Log "Copy the Hadoop bits to $nodeInstallRoot"
    $xcopy_cmd = "xcopy /EIYF `"$HDP_RESOURCES_DIR\hadoop-$HadoopCoreVersion`" `"$hadoopInstallToDir`""
    Invoke-CmdChk $xcopy_cmd

    ###
    ###  Copy template config files
    ###
    $xcopy_cmd = "xcopy /EIYF `"$HDP_INSTALL_PATH\..\template\conf\*.xml`" `"$hadoopInstallToDir\etc\hadoop`""
    Invoke-CmdChk $xcopy_cmd

    $xcopy_cmd = "xcopy /EIYF `"$HDP_INSTALL_PATH\..\template\conf\*.properties`" `"$hadoopInstallToDir\etc\hadoop`""
    Invoke-CmdChk $xcopy_cmd

    ###
    ### Grant Hadoop user access to $hadoopInstallToDir
    ###
    GiveFullPermissions $hadoopInstallToDir $username

    ###
    ### ACL Hadoop logs directory such that machine users can write to it
    ###
    $hadooplogdir = "$hadoopInstallToDir\logs"
    if(Test-Path ENV:HADOOP_LOG_DIR)
    {
        $hadooplogdir = $ENV:HADOOP_LOG_DIR
    }
    if( -not (Test-Path "$hadooplogdir"))
    {
        Write-Log "Creating Hadoop logs folder"
        $cmd = "mkdir `"$hadooplogdir`""
        Invoke-CmdChk $cmd
    }
    GiveFullPermissions "$hadooplogdir" "Users"

    Write-Log "Installation of Apache Hadoop Core complete"
}

###############################################################################
###
### Uninstalls Hadoop Core component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###
### Dev Note: We use non-Chk Invoke methods on uninstall, since uninstall should
###           not fail.
###
###############################################################################
function UninstallCore(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [bool]
    [Parameter( Position=1, Mandatory=$false )]
    $destroyData = $false)
{
    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-$HadoopCoreVersion.winpkg.log" $ENV:WINPKG_BIN
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"

    ### If Hadoop Core root does not exist exit early
    if ( -not (Test-Path $hadoopInstallToDir) )
    {
        return
    }

    ###
    ### Remove Hadoop Core folders defined in configuration files
    ###
    $xmlFile = Join-Path $hadoopInstallToDir "etc\hadoop\core-site.xml"
    foreach ($property in $CorePropertyTempFolderList)
    {
        [String]$folder = FindXmlPropertyValue $xmlFile $property
        $folder = $folder.Trim()
        if ( ( $folder -ne $null ) -and ( Test-Path $folder ) )
        {
            Write-Log "Removing Hadoop `"$property`" located under `"$folder`""
            $cmd = "rd /s /q `"$folder`""
            Invoke-Cmd $cmd
        }
    }
    DataDirectoriesDelete-Chk $xmlFile $CorePropertyFolderList $destroyData $false

    ###
    ### Remove all Hadoop binaries
    ###
    Write-Log "Removing Hadoop `"$hadoopInstallToDir`""
    $cmd = "rd /s /q `"$hadoopInstallToDir`""
    Invoke-Cmd $cmd

    Write-Log "Removing the HADOOP_HOME, HADOOP_CONF_DIR and HADOOP_COMMON_HOME environment variables"
    [Environment]::SetEnvironmentVariable( "HADOOP_HOME", $null, [EnvironmentVariableTarget]::Machine )
    [Environment]::SetEnvironmentVariable("HADOOP_COMMON_HOME", $null, [EnvironmentVariableTarget]::Machine)
    [Environment]::SetEnvironmentVariable("HADOOP_CONF_DIR", $null, [EnvironmentVariableTarget]::Machine)
    Write-Log "Removing the HADOOP_HDFS_HOME environment variables"
    [Environment]::SetEnvironmentVariable("HADOOP_HDFS_HOME", $null, [EnvironmentVariableTarget]::Machine)
    Write-Log "Removing the HADOOP_MAPRED_HOME environment variables"
    [Environment]::SetEnvironmentVariable("HADOOP_MAPRED_HOME", $null, [EnvironmentVariableTarget]::Machine)
    Write-Log "Removing the HADOOP_YARN_HOME environment variables"
    [Environment]::SetEnvironmentVariable("HADOOP_YARN_HOME", $null, [EnvironmentVariableTarget]::Machine)
}

### Creates and configures the service.
function CreateAndConfigureHadoopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $service,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $hdpResourcesDir,
    [String]
    [Parameter( Position=2, Mandatory=$true )]
    $serviceBinDir,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=3, Mandatory=$true )]
    $serviceCredential
)
{
    if ( -not ( Get-Service "$service" -ErrorAction SilentlyContinue ) )
    {
        Write-Log "Creating service `"$service`" as $serviceBinDir\$service.exe"
        $xcopyServiceHost_cmd = "copy /Y `"$hdpResourcesDir\serviceHost.exe`" `"$serviceBinDir\$service.exe`""
        Invoke-CmdChk $xcopyServiceHost_cmd

        #HadoopServiceHost.exe will write to this log but does not create it
        #Creating the event log needs to be done from an elevated process, so we do it here
        if( -not ([Diagnostics.EventLog]::SourceExists( "$service" )))
        {
            [Diagnostics.EventLog]::CreateEventSource( "$service", "" )
        }

        Write-Log "Adding service $service"
        $s = New-Service -Name "$service" -BinaryPathName "$serviceBinDir\$service.exe" -Credential $serviceCredential -DisplayName "Apache Hadoop $service"
        if ( $s -eq $null )
        {
            throw "CreateAndConfigureHadoopService: Service `"$service`" creation failed"
        }

        $cmd="$ENV:WINDIR\system32\sc.exe failure $service reset= 30 actions= restart/5000"
        Invoke-CmdChk $cmd

        $cmd="$ENV:WINDIR\system32\sc.exe config $service start= demand"
        Invoke-CmdChk $cmd

        Set-ServiceAcl $service
    }
    else
    {
        Write-Log "Service `"$service`" already exists, skipping service creation"
    }
}

### Creates and configures the service.
function CreateAndConfigureHadoopVirtualService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $service,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $hdpResourcesDir,
    [String]
    [Parameter( Position=2, Mandatory=$true )]
    $serviceBinDir,
    [String]
    [Parameter( Position=3, Mandatory=$true )]
    $hadoopInstallToDir
)
{
    if ( -not ( Get-Service "$service" -ErrorAction SilentlyContinue ) )
    {
        Write-Log "Creating service `"$service`" as $serviceBinDir\$service.exe"
        $xcopyServiceHost_cmd = "copy /Y `"$hdpResourcesDir\serviceHost.exe`" `"$serviceBinDir\$service.exe`""
        Invoke-CmdChk $xcopyServiceHost_cmd

        #HadoopServiceHost.exe will write to this log but does not create it
        #Creating the event log needs to be done from an elevated process, so we do it here
        if( -not ([Diagnostics.EventLog]::SourceExists( "$service" )))
        {
            [Diagnostics.EventLog]::CreateEventSource( "$service", "" )
        }

        Write-Log "Adding service $service"

        $cmd="$ENV:WINDIR\system32\sc.exe create `"$service`" binPath= `"$serviceBinDir\$service.exe`" obj= `"NT SERVICE\$service`" DisplayName= `"Apache Hadoop $service`" "
        try
        {
            Invoke-CmdChk $cmd
        }
        catch
        {
            throw "CreateAndConfigureHadoopService: Service `"$service`" creation failed"
        }

        $cmd="$ENV:WINDIR\system32\sc.exe failure $service reset= 30 actions= restart/5000"
        Invoke-CmdChk $cmd

        $cmd="$ENV:WINDIR\system32\sc.exe config $service start= demand"
        Invoke-CmdChk $cmd

        $serviceSid = Get-ServiceSid "$service"
        $cmd="icacls `"$hadoopInstallToDir`" /grant *${serviceSid}:(OI)(CI)F"
        Invoke-CmdChk $cmd

        Set-ServiceAcl $service
    }
    else
    {
        Write-Log "Service `"$service`" already exists, skipping service creation"
    }
}

function Get-ServiceSid ($service)
{
    $cmd = "sc.exe showsid $service"
    $out = Invoke-CmdChk $cmd
    foreach ($line in $out)
    {
        if ($line -match "SERVICE SID: (S-.+)")
        {
            return $matches[1].Trim()
        }
    }
}

### Stops and deletes the Hadoop service.
function StopAndDeleteHadoopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $service
)
{
    Write-Log "Stopping $service"
    $s = Get-Service $service -ErrorAction SilentlyContinue

    if( $s -ne $null )
    {
        Stop-Service $service
        $cmd = "sc.exe delete $service"
        Invoke-Cmd $cmd
    }
}

###############################################################################
###
### Installs Hadoop HDFS component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     hdfsRole: Space separated list of HDFS roles that should be installed.
###               (for example, "namenode secondarynamenode")
###
###############################################################################
function InstallHdfs(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$false )]
    $serviceCredential,
    [String]
    [Parameter( Position=2, Mandatory=$false )]
    $hdfsRole
    )
{
    $username = $serviceCredential.UserName
    if ( $serviceCredential -ne $null)
    {
        Write-Log "In InstallHDFS function nodeInstallRoot=$nodeInstallRoot serviceCredentioal=$serviceCredential hdfsRole=$hdfsRole"
    }
    else
    {
        Write-Log "In InstallHDFS function nodeInstallRoot=$nodeInstallRoot hdfsRole=$hdfsRole (virtual account enabled)"
    }
    ###
    ### Setup defaults if not specified
    ###

    if( $hdfsRole -eq $null )
    {
        $hdfsRole = "namenode datanode secondarynamenode"
    }
    $hdfsRole = $hdfsRole.Trim()
    ### Verify that hdfsRole are in the supported set
    CheckRole $hdfsRole @("namenode","datanode","secondarynamenode", "journalnode", "zkfc")

    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-@version@.winpkg.log"
    Test-JavaHome

    ### $hadoopInstallDir: the directory that contains the application, after unzipping
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"
    $hadoopInstallToBin = Join-Path "$hadoopInstallToDir" "bin"

    ### Hadoop Core must be installed before HDFS
    if( -not (Test-Path $hadoopInstallToDir ))
    {
        throw "InstallHdfs: InstallCore must be called before InstallHdfs"
    }

    Write-Log "HdfsRole: $hdfsRole"

    ###
    ### Copy hdfs configs
    ###
    Write-Log "Copying HDFS configs"
    $xcopy_cmd = "xcopy /EIYF `"$HDP_INSTALL_PATH\..\template\conf\hdfs-site.xml`" `"$hadoopInstallToDir\etc\hadoop`""
    Invoke-CmdChk $xcopy_cmd

    ###
    ### Create Hadoop Windows Services and grant user ACLS to start/stop
    ###

    Write-Log "Node HDFS Role Services: $hdfsRole"

    if ($hdfsRole) {
        Write-Log "Installing services $hdfsRole"

        foreach( $service in empty-null ($hdfsRole -Split('\s+')))
        {
            if ( $serviceCredential -ne $null )
            {
                CreateAndConfigureHadoopService $service $HDP_RESOURCES_DIR $hadoopInstallToBin $serviceCredential
            }
            else
            {
                CreateAndConfigureHadoopVirtualService $service $HDP_RESOURCES_DIR $hadoopInstallToBin $hadoopInstallToDir
            }
        }

        ###
        ### Setup HDFS service config
        ###
        Write-Log "Copying configuration for $hdfsRole"

        foreach( $service in empty-null ($hdfsRole -Split('\s+')))
        {
            Write-Log "Creating service config ${hadoopInstallToBin}\$service.xml"
            $cmd = "$hadoopInstallToBin\hdfs.cmd --service $service > `"$hadoopInstallToBin\$service.xml`""
            Invoke-CmdChk $cmd
        }
    }
    else{
        Write-Log "No Services needed to be installed for HDFS on this node."
    }

    Write-Log "Installation of Hadoop HDFS complete"
}

###############################################################################
###
### Uninstalls Hadoop HDFS component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###
### Dev Note: We use non-Chk Invoke methods on uninstall, since uninstall should
###           not fail.
###
###############################################################################
function UninstallHdfs(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [bool]
    [Parameter( Position=1, Mandatory=$false )]
    $destroyData = $false)
{
    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-$HadoopCoreVersion.winpkg.log" $ENV:WINPKG_BIN
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"

    ###
    ### Stop and delete services
    ###
    foreach( $service in ("namenode", "datanode", "secondarynamenode"))
    {
        StopAndDeleteHadoopService $service
    }

    ### If Hadoop Core root does not exist exit early
    if ( -not (Test-Path $hadoopInstallToDir) )
    {
        return
    }

    ###
    ### Remove Hadoop HDFS folders defined in configuration files
    ###
    $xmlFile = Join-Path $hadoopInstallToDir "etc\hadoop\hdfs-site.xml"
    DataDirectoriesDelete-Chk $xmlFile $HdfsPropertyFolderList $destroyData $false
}

###############################################################################
###
### Installs Hadoop MapReduce component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     mapredRole: Space separated list of MapRed roles that should be installed.
###               (for example, "historyserver")
###
###############################################################################
function InstallMapRed(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$true )]
    $serviceCredential,
    [String]
    [Parameter( Position=2, Mandatory=$false )]
    $mapredRole
    )
{
    $username = $serviceCredential.UserName

    ###
    ### Setup defaults if not specified
    ###
    Write-Log "In InstallMapRed function nodeInstallRoot=$nodeInstallRoot serviceCredentioal=$serviceCredential mapredRole=$mapredRole="

    if( $mapredRole -eq $null )
    {
        $mapredRole = "jobclient historyserver"
    }

    $mapredRole = $mapredRole.Trim()
    ### Verify that mapredRole are in the supported set
    CheckRole $mapredRole @("jobclient","historyserver")

    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-@version@.winpkg.log"
    Test-JavaHome

    ### $hadoopInstallDir: the directory that contains the application, after unzipping
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"
    $hadoopInstallToBin = Join-Path "$hadoopInstallToDir" "bin"

    ### Hadoop Core must be installed before MapRed
    if( -not (Test-Path $hadoopInstallToDir ))
    {
        throw "InstallMapRed: InstallCore must be called before InstallMapRed"
    }

    ###
    ### Copy mapred configs
    ###
    Write-Log "Copying MapRed configs"
    $xcopy_cmd = "xcopy /EIYF `"$HDP_INSTALL_PATH\..\template\conf\mapred-site.xml`" `"$hadoopInstallToDir\etc\hadoop`""
    Invoke-CmdChk $xcopy_cmd

    ###
    ### Create Hadoop Windows Services and grant user ACLS to start/stop
    ###
    if ($mapredRole) {
        Write-Log "Node MapRed Role Services: $mapredRole"

        Write-Log "Installing services $mapredRole"

        foreach( $service in empty-null ($mapredRole -Split('\s+')))
        {
            if ( $service -eq "jobclient")
            {
              Write-Log "Skipping installing JobClient as a service"
              continue
            }

            CreateAndConfigureHadoopService $service $HDP_RESOURCES_DIR $hadoopInstallToBin $serviceCredential
            ###
            ### Setup MapRed service config
            ###
            Write-Log "Copying configuration for $service"

            Write-Log "Creating service config ${hadoopInstallToBin}\$service.xml"
            $cmd = "$hadoopInstallToBin\mapred.cmd --service $service > `"$hadoopInstallToBin\$service.xml`""
            Invoke-CmdChk $cmd
        }
    }
    else{
        Write-Log "No Services needed to be installed for MapRed on this Node."
    }
    Write-Log "Installation of Hadoop MapReduce complete"
}

###############################################################################
###
### Uninstalls Hadoop MapRed component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###
### Dev Note: We use non-Chk Invoke methods on uninstall, since uninstall should
###           not fail.
###
###############################################################################
function UninstallMapRed(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [bool]
    [Parameter( Position=1, Mandatory=$false )]
    $destroyData = $false)
{
    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-$HadoopCoreVersion.winpkg.log" $ENV:WINPKG_BIN
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"

    ###
    ### Stop and delete services
    ###
    foreach( $service in ("historyserver"))
    {
        StopAndDeleteHadoopService $service
    }

    ### If Hadoop Core root does not exist exit early
    if ( -not (Test-Path $hadoopInstallToDir) )
    {
        return
    }

    ###
    ### Remove Hadoop MapRed folders defined in configuration files
    ###
    $xmlFile = Join-Path $hadoopInstallToDir "etc\hadoop\mapred-site.xml"
    DataDirectoriesDelete-Chk $xmlFile $MapRedPropertyFolderList $destroyData $false
    foreach ($property in $MapRedPropertyFolderList)
    {
        [String]$folder = FindXmlPropertyValue $xmlFile $property
        $folder = $folder.Trim()
        if ( ( $folder -ne $null ) -and ( Test-Path $folder ) )
        {
            Write-Log "Removing Hadoop `"$property`" located under `"$folder`""
            $cmd = "rd /s /q `"$folder`""
            Invoke-Cmd $cmd
        }
    }
}

###############################################################################
###
### Installs Hadoop YARN component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     hdfsRole: Space separated list of YARN roles that should be installed.
###               (for example, "resourcemanager nodemanager")
###
###############################################################################
function InstallYarn(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$true )]
    $serviceCredential,
    [String]
    [Parameter( Position=2, Mandatory=$false )]
    $yarnRole
    )
{
    $username = $serviceCredential.UserName

    ###
    ### Setup defaults if not specified
    ###
    Write-Log "In InstallYarn function nodeInstallRoot=$nodeInstallRoot serviceCredentioal=$serviceCredential mapredRole=$mapredRole="

    if( $yarnRole -eq $null )
    {
        $yarnRole = "resourcemanager nodemanager"
    }

    $yarnRole = $yarnRole.Trim()
    ### Verify that yarnRoles are in the supported set
    CheckRole $yarnRole @("resourcemanager","nodemanager")

    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-@version@.winpkg.log"
    Test-JavaHome

    ### $hadoopInstallDir: the directory that contains the application, after unzipping
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"
    $hadoopInstallToBin = Join-Path "$hadoopInstallToDir" "bin"

    ### Hadoop Core must be installed before YARN
    if( -not (Test-Path $hadoopInstallToDir ))
    {
        throw "InstallYarn: InstallCore must be called before InstallYarn"
    }

    Write-Log "YarnRole: $yarnRole"

    ###
    ### Copy YARN configs
    ###
    Write-Log "Copying YARN configs"
    ###$xcopy_cmd = "xcopy /EIYF `"$HDP_INSTALL_PATH\..\template\conf\mapred-site.xml`" `"$hadoopInstallToDir\conf`""
    ###Invoke-CmdChk $xcopy_cmd

    ###
    ### Create Hadoop Windows Services and grant user ACLS to start/stop
    ###
    if ($yarnRole) {
        Write-Log "Node YARN Role Services: $yarnRole"
        #$allServices = $yarnRole

        Write-Log "Installing services $yarnRole"

        foreach( $service in empty-null ($yarnRole -Split('\s+')))
        {
            CreateAndConfigureHadoopService $service $HDP_RESOURCES_DIR $hadoopInstallToBin $serviceCredential

            ###
            ### Setup YARN service config
            ###
            Write-Log "Copying configuration for $service"

            Write-Log "Creating service config ${hadoopInstallToBin}\$service.xml"
            $cmd = "$hadoopInstallToBin\yarn.cmd --service $service > `"$hadoopInstallToBin\$service.xml`""
            Invoke-CmdChk $cmd
        }
    }
    else
    {
        Write-Log "No Services needed to be installed for YARN on this Node."
    }
    Write-Log "Installation of Hadoop YARN complete"
}

###############################################################################
###
### Uninstalls Hadoop YARN component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###
### Dev Note: We use non-Chk Invoke methods on uninstall, since uninstall should
###           not fail.
###
###############################################################################
function UninstallYarn(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [bool]
    [Parameter( Position=1, Mandatory=$false )]
    $destroyData = $false)
{
    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-$HadoopCoreVersion.winpkg.log" $ENV:WINPKG_BIN
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"

    ###
    ### Stop and delete services
    ###
    foreach( $service in ("resourcemanager", "nodemanager"))
    {
        StopAndDeleteHadoopService $service
    }

    ### If Hadoop Core root does not exist exit early
    if ( -not (Test-Path $hadoopInstallToDir) )
    {
        return
    }

    ###
    ### Remove Hadoop YARN folders defined in configuration files
    ###
    $xmlFile = Join-Path $hadoopInstallToDir "etc\hadoop\yarn-site.xml"
    DataDirectoriesDelete-Chk $xmlFile $YarnPropertyFolderList $destroyData $false
}


### Helper routine that updates the given fileName XML file with the given
### key/value configuration values. The XML file is expected to be in the
### Hadoop format. For example:
### <configuration>
###   <property>
###     <name.../><value.../>
###   </property>
### </configuration>
function UpdateXmlConfig(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $fileName,
    [hashtable]
    [parameter( Position=1 )]
    $config = @{} )
{
    $xml = [xml] (Get-Content $fileName)

    foreach( $key in empty-null $config.Keys )
    {
        $value = $config[$key]
        $found = $False
        $xml.SelectNodes('/configuration/property') | ? { $_.name -eq $key } | % { $_.value = $value; $found = $True }
        if ( -not $found )
        {
            $xml["configuration"].AppendChild($xml.CreateWhitespace("`r`n  ")) | Out-Null
            $newItem = $xml.CreateElement("property")
            $newItem.AppendChild($xml.CreateWhitespace("`r`n    ")) | Out-Null
            $newItem.AppendChild($xml.CreateElement("name")) | Out-Null
            $newItem.AppendChild($xml.CreateWhitespace("`r`n    ")) | Out-Null
            $newItem.AppendChild($xml.CreateElement("value")) | Out-Null
            $newItem.AppendChild($xml.CreateWhitespace("`r`n  ")) | Out-Null
            $newItem.name = $key
            $newItem.value = $value
            $xml["configuration"].AppendChild($newItem) | Out-Null
            $xml["configuration"].AppendChild($xml.CreateWhitespace("`r`n")) | Out-Null
        }
    }

    $xml.Save($fileName)
    $xml.ReleasePath
}

### Helper routine that converts a path in url into a Windows path are are
### recognizable by normal Windows commands. For example:
###  file:///c:/hdfs/dn -> c:\hdfs\dn
###  file:///d -> c:\d
###  /hadoop/mapred/temp -> c:\hadoop\mapred\temp
###  file:///hadoop/nm-local-dir -> c:\hadoop\nm-local-dir
function NormalizePath(
[string]
[parameter( Position=0, Mandatory=$true )]
$path
)
{
  $path = $path.Trim()
  # remove "file:///"
  $path = $path.TrimStart("file:///")
  # replace "/" with "\"
  $path = $path.Replace("/", "\")
  # prepend drive letter when necessary
  if ( ($path.Length -lt 3) -or ($path.Substring(1,2) -ne ":\") )
  {
    $path = (Get-Location).Drive.Name.ToLower() + ":\" + $path
  }
  return $path
}


### Helper routine that ACLs the folders defined in folderList properties.
### The routine will look for the property value in the given xml config file
### and give full permissions on that folder to the given username.
###
### Dev Note: All folders that need to be ACLed must be defined in *-site.xml
### files.
function AclFoldersForUser(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $xmlFileName,
    [string]
    [parameter( Position=1, Mandatory=$true )]
    $username,
    [array]
    [parameter( Position=2, Mandatory=$true )]
    [AllowEmptyCollection()]
    $folderList )
{
    $xml = [xml] (Get-Content $xmlFileName)

    foreach( $key in empty-null $folderList )
    {
        $folderName = $null
        $xml.SelectNodes('/configuration/property') | ? { $_.name -eq $key } | % { $folderName = $_.value }
        if ( $folderName -eq $null )
        {
            throw "AclFoldersForUser: Trying to ACLs the folder $key which is not defined in $xmlFileName"
        }

        ### TODO: Support for JBOD and NN Replication
        foreach ($folder in $folderName.Split(","))
        {
            $folder = NormalizePath($folder)
            try
            {
                $folderParent = Split-Path $folder -parent

                if( -not (Test-Path $folderParent))
                {
                    Write-Log "AclFoldersForUser: Creating Directory `"$folderParent`" for ACLing"
                    mkdir $folderParent
                }
                GiveFullPermissions $folderParent $username
            }
            catch
            {
                Write-Log "AclFoldersForUser: Skipped folder `"$folderName`", with exception: $_.Exception.ToString()"
            }
        }
    }

    $xml.ReleasePath
}

function AclFoldersForVirtualAccount(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $xmlFileName,
    [string]
    [parameter( Position=1, Mandatory=$true )]
    $service,
    [array]
    [parameter( Position=2, Mandatory=$false )]
    $folderList )
{
    $xml = [xml] (Get-Content $xmlFileName)

    foreach( $key in empty-null $folderList )
    {
        $folderName = $null
        $xml.SelectNodes('/configuration/property') | ? { $_.name -eq $key } | % { $folderName = $_.value }
        if ( $folderName -eq $null )
        {
            throw "AclFoldersForVirtualAccount: Trying to ACLs the folder $key which is not defined in $xmlFileName"
        }
        foreach ($folder in $folderName.Split(","))
        {
            $folder = NormalizePath($folder)
            try
            {
                $folderParent = Split-Path $folder -parent

                if( -not (Test-Path $folderParent))
                {
                    Write-Log "AclFoldersForVirtualAccount: Creating Directory `"$folderParent`" for ACLing"
                    mkdir $folderParent
                }
                if ( (CheckIfACLNeededForVirtualAccount $service $folderParent) -eq $true )
                {
                    Write-Log "AclFoldersForVirtualAccount: Acl `"$folderParent`" for $service"
                    $serviceSid = Get-ServiceSid "$service"
                    $cmd="icacls `"$folderParent`" /grant *${serviceSid}:(OI)(CI)F"
                    Invoke-CmdChk $cmd
                 }
            }
            catch
            {
                Write-Log "AclFoldersForVirtualAccount: Skipped folder `"$folderName`", with exception: $_.Exception.ToString()"
            }
        }
    }
    $xml.ReleasePath
}

function CheckIfACLNeededForVirtualAccount (
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $service,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $folder
)
{
    $s = Get-WmiObject -Class Win32_Service -Filter "Name='$service'"
    if ($s -eq $null)
    {
        return $false
    }

    $cmd="icacls `"$folder`""
    $out = Invoke-CmdChk $cmd
    foreach ($line in $out)
    {
        if ($line -match "$service")
        {
            return $false
        }
    }
    return $true
}
### Runs the given configs thru the alias transformation and returns back
### the new list of configs with all alias dependent options resolved.
###
### Supported aliases:
###  core-site:
###     hdfs_namenode_host -> (fs.defaultFS, hdfs://$value:8020)
###
###  hdfs-site:
###     hdfs_namenode_host -> (dfs.namenode.http-address, $value:50070)
###                           (dfs.namenode.https-address, $value:50470)
###     hdfs_secondary_namenode_host ->
###             (dfs.namenode.secondary.http-address, $value:50090)
###
###  mapred-site:
###     mapred_historyservice_host -> (mapreduce.jobhistory.address, $value:50300)
###                                  (mapreduce.jobhistory.webapp.address, $value:50030)
###
###  yarn-site:
###     yarn_resourcemanager_host -> (yarn.resourcemanager.address, $value:50300)
###                                  (yarn.resourcemanager.webapp.address, $value:50030)
###
function ProcessAliasConfigOptions(
    [String]
    [parameter( Position=0 )]
    $component,
    [hashtable]
    [parameter( Position=1 )]
    $configs)
{
    $result = @{}
    Write-Log "ProcessAliasConfigOptions: Resolving `"$component`" configs"
    if ( $component -eq "core" )
    {
        foreach( $key in empty-null $configs.Keys )
        {
            if ( $key -eq "hdfs_namenode_host" )
            {
                $result.Add("fs.defaultFs",  "hdfs://localhost:8020".Replace("localhost", $configs[$key]))
            }
            else
            {
                $result.Add($key, $configs[$key])
            }
        }
    }
    elseif ( $component -eq "hdfs" )
    {
        foreach( $key in empty-null $configs.Keys )
        {
            if ( $key -eq "hdfs_namenode_host" )
            {
                $result.Add("dfs.namenode.http-address", "localhost:50070".Replace("localhost", $configs[$key]))
                $result.Add("dfs.namenode.https-address", "localhost:50470".Replace("localhost", $configs[$key]))
            }
            elseif ( $key -eq "hdfs_secondary_namenode_host" )
            {
                $result.Add("dfs.namenode.secondary.http-address", "localhost:50090".Replace("localhost", $configs[$key]))
            }
            else
            {
                $result.Add($key, $configs[$key])
            }
        }
    }
    elseif ( $component -eq "mapreduce" )
    {
        foreach( $key in empty-null $configs.Keys )
        {
            if ( $key -eq "mapreduce_historyserver_host" )
            {
                $result.Add("mapreduce.jobhistory.address",  "localhost:10020".Replace("localhost", $configs[$key]))
                $result.Add("mapreduce.jobhistory.webapp.address",  "localhost:19888".Replace("localhost", $configs[$key]))
            }
            else
            {
                $result.Add($key, $configs[$key])
            }
        }
    }
    elseif ( $component -eq "yarn" )
    {
        foreach( $key in empty-null $configs.Keys )
        {
            if ( $key -eq "yarn_resourcemanager_host" )
            {
                $result.Add("yarn.resourcemanager.webapp.address", "localhost:8088".Replace("localhost", $configs[$key]))
                $result.Add("yarn.resourcemanager.address", "localhost:8032".Replace("localhost", $configs[$key]))
            }
            else
            {
                $result.Add($key, $configs[$key])
            }
        }
    }
    else
    {
        throw "ProcessAliasConfigOptions: Unknown component name `"$component`""
    }

    return $result
}

###############################################################################
###
### Alters the configuration of the Hadoop Core component.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     configs: Configuration that should be applied.
###              For example, @{"dfs.namenode.checkpoint.edits.dir" = "C:\Hadoop\hdfs\2nne"}
###     aclAllFolders: If true, all folders defined in core-site.xml will be ACLed
###                    If false, only the folders listed in configs will be ACLed.
###
###############################################################################
function ConfigureCore(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$true )]
    $serviceCredential,
    [hashtable]
    [parameter( Position=2 )]
    $configs = @{},
    [bool]
    [parameter( Position=3 )]
    $aclAllFolders = $True
    )
{
    $username = $serviceCredential.UserName

    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-$HadoopCoreVersion.winpkg.log" $ENV:WINPKG_BIN
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"

    ### Hadoop Core must be installed before ConfigureCore is called
    if( -not ( Test-Path $hadoopInstallToDir ))
    {
        throw "ConfigureCore: InstallCore must be called before ConfigureCore"
    }

    ###
    ### Apply core-site.xml configuration changes
    ###
    $coreSiteXmlFile = Join-Path $hadoopInstallToDir "etc\hadoop\core-site.xml"
    UpdateXmlConfig $coreSiteXmlFile $configs

    if ($aclAllFolders)
    {
        ###
        ### ACL all folders
        ###
        AclFoldersForUser $coreSiteXmlFile $username $CorePropertyFolderList
    }
    else
    {
        ###
        ### ACL only folders that were modified as part of the config updates
        ###
        foreach( $key in empty-null $config.Keys )
        {
            $folderList = @()
            if ($CorePropertyFolderList -contains $key)
            {
                $folderList = $folderList + @($key)
            }

            AclFoldersForUser $coreSiteXmlFile $username $folderList
        }
    }
}

###############################################################################
###
### Alters the configuration of the Hadoop HDFS component.
###
### Arguments:
###   See ConfigureCore
###############################################################################
function ConfigureHdfs(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$false )]
    $serviceCredential,
    [hashtable]
    [parameter( Position=2 )]
    $configs = @{},
    [bool]
    [parameter( Position=3 )]
    $aclAllFolders = $True
    )
{
    $username = $serviceCredential.UserName

    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-$HadoopCoreVersion.winpkg.log" $ENV:WINPKG_BIN
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"

    if( -not (Test-Path $hadoopInstallToDir ))
    {
        throw "ConfigureHdfs: InstallCore and InstallHdfs must be called before ConfigureHdfs"
    }

    ### TODO: Support JBOD and NN replication

    ###
    ### Apply configuration changes to hdfs-site.xml
    ###
    $xmlFile = Join-Path $hadoopInstallToDir "etc\hadoop\hdfs-site.xml"
    UpdateXmlConfig $xmlFile $configs

    if ($aclAllFolders)
    {
        ###
        ### ACL all folders
        ###
        if ( $serviceCredential -eq $null )
        {
            foreach( $service in ("namenode", "datanode") )
            {
                AclFoldersForVirtualAccount $xmlFile $service $HdfsPropertyFolderList
            }
        }
        else
        {
            AclFoldersForUser $xmlFile $username $HdfsPropertyFolderList
        }
    }
    else
    {
        ###
        ### ACL only folders that were modified as part of the config updates
        ###
        foreach( $key in empty-null $config.Keys )
        {
            $folderList = @()
            if ($HdfsPropertyFolderList -contains $key)
            {
                $folderList = $folderList + @($key)
            }

            if ( $serviceCredential -eq $null )
            {
                foreach( $service in ("namenode", "datanode"))
                {
                    AclFoldersForVirtualAccount $xmlFile $service $folderList
                }
            }
            else
            {
                AclFoldersForUser $xmlFile $username $folderList
            }
        }
    }
}

### Helper method that extracts all capacity-scheduler configs from the "config"
### hashmap and applies them to capacity-scheduler.xml.
### The function returns the list of configs that are not specific to capacity
### scheduler.
function ConfigureCapacityScheduler(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $fileName,
    [hashtable]
    [parameter( Position=1 )]
    $config = @{} )
{
    [hashtable]$newConfig = @{}
    [hashtable]$newCapSchedulerConfig = @{}
    foreach( $key in empty-null $config.Keys )
    {
        [string]$keyString = $key
        $value = $config[$key]
        if ( $keyString.StartsWith("yarn.scheduler.capacity", "CurrentCultureIgnoreCase") )
        {
            $newCapSchedulerConfig.Add($key, $value) > $null
        }
        else
        {
            $newConfig.Add($key, $value) > $null
        }
    }

    UpdateXmlConfig $fileName $newCapSchedulerConfig > $null
    return $newConfig
}

###############################################################################
###
### Alters the configuration of the Hadoop MapRed component.
###
### Arguments:
###   See ConfigureCore
###############################################################################
function ConfigureMapRed(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$true )]
    $serviceCredential,
    [hashtable]
    [parameter( Position=2 )]
    $configs = @{},
    [bool]
    [parameter( Position=3 )]
    $aclAllFolders = $True
    )
{
    $username = $serviceCredential.UserName

    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-$HadoopCoreVersion.winpkg.log" $ENV:WINPKG_BIN
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"

    if( -not (Test-Path $hadoopInstallToDir ))
    {
        throw "ConfigureMapRed: InstallCore and InstallMapRed must be called before ConfigureMapRed"
    }

    ###
    ### Apply configuration changes to mapred-site.xml
    ###
    $xmlFile = Join-Path $hadoopInstallToDir "etc\hadoop\mapred-site.xml"
    UpdateXmlConfig $xmlFile $configs

    if ($aclAllFolders)
    {
        ###
        ### ACL all folders
        ###
        AclFoldersForUser $xmlFile $username $MapRedPropertyFolderList
    }
    else
    {
        ###
        ### ACL only folders that were modified as part of the config updates
        ###
        foreach( $key in empty-null $config.Keys )
        {
            $folderList = @()
            if ($MapRedPropertyFolderList -contains $key)
            {
                $folderList = $folderList + @($key)
            }

            AclFoldersForUser $xmlFile $username $folderList
        }
    }
}

###############################################################################
###
### Alters the configuration of the Hadoop YARN component.
###
### Arguments:
###   See ConfigureCore
###############################################################################
function ConfigureYarn(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=1, Mandatory=$true )]
    $serviceCredential,
    [hashtable]
    [parameter( Position=2 )]
    $configs = @{},
    [bool]
    [parameter( Position=3 )]
    $aclAllFolders = $True
    )
{
    $username = $serviceCredential.UserName

    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-$HadoopCoreVersion.winpkg.log" $ENV:WINPKG_BIN
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"

    if( -not (Test-Path $hadoopInstallToDir ))
    {
        throw "ConfigureYarn: InstallCore and InstallYarn must be called before ConfigureYarn"
    }

    ###
    ### Apply capacity-scheduler.xml configuration changes. All such configuration properties
    ### have "yarn.scheduler.capacity" prefix, so it is easy to properly separate them out
    ### from other Hadoop configs.
    ###
    $capacitySchedulerXmlFile = Join-Path $hadoopInstallToDir "etc\hadoop\capacity-scheduler.xml"
    [hashtable]$configs = ConfigureCapacityScheduler $capacitySchedulerXmlFile $configs

    ###
    ### Apply configuration changes to yarn-site.xml
    ###
    $xmlFile = Join-Path $hadoopInstallToDir "etc\hadoop\yarn-site.xml"
    UpdateXmlConfig $xmlFile $configs

    if ($aclAllFolders)
    {
        ###
        ### ACL all folders
        ###
        AclFoldersForUser $xmlFile $username $YarnPropertyFolderList
    }
    else
    {
        ###
        ### ACL only folders that were modified as part of the config updates
        ###
        foreach( $key in empty-null $config.Keys )
        {
            $folderList = @()
            if ($YarnPropertyFolderList -contains $key)
            {
                $folderList = $folderList + @($key)
            }

            AclFoldersForUser $xmlFile $username $folderList
        }
    }
}

###############################################################################
###
### Installs Hadoop component.
###
### Arguments:
###     component: Component to be installed, it can be "core, "hdfs" or "mapreduce"
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     role: Space separated list of roles that should be installed.
###           (for example, "resourcemanager historyserver" for mapreduce)
###
###############################################################################
function Install(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$false )]
    $serviceCredential,
    [String]
    [Parameter( Position=3, Mandatory=$false )]
    $role
    )
{
    if ( $component -eq "core" )
    {
        Write-log "nodeInstallRoto = $nodeInstallRoot serviceCredential = $serviceCredential"
        InstallCore $nodeInstallRoot $serviceCredential
    }
    elseif ( $component -eq "hdfs" )
    {
        if ( $serviceCredential -eq $null)
        {
            Write-Log "Install: Virtual account is enabled for HDFS"
        }
        InstallHdfs $nodeInstallRoot $serviceCredential $role
    }
    elseif ( $component -eq "mapreduce" )
    {
        InstallMapRed $nodeInstallRoot $serviceCredential $role
    }
    elseif ( $component -eq "yarn" )
    {
        InstallYarn $nodeInstallRoot $serviceCredential $role
    }
    else
    {
        throw "Install: Unsupported compoment argument."
    }
}

###############################################################################
###
### Uninstalls Hadoop component.
###
### Arguments:
###     component: Component to be uninstalled, it can be "core, "hdfs" or "mapreduce"
###     nodeInstallRoot: Install folder (for example "C:\Hadoop")
###
###############################################################################
function Uninstall(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot,
    [bool]
    [Parameter( Position=2, Mandatory=$false )]
    $destroyData = $false)
{
    if ( $component -eq "core" )
    {
        UninstallCore $nodeInstallRoot $destroyData
    }
    elseif ( $component -eq "hdfs" )
    {
        UninstallHdfs $nodeInstallRoot  $destroyData
    }
    elseif ( $component -eq "mapreduce" )
    {
        UninstallMapRed $nodeInstallRoot $destroyData
    }
    elseif ( $component -eq "yarn" )
    {
        UninstallYarn $nodeInstallRoot $destroyData
    }
    else
    {
        throw "Uninstall: Unsupported compoment argument."
    }
}

###############################################################################
###
### Alters the configuration of the Hadoop Core component.
###
### Arguments:
###     component: Component to be configured, it can be "core, "hdfs" or "mapreduce"
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     configs: Configuration that should be applied.
###              For example, @{"dfs.namenode.checkpoint.edits.dir" = "C:\Hadoop\hdfs\2nne"}
###              Some configuration parameter are aliased, see ProcessAliasConfigOptions
###              for details.
###     aclAllFolders: If true, all folders defined in core-site.xml will be ACLed
###                    If false, only the folders listed in configs will be ACLed.
###
###############################################################################
function Configure(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$false )]
    $serviceCredential,
    [hashtable]
    [parameter( Position=3 )]
    $configs = @{},
    [bool]
    [parameter( Position=4 )]
    $aclAllFolders = $True
    )
{
    ### Process alias config options first
    $configs = ProcessAliasConfigOptions $component $configs

    if ( $component -eq "core" )
    {
        ConfigureCore $nodeInstallRoot $serviceCredential $configs $aclAllFolders
    }
    elseif ( $component -eq "hdfs" )
    {
        if ( $serviceCredential -eq $null )
        {
            Write-Log "Configure: Virtual account is enabled for HDFS"
        }
        ConfigureHdfs $nodeInstallRoot $serviceCredential $configs $aclAllFolders
    }
    elseif ( $component -eq "mapreduce" )
    {
        ConfigureMapRed $nodeInstallRoot $serviceCredential $configs $aclAllFolders
    }
    elseif ( $component -eq "yarn" )
    {
        ConfigureYarn $nodeInstallRoot $serviceCredential $configs $aclAllFolders
    }
    else
    {
        throw "Configure: Unsupported compoment argument."
    }
}

###############################################################################
###
### Alters the configuration of the Hadoop Core component.
###
### Arguments:
###     component: Component to be configured, it can be "core, "hdfs" or "mapreduce"
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     configFilePath: Configuration that will be copied to $HADOOP_HOME\conf
###
###############################################################################
function ConfigureWithFile(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$false )]
    $serviceCredential,
    [String]
    [parameter( Position=3 )]
    $configFilePath
    )
{
    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $ScriptDir "hadoop-$HadoopCoreVersion.winpkg.log" $ENV:WINPKG_BIN
    $hadoopInstallToDir = Join-Path "$nodeInstallRoot" "hadoop-$HadoopCoreVersion"

    ### TODO: We need additional checks on the input params.

    ### Copy over the given file
    $xcopy_cmd = "xcopy /IYF `"$configFilePath`" `"$hadoopInstallToDir\etc\hadoop`""
    Invoke-CmdChk $xcopy_cmd

    if ( $component -eq "core" )
    {
        ConfigureCore $nodeInstallRoot $serviceCredential
    }
    elseif ( $component -eq "hdfs" )
    {
        if ( $serviceCredential -eq $null)
        {
            Write-Log "ConfigureWithFile: Virtual account is enabled for HDFS"
        }
        ConfigureHdfs $nodeInstallRoot $serviceCredential
    }
    elseif ( $component -eq "mapreduce" )
    {
        ConfigureMapRed $nodeInstallRoot $serviceCredential
    }
    elseif ( $component -eq "yarn" )
    {
        ConfigureYarn $nodeInstallRoot $serviceCredential
    }
    else
    {
        throw "Configure: Unsupported compoment argument."
    }
}

###############################################################################
###
### Start component services.
###
### Arguments:
###     component: Component name
###     roles: List of space separated service to start
###
###############################################################################
function StartService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $roles
    )
{
    Write-Log "Starting `"$component`" `"$roles`" services"

    if ( $component -eq "core" )
    {
        Write-Log "StartService: Hadoop Core does not have any services"
    }
    elseif ( $component -eq "hdfs" )
    {
        ### Verify that roles are in the supported set
        CheckRole $roles @("namenode","datanode","secondarynamenode")

        foreach ( $role in $roles.Split(" ") )
        {
            Write-Log "Starting $role service"
            Start-Service $role
        }
    }
    elseif ( $component -eq "mapred" )
    {
        ### Verify that roles are in the supported set
        CheckRole $roles @("historyserver")

        foreach ( $role in $roles.Split(" ") )
        {
            Write-Log "Starting $role service"
            Start-Service $role
        }
    }
    elseif ( $component -eq "yarn" )
    {
        ### Verify that roles are in the supported set
        CheckRole $roles @("resourcemanager","nodemanager")

        foreach ( $role in $roles.Split(" ") )
        {
            Write-Log "Starting $role service"
            Start-Service $role
        }
    }
    else
    {
        throw "StartService: Unsupported compoment argument."
    }
}

###############################################################################
###
### Stop component services.
###
### Arguments:
###     component: Component name
###     roles: List of space separated service to stop
###
###############################################################################
function StopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $roles
    )
{
    Write-Log "Stopping `"$component`" `"$roles`" services"

    if ( $component -eq "core" )
    {
        Write-Log "StopService: Hadoop Core does not have any services"
    }
    elseif ( $component -eq "hdfs" )
    {
        ### Verify that roles are in the supported set
        CheckRole $roles @("namenode","datanode","secondarynamenode")

        foreach ( $role in $roles -Split("\s+") )
        {
            try
            {
                Write-Log "Stopping $role "
                if (Get-Service "$role" -ErrorAction SilentlyContinue)
                {
                    Write-Log "Service $role exists, stopping it"
                    Stop-Service $role
                }
                else
                {
                Write-Log "Service $role does not exist, moving to next"
                }
            }
            catch [Exception]
            {
                Write-Host "Can't stop service $role"
            }
        }
    }
    elseif ( $component -eq "mapreduce" )
    {
        ### Verify that roles are in the supported set
        CheckRole $roles @("historyserver")

        foreach ( $role in $roles -Split("\s+") )
        {
            try
            {
                Write-Log "Stopping $role "
                if (Get-Service "$role" -ErrorAction SilentlyContinue)
                {
                    Write-Log "Service $role exists, stopping it"
                    Stop-Service $role
                }
                else
                {
                Write-Log "Service $role does not exist, moving to next"
                }
            }
            catch [Exception]
            {
                Write-Host "Can't stop service $role"
            }
        }
    }
    elseif ( $component -eq "yarn" )
    {
        ### Verify that roles are in the supported set
        CheckRole $roles @("resourcemanager","nodemanager")

        foreach ( $role in $roles -Split("\s+") )
        {
            try
            {
                Write-Log "Stopping $role "
                if (Get-Service "$role" -ErrorAction SilentlyContinue)
                {
                    Write-Log "Service $role exists, stopping it"
                    Stop-Service $role
                }
                else
                {
                Write-Log "Service $role does not exist, moving to next"
                }
            }
            catch [Exception]
            {
                Write-Host "Can't stop service $role"
            }
        }
    }
    else
    {
        throw "StartService: Unsupported compoment argument."
    }
}

###############################################################################
###
### Formats the namenode.
###
### Arguments:
###
###############################################################################
function FormatNamenode(
    [bool]
    [Parameter( Position=0, Mandatory=$false )]
    $force = $false)
{
    Write-Log "Formatting Namenode"

    if ( -not ( Test-Path ENV:HADOOP_HOME ) )
    {
        throw "FormatNamenode: HADOOP_HOME not set"
    }
    Write-Log "HADOOP_HOME set to `"$env:HADOOP_HOME`""

    if ( $force )
    {
        $cmd = "echo Y | $ENV:HADOOP_HOME\bin\hdfs.cmd namenode -format"
    }
    else
    {
        $cmd = "$ENV:HADOOP_HOME\bin\hdfs.cmd namenode -format"
    }
    Invoke-CmdChk $cmd
}

###
### Public API
###
Export-ModuleMember -Function Install
Export-ModuleMember -Function Uninstall
Export-ModuleMember -Function Configure
Export-ModuleMember -Function ConfigureWithFile
Export-ModuleMember -Function StartService
Export-ModuleMember -Function StopService
Export-ModuleMember -Function FormatNamenode
Export-ModuleMember -Function CheckDataDirectories
Export-ModuleMember -Function Get-AppendedPath

###
### Private API (exposed for test only)
###
Export-ModuleMember -Function UpdateXmlConfig
Export-ModuleMember -Function ProcessAliasConfigOptions
