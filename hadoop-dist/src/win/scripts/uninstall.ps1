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

function Main
{
    if ( -not (Test-Path ENV:WINPKG_LOG))
    {
        $ENV:WINPKG_LOG = "hadoop.core.winpkg.log"
    }

    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "hadoop-@version@.winpkg.log"
    $nodeInstallRoot = "$ENV:HADOOP_NODE_INSTALL_ROOT"

    if ( -not (Test-Path ENV:HDFS_DATA_DIR))
    {
        $ENV:HDFS_DATA_DIR = Join-Path "$ENV:HADOOP_NODE_INSTALL_ROOT" "hdfs"
    }
    $forceclean = $false
    if ( (-not (Test-Path ENV:DESTROY_DATA)) -or ($ENV:DESTROY_DATA -eq "yes") ) {
        $forceclean = $true
    }
    ###
    ### Uninstall MapRed, Hdfs and Core
    ###
    Uninstall "MapReduce" $nodeInstallRoot
    Uninstall "Yarn" $nodeInstallRoot
    Uninstall "Hdfs" $nodeInstallRoot
    Uninstall "Core" $nodeInstallRoot

    ###
    ### Cleanup any remaining content under HDFS data dir
    ###
    Write-Log "Removing HDFS_DATA_DIR `"$ENV:HDFS_DATA_DIR`""
    foreach ($folder in ${ENV:HDFS_DATA_DIR}.Split(","))
    {
        $folder = $folder.Trim()
        if ( ($folder -ne $null) -and (Test-Path "$folder\*") )
        {
            if ($forceclean -eq $true)
            {
                $cmd = "rd /s /q `"$folder`""
                Invoke-Cmd $cmd
            }
        }
    }

    Write-Log "Uninstall of Hadoop Core, HDFS, MapRed completed successfully"
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
	Write-Log "Continuing anyways..."
}
finally
{
    if( $utilsModule -ne $null )
    {
        Remove-Module $apiModule
        Remove-Module $utilsModule
    }
}
