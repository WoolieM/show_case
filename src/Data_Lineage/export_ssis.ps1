## Refernce: https://github.com/sqlrider/Powershell-SSIS/blob/master/Export-SSISProjectSSISDB.ps1
Import-Module SqlServer

function Export-SSISProjectSSISDB
{
    param
    (
        [Parameter(position=0, mandatory=$true)][string]$Instance,
        [Parameter(position=1, mandatory=$true)][string]$OutputDir
    )

    Write-Output "Starting export process..."

    if(!(Test-Path $OutputDir))
    {
        Write-Output "Error - invalid path specified in OutputDir"
        return
    }

    Write-Output "Output directory is valid."

    $testquery = "SELECT COUNT(*) AS 'Result' FROM sys.databases WHERE name = 'SSISDB'"

    try
    {
        Write-Output "Connecting to SQL Server instance $Instance..."
        $result = (Invoke-Sqlcmd -ServerInstance $Instance -Query $testquery -ConnectionTimeout 5 -QueryTimeout 5 -ErrorAction Stop -TrustServerCertificate).Result
        Write-Output "Connection successful."

        if($result -eq 0)
        {
            Write-Output "Error - no SSISDB present on instance or no permission to view it"
            return
        }
    }
    catch
    {
        Write-Output "Error - failure connecting to instance"
        return
    }

    Write-Output "Loading SSIS assembly..."
    [System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Management.IntegrationServices") | Out-Null

    $SSISnamespace = "Microsoft.SqlServer.Management.IntegrationServices"

    $connstring = "Data source=$($Instance);Initial Catalog=master;Integrated Security=SSPI;"
    $sqlconn = New-Object System.Data.SqlClient.SqlConnection $connstring

    Write-Output "Creating SSIS object..."
    $SSIS = New-Object $SSISnamespace".IntegrationServices" $sqlconn

    $catalog = $SSIS.Catalogs["SSISDB"]

    foreach($folder in $catalog.Folders)
    {
        Write-Output "Processing folder: $($folder.Name)"
        Set-Location -Path $OutputDir

        New-Item -ItemType Directory -Name $folder.Name -Force | Out-Null

        $folderpath = $OutputDir + "\" + $folder.Name

        Set-Location -path $folderpath

        $projects = $folder.Projects

        if($projects.Count -gt 0)
        {
            foreach($project in $projects)
            {
                $projectpath = $folderpath + "\" + $project.Name + ".ispac"
                Write-Output "Exporting project: $($project.Name) to $($projectpath)"
                [System.IO.File]::WriteAllBytes($projectpath, $project.GetProjectBytes())
            }
        }
        else
        {
            Write-Output "No projects found in folder: $($folder.Name)"
        }
    }

    Write-Output "Export process completed."
    Set-Location -Path $OutputDir
}

Export-SSISProjectSSISDB -Instance 'cso-sql01' -OutputDir 'C:\Users\wchen\Documents\ssis export'