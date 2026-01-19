# Call the script with the path to the project directory as an argument:
#     .\publish-packages.ps1 "C:\Projects\sttp\gsfapi"

param(
    [string]$projectDir,
    [string]$buildConfig = "Release"
)

# Script Constants
Set-Variable sttpVersionFile -Option Constant -Scope Script -Value "build\scripts\sttp.version"
Set-Variable libBuildFolder  -Option Constant -Scope Script -Value "build\$buildConfig"

# Script Functions

function Publish-Package([string]$package) {
    Invoke-Command -ScriptBlock {
        # Sign NuGet package
        if ($env:NuGetCertFingerprint -ne $null) {
            # Prime the certificate store to avoid issues with signing
            certutil -scinfo | Out-Null     
            & dotnet nuget sign $using:package --certificate-fingerprint $env:NuGetCertFingerprint --timestamper http://timestamp.digicert.com
            if ($LASTEXITCODE -ne 0) { throw "dotnet nuget sign failed ($LASTEXITCODE) for: $using:package" }
        }

        # Push package to NuGet
        if ($env:GemstoneNuGetApiKey) {
            Write-Host "Pushing package to NuGet..."
            & dotnet nuget push $using:package -k $env:GemstoneNuGetApiKey --skip-duplicate -s "https://api.nuget.org/v3/index.json"
            if ($LASTEXITCODE -ne 0) { Write-Warning "NuGet push failed ($LASTEXITCODE) for: $using:package" }
        }
    } | Write-Host
}

# --------- Start Script ---------

$versionPath = Join-Path $projectDir $sttpVersionFile
$buildPath   = Join-Path $projectDir $libBuildFolder

# Get current STTP version
$version = (Get-Content -Path "$versionPath" -TotalCount 1).Trim()

Write-Host "Current STTP Library version = $version"

# Query file system for package files to get proper casing
$packages = [IO.Directory]::GetFiles("$buildPath", "*.$version.nupkg")

if ($packages.Length -eq 0) {
    Write-Host "WARNING: No STTP v$version package found in $buildPath, build failure? No packages pushed."
}

foreach ($package in $packages) {
    Publish-Package $package
}
