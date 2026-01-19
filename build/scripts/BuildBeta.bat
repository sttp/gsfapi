::*******************************************************************************************************
::  BuildBeta.bat - Gbtc
::
::  Tennessee Valley Authority, 2009
::  No copyright is claimed pursuant to 17 USC § 105.  All Other Rights Reserved.
::
::  This software is made freely available under the TVA Open Source Agreement (see below).
::
::  Code Modification History:
::  -----------------------------------------------------------------------------------------------------
::  10/05/2009 - Pinal C. Patel
::       Generated original version of source code.
::  10/20/2009 - Pinal C. Patel
::       Modified to force a build and suppress archives from being published to public locations.
::  10/03/2010 - Pinal C. Patel
::       Updated to use MSBuild 4.0.
::
::*******************************************************************************************************

@ECHO OFF

SetLocal

IF NOT "%1" == "" SET "logFlag=/fl /flp:logfile=%~1;verbosity=diagnostic;encoding=UTF-8;append=false"

REM Pick first installed 9.0 SDK and pin it for this repo
for /f "delims=" %%V in ('dotnet --list-sdks ^| findstr /r "^9\.[0-9][0-9]*\."') do (
  set "SDK9=%%V"
  goto :got9
)

:got9
for /f "tokens=1" %%V in ("%SDK9%") do set "SDK9VER=%%V"

echo Pinning SDK %SDK9VER% via global.json in repo root...
> "C:\Users\buildbot\Projects\sttp-gsfapi\global.json" (
  echo { "sdk": { "version": "%SDK9VER%", "rollForward": "latestFeature" } }
)

ECHO BuildBeta: dotnet msbuild sttp.buildproj /p:ForceBuild=true %logflag%
dotnet msbuild sttp.buildproj /p:ForceBuild=true %logFlag%

 CALL PowerShell -NoProfile -ExecutionPolicy ByPass -File ".\publish-packages.ps1" "C:\Users\buildbot\Projects\sttp-gsfapi"
