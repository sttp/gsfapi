//******************************************************************************************************
//  MetadataSynchronizer.cs - Gbtc
//
//  Copyright © 2026, Grid Protection Alliance.  All Rights Reserved.
//
//  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements. See
//  the NOTICE file distributed with this work for additional information regarding copyright ownership.
//  The GPA licenses this file to you under the MIT License (MIT), the "License"; you may
//  not use this file except in compliance with the License. You may obtain a copy of the License at:
//
//      http://www.opensource.org/licenses/MIT
//
//  Unless agreed to in writing, the subject software distributed under the License is distributed on an
//  "AS-IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. Refer to the
//  License for the specific language governing permissions and limitations.
//
//  Code Modification History:
//  ----------------------------------------------------------------------------------------------------
//  08/13/2026 - J. Ritchie Carroll
//       Generated original version of source code.
//
//******************************************************************************************************

namespace sttp;

/// <summary>
/// Orchestrates synchronization of a received meta-data <see cref="DataSet"/> into the local
/// configuration database.
/// </summary>
/// <remarks>
/// This type owns the ordering of the individual table synchronization operations - devices must be
/// synchronized first since both measurements and phasors are associated with them through
/// <see cref="MetadataSyncContext.DeviceIDs"/>.
/// </remarks>
internal static class MetadataSynchronizer
{
    /// <summary>
    /// Synchronizes the supplied meta-data into the local configuration database.
    /// </summary>
    /// <param name="context">Synchronization context, already associated with an open connection and command.</param>
    /// <param name="metadata">Received meta-data to synchronize.</param>
    /// <param name="runtimeID">Run-time ID of the owning data subscriber adapter.</param>
    /// <returns><c>true</c> if synchronization was performed; otherwise, <c>false</c>.</returns>
    public static bool Synchronize(MetadataSyncContext context, DataSet metadata, int runtimeID)
    {
        if (!LoadSubscriberDeviceInfo(context, runtimeID))
            return false;

        // Determine whether the SQL Server bulk insert path may be used. Databases other than SQL Server simply
        // do not qualify and that is not worth reporting, but a SQL Server connection that declines the path has
        // a specific reason the operator should see.
        if (context.UseBulkLoad && context.Database.IsSQLServer)
        {
            context.BulkLoadEnabled = SqlServerBulkInsert.IsSupported(context, out string? declineReason);

            if (!context.BulkLoadEnabled)
                context.BulkLoadStatus = $"Bulk meta-data loading was requested but is not being used: {declineReason}.";
        }

        // Ascertain total number of actions required for all meta-data synchronization so some level feed back can be provided on progress
        context.InitProgress(metadata.Tables.Cast<DataTable>().Select(dataTable => (long)dataTable.Rows.Count).Sum() + 3);

        Ticks phaseStartTime = DateTime.UtcNow.Ticks;

        // Check to see if data for the "DeviceDetail" table was included in the meta-data
        if (metadata.Tables.Contains("DeviceDetail"))
            DeviceMetadataSync.Synchronize(context, metadata.Tables["DeviceDetail"]!);

        context.DeviceSyncTime = DateTime.UtcNow.Ticks - phaseStartTime;
        phaseStartTime = DateTime.UtcNow.Ticks;

        // Check to see if data for the "MeasurementDetail" table was included in the meta-data
        if (metadata.Tables.Contains("MeasurementDetail"))
            MeasurementMetadataSync.Synchronize(context, metadata.Tables["MeasurementDetail"]!);

        context.MeasurementSyncTime = DateTime.UtcNow.Ticks - phaseStartTime;
        phaseStartTime = DateTime.UtcNow.Ticks;

        // Check to see if data for the "PhasorDetail" table was included in the meta-data
        if (metadata.Tables.Contains("PhasorDetail"))
            PhasorMetadataSync.Synchronize(context, metadata.Tables["PhasorDetail"]!);

        context.PhasorSyncTime = DateTime.UtcNow.Ticks - phaseStartTime;

        return true;
    }

    /// <summary>
    /// Resolves the local device record that represents this subscriber connection, along with the values
    /// derived from it that the table synchronization operations depend upon.
    /// </summary>
    /// <returns><c>false</c> when no subscriber device record could be resolved, in which case synchronization is skipped.</returns>
    private static bool LoadSubscriberDeviceInfo(MetadataSyncContext context, int runtimeID)
    {
        // Query the actual record ID based on the known run-time ID for this subscriber device
        object? sourceID = context.ExecuteScalar($"SELECT SourceID FROM Runtime WHERE ID = {runtimeID} AND SourceTable='Device'");

        if (sourceID is null || sourceID == DBNull.Value)
            return false;

        context.ParentID = Convert.ToInt32(sourceID);

        // Validate that the subscriber device is marked as a concentrator (we are about to associate children devices with it)
        if (!(context.ExecuteScalar($"SELECT IsConcentrator FROM Device WHERE ID = {context.ParentID}")?.ToString() ?? "false").ParseBoolean())
            context.ExecuteNonQuery($"UPDATE Device SET IsConcentrator = 1 WHERE ID = {context.ParentID}");

        // Get any historian associated with the subscriber device
        context.HistorianID = context.ExecuteScalar($"SELECT HistorianID FROM Device WHERE ID = {context.ParentID}");

    #if !NET
        // Determine the active node ID - we cache this since this value won't change for the lifetime of the owning class
        if (context.NodeID == Guid.Empty)
            context.NodeID = Guid.Parse(context.ExecuteScalar($"SELECT NodeID FROM IaonInputAdapter WHERE ID = {runtimeID}")?.ToString() ?? Guid.Empty.ToString());

        // Determine the protocol record auto-inc ID value for STTP - this value is also cached since it shouldn't change for the lifetime of the owning class
        if (context.ProtocolID == 0)
            context.ProtocolID = int.Parse(context.ExecuteScalar("SELECT ID FROM Protocol WHERE Acronym='STTP'")?.ToString() ?? "0");
    #endif

        // Devices synchronized independently track ownership through the text-based 'OriginalSource' field rather
        // than the integer 'ParentID' field, since they are not parented to the subscriber device record
        context.ParentColumn = context.SyncIndependentDevices ? "OriginalSource" : "ParentID";
        context.ParentIDValue = context.SyncIndependentDevices ? context.ParentID.ToString() : context.ParentID;

        return true;
    }
}
