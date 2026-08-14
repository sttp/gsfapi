//******************************************************************************************************
//  MeasurementMetadataSync.cs - Gbtc
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
/// Synchronizes the <c>MeasurementDetail</c> meta-data table into the local <c>Measurement</c> table.
/// </summary>
internal static class MeasurementMetadataSync
{
    /// <summary>
    /// Synchronizes measurement records for all devices captured in <see cref="MetadataSyncContext.DeviceIDs"/>.
    /// </summary>
    public static void Synchronize(MetadataSyncContext context, DataTable measurementDetail)
    {
        AdoDataConnection database = context.Database;

        // Load signal type ID's from local database associated with their acronym for proper signal type translation
        foreach (DataRow row in context.RetrieveData("SELECT ID, Acronym FROM SignalType").Rows)
        {
            string? signalTypeAcronym = row.Field<string>("Acronym");

            if (!string.IsNullOrWhiteSpace(signalTypeAcronym))
                context.SignalTypeIDs[signalTypeAcronym] = row.ConvertField<int>("ID");
        }

        // Determine which measurement rows should be synchronized based on operational mode flags
        DataRow[] measurementRows;

        if (context.ReceiveInternalMetadata && context.ReceiveExternalMetadata)
            measurementRows = measurementDetail.Select();
        else if (context.ReceiveInternalMetadata)
            measurementRows = measurementDetail.Select("Internal <> 0");
        else if (context.ReceiveExternalMetadata)
            measurementRows = measurementDetail.Select("Internal = 0");
        else
            measurementRows = [];

        // Check existence of optional meta-data fields
        DataColumnCollection columns = measurementDetail.Columns;
        bool phasorSourceIndexFieldExists = columns.Contains("PhasorSourceIndex");
        bool updatedOnFieldExists = columns.Contains("UpdatedOn");
        bool alternateTagFieldExists = columns.Contains("AlternateTag");

        // Define the batched statements used while applying measurement changes. Note that the guid-based signal ID
        // is supplied directly on insert. The previous implementation inserted a temporary value into the alternate
        // tag field and then issued a corrective 'UPDATE ... WHERE AlternateTag = <temp guid>' - that column is
        // large-object typed and cannot be indexed, so every new measurement cost a full table scan.
        InsertBatch insertMeasurements = new(context,
            "INSERT INTO Measurement(SignalID, DeviceID, HistorianID, PointTag, AlternateTag, SignalTypeID, PhasorSourceIndex, SignalReference, Description, Internal, Subscribed, Enabled)",
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 1");

        InsertBatch identityInsertMeasurements = new(context,
            "INSERT INTO Measurement(PointID, SignalID, DeviceID, HistorianID, PointTag, AlternateTag, SignalTypeID, PhasorSourceIndex, SignalReference, Description, Internal, Subscribed, Enabled)",
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 1");

        StatementBatch updateMeasurements = new(context,
            "UPDATE Measurement SET HistorianID = ?, PointTag = ?, AlternateTag = ?, SignalTypeID = ?, PhasorSourceIndex = ?, SignalReference = ?, Description = ?, Internal = ? WHERE SignalID = ?");

        StatementBatch identityUpdateMeasurements = new(context,
            "UPDATE Measurement SET DeviceID = ?, HistorianID = ?, PointTag = ?, AlternateTag = ?, SignalTypeID = ?, PhasorSourceIndex = ?, SignalReference = ?, Description = ?, Internal = ?, Subscribed = 0, Enabled = 1, SignalID = ? WHERE PointID = ?");

        // Collect the signal IDs, and optionally point IDs, that this synchronization pass will touch
        List<Guid> metadataSignalIDs = [];
        List<long> metadataPointIDs = [];

        foreach (DataRow row in measurementRows)
        {
            string deviceAcronym = row.Field<string>("DeviceAcronym") ?? string.Empty;
            string signalTypeAcronym = row.Field<string>("SignalAcronym") ?? string.Empty;

            if (string.IsNullOrWhiteSpace(deviceAcronym) || !context.DeviceIDs.ContainsKey(deviceAcronym) || string.IsNullOrWhiteSpace(signalTypeAcronym) || !context.SignalTypeIDs.ContainsKey(signalTypeAcronym))
                continue;

            metadataSignalIDs.Add(row.ConvertGuidField("SignalID"));

            if (context.UseIdentityInserts && MeasurementKey.TryParse(row.Field<string>("ID")!, out MeasurementKey measurementKey))
                metadataPointIDs.Add((long)measurementKey.ID);
        }

        // Load existing records in bulk, replacing the per-row existence probes
        HashSet<Guid> existingSignalIDs = LoadExistingSignalIDs(context, metadataSignalIDs);
        HashSet<long> existingPointIDs = context.UseIdentityInserts ? LoadExistingPointIDs(context, metadataPointIDs) : [];

        object phasorSourceIndex = DBNull.Value;
        object alternateTag = DBNull.Value;
        List<Guid> signalIDs = [];

        if (context.UseIdentityInserts && database.IsSQLServer)
            context.ExecuteNonQuery("SET IDENTITY_INSERT Measurement ON");

        try
        {
            foreach (DataRow row in measurementRows)
            {
                bool recordNeedsUpdating = context.RecordNeedsUpdating(row, updatedOnFieldExists);

                // Get device and signal type acronyms
                string deviceAcronym = row.Field<string>("DeviceAcronym") ?? string.Empty;
                string signalTypeAcronym = row.Field<string>("SignalAcronym") ?? string.Empty;

                // Get phasor source index if field is defined
                if (phasorSourceIndexFieldExists)
                {
                    // Using ConvertNullableField extension since publisher could use SQLite database in which case
                    // all integers would arrive in data set as longs and need to be converted back to integers
                    int? index = row.ConvertNullableField<int>("PhasorSourceIndex");
                    phasorSourceIndex = index ?? (object)DBNull.Value;
                }

                // Get alternate tag if field is defined
                if (alternateTagFieldExists)
                    alternateTag = row.Field<string>("AlternateTag") ?? (object)DBNull.Value;

                // Make sure we have an associated device and signal type already defined for the measurement
                if (!string.IsNullOrWhiteSpace(deviceAcronym) && context.DeviceIDs.ContainsKey(deviceAcronym) && !string.IsNullOrWhiteSpace(signalTypeAcronym) && context.SignalTypeIDs.ContainsKey(signalTypeAcronym))
                {
                    Guid signalID = row.ConvertGuidField("SignalID");

                    // Track unique measurement signal Guids in this meta-data session, we'll need to remove any old associated measurements that no longer exist
                    signalIDs.Add(signalID);

                    // Prefix the tag name with the "updated" device name
                    string pointTag = context.SourcePrefix + row.Field<string>("PointTag");

                    // Look up associated device ID (local DB auto-inc)
                    int deviceID = context.DeviceIDs[deviceAcronym];
                    int signalTypeID = context.SignalTypeIDs[signalTypeAcronym];
                    string signalReference = context.SourcePrefix + row.Field<string>("SignalReference");
                    string description = row.Field<string>("Description") ?? string.Empty;

                    // Determine if measurement record already exists
                    if (!existingSignalIDs.Contains(signalID))
                    {
                        // Insert new measurement record
                        if (context.UseIdentityInserts && MeasurementKey.TryParse(row.Field<string>("ID")!, out MeasurementKey measurementKey))
                        {
                            long pointID = (long)measurementKey.ID;

                            if (!existingPointIDs.Contains(pointID))
                                identityInsertMeasurements.Add(pointID, database.Guid(signalID), deviceID, context.HistorianID, pointTag, alternateTag, signalTypeID, phasorSourceIndex, signalReference, description, database.Bool(context.Internal));
                            else
                                identityUpdateMeasurements.Add(deviceID, context.HistorianID, pointTag, alternateTag, signalTypeID, phasorSourceIndex, signalReference, description, database.Bool(context.Internal), database.Guid(signalID), pointID);
                        }
                        else
                        {
                            insertMeasurements.Add(database.Guid(signalID), deviceID, context.HistorianID, pointTag, alternateTag, signalTypeID, phasorSourceIndex, signalReference, description, database.Bool(context.Internal));
                        }
                    }
                    else if (recordNeedsUpdating)
                    {
                        // Update existing measurement record. Note that this update assumes that measurements will remain associated with a static source device.
                        updateMeasurements.Add(context.HistorianID, pointTag, alternateTag, signalTypeID, phasorSourceIndex, signalReference, description, database.Bool(context.Internal), database.Guid(signalID));
                    }
                }

                // Periodically notify user about synchronization progress
                context.UpdateProgress();
            }

            // Pending rows must reach the database before identity inserts are disabled below, and before the
            // retirement pass reads back the current measurement set
            insertMeasurements.Flush();
            identityInsertMeasurements.Flush();
            updateMeasurements.Flush();
            identityUpdateMeasurements.Flush();
        }
        finally
        {
            if (context.UseIdentityInserts && database.IsSQLServer)
                context.ExecuteNonQuery("SET IDENTITY_INSERT Measurement OFF");
        }

        // Remove any measurement records associated with existing devices in this session but no longer exist in the meta-data
        if (signalIDs.Count > 0)
        {
            RemoveRetiredMeasurements(context, signalIDs);
            context.UpdateProgress();
        }
    }

    /// <summary>
    /// Loads the set of signal IDs that already exist in the local database.
    /// </summary>
    private static HashSet<Guid> LoadExistingSignalIDs(MetadataSyncContext context, List<Guid> signalIDs)
    {
        HashSet<Guid> existing = [];

        foreach (Guid[] chunk in MetadataSyncContext.Chunk(signalIDs))
        {
            string querySql = context.BuildInListQuery("SELECT SignalID FROM Measurement WHERE SignalID IN (", chunk.Length, ")", "signalID");

            foreach (DataRow row in context.RetrieveData(querySql, chunk.Select(signalID => context.Database.Guid(signalID)).ToArray()).Rows)
                existing.Add(row.ConvertGuidField("SignalID"));
        }

        return existing;
    }

    /// <summary>
    /// Loads the set of point IDs that already exist in the local database, used by the identity insert path.
    /// </summary>
    private static HashSet<long> LoadExistingPointIDs(MetadataSyncContext context, List<long> pointIDs)
    {
        HashSet<long> existing = [];

        foreach (long[] chunk in MetadataSyncContext.Chunk(pointIDs))
        {
            string querySql = context.BuildInListQuery("SELECT PointID FROM Measurement WHERE PointID IN (", chunk.Length, ")", "pointID");

            foreach (DataRow row in context.RetrieveData(querySql, chunk.Select(pointID => (object)pointID).ToArray()).Rows)
                existing.Add(Convert.ToInt64(row["PointID"]));
        }

        return existing;
    }

    /// <summary>
    /// Removes measurement records that are associated with synchronized devices but no longer appear in the meta-data.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The previous implementation queried the <c>ActiveMeasurement</c> view - an eleven table join that includes a
    /// cross join - and then issued a separate query per candidate row to discover its associated device. Both are
    /// replaced here by a single restricted query against the <c>Measurement</c> table.
    /// </para>
    /// <para>
    /// The enabled-state restrictions reproduce the filtering that the <c>ActiveMeasurement</c> view applied, so that
    /// measurements belonging to a disabled device continue to be left alone.
    /// </para>
    /// </remarks>
    private static void RemoveRetiredMeasurements(MetadataSyncContext context, List<Guid> signalIDs)
    {
        // Independently synchronized devices are not parented to the subscriber device, so the ActiveMeasurement
        // view resolved each device to its own run-time ID rather than the subscriber's. The lookup this replaces
        // was restricted to the subscriber's run-time ID, which means it never matched anything in this mode and
        // retired measurements were silently left in place. That behavior is preserved deliberately: a direct
        // query would begin deleting records that have never been deleted before. Whether independently
        // synchronized devices should participate in measurement retirement is a separate decision.
        if (context.SyncIndependentDevices)
            return;

        HashSet<Guid> retainedSignalIDs = new(signalIDs);
        List<int> deviceIDs = context.DeviceIDs.Values.Where(deviceID => deviceID > 0).Distinct().ToList();
        List<Guid> retiredSignalIDs = [];

        foreach (int[] chunk in MetadataSyncContext.Chunk(deviceIDs))
        {
            string querySql = context.BuildInListQuery("SELECT M.SignalID FROM Measurement M INNER JOIN Device D ON M.DeviceID = D.ID WHERE M.DeviceID IN (", chunk.Length, ") AND M.Enabled <> 0 AND D.Enabled <> 0", "deviceID");

            foreach (DataRow row in context.RetrieveData(querySql, chunk.Select(deviceID => (object)deviceID).ToArray()).Rows)
            {
                Guid signalID = row.ConvertGuidField("SignalID");

                if (!retainedSignalIDs.Contains(signalID))
                    retiredSignalIDs.Add(signalID);
            }
        }

        if (retiredSignalIDs.Count == 0)
            return;

        // Define local signal type ID deletion exclusion set
        string deleteCondition = "";

        if (context.MutualSubscription && !context.Internal)
        {
            // For mutual subscriptions where this subscription is renter (i.e., internal is false), do not delete measurements that are locally owned.
            // Note that "=" is used here, not "==": the latter is only accepted by SQLite and is a syntax error on all other supported database types.
            deleteCondition = " AND Internal = 0";
        }
        else
        {
            List<int> excludedSignalTypeIDs = [];

            // We are intentionally ignoring CALC and ALRM signals during measurement deletion since if you have subscribed to a device and subsequently created local
            // calculations and alarms associated with this device, these signals are locally owned and not part of the publisher subscription stream. As a result any
            // CALC or ALRM measurements that are created at source and then removed could be orphaned in subscriber. The best fix would be to have a simple flag that
            // clearly designates that a measurement was created locally and is not part of the remote synchronization set.
            if (!context.AutoDeleteCalculatedMeasurements && context.SignalTypeIDs.TryGetValue("CALC", out int signalTypeID))
                excludedSignalTypeIDs.Add(signalTypeID);

            if (!context.AutoDeleteAlarmMeasurements && context.SignalTypeIDs.TryGetValue("ALRM", out signalTypeID))
                excludedSignalTypeIDs.Add(signalTypeID);

            if (excludedSignalTypeIDs.Count > 0)
                deleteCondition = $" AND NOT SignalTypeID IN ({excludedSignalTypeIDs.ToDelimitedString(',')})";
        }

        // Deleting in batches matters disproportionately here: the SQL Server schema defines an INSTEAD OF DELETE
        // trigger on Measurement that issues seven dependent delete statements, and that cost was previously paid
        // once per retired measurement rather than once per batch.
        foreach (Guid[] chunk in MetadataSyncContext.Chunk(retiredSignalIDs))
        {
            string deleteMeasurementSql = context.BuildInListQuery("DELETE FROM Measurement WHERE SignalID IN (", chunk.Length, $"){deleteCondition}", "signalID");
            context.ExecuteNonQuery(deleteMeasurementSql, chunk.Select(signalID => context.Database.Guid(signalID)).ToArray());
        }
    }
}
