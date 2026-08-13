//******************************************************************************************************
//  PhasorMetadataSync.cs - Gbtc
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
/// Synchronizes the <c>PhasorDetail</c> meta-data table into the local <c>Phasor</c> table.
/// </summary>
/// <remarks>
/// Phasor data is normally only needed so that the user can properly generate a mirrored IEEE C37.118
/// output stream from the source data. This is necessary since, in this protocol, the phasors are
/// described (i.e., labeled) as a unit (i.e., as a complex number) instead of as two distinct angle and
/// magnitude measurements.
/// </remarks>
internal static class PhasorMetadataSync
{
    /// <summary>
    /// Synchronizes phasor records for all devices captured in <see cref="MetadataSyncContext.DeviceIDs"/>.
    /// </summary>
    public static void Synchronize(MetadataSyncContext context, DataTable phasorDetail)
    {
    #if NET
        const string PrimaryVoltageID = "PrimaryVoltageID";
        const string DestinationPhasorID = "DestinationPhasorID";
    #else
        const string PrimaryVoltageID = "DestinationPhasorID";
        const string DestinationPhasorID = "PrimaryVoltageID";
    #endif

        AdoDataConnection database = context.Database;

        // Check existence of optional meta-data fields
        DataColumnCollection columns = phasorDetail.Columns;
        bool phasorIDFieldExists = columns.Contains("ID");
        bool primaryVoltageIDFieldExists = columns.Contains(PrimaryVoltageID) || columns.Contains(DestinationPhasorID);
        bool baseKVFieldExists = columns.Contains("BaseKV");

        // Define SQL statements used while applying phasor changes. The BaseKV assignment is folded into the primary
        // insert and update statements rather than being issued as a separate follow-up statement: every phasor update
        // fires a trigger that joins the eleven table ActiveMeasurement view, so halving the number of write statements
        // against this table halves that cost.
        string insertPhasorSql;
        string updatePhasorSql;

    #if NET
        if (baseKVFieldExists)
        {
            insertPhasorSql = database.ParameterizedQueryString("INSERT INTO Phasor(DeviceID, Label, Type, Phase, SourceIndex, Internal, BaseKV) VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6})", "deviceID", "label", "type", "phase", "sourceIndex", "internal", "baseKV");
            updatePhasorSql = database.ParameterizedQueryString("UPDATE Phasor SET Label = {0}, Type = {1}, Phase = {2}, Internal = {3}, BaseKV = {4} WHERE DeviceID = {5} AND SourceIndex = {6}", "label", "type", "phase", "internal", "baseKV", "deviceID", "sourceIndex");
        }
        else
        {
            insertPhasorSql = database.ParameterizedQueryString("INSERT INTO Phasor(DeviceID, Label, Type, Phase, SourceIndex, Internal) VALUES ({0}, {1}, {2}, {3}, {4}, {5})", "deviceID", "label", "type", "phase", "sourceIndex", "internal");
            updatePhasorSql = database.ParameterizedQueryString("UPDATE Phasor SET Label = {0}, Type = {1}, Phase = {2}, Internal = {3} WHERE DeviceID = {4} AND SourceIndex = {5}", "label", "type", "phase", "internal", "deviceID", "sourceIndex");
        }
    #else
        if (baseKVFieldExists)
        {
            insertPhasorSql = database.ParameterizedQueryString("INSERT INTO Phasor(DeviceID, Label, Type, Phase, SourceIndex, BaseKV) VALUES ({0}, {1}, {2}, {3}, {4}, {5})", "deviceID", "label", "type", "phase", "sourceIndex", "baseKV");
            updatePhasorSql = database.ParameterizedQueryString("UPDATE Phasor SET Label = {0}, Type = {1}, Phase = {2}, BaseKV = {3} WHERE DeviceID = {4} AND SourceIndex = {5}", "label", "type", "phase", "baseKV", "deviceID", "sourceIndex");
        }
        else
        {
            insertPhasorSql = database.ParameterizedQueryString("INSERT INTO Phasor(DeviceID, Label, Type, Phase, SourceIndex) VALUES ({0}, {1}, {2}, {3}, {4})", "deviceID", "label", "type", "phase", "sourceIndex");
            updatePhasorSql = database.ParameterizedQueryString("UPDATE Phasor SET Label = {0}, Type = {1}, Phase = {2} WHERE DeviceID = {3} AND SourceIndex = {4}", "label", "type", "phase", "deviceID", "sourceIndex");
        }
    #endif

        // Define SQL statement to update destination phasor ID field of existing phasor record
        string updatePrimaryVoltageIDSql = database.ParameterizedQueryString($"UPDATE Phasor SET {PrimaryVoltageID} = {{0}} WHERE ID = {{1}}", "primaryVoltageID", "id");

        // Load a snapshot of existing phasor records, replacing the per-row existence and record ID lookups
        Dictionary<(int DeviceID, int SourceIndex), int> snapshot = LoadSnapshot(context);

        Dictionary<int, List<int>> definedSourceIndices = new();
        Dictionary<int, int> sourceToDestinationIDMap = new();
        List<(int SourcePhasorID, int DeviceID, int SourceIndex)> phasorIDLookups = [];

        foreach (DataRow row in phasorDetail.Rows)
        {
            // Get device acronym
            string deviceAcronym = row.Field<string>("DeviceAcronym") ?? string.Empty;

            // Make sure we have an associated device already defined for the phasor record
            if (!string.IsNullOrWhiteSpace(deviceAcronym) && context.DeviceIDs.TryGetValue(deviceAcronym, out int deviceID))
            {
                bool recordNeedsUpdating = context.RecordNeedsUpdating(row, true);

                int sourceIndex = row.ConvertField<int>("SourceIndex");
                string label = row.Field<string>("Label") ?? "undefined";
                string type = (row.Field<string>("Type") ?? "V").TruncateLeft(1);
                string phase = (row.Field<string>("Phase") ?? "+").TruncateLeft(1);

                // Determine if phasor record already exists
                if (!snapshot.ContainsKey((deviceID, sourceIndex)))
                {
                    // Insert new phasor record
                #if NET
                    if (baseKVFieldExists)
                        context.ExecuteNonQuery(insertPhasorSql, deviceID, label, type, phase, sourceIndex, database.Bool(context.Internal), row.ConvertField<int>("BaseKV"));
                    else
                        context.ExecuteNonQuery(insertPhasorSql, deviceID, label, type, phase, sourceIndex, database.Bool(context.Internal));
                #else
                    if (baseKVFieldExists)
                        context.ExecuteNonQuery(insertPhasorSql, deviceID, label, type, phase, sourceIndex, row.ConvertField<int>("BaseKV"));
                    else
                        context.ExecuteNonQuery(insertPhasorSql, deviceID, label, type, phase, sourceIndex);
                #endif
                }
                else if (recordNeedsUpdating)
                {
                    // Update existing phasor record
                #if NET
                    if (baseKVFieldExists)
                        context.ExecuteNonQuery(updatePhasorSql, label, type, phase, database.Bool(context.Internal), row.ConvertField<int>("BaseKV"), deviceID, sourceIndex);
                    else
                        context.ExecuteNonQuery(updatePhasorSql, label, type, phase, database.Bool(context.Internal), deviceID, sourceIndex);
                #else
                    if (baseKVFieldExists)
                        context.ExecuteNonQuery(updatePhasorSql, label, type, phase, row.ConvertField<int>("BaseKV"), deviceID, sourceIndex);
                    else
                        context.ExecuteNonQuery(updatePhasorSql, label, type, phase, deviceID, sourceIndex);
                #endif
                }

                if (phasorIDFieldExists && primaryVoltageIDFieldExists)
                {
                    int sourcePhasorID = row.ConvertField<int>("ID");

                    // Using ConvertNullableField extension since publisher could use SQLite database in which case
                    // all integers would arrive in data set as longs and need to be converted back to integers
                    int? destinationPhasorID = row.ConvertNullableField<int>(columns.Contains(PrimaryVoltageID) ?
                        PrimaryVoltageID :
                        DestinationPhasorID);

                    if (destinationPhasorID.HasValue)
                        sourceToDestinationIDMap[sourcePhasorID] = destinationPhasorID.Value;

                    // Map all metadata phasor IDs to associated local database phasor IDs - resolved in bulk below
                    phasorIDLookups.Add((sourcePhasorID, deviceID, sourceIndex));
                }

                // Track defined phasors for each device
                definedSourceIndices.GetOrAdd(deviceID, _ => []).Add(sourceIndex);
            }

            // Periodically notify user about synchronization progress
            context.UpdateProgress();
        }

        // Once all phasor records have been processed, handle updating of destination phasor IDs
        if (phasorIDLookups.Count > 0)
        {
            // Reload the snapshot so that records inserted above are included, then resolve every metadata phasor
            // ID to its local record ID in one pass rather than one query per phasor
            Dictionary<(int DeviceID, int SourceIndex), int> resolved = LoadSnapshot(context);
            Dictionary<int, int> metadataToDatabaseIDMap = new();

            foreach ((int sourcePhasorID, int deviceID, int sourceIndex) in phasorIDLookups)
                metadataToDatabaseIDMap[sourcePhasorID] = resolved.TryGetValue((deviceID, sourceIndex), out int phasorID) ? phasorID : 0;

            foreach (KeyValuePair<int, int> item in sourceToDestinationIDMap)
            {
                if (metadataToDatabaseIDMap.TryGetValue(item.Key, out int sourcePhasorID) && metadataToDatabaseIDMap.TryGetValue(item.Value, out int destinationPhasorID))
                    context.ExecuteNonQuery(updatePrimaryVoltageIDSql, destinationPhasorID, sourcePhasorID);
            }
        }

        // For mutual subscriptions where this subscription is owner (i.e., internal is true), do not delete any phasor data - it will be managed by owner only
        if (context.MutualSubscription && context.Internal)
            return;

        RemoveRetiredPhasors(context, definedSourceIndices);
    }

    /// <summary>
    /// Loads existing phasor records for all synchronized devices, keyed by device and source index.
    /// </summary>
    private static Dictionary<(int DeviceID, int SourceIndex), int> LoadSnapshot(MetadataSyncContext context)
    {
        Dictionary<(int, int), int> snapshot = new();
        List<int> deviceIDs = context.DeviceIDs.Values.Where(deviceID => deviceID > 0).Distinct().ToList();

        foreach (int[] chunk in MetadataSyncContext.Chunk(deviceIDs))
        {
            string querySql = context.BuildInListQuery("SELECT ID, DeviceID, SourceIndex FROM Phasor WHERE DeviceID IN (", chunk.Length, ")", "deviceID");

            foreach (DataRow row in context.RetrieveData(querySql, chunk.Select(deviceID => (object)deviceID).ToArray()).Rows)
                snapshot[(row.ConvertField<int>("DeviceID"), row.ConvertField<int>("SourceIndex"))] = row.ConvertField<int>("ID");
        }

        return snapshot;
    }

    /// <summary>
    /// Removes phasor records associated with synchronized devices that no longer appear in the meta-data.
    /// </summary>
    private static void RemoveRetiredPhasors(MetadataSyncContext context, Dictionary<int, List<int>> definedSourceIndices)
    {
        // Devices that reported no phasors at all can be cleared in batches; devices that reported some phasors need
        // a per-device statement since each carries its own retained source index list
        List<int> devicesWithoutPhasors = [];

        foreach (int deviceID in context.DeviceIDs.Values.Where(deviceID => deviceID > 0).Distinct())
        {
            if (definedSourceIndices.TryGetValue(deviceID, out List<int>? sourceIndices))
            {
                string deletePhasorSql = context.Database.ParameterizedQueryString($"DELETE FROM Phasor WHERE DeviceID = {{0}} AND SourceIndex NOT IN ({string.Join(",", sourceIndices)})", "deviceID");
                context.ExecuteNonQuery(deletePhasorSql, deviceID);
            }
            else
            {
                devicesWithoutPhasors.Add(deviceID);
            }
        }

        foreach (int[] chunk in MetadataSyncContext.Chunk(devicesWithoutPhasors))
        {
            string deletePhasorSql = context.BuildInListQuery("DELETE FROM Phasor WHERE DeviceID IN (", chunk.Length, ")", "deviceID");
            context.ExecuteNonQuery(deletePhasorSql, chunk.Select(deviceID => (object)deviceID).ToArray());
        }
    }
}
