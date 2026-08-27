//******************************************************************************************************
//  DeviceMetadataSync.cs - Gbtc
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
/// Synchronizes the <c>DeviceDetail</c> meta-data table into the local <c>Device</c> table.
/// </summary>
internal static class DeviceMetadataSync
{
    /// <summary>
    /// Describes an existing local device record, as loaded by the pre-synchronization snapshot.
    /// </summary>
    private sealed class DeviceSnapshot
    {
        public int ID;
        public int? ParentID;
        public string? OriginalSource;
    }

    /// <summary>
    /// Synchronizes device records and populates <see cref="MetadataSyncContext.DeviceIDs"/>, which
    /// subsequent measurement and phasor synchronization depend upon.
    /// </summary>
    public static void Synchronize(MetadataSyncContext context, DataTable deviceDetail)
    {
        AdoDataConnection database = context.Database;

        // Determine which device rows should be synchronized based on operational mode flags
        DataRow[] deviceRows;

        if (context.ReceiveInternalMetadata && context.ReceiveExternalMetadata || context.MutualSubscription)
            deviceRows = deviceDetail.Select();
        else if (context.ReceiveInternalMetadata)
            deviceRows = deviceDetail.Select("OriginalSource IS NULL");
        else if (context.ReceiveExternalMetadata)
            deviceRows = deviceDetail.Select("OriginalSource IS NOT NULL");
        else
            deviceRows = [];

        // Check existence of optional meta-data fields
        DataColumnCollection columns = deviceDetail.Columns;
        bool accessIDFieldExists = columns.Contains("AccessID");
        bool longitudeFieldExists = columns.Contains("Longitude");
        bool latitudeFieldExists = columns.Contains("Latitude");
        bool companyAcronymFieldExists = columns.Contains("CompanyAcronym");
        bool protocolNameFieldExists = columns.Contains("ProtocolName");
        bool vendorAcronymFieldExists = columns.Contains("VendorAcronym");
        bool vendorDeviceNameFieldExists = columns.Contains("VendorDeviceName");
        bool interconnectionNameFieldExists = columns.Contains("InterconnectionName");
        bool updatedOnFieldExists = columns.Contains("UpdatedOn");
        bool connectionStringFieldExists = columns.Contains("ConnectionString");
    #if !NET
        bool framesPerSecondFieldExists = columns.Contains("FramesPerSecond");
    #endif

        List<Guid> uniqueIDs = deviceRows
            .Select(deviceRow => deviceRow.ConvertGuidField("UniqueID"))
            .ToList();

        // Load a snapshot of all local device records that are relevant to this synchronization pass. This
        // replaces the per-row existence, ownership and record ID lookups that were previously issued for
        // every single device row.
        Dictionary<Guid, DeviceSnapshot> snapshot = LoadSnapshot(context, uniqueIDs, out List<Guid> ownedUniqueIDs);

        // Remove any device records associated with this subscriber that no longer exist in the meta-data
        if (uniqueIDs.Count > 0)
        {
            HashSet<Guid> retainedUniqueIDs = new(uniqueIDs);

            List<Guid> retiredUniqueIDs = ownedUniqueIDs
                .Where(uniqueID => !retainedUniqueIDs.Contains(uniqueID))
                .ToList();

            if (retiredUniqueIDs.Count > 0)
            {
                foreach (Guid[] chunk in MetadataSyncContext.Chunk(retiredUniqueIDs))
                {
                    string deleteDeviceSql = context.BuildInListQuery("DELETE FROM Device WHERE UniqueID IN (", chunk.Length, ")", "uniqueID");
                    context.ExecuteNonQuery(deleteDeviceSql, chunk.Select(uniqueID => database.Guid(uniqueID)).ToArray());
                }
            }

            context.UpdateProgress();
        }

        // Define the batched statements used while applying device changes
        string enabledLiteral = context.AutoEnableSyncedDevices ? "1" : "0";

    #if NET
        InsertBatch insertDevices = new(context,
            "INSERT INTO Device(UniqueID, ParentID, HistorianID, Acronym, Name, OriginalSource, AccessID, Longitude, Latitude, ContactList, ConnectionString, IsConcentrator, Internal, Enabled)",
            $"?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, {enabledLiteral}");

        StatementBatch updateDevices = new(context,
            "UPDATE Device SET Acronym = ?, Name = ?, OriginalSource = ?, HistorianID = ?, AccessID = ?, Longitude = ?, Latitude = ?, ContactList = ?, Internal = ? WHERE UniqueID = ?");

        StatementBatch updateDevicesWithConnectionString = new(context,
            "UPDATE Device SET Acronym = ?, Name = ?, OriginalSource = ?, HistorianID = ?, AccessID = ?, Longitude = ?, Latitude = ?, ContactList = ?, ConnectionString = ?, Internal = ? WHERE UniqueID = ?");
    #else
        InsertBatch insertDevices = new(context,
            "INSERT INTO Device(NodeID, UniqueID, ParentID, HistorianID, Acronym, Name, ProtocolID, FramesPerSecond, OriginalSource, AccessID, Longitude, Latitude, ContactList, ConnectionString, IsConcentrator, Enabled)",
            $"?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, {enabledLiteral}");

        StatementBatch updateDevices = new(context,
            "UPDATE Device SET Acronym = ?, Name = ?, OriginalSource = ?, ProtocolID = ?, FramesPerSecond = ?, HistorianID = ?, AccessID = ?, Longitude = ?, Latitude = ?, ContactList = ? WHERE UniqueID = ?");

        StatementBatch updateDevicesWithConnectionString = new(context,
            "UPDATE Device SET Acronym = ?, Name = ?, OriginalSource = ?, ProtocolID = ?, FramesPerSecond = ?, HistorianID = ?, AccessID = ?, Longitude = ?, Latitude = ?, ContactList = ?, ConnectionString = ? WHERE UniqueID = ?");

        // Define SQL statement to look up a protocol record ID by name - only used when synchronizing independent devices
        string queryProtocolIDSql = database.ParameterizedQueryString("SELECT ID FROM Protocol WHERE Name = {0}", "protocolName");
    #endif

        // Devices that exist locally but belong to another connection are left untouched - and so are all of
        // their child records, since measurement and phasor synchronization both key off 'DeviceIDs'.
        HashSet<string> preservedAcronyms = new(StringComparer.OrdinalIgnoreCase);
        int accessID = 0;

        foreach (DataRow row in deviceRows)
        {
            Guid uniqueID = row.ConvertGuidField("UniqueID");
            bool recordNeedsUpdating = context.RecordNeedsUpdating(row, updatedOnFieldExists);

            // We will synchronize meta-data only if the source owns this device, and it's not defined as a concentrator (these should normally be filtered by publisher - but we check just in case).
            if (!row["IsConcentrator"].ToNonNullString("0").ParseBoolean())
            {
                if (accessIDFieldExists)
                    accessID = row.ConvertField<int>("AccessID");

                // Get longitude and latitude values if they are defined
                decimal longitude = 0M;
                decimal latitude = 0M;
                decimal? location;
                string protocolName = string.Empty;
                string connectionString = string.Empty;

                if (longitudeFieldExists)
                {
                    location = row.ConvertNullableField<decimal>("Longitude");

                    if (location.HasValue)
                        longitude = location.Value;
                }

                if (latitudeFieldExists)
                {
                    location = row.ConvertNullableField<decimal>("Latitude");

                    if (location.HasValue)
                        latitude = location.Value;
                }

                if (protocolNameFieldExists)
                    protocolName = row.Field<string>("ProtocolName") ?? string.Empty;

                if (connectionStringFieldExists)
                    connectionString = row.Field<string>("ConnectionString") ?? string.Empty;

                // Save any reported extraneous values from device meta-data in connection string formatted contact list - all fields are considered optional
                Dictionary<string, string> contactList = new();

                if (companyAcronymFieldExists)
                    contactList["companyAcronym"] = row.Field<string>("CompanyAcronym") ?? string.Empty;

                if (protocolNameFieldExists)
                    contactList["protocolName"] = protocolName;

                if (vendorAcronymFieldExists)
                    contactList["vendorAcronym"] = row.Field<string>("VendorAcronym") ?? string.Empty;

                if (vendorDeviceNameFieldExists)
                    contactList["vendorDeviceName"] = row.Field<string>("VendorDeviceName") ?? string.Empty;

                if (interconnectionNameFieldExists)
                    contactList["interconnectionName"] = row.Field<string>("InterconnectionName") ?? string.Empty;

            #if !NET
                int protocolID = context.ProtocolID;
            #endif

                // If we are synchronizing independent devices, we need to determine the protocol ID for the device
                // based on the protocol name defined in the meta-data
                if (context.SyncIndependentDevices && !string.IsNullOrWhiteSpace(protocolName))
                {
                #if NET
                    Dictionary<string, string> settings = connectionString.ParseKeyValuePairs();
                    settings["phasorProtocol"] = protocolName;
                    connectionString = settings.JoinKeyValuePairs();
                #else
                    object? protocolIDValue = context.ExecuteScalar(queryProtocolIDSql, protocolName);

                    if (protocolIDValue is not null && protocolIDValue is not DBNull)
                        protocolID = Convert.ToInt32(protocolIDValue);

                    if (protocolID == 0)
                        protocolID = context.ProtocolID;
                #endif
                }

                // For mutual subscriptions where this subscription is owner (i.e., internal is true), we only sync devices that we did not provide
                if (!context.MutualSubscription || !context.Internal || string.IsNullOrEmpty(row.Field<string>("OriginalSource")))
                {
                    // Gateway is assuming ownership of the device records when the "internal" flag is true - this means the device's measurements can be forwarded to another party. From a device record perspective,
                    // ownership is inferred by setting 'OriginalSource' to null. When gateway doesn't own device records (i.e., the "internal" flag is false), this means the device's measurements can only be consumed
                    // locally - from a device record perspective this means the 'OriginalSource' field is set to the acronym of the PDC or PMU that generated the source measurements. This field allows a mirrored source
                    // restriction to be implemented later to ensure all devices in an output protocol came from the same original source connection, if desired.
                    object originalSource = context.SyncIndependentDevices ? context.ParentID.ToString() : context.Internal ? DBNull.Value :
                        string.IsNullOrEmpty(row.Field<string>("ParentAcronym")) ?
                            context.SourcePrefix + row.Field<string>("Acronym") :
                            context.SourcePrefix + row.Field<string>("ParentAcronym");

                    if (!snapshot.TryGetValue(uniqueID, out DeviceSnapshot? existing))
                    {
                        // Insert new device record. Note that the guid-based unique ID is supplied directly rather than
                        // relying on a database default followed by a corrective update - the previous approach cost an
                        // extra write per device and could not seek an index, since the unique acronym index is keyed on
                        // node and acronym together rather than on acronym alone.
                    #if NET
                        insertDevices.Add(database.Guid(uniqueID), context.SyncIndependentDevices ? DBNull.Value : context.ParentID,
                            context.HistorianID, context.SourcePrefix + row.Field<string>("Acronym"), row.Field<string>("Name"), originalSource,
                            accessID, longitude, latitude, contactList.JoinKeyValuePairs(), connectionString, database.Bool(context.Internal));
                    #else
                        insertDevices.Add(database.Guid(context.NodeID), database.Guid(uniqueID), context.SyncIndependentDevices ? DBNull.Value : context.ParentID,
                            context.HistorianID, context.SourcePrefix + row.Field<string>("Acronym"), row.Field<string>("Name"), protocolID,
                            framesPerSecondFieldExists ? row.ConvertField<int>("FramesPerSecond") : 30, originalSource, accessID,
                            longitude, latitude, contactList.JoinKeyValuePairs(), connectionString);
                    #endif
                    }
                    else
                    {
                        // Perform safety check to preserve device records which are not safe to overwrite (e.g., device already exists locally as part of another connection).
                        // Skipping the record here also keeps it out of the 'DeviceIDs' lookup, which in turn causes all of its measurements and phasors to be skipped as well.
                        // This is the desired behavior: those local records belong to another connection.
                        //
                        // Note that this check is evaluated for every existing record, not only for records that have changed since the last synchronization. It was
                        // previously nested inside the change check, which meant the protection only held while the source kept updating the device record: once a
                        // device stopped changing, the check was skipped, the device entered the 'DeviceIDs' lookup, and its measurements and phasors became subject
                        // to this connection's inserts, updates and - most importantly - the retirement passes, which delete records this connection did not report.
                        if (!IsOwnedByThisConnection(context, existing))
                        {
                            preservedAcronyms.Add(row.Field<string>("Acronym")!);
                            context.UpdateProgress();
                            continue;
                        }

                        if (recordNeedsUpdating)
                        {
                        #if NET
                            // Update existing device record
                            if (connectionStringFieldExists)
                                updateDevicesWithConnectionString.Add(context.SourcePrefix + row.Field<string>("Acronym"), row.Field<string>("Name"),
                                    originalSource, context.HistorianID, accessID, longitude, latitude, contactList.JoinKeyValuePairs(), connectionString, database.Bool(context.Internal), database.Guid(uniqueID));
                            else
                                updateDevices.Add(context.SourcePrefix + row.Field<string>("Acronym"), row.Field<string>("Name"),
                                    originalSource, context.HistorianID, accessID, longitude, latitude, contactList.JoinKeyValuePairs(), database.Bool(context.Internal), database.Guid(uniqueID));
                        #else
                            // Update existing device record
                            if (connectionStringFieldExists)
                                updateDevicesWithConnectionString.Add(context.SourcePrefix + row.Field<string>("Acronym"), row.Field<string>("Name"),
                                    originalSource, protocolID, framesPerSecondFieldExists ? row.ConvertField<int>("FramesPerSecond") : 30, context.HistorianID, accessID, longitude, latitude, contactList.JoinKeyValuePairs(), connectionString, database.Guid(uniqueID));
                            else
                                updateDevices.Add(context.SourcePrefix + row.Field<string>("Acronym"), row.Field<string>("Name"),
                                    originalSource, protocolID, framesPerSecondFieldExists ? row.ConvertField<int>("FramesPerSecond") : 30, context.HistorianID, accessID, longitude, latitude, contactList.JoinKeyValuePairs(), database.Guid(uniqueID));
                        #endif
                        }
                    }
                }
            }

            // Periodically notify user about synchronization progress
            context.UpdateProgress();
        }

        // Pending rows must reach the database before record IDs are read back below
        insertDevices.Flush();
        updateDevices.Flush();
        updateDevicesWithConnectionString.Flush();

        // Capture local device ID auto-inc values for measurement and phasor association. Records inserted above
        // do not appear in the snapshot, so record IDs are resolved in bulk here rather than one query per device.
        ResolveDeviceIDs(context, deviceRows, uniqueIDs, preservedAcronyms);
    }

    /// <summary>
    /// Determines whether an existing local device record belongs to this subscriber connection and may
    /// therefore be safely overwritten.
    /// </summary>
    /// <remarks>
    /// This mirrors the SQL predicate that was previously evaluated per device row. Note the null handling:
    /// the original <c>OriginalSource &lt;&gt; {parentID}</c> comparison never matches a null value, since
    /// SQL comparisons against null yield null rather than true, so a null original source is treated as
    /// owned. The <c>ParentID</c> form explicitly tested for null and treated it as not owned.
    /// </remarks>
    private static bool IsOwnedByThisConnection(MetadataSyncContext context, DeviceSnapshot existing)
    {
        if (context.SyncIndependentDevices)
            return existing.OriginalSource is null || existing.OriginalSource.Equals(context.ParentID.ToString(), StringComparison.Ordinal);

        return existing.ParentID.HasValue && existing.ParentID.Value == context.ParentID;
    }

    /// <summary>
    /// Loads existing local device records relevant to this synchronization pass.
    /// </summary>
    /// <param name="context">Current synchronization context.</param>
    /// <param name="uniqueIDs">Unique IDs of all devices present in the received meta-data.</param>
    /// <param name="ownedUniqueIDs">Unique IDs of all local devices currently owned by this connection.</param>
    private static Dictionary<Guid, DeviceSnapshot> LoadSnapshot(MetadataSyncContext context, List<Guid> uniqueIDs, out List<Guid> ownedUniqueIDs)
    {
        Dictionary<Guid, DeviceSnapshot> snapshot = new();
        ownedUniqueIDs = [];

        // Load all devices currently owned by this connection - used both to detect retired records and as part
        // of the existence check
        string queryOwnedDevicesSql = context.Database.ParameterizedQueryString($"SELECT ID, UniqueID, ParentID, OriginalSource FROM Device WHERE {context.ParentColumn} = {{0}}", "parentID");

        foreach (DataRow row in context.RetrieveData(queryOwnedDevicesSql, context.ParentIDValue).Rows)
        {
            Guid uniqueID = row.ConvertGuidField("UniqueID");
            ownedUniqueIDs.Add(uniqueID);
            snapshot[uniqueID] = CreateSnapshot(row);
        }

        // Devices present in the meta-data may already exist locally under a different owner, so they are looked
        // up by unique ID as well - the unique ID column is uniquely indexed, so these lookups seek
        foreach (Guid[] chunk in MetadataSyncContext.Chunk(uniqueIDs))
        {
            string querySql = context.BuildInListQuery("SELECT ID, UniqueID, ParentID, OriginalSource FROM Device WHERE UniqueID IN (", chunk.Length, ")", "uniqueID");

            foreach (DataRow row in context.RetrieveData(querySql, chunk.Select(uniqueID => context.Database.Guid(uniqueID)).ToArray()).Rows)
                snapshot[row.ConvertGuidField("UniqueID")] = CreateSnapshot(row);
        }

        return snapshot;
    }

    private static DeviceSnapshot CreateSnapshot(DataRow row)
    {
        return new DeviceSnapshot
        {
            ID = row.ConvertField<int>("ID"),
            ParentID = row.ConvertNullableField<int>("ParentID"),
            OriginalSource = row["OriginalSource"] == DBNull.Value ? null : row.Field<string>("OriginalSource")
        };
    }

    /// <summary>
    /// Populates <see cref="MetadataSyncContext.DeviceIDs"/> with the local record ID for every synchronized device.
    /// </summary>
    /// <remarks>
    /// Devices absent from the local database resolve to zero, matching the behavior of the per-row lookup this
    /// replaces. Devices preserved because they belong to another connection are deliberately excluded.
    /// </remarks>
    private static void ResolveDeviceIDs(MetadataSyncContext context, DataRow[] deviceRows, List<Guid> uniqueIDs, HashSet<string> preservedAcronyms)
    {
        Dictionary<Guid, int> deviceIDsByUniqueID = new();

        foreach (Guid[] chunk in MetadataSyncContext.Chunk(uniqueIDs))
        {
            string querySql = context.BuildInListQuery("SELECT ID, UniqueID FROM Device WHERE UniqueID IN (", chunk.Length, ")", "uniqueID");

            foreach (DataRow row in context.RetrieveData(querySql, chunk.Select(uniqueID => context.Database.Guid(uniqueID)).ToArray()).Rows)
                deviceIDsByUniqueID[row.ConvertGuidField("UniqueID")] = row.ConvertField<int>("ID");
        }

        foreach (DataRow row in deviceRows)
        {
            string acronym = row.Field<string>("Acronym")!;

            if (preservedAcronyms.Contains(acronym))
                continue;

            context.DeviceIDs[acronym] = deviceIDsByUniqueID.TryGetValue(row.ConvertGuidField("UniqueID"), out int deviceID) ? deviceID : 0;
        }
    }
}
