//******************************************************************************************************
//  MetadataSyncContext.cs - Gbtc
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
/// Represents the shared state and database primitives used while synchronizing received meta-data
/// into the local configuration database.
/// </summary>
/// <remarks>
/// <para>
/// A single instance of this class is created per meta-data synchronization pass and handed to each of
/// the per-table synchronization operations, i.e., <see cref="DeviceMetadataSync"/>,
/// <see cref="MeasurementMetadataSync"/> and <see cref="PhasorMetadataSync"/>. It owns the reused
/// <c>IDbCommand</c>, the query helpers and the values that flow between the table operations, most
/// notably <see cref="DeviceIDs"/>.
/// </para>
/// <para>
/// The query helpers deliberately route every statement through a single place so that statement counts
/// can be tracked for the completion status message.
/// </para>
/// </remarks>
internal sealed class MetadataSyncContext
{
    #region [ Members ]

    // Constants

    /// <summary>
    /// Maximum number of values placed in a single generated <c>IN (...)</c> clause.
    /// </summary>
    /// <remarks>
    /// SQL Server allows a hard maximum of 2,100 parameters per command and large <c>IN</c> lists get
    /// progressively more expensive for the query optimizer to compile, so lists are chunked well below
    /// the limit. SQLite's default limit is lower still on older builds, hence the conservative value.
    /// </remarks>
    public const int MaxInListSize = 500;

    // Fields

    /// <summary>Active database connection.</summary>
    public readonly AdoDataConnection Database;

#if NET
    /// <summary>Reused command, already associated with any active transaction.</summary>
    public readonly DbCommand Command;
#else
    /// <summary>Reused command, already associated with any active transaction.</summary>
    public readonly IDbCommand Command;
#endif

    /// <summary>Timeout, in seconds, applied to each meta-data synchronization query.</summary>
    public readonly int Timeout;

    /// <summary>Record ID of the local device record that represents this subscriber connection.</summary>
    public int ParentID;

    /// <summary>Historian associated with the subscriber device, if any.</summary>
    public object? HistorianID;

    /// <summary>Prefix applied to synchronized device and point tag names to keep them unique.</summary>
    public string SourcePrefix = "";

#if !NET
    /// <summary>Active node ID - only defined for the .NET Framework schema.</summary>
    public Guid NodeID;

    /// <summary>Record ID of the STTP protocol - only defined for the .NET Framework schema.</summary>
    public int ProtocolID;
#endif

    /// <summary>Value used to restrict device records to those owned by this subscriber connection.</summary>
    /// <remarks>
    /// This is the parent device record ID, expressed as a string when <see cref="SyncIndependentDevices"/>
    /// is enabled since independently synchronized devices track ownership through the text-based
    /// <c>OriginalSource</c> field instead of the integer <c>ParentID</c> field.
    /// </remarks>
    public object ParentIDValue = null!;

    /// <summary>Name of the device field that identifies ownership by this subscriber connection.</summary>
    public string ParentColumn = "ParentID";

    // Synchronization option flags, captured from the owning data subscriber
    public bool Internal;
    public bool MutualSubscription;
    public bool SyncIndependentDevices;
    public bool AutoEnableSyncedDevices;
    public bool UseIdentityInserts;
    public bool AutoDeleteCalculatedMeasurements;
    public bool AutoDeleteAlarmMeasurements;
    public bool ReceiveInternalMetadata;
    public bool ReceiveExternalMetadata;

    /// <summary>Time of the last successful meta-data refresh, used to skip unchanged records.</summary>
    public DateTime LastMetadataRefreshTime;

    /// <summary>Latest <c>UpdatedOn</c> value encountered across all synchronized records.</summary>
    public DateTime LatestUpdateTime = DateTime.MinValue;

    /// <summary>Maps a source device acronym to its local device record ID.</summary>
    public readonly Dictionary<string, int> DeviceIDs = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Maps a signal type acronym to its local signal type record ID.</summary>
    public readonly Dictionary<string, int> SignalTypeIDs = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Total number of database statements issued during this synchronization pass.</summary>
    public long StatementCount;

    /// <summary>Time spent synchronizing device records.</summary>
    public Ticks DeviceSyncTime;

    /// <summary>Time spent synchronizing measurement records.</summary>
    public Ticks MeasurementSyncTime;

    /// <summary>Time spent synchronizing phasor records.</summary>
    public Ticks PhasorSyncTime;

    private readonly Action m_updateProgress;
    private readonly Action<long> m_initProgress;

    #endregion

    #region [ Constructors ]

    /// <summary>
    /// Creates a new <see cref="MetadataSyncContext"/>.
    /// </summary>
#if NET
    public MetadataSyncContext(AdoDataConnection database, DbCommand command, int timeout, Action<long> initProgress, Action updateProgress)
#else
    public MetadataSyncContext(AdoDataConnection database, IDbCommand command, int timeout, Action<long> initProgress, Action updateProgress)
#endif
    {
        Database = database;
        Command = command;
        Timeout = timeout;
        m_initProgress = initProgress;
        m_updateProgress = updateProgress;
    }

    #endregion

    #region [ Methods ]

    /// <summary>
    /// Establishes the total number of actions expected during this synchronization pass.
    /// </summary>
    public void InitProgress(long totalActions)
    {
        m_initProgress(totalActions);
    }

    /// <summary>
    /// Reports incremental synchronization progress to the user.
    /// </summary>
    public void UpdateProgress()
    {
        m_updateProgress();
    }

    /// <summary>
    /// Executes a query that returns a result set.
    /// </summary>
    public DataTable RetrieveData(string sql, params object?[] parameters)
    {
        StatementCount++;
    #if NET
        return Command.RetrieveData(Timeout, sql, parameters);
    #else
        return Command.RetrieveData(Database.AdapterType, sql, Timeout, parameters);
    #endif
    }

    /// <summary>
    /// Executes a statement that returns no result set.
    /// </summary>
    public void ExecuteNonQuery(string sql, params object?[] parameters)
    {
        StatementCount++;
    #if NET
        Command.ExecuteNonQuery(Timeout, sql, parameters);
    #else
        Command.ExecuteNonQuery(sql, Timeout, parameters);
    #endif
    }

    /// <summary>
    /// Executes a query that returns a single value.
    /// </summary>
    public object? ExecuteScalar(string sql, params object?[] parameters)
    {
        StatementCount++;
    #if NET
        return Command.ExecuteScalar(Timeout, sql, parameters);
    #else
        return Command.ExecuteScalar(sql, Timeout, parameters);
    #endif
    }

    /// <summary>
    /// Tracks the latest record update time and determines whether a record has changed since the last
    /// synchronization pass.
    /// </summary>
    /// <param name="row">Source meta-data row.</param>
    /// <param name="updatedOnFieldExists">Flag that determines if the optional <c>UpdatedOn</c> field is defined.</param>
    /// <returns><c>true</c> if the record should be updated; otherwise, <c>false</c>.</returns>
    /// <remarks>
    /// When the <c>UpdatedOn</c> field is missing or cannot be parsed, records are always considered
    /// changed - this matches long-standing behavior and errs toward synchronizing too much rather than
    /// too little.
    /// </remarks>
    public bool RecordNeedsUpdating(DataRow row, bool updatedOnFieldExists)
    {
        if (!updatedOnFieldExists)
            return true;

        try
        {
            DateTime updateTime = Convert.ToDateTime(row["UpdatedOn"]);

            if (updateTime > LatestUpdateTime)
                LatestUpdateTime = updateTime;

            return updateTime > LastMetadataRefreshTime;
        }
        catch
        {
            return true;
        }
    }

    /// <summary>
    /// Builds a parameterized statement containing a generated <c>IN (...)</c> value list.
    /// </summary>
    /// <param name="prefixSql">SQL text appearing immediately before the value list, e.g., <c>"DELETE FROM Device WHERE UniqueID IN ("</c>.</param>
    /// <param name="valueCount">Number of values in the list.</param>
    /// <param name="suffixSql">SQL text appearing immediately after the value list, e.g., <c>")"</c>.</param>
    /// <param name="parameterName">Base name used for the generated parameters.</param>
    /// <returns>Parameterized SQL statement.</returns>
    /// <remarks>
    /// The generated text is deliberately kept on a single line with values separated by <c>", "</c>.
    /// The .NET Framework parameter tokenizer only treats space, parenthesis, comma and equals as
    /// delimiters, so a parameter placed adjacent to any other character - a line break in particular -
    /// would not be recognized and the statement would fail with a parameter count mismatch.
    /// </remarks>
    public string BuildInListQuery(string prefixSql, int valueCount, string suffixSql, string parameterName)
    {
        StringBuilder placeholders = new();
        string[] parameterNames = new string[valueCount];

        for (int i = 0; i < valueCount; i++)
        {
            if (i > 0)
                placeholders.Append(", ");

            placeholders.Append('{').Append(i).Append('}');
            parameterNames[i] = parameterName + i;
        }

        return Database.ParameterizedQueryString($"{prefixSql}{placeholders}{suffixSql}", parameterNames);
    }

    /// <summary>
    /// Splits a sequence into chunks no larger than <see cref="MaxInListSize"/>.
    /// </summary>
    public static IEnumerable<T[]> Chunk<T>(IReadOnlyList<T> items)
    {
        for (int index = 0; index < items.Count; index += MaxInListSize)
        {
            int length = Math.Min(MaxInListSize, items.Count - index);
            T[] chunk = new T[length];

            for (int i = 0; i < length; i++)
                chunk[i] = items[index + i];

            yield return chunk;
        }
    }

    #endregion
}
