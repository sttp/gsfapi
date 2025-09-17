//******************************************************************************************************
//  DataSubscriber.cs - Gbtc
//
//  Copyright © 2012, Grid Protection Alliance.  All Rights Reserved.
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
//  08/20/2010 - J. Ritchie Carroll
//       Generated original version of source code.
//  02/07/2012 - Mehulbhai Thakkar
//       Modified SynchronizeMetadata to filter devices by original source and modified insert query
//       to populate OriginalSource value. Added to flag to optionally avoid meta-data synchronization.
//  12/20/2012 - Starlynn Danyelle Gilliam
//       Modified Header.
//
//******************************************************************************************************
// ReSharper disable ArrangeObjectCreationWhenTypeNotEvident
// ReSharper disable BadControlBracesIndent
// ReSharper disable UnusedVariable

#pragma warning disable SYSLIB0057

using sttp.tssc;

namespace sttp;

/// <summary>
/// Represents a data subscribing client that will connect to a data publisher for a data subscription.
/// </summary>
[Description("STTP Subscriber: client that subscribes to an STTP-style publishing server for a streaming data.")]
[EditorBrowsable(EditorBrowsableState.Advanced)] // Normally defined as an input device protocol
#if NET
[AdapterProtocol("STTP", "Streaming Telemetry Transport Protocol", ProtocolType.Measurement, UIVisibility.Input, false)]
[UIAdapterProtocol("STTP", "sttp.gemstone", "sttp.UI.STTP.js")]
[UIAdapterProtocol("STTP", "sttp.gemstone", "sttp.UI.STTPChunk.js")]
#endif
public class DataSubscriber : InputAdapterBase
{
    #region [ Members ]

    // Nested Types

    private class SubscribedDevice : IDevice, IDisposable
    {
        #region [ Members ]

        // Fields
        private long m_dataQualityErrors;
        private long m_timeQualityErrors;
        private long m_deviceErrors;
        private long m_measurementsReceived;
        private double m_measurementsExpected;
        private long m_measurementsWithError;
        private long m_measurementsDefined;
        private bool m_disposed;

        #endregion

        #region [ Constructors ]

        public SubscribedDevice(string name)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            StatisticsEngine.Register(this, name, "Device", "PMU");
        }

        /// <summary>
        /// Releases the unmanaged resources before the <see cref="SubscribedDevice"/> object is reclaimed by <see cref="GC"/>.
        /// </summary>
        ~SubscribedDevice()
        {
            Unregister();
        }

        #endregion

        #region [ Properties ]

        public string Name { get; }

        public Guid StatusFlagsID { get; set; }

        public Guid FrequencyID { get; set; }

        public Guid DeltaFrequencyID { get; set; }

        public long DataQualityErrors
        {
            get => Interlocked.Read(ref m_dataQualityErrors);
            set => Interlocked.Exchange(ref m_dataQualityErrors, value);
        }

        public long TimeQualityErrors
        {
            get => Interlocked.Read(ref m_timeQualityErrors);
            set => Interlocked.Exchange(ref m_timeQualityErrors, value);
        }

        public long DeviceErrors
        {
            get => Interlocked.Read(ref m_deviceErrors);
            set => Interlocked.Exchange(ref m_deviceErrors, value);
        }

        public long MeasurementsReceived
        {
            get => Interlocked.Read(ref m_measurementsReceived);
            set => Interlocked.Exchange(ref m_measurementsReceived, value);
        }

        public long MeasurementsExpected
        {
            get => (long)Interlocked.CompareExchange(ref m_measurementsExpected, 0.0D, 0.0D);
            set => Interlocked.Exchange(ref m_measurementsExpected, value);
        }

        public long MeasurementsWithError
        {
            get => Interlocked.Read(ref m_measurementsWithError);
            set => Interlocked.Exchange(ref m_measurementsWithError, value);
        }

        public long MeasurementsDefined
        {
            get => Interlocked.Read(ref m_measurementsDefined);
            set => Interlocked.Exchange(ref m_measurementsDefined, value);
        }

        #endregion

        #region [ Methods ]

        public override bool Equals(object? obj)
        {
            return obj is SubscribedDevice subscribedDevice && Name.Equals(subscribedDevice.Name);
        }

        public override int GetHashCode()
        {
            return Name.GetHashCode();
        }

        /// <summary>
        /// Releases all the resources used by the <see cref="SubscribedDevice"/> object.
        /// </summary>
        public void Dispose()
        {
            Unregister();
            GC.SuppressFinalize(this);
        }

        private void Unregister()
        {
            if (m_disposed)
                return;

            try
            {
                StatisticsEngine.Unregister(this);
            }
            finally
            {
                m_disposed = true; // Prevent duplicate dispose.
            }
        }

        #endregion
    }

    /// <summary>
    /// EventArgs implementation for handling user commands.
    /// </summary>
    public class UserCommandArgs : EventArgs
    {
        /// <summary>
        /// Creates a new instance of the <see cref="UserCommandArgs"/> class.
        /// </summary>
        /// <param name="command">The code for the user command.</param>
        /// <param name="response">The code for the server's response.</param>
        /// <param name="buffer">Buffer containing the message from the server.</param>
        /// <param name="startIndex">Index into the buffer used to skip the header.</param>
        /// <param name="length">The length of the message in the buffer, including the header.</param>
        public UserCommandArgs(ServerCommand command, ServerResponse response, byte[] buffer, int startIndex, int length)
        {
            Command = command;
            Response = response;
            Buffer = buffer;
            StartIndex = startIndex;
            Length = length;
        }

        /// <summary>
        /// Gets the code for the user command.
        /// </summary>
        public ServerCommand Command { get; }

        /// <summary>
        /// Gets the code for the server's response.
        /// </summary>
        public ServerResponse Response { get; }

        /// <summary>
        /// Gets the buffer containing the message from the server.
        /// </summary>
        public byte[] Buffer { get; }

        /// <summary>
        /// Gets the index into the buffer used to skip the header.
        /// </summary>
        public int StartIndex { get; }

        /// <summary>
        /// Gets the length of the message in the buffer, including the header.
        /// </summary>
        public int Length { get; }
    }

    // Constants

    /// <summary>
    /// Defines default value for <see cref="OperationalModes"/> property.
    /// </summary>
    public const OperationalModes DefaultOperationalModes = (OperationalModes)((uint)OperationalModes.VersionMask & 2U) | OperationalModes.CompressMetadata | OperationalModes.CompressSignalIndexCache | OperationalModes.ReceiveInternalMetadata;

    /// <summary>
    /// Defines the default value for the <see cref="MetadataSynchronizationTimeout"/> property.
    /// </summary>
    public const int DefaultMetadataSynchronizationTimeout = 0;

    /// <summary>
    /// Defines the default value for the <see cref="UseTransactionForMetadata"/> property.
    /// </summary>
    public const bool DefaultUseTransactionForMetadata = true;

    /// <summary>
    /// Default value for <see cref="LoggingPath"/> property.
    /// </summary>
    public const string DefaultLoggingPath = "ConfigurationCache";

    /// <summary>
    /// Specifies the default value for the <see cref="AllowedParsingExceptions"/> property.
    /// </summary>
    public const int DefaultAllowedParsingExceptions = 10;

    /// <summary>
    /// Specifies the default value for the <see cref="ParsingExceptionWindow"/> property.
    /// </summary>
    public const long DefaultParsingExceptionWindow = 50000000L; // 5 seconds

    private const int EvenKey = 0; // Even key/IV index
    private const int OddKey = 1; // Odd key/IV index
    private const int KeyIndex = 0; // Index of cipher key component in keyIV array
    private const int IVIndex = 1; // Index of initialization vector component in keyIV array

    private const long MissingCacheWarningInterval = 20000000;

    // Events

    /// <summary>
    /// Occurs when client connection to the data publication server is established.
    /// </summary>
    public event EventHandler? ConnectionEstablished;

    /// <summary>
    /// Occurs when client connection to the data publication server is terminated.
    /// </summary>
    public event EventHandler? ConnectionTerminated;

    /// <summary>
    /// Occurs when client connection to the data publication server has successfully authenticated.
    /// </summary>
    public event EventHandler? ConnectionAuthenticated;

    /// <summary>
    /// Occurs when client receives response from the server.
    /// </summary>
    public event EventHandler<EventArgs<ServerResponse, ServerCommand>>? ReceivedServerResponse;

    /// <summary>
    /// Occurs when client receives message from the server in response to a user command.
    /// </summary>
    public event EventHandler<UserCommandArgs>? ReceivedUserCommandResponse;

    /// <summary>
    /// Occurs when client receives requested meta-data transmitted by data publication server.
    /// </summary>
    public event EventHandler<EventArgs<DataSet>>? MetaDataReceived;

    /// <summary>
    /// Occurs when first measurement is transmitted by data publication server.
    /// </summary>
    public event EventHandler<EventArgs<Ticks>>? DataStartTime;

    /// <summary>
    /// Indicates that processing for an input adapter (via temporal session) has completed.
    /// </summary>
    /// <remarks>
    /// This event is expected to only be raised when an input adapter has been designed to process
    /// a finite amount of data, e.g., reading a historical range of data during temporal processing.
    /// </remarks>
    public new event EventHandler<EventArgs<string>>? ProcessingComplete;

    /// <summary>
    /// Occurs when a notification has been received from the <see cref="DataPublisher"/>.
    /// </summary>
    public event EventHandler<EventArgs<string>>? NotificationReceived;

    /// <summary>
    /// Occurs when the server has sent a notification that its configuration has changed, this
    /// can allow subscriber to request updated meta-data if desired.
    /// </summary>
    public event EventHandler? ServerConfigurationChanged;

    /// <summary>
    /// Occurs when number of parsing exceptions exceed <see cref="AllowedParsingExceptions"/> during <see cref="ParsingExceptionWindow"/>.
    /// </summary>
    public event EventHandler? ExceededParsingExceptionThreshold;

    // Fields
    private IClient? m_clientCommandChannel;
    private IServer? m_serverCommandChannel;
    private UdpClient? m_dataChannel;
    private Guid m_activeClientID;
    private string? m_connectionID;
    private SharedTimer? m_dataStreamMonitor;
    private long m_commandChannelConnectionAttempts;
    private long m_dataChannelConnectionAttempts;
    private volatile SignalIndexCache? m_remoteSignalIndexCache;
    private volatile SignalIndexCache?[]? m_signalIndexCache;
#if NET
    private readonly Lock m_signalIndexCacheLock;
#else
    private readonly object m_signalIndexCacheLock;
#endif
    private volatile int m_cacheIndex;
    private volatile long[]? m_baseTimeOffsets;
    private volatile int m_timeIndex;
    private volatile byte[][][]? m_keyIVs;
    private volatile bool m_subscribed;
    private volatile int m_lastBytesReceived;
    private DateTime m_lastReceivedAt;
    private long m_monitoredBytesReceived;
    private long m_lastMissingCacheWarning;
#if !NET
    private Guid m_nodeID;
    private int m_sttpProtocolID;
#endif
    private bool m_includeTime;
    private bool m_metadataRefreshPending;
    private readonly LongSynchronizedOperation m_synchronizeMetadataOperation;
    private volatile DataSet? m_receivedMetadata;
    private DataSet? m_synchronizedMetadata;
    private DateTime m_lastMetaDataRefreshTime;
    private OperationalModes m_operationalModes;
    private string? m_loggingPath;
    private RunTimeLog? m_runTimeLog;
    private bool m_dataGapRecoveryEnabled;
    private DataGapRecoverer? m_dataGapRecoverer;
    private int m_parsingExceptionCount;
    private long m_lastParsingExceptionTime;

    private bool m_supportsTemporalProcessing;
    private volatile Dictionary<Guid, DeviceStatisticsHelper<SubscribedDevice>>? m_subscribedDevicesLookup;
    private volatile List<DeviceStatisticsHelper<SubscribedDevice>>? m_statisticsHelpers;
    private readonly LongSynchronizedOperation m_registerStatisticsOperation;

    private readonly List<BufferBlockMeasurement?> m_bufferBlockCache;
    private uint m_expectedBufferBlockSequenceNumber;

    private Ticks m_realTime;
    private Ticks m_lastStatisticsHelperUpdate;
    private SharedTimer? m_subscribedDevicesTimer;

    private long m_totalMeasurementsPerSecond;
    private long m_measurementsPerSecondCount;
    private long m_measurementsInSecond;
    private long m_lastSecondsSinceEpoch;
    private long m_lifetimeTotalLatency;
    private long m_lifetimeMinimumLatency;
    private long m_lifetimeMaximumLatency;
    private long m_lifetimeLatencyMeasurements;

    private long m_syncProgressTotalActions;
    private long m_syncProgressActionsCount;
    private long m_syncProgressLastMessage;

    private bool m_disposed;

    #endregion

    #region [ Constructors ]

    /// <summary>
    /// Creates a new <see cref="DataSubscriber"/>.
    /// </summary>
    public DataSubscriber()
    {
        m_registerStatisticsOperation = new LongSynchronizedOperation(HandleDeviceStatisticsRegistration)
        {
            IsBackground = true
        };

        m_synchronizeMetadataOperation = new LongSynchronizedOperation(SynchronizeMetadata)
        {
            IsBackground = true
        };

        Encoding = Encoding.Unicode;
        m_operationalModes = DefaultOperationalModes;
        MetadataSynchronizationTimeout = DefaultMetadataSynchronizationTimeout;
        AllowedParsingExceptions = DefaultAllowedParsingExceptions;
        ParsingExceptionWindow = DefaultParsingExceptionWindow;

        string loggingPath = FilePath.GetDirectoryName(FilePath.GetAbsolutePath(DefaultLoggingPath));

        if (Directory.Exists(loggingPath))
            m_loggingPath = loggingPath;

        // Default to not using transactions for meta-data on SQL server (helps avoid deadlocks)
        try
        {
        #if NET
            using AdoDataConnection database = new(ConfigSettings.Instance);
        #else
            using AdoDataConnection database = new("systemSettings");
        #endif
            UseTransactionForMetadata = database.DatabaseType != DatabaseType.SQLServer;
        }
        catch
        {
            UseTransactionForMetadata = DefaultUseTransactionForMetadata;
        }

        DataLossInterval = 10.0D;
        m_bufferBlockCache = [];
        UseLocalClockAsRealTime = true;
        UseSourcePrefixNames = true;
        m_signalIndexCacheLock = new();
    }

    #endregion

    #region [ Properties ]

    /// <summary>
    /// Gets or sets the security mode used for communications over the command channel.
    /// </summary>
    public SecurityMode SecurityMode { get; set; }

    /// <summary>
    /// Gets or sets logging path to be used to be runtime and outage logs of the subscriber which are required for
    /// automated data recovery.
    /// </summary>
    /// <remarks>
    /// Leave value blank for default path, i.e., installation folder. Can be a fully qualified path or a path that
    /// is relative to the installation folder, e.g., a value of "ConfigurationCache" might resolve to
    /// "C:\Program Files\MyTimeSeriesApp\ConfigurationCache\".
    /// </remarks>
    public string LoggingPath
    {
        get => m_loggingPath ?? "";
        set
        {
            if (!string.IsNullOrWhiteSpace(value))
            {
                string loggingPath = FilePath.GetDirectoryName(FilePath.GetAbsolutePath(value));

                if (Directory.Exists(loggingPath))
                    value = loggingPath;
            }

            m_loggingPath = value;
        }
    }

    /// <summary>
    /// Gets or sets flag that determines if <see cref="DataSubscriber"/> should attempt to auto-connection to <see cref="DataPublisher"/> using defined connection settings.
    /// </summary>
    public bool AutoConnect { get; set; }

    /// <summary>
    /// Gets or sets flag that determines if <see cref="DataSubscriber"/> should
    /// automatically request meta-data synchronization and synchronize publisher
    /// meta-data with its own database configuration.
    /// </summary>
    public bool AutoSynchronizeMetadata { get; set; }

    /// <summary>
    /// Gets flag that indicates whether the connection will be persisted
    /// even while the adapter is offline in order to synchronize metadata.
    /// </summary>
    public bool PersistConnectionForMetadata =>
        !AutoStart && AutoSynchronizeMetadata && !this.TemporalConstraintIsDefined();

    /// <summary>
    /// Gets or sets flag that determines if child devices associated with a subscription
    /// should be prefixed with the subscription name and an exclamation point to ensure
    /// device name uniqueness - recommended value is <c>true</c>.
    /// </summary>
    public bool UseSourcePrefixNames { get; set; }

    /// <summary>
    /// Gets or sets requested meta-data filter expressions to be applied by <see cref="DataPublisher"/> before meta-data is sent.
    /// </summary>
    /// <remarks>
    /// Multiple meta-data filters, such filters for different data tables, should be separated by a semicolon. Specifying fields in the filter
    /// expression that do not exist in the data publisher's current meta-data set could cause filter expressions to not be applied and possibly
    /// result in no meta-data being received for the specified data table.
    /// </remarks>
    /// <example>
    /// FILTER MeasurementDetail WHERE SignalType &lt;&gt; 'STAT'; FILTER PhasorDetail WHERE Phase = '+'
    /// </example>
    public string? MetadataFilters { get; set; }

    /// <summary>
    /// Gets or sets flag that determines if a subscription is mutual, i.e., bidirectional pub/sub. In this mode one node will
    /// be the owner and set <c>Internal = True</c> and the other node will be the renter and set <c>Internal = False</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This flag is intended to be used in scenarios where a remote subscriber can add new measurements associated with a
    /// source device, e.g., creating new calculated result measurements on a remote machine for load distribution that should
    /// get associated with a device on the local machine, thus becoming part of the local measurement set.
    /// </para>
    /// <para>
    /// For best results, both the owner and renter subscriptions should be reduced to needed measurements, i.e., renter should
    /// only receive measurements needed for remote calculations and owner should only receive new calculated results. Note that
    /// when used with a TLS-style subscription this can be accomplished by using the subscription UI screens that control the
    /// measurement <c>subscribed</c> flag. For internal subscriptions, reduction of metadata and subscribed measurements will
    /// need to be controlled via connection string with <c>metadataFilters</c> and <c>outputMeasurements</c>, respectively.
    /// </para>
    /// <para>
    /// Setting <see cref="MutualSubscription"/> to <c>true</c> will force <see cref="ReceiveInternalMetadata"/> to <c>true</c>,
    /// <see cref="ReceiveExternalMetadata"/> to <c>false</c>, and <see cref="FilterOutputMeasurements"/> to <c>false</c>.
    /// </para>
    /// </remarks>
    public bool MutualSubscription { get; set; }

    /// <summary>
    /// Gets or sets flag that informs publisher if base time-offsets can use millisecond resolution to conserve bandwidth.
    /// </summary>
    [Obsolete("SubscriptionInfo object defines this parameter.", false)]
    public bool UseMillisecondResolution { get; set; }

#if !NET
    /// <summary>
    /// Gets or sets flag that determines if a <see cref="TcpSimpleClient"/> should be used for command channel connections.
    /// </summary>
    public bool UseSimpleTcpClient { get; set; }
#endif

    /// <summary>
    /// Gets flag that determines whether the command channel is connected.
    /// </summary>
    public bool CommandChannelConnected => m_clientCommandChannel?.Enabled ?? m_serverCommandChannel?.Enabled ?? false;

    /// <summary>
    /// Gets total data packet bytes received during this session.
    /// </summary>
    public long TotalBytesReceived { get; private set; }

    /// <summary>
    /// Gets or sets data loss monitoring interval, in seconds. Set to zero to disable monitoring.
    /// </summary>
    public double DataLossInterval
    {
        get => m_dataStreamMonitor?.Interval / 1000.0D ?? 0.0D;
        set
        {
            if (value > 0.0D)
            {
                if (m_dataStreamMonitor is null)
                {
                    // Create data stream monitoring timer
                    m_dataStreamMonitor = Common.TimerScheduler.CreateTimer();
                    m_dataStreamMonitor!.Elapsed += DataStreamMonitor_Elapsed;
                    m_dataStreamMonitor.AutoReset = true;
                    m_dataStreamMonitor.Enabled = false;
                }

                // Set user specified interval
                m_dataStreamMonitor.Interval = (int)(value * 1000.0D);
            }
            else
            {
                // Disable data monitor
                if (m_dataStreamMonitor is not null)
                {
                    m_dataStreamMonitor.Elapsed -= DataStreamMonitor_Elapsed;
                    m_dataStreamMonitor.Dispose();
                }

                m_dataStreamMonitor = null;
            }
        }
    }

    /// <summary>
    /// Gets or sets a set of flags that define ways in which the subscriber and publisher communicate.
    /// </summary>
    public OperationalModes OperationalModes
    {
        get => m_operationalModes;
        set
        {
            m_operationalModes = value;
            OperationalEncoding operationalEncoding = (OperationalEncoding)(value & OperationalModes.EncodingMask);
            Encoding = GetCharacterEncoding(operationalEncoding);
        }
    }

    /// <summary>
    /// Gets or sets the operational mode flag to compress meta-data.
    /// </summary>
    public bool CompressMetadata
    {
        get => m_operationalModes.HasFlag(OperationalModes.CompressMetadata);
        set
        {
            if (value)
                m_operationalModes |= OperationalModes.CompressMetadata;
            else
                m_operationalModes &= ~OperationalModes.CompressMetadata;
        }
    }

    /// <summary>
    /// Gets or sets the operational mode flag to compress the signal index cache.
    /// </summary>
    public bool CompressSignalIndexCache
    {
        get => m_operationalModes.HasFlag(OperationalModes.CompressSignalIndexCache);
        set
        {
            if (value)
                m_operationalModes |= OperationalModes.CompressSignalIndexCache;
            else
                m_operationalModes &= ~OperationalModes.CompressSignalIndexCache;
        }
    }

    /// <summary>
    /// Gets or sets the operational mode flag to compress data payloads.
    /// </summary>
    public bool CompressPayload
    {
        get => m_operationalModes.HasFlag(OperationalModes.CompressPayloadData);
        set
        {
            if (value)
                m_operationalModes |= OperationalModes.CompressPayloadData;
            else
                m_operationalModes &= ~OperationalModes.CompressPayloadData;
        }
    }

    /// <summary>
    /// Gets or sets the operational mode flag to receive internal meta-data.
    /// </summary>
    public bool ReceiveInternalMetadata
    {
        get => m_operationalModes.HasFlag(OperationalModes.ReceiveInternalMetadata);
        set
        {
            if (value)
                m_operationalModes |= OperationalModes.ReceiveInternalMetadata;
            else
                m_operationalModes &= ~OperationalModes.ReceiveInternalMetadata;
        }
    }

    /// <summary>
    /// Gets or sets flag that determines if measurements for this data subscription should be marked as "internal", i.e., owned and allowed for proxy.
    /// </summary>
    public bool Internal { get; set; }

    /// <summary>
    /// Gets or sets flag that determines if output measurements should be automatically filtered to only those belonging to the subscriber.
    /// </summary>
    public bool FilterOutputMeasurements { get; set; }

    /// <summary>
    /// Gets or sets flag that determines if identity inserts should be used for SQL Server connections during meta-data synchronization.
    /// </summary>
    public bool UseIdentityInsertsForMetadata { get; set; }

    /// <summary>
    /// Gets or sets flag that determines if CALC signals not defined in metadata should be deleted during synchronizations. Do not set this
    /// value to <c>true</c> if local calculations are being created, and associated with, data arriving from STTP stream.
    /// </summary>
    public bool AutoDeleteCalculatedMeasurements { get; set; }

    /// <summary>
    /// Gets or sets flag that determines if ALARM signals not defined in metadata should be deleted during synchronizations. Do not set this
    /// value to <c>true</c> if local alarms are being created, and associated with, data arriving from STTP stream.
    /// </summary>
    public bool AutoDeleteAlarmMeasurements { get; set; }

    /// <summary>
    /// Gets or sets flag that determines if the data subscriber should attempt to synchronize device metadata as independent devices, i.e.,
    /// not as children of the parent STTP device connection.
    /// Defaults to <c>false</c>.
    /// </summary>
    /// <remarks>
    /// This is useful when using an STTP connection to only synchronize metadata from a publisher, but not to receive data. When enabled,
    /// the device enabled state will not be synchronized upon creation unless <see cref="AutoEnableIndependentlySyncedDevices"/> is set to
    /// <c>true</c>. In this mode it may be useful to add the original "ConnectionString" field to the publisher's device metadata so it can
    /// be synchronized to the subscriber. To ensure no data is received, the subscriber should be configured with an "OutputMeasurements"
    /// filter in the adapter's connection string that does not include any measurements, e.g.:
    /// <code>outputMeasurements={FILTER ActiveMeasurements WHERE False}</code>
    /// </remarks>
    public bool SyncIndependentDevices { get; set; }

    /// <summary>
    /// Gets or sets flag that determines if the data subscriber should automatically enable independently synced devices.
    /// Defaults to <c>false</c>.
    /// </summary>
    public bool AutoEnableIndependentlySyncedDevices { get; set; }

    /// <summary>
    /// Gets or sets flag that determines if statistics engine should be enabled for the data subscriber.
    /// </summary>
    public bool BypassStatistics { get; set; }

    /// <summary>
    /// Gets or sets the operational mode flag to receive external meta-data.
    /// </summary>
    public bool ReceiveExternalMetadata
    {
        get => m_operationalModes.HasFlag(OperationalModes.ReceiveExternalMetadata);
        set
        {
            if (value)
                m_operationalModes |= OperationalModes.ReceiveExternalMetadata;
            else
                m_operationalModes &= ~OperationalModes.ReceiveExternalMetadata;
        }
    }

    /// <summary>
    /// Gets or sets the <see cref="OperationalEncoding"/> used by the subscriber and publisher.
    /// </summary>
    public OperationalEncoding OperationalEncoding
    {
        get => (OperationalEncoding)(m_operationalModes & OperationalModes.EncodingMask);
        set
        {
            m_operationalModes &= ~OperationalModes.EncodingMask;
            m_operationalModes |= (OperationalModes)value;
            Encoding = GetCharacterEncoding(value);
        }
    }

    /// <summary>
    /// Gets or sets the <see cref="CompressionModes"/> used by the subscriber and publisher.
    /// </summary>
    public CompressionModes CompressionModes
    {
        get => (CompressionModes)(m_operationalModes & OperationalModes.CompressionModeMask);
        set
        {
            m_operationalModes &= ~OperationalModes.CompressionModeMask;
            m_operationalModes |= (OperationalModes)value;

            if (value.HasFlag(CompressionModes.TSSC))
                CompressPayload = true;
        }
    }

    /// <summary>
    /// Gets or sets the version number of the protocol in use by this subscriber.
    /// </summary>
    public int Version
    {
        get => (int)(m_operationalModes & OperationalModes.VersionMask);
        set
        {
            m_operationalModes &= ~OperationalModes.VersionMask;
            m_operationalModes |= (OperationalModes)value;
        }
    }

    /// <summary>
    /// Gets the character encoding defined by the
    /// <see cref="OperationalEncoding"/> of the communications stream.
    /// </summary>
    public Encoding Encoding { get; private set; }

    /// <summary>
    /// Gets flag indicating if this adapter supports real-time processing.
    /// </summary>
    /// <remarks>
    /// Setting this value to false indicates that the adapter should not be enabled unless it exists within a temporal session.
    /// As an example, this flag can be used in a gateway system to set up two separate subscribers: one to the PDC for real-time
    /// data streams and one to the historian for historical data streams. In this scenario, the assumption is that the PDC is
    /// the data source for the historian, implying that only local data is destined for archival.
    /// </remarks>
    public bool SupportsRealTimeProcessing { get; private set; }

    /// <summary>
    /// Gets the flag indicating if this adapter supports temporal processing.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Although the data subscriber provisions support for temporal processing by receiving historical data from a remote source,
    /// the adapter opens sockets and does not need to be engaged within an actual temporal <see cref="IaonSession"/>, therefore
    /// this method normally returns <c>false</c> to make sure the adapter doesn't get instantiated within a temporal session.
    /// </para>
    /// <para>
    /// Setting this to <c>true</c> means that a subscriber will be initialized within a temporal session to provide historical
    /// data from a remote source - this should only be enabled in cases where (1) there is no locally defined, e.g., in-process,
    /// historian that can already provide historical data for temporal sessions, and (2) a remote subscriber should be allowed
    /// to proxy temporal requests, e.g., those requested for data gap recovery, to an up-stream subscription. This is useful in
    /// cases where a primary data subscriber that has data gap recovery enabled can also allow a remote subscription to proxy in
    /// data gap recovery requests. It is recommended that remote data gap recovery request parameters be (1) either slightly
    /// looser than those of local system to reduce the possibility of duplicated recovery sessions for the same data loss, or
    /// (2) only enabled in the end-most system that most needs the recovered data, like a historian.
    /// </para>
    /// </remarks>
    public override bool SupportsTemporalProcessing => m_supportsTemporalProcessing;

    /// <summary>
    /// Gets or sets the desired processing interval, in milliseconds, for the adapter.
    /// </summary>
    /// <remarks>
    /// Except for the values of -1 and 0, this value specifies the desired processing interval for data, i.e., basically a delay,
    /// or timer interval, over which to process data. A value of -1 means to use the default processing interval while a value of
    /// 0 means to process data as fast as possible.
    /// </remarks>
    public override int ProcessingInterval
    {
        get => base.ProcessingInterval;
        set
        {
            base.ProcessingInterval = value;

            // Request server update the processing interval
            SendServerCommand(ServerCommand.UpdateProcessingInterval, BigEndian.GetBytes(value));
        }
    }

    /// <summary>
    /// Gets or sets the flag that determines whether to request that the subscription be throttled to certain publication interval, see <see cref="PublishInterval"/>.
    /// </summary>
    public bool Throttled { get; set; }

    /// <summary>
    /// Gets or sets the interval, in seconds, at which data should be published when using a throttled subscription.
    /// </summary>
    public double PublishInterval { get; set; } = -1.0D;

    /// <summary>
    /// Gets or sets the timeout used when executing database queries during meta-data synchronization.
    /// </summary>
    public int MetadataSynchronizationTimeout { get; set; }

    /// <summary>
    /// Gets or sets flag that determines if meta-data synchronization should be performed within a transaction.
    /// </summary>
    public bool UseTransactionForMetadata { get; set; }

    /// <summary>
    /// Gets or sets flag that determines whether to use the local clock when calculating statistics.
    /// </summary>
    public bool UseLocalClockAsRealTime { get; set; }

    /// <summary>
    /// Gets or sets number of parsing exceptions allowed during <see cref="ParsingExceptionWindow"/> before connection is reset.
    /// </summary>
    public int AllowedParsingExceptions { get; set; }

    /// <summary>
    /// Gets or sets time duration, in <see cref="Ticks"/>, to monitor parsing exceptions.
    /// </summary>
    public Ticks ParsingExceptionWindow { get; set; }

    /// <summary>
    /// Gets or sets <see cref="DataSet"/> based data source available to this <see cref="DataSubscriber"/>.
    /// </summary>
    public override DataSet? DataSource
    {
        get => base.DataSource;
        set
        {
            base.DataSource = value;
        #if NET
            m_registerStatisticsOperation.RunAsync();
        #else
            m_registerStatisticsOperation.RunOnce();
        #endif

            bool outputMeasurementsUpdated = AutoConnect && UpdateOutputMeasurements();

            // For automatic connections, when meta-data refresh is complete, update output measurements to see if any
            // points for subscription have changed after re-application of filter expressions and if so, resubscribe
            if (outputMeasurementsUpdated && Enabled && CommandChannelConnected)
            {
                OnStatusMessage(MessageLevel.Info, "Meta-data received from publisher modified measurement availability, adjusting active subscription...");

                // Updating subscription will restart data stream monitor upon successful resubscribe
                if (AutoStart)
                    SubscribeToOutputMeasurements(true);
            }

            if (m_dataGapRecoverer is not null)
                m_dataGapRecoverer.DataSource = value;
        }
    }

    /// <summary>
    /// Gets or sets output measurement keys that are requested by other adapters based on what adapter says it can provide.
    /// </summary>
    public override MeasurementKey[]? RequestedOutputMeasurementKeys
    {
        get => base.RequestedOutputMeasurementKeys;
        set
        {
            MeasurementKey[] oldKeys = base.RequestedOutputMeasurementKeys ?? [];
            MeasurementKey[] newKeys = value ?? [];
            HashSet<MeasurementKey> oldKeySet = [.. oldKeys];

            base.RequestedOutputMeasurementKeys = value;

            if (AutoStart || !Enabled || !CommandChannelConnected || oldKeySet.SetEquals(newKeys))
                return;

            OnStatusMessage(MessageLevel.Info, "Requested measurements have changed, adjusting active subscription...");
            SubscribeToOutputMeasurements(true);
        }
    }

    /// <summary>
    /// Gets or sets output measurements that the <see cref="AdapterBase"/> will produce, if any.
    /// </summary>
    public override IMeasurement[]? OutputMeasurements
    {
        get => base.OutputMeasurements;

        set
        {
            base.OutputMeasurements = value;

            if (m_dataGapRecoverer is not null)
                m_dataGapRecoverer.FilterExpression = this.OutputMeasurementKeys().Select(key => key.SignalID.ToString()).ToDelimitedString(';');
        }
    }

    /// <summary>
    /// Gets connection info for adapter, if any.
    /// </summary>
    public override string? ConnectionInfo
    {
        get
        {
            if (m_serverCommandChannel is not null && string.IsNullOrWhiteSpace(m_connectionID))
            {
                Guid clientID = m_serverCommandChannel.ClientIDs.FirstOrDefault();
                IPEndPoint? endPoint = GetCommandChannelSocket()?.RemoteEndPoint as IPEndPoint;
                m_connectionID = SubscriberConnection.GetEndPointConnectionID(clientID, endPoint);
            }

            string? commandChannelServerUri = m_clientCommandChannel?.ServerUri ?? m_connectionID;
            string? dataChannelServerUri = m_dataChannel?.ServerUri;

            if (string.IsNullOrWhiteSpace(commandChannelServerUri) && string.IsNullOrWhiteSpace(dataChannelServerUri))
                return null;

            if (string.IsNullOrWhiteSpace(dataChannelServerUri))
                return commandChannelServerUri;

            return string.IsNullOrWhiteSpace(commandChannelServerUri) ? dataChannelServerUri : $"{commandChannelServerUri} / {dataChannelServerUri}";
        }
    }

    /// <summary>
    /// Gets the status of this <see cref="DataSubscriber"/>.
    /// </summary>
    /// <remarks>
    /// Derived classes should provide current status information about the adapter for display purposes.
    /// </remarks>
    public override string Status
    {
        get
        {
            StringBuilder status = new();

            status.AppendLine($"          Protocol version: {Version}");
            status.AppendLine($"                 Connected: {CommandChannelConnected}");
            status.AppendLine($"                Subscribed: {m_subscribed}");
            status.AppendLine($"             Security mode: {SecurityMode}");
            status.AppendLine($"             Authenticated: {SecurityMode == SecurityMode.TLS && CommandChannelConnected}");
            status.AppendLine($"         Compression modes: {CompressionModes}");
            status.AppendLine($"       Mutual subscription: {MutualSubscription}{(MutualSubscription ? $" - System has {(Internal ? "Owner" : "Renter")} Role" : "")}");
            status.AppendLine($" Mark received as internal: {Internal}");
            status.AppendLine($" Receive internal metadata: {ReceiveInternalMetadata}");
            status.AppendLine($" Receive external metadata: {ReceiveExternalMetadata}");
            status.AppendLine($"Filter output measurements: {FilterOutputMeasurements}");
            status.AppendLine($"  Synchronize metadata IDs: {UseIdentityInsertsForMetadata}");
            status.AppendLine($"  Auto delete CALC signals: {AutoDeleteCalculatedMeasurements}");
            status.AppendLine($"  Auto delete ALRM signals: {AutoDeleteAlarmMeasurements}");
            status.AppendLine($"  Sync independent devices: {SyncIndependentDevices}");

            if (SyncIndependentDevices)
                status.AppendLine($"Independent synced devices: {(AutoEnableIndependentlySyncedDevices ? "enabled" : "disabled")} on creation");

            status.AppendLine($"  Bypass statistics engine: {BypassStatistics}");
            status.AppendLine($"      Total bytes received: {TotalBytesReceived:N0}");
            status.AppendLine($"      Data packet security: {(SecurityMode == SecurityMode.TLS && m_dataChannel is null ? "Secured via TLS" : m_keyIVs is null ? "Unencrypted" : "AES Encrypted")}");
            status.AppendLine($"      Data monitor enabled: {m_dataStreamMonitor is not null && m_dataStreamMonitor.Enabled}");
            status.AppendLine($"              Logging path: {FilePath.TrimFileName(m_loggingPath.ToNonNullNorWhiteSpace(FilePath.GetAbsolutePath("")), 51)}");
            status.AppendLine($"No data reconnect interval: {(DataLossInterval > 0.0D ? $"{DataLossInterval:0.000} seconds" : "Disabled")}");
            status.AppendLine($"    Data gap recovery mode: {(m_dataGapRecoveryEnabled ? "Enabled" : "Disabled")}");

            if (m_dataGapRecoveryEnabled && m_dataGapRecoverer is not null)
                status.Append(m_dataGapRecoverer.Status);

            if (m_runTimeLog is not null)
            {
                status.AppendLine();
                status.AppendLine("Run-Time Log Status".CenterText(50));
                status.AppendLine("-------------------".CenterText(50));
                status.Append(m_runTimeLog.Status);
            }

            if (m_dataChannel is not null)
            {
                status.AppendLine();
                status.AppendLine("Data Channel Status".CenterText(50));
                status.AppendLine("-------------------".CenterText(50));
                status.Append(m_dataChannel.Status);
            }

            if (m_clientCommandChannel is not null)
            {
                status.AppendLine();
                status.AppendLine("Command Channel Status".CenterText(50));
                status.AppendLine("----------------------".CenterText(50));
                status.Append(m_clientCommandChannel.Status);

            #if !NET
                status.AppendLine($"   Using simple TCP client: {UseSimpleTcpClient}");
            #endif
            }

            if (m_serverCommandChannel is not null)
            {
                status.AppendLine();
                status.AppendLine("Command Channel Status".CenterText(50));
                status.AppendLine("----------------------".CenterText(50));
                status.Append(m_serverCommandChannel.Status);
            }

            status.Append(base.Status);

            return status.ToString();
        }
    }

    /// <summary>
    /// Gets a flag that determines if this <see cref="DataSubscriber"/> uses an asynchronous connection.
    /// </summary>
    protected override bool UseAsyncConnect => true;

    /// <summary>
    /// Gets or sets reference to <see cref="UdpClient"/> data channel, attaching and/or detaching to events as needed.
    /// </summary>
    protected UdpClient? DataChannel
    {
        get => m_dataChannel;
        set
        {
            if (m_dataChannel is not null)
            {
                // Detach from events on existing data channel reference
                m_dataChannel.ConnectionException -= DataChannelConnectionException;
                m_dataChannel.ConnectionAttempt -= DataChannelConnectionAttempt;
                m_dataChannel.ReceiveData -= DataChannelReceiveData;
                m_dataChannel.ReceiveDataException -= DataChannelReceiveDataException;

                if (m_dataChannel != value)
                    m_dataChannel.Dispose();
            }

            // Assign new data channel reference
            m_dataChannel = value;

            if (m_dataChannel is not null)
            {
                // Attach to desired events on new data channel reference
                m_dataChannel.ConnectionException += DataChannelConnectionException;
                m_dataChannel.ConnectionAttempt += DataChannelConnectionAttempt;
                m_dataChannel.ReceiveData += DataChannelReceiveData;
                m_dataChannel.ReceiveDataException += DataChannelReceiveDataException;
            }
        }
    }

    /// <summary>
    /// Gets or sets reference to <see cref="TcpClient"/> command channel, attaching and/or detaching to events as needed.
    /// </summary>
    protected IClient? ClientCommandChannel
    {
        get => m_clientCommandChannel;
        set
        {
            if (m_clientCommandChannel is not null)
            {
                // Detach from events on existing command channel reference
                m_clientCommandChannel.ConnectionAttempt -= ClientCommandChannelConnectionAttempt;
                m_clientCommandChannel.ConnectionEstablished -= ClientCommandChannelConnectionEstablished;
                m_clientCommandChannel.ConnectionException -= ClientCommandChannelConnectionException;
                m_clientCommandChannel.ConnectionTerminated -= ClientCommandChannelConnectionTerminated;
                m_clientCommandChannel.ReceiveData -= ClientCommandChannelReceiveData;
                m_clientCommandChannel.ReceiveDataException -= ClientCommandChannelReceiveDataException;
                m_clientCommandChannel.SendDataException -= ClientCommandChannelSendDataException;

                if (m_clientCommandChannel != value)
                    m_clientCommandChannel.Dispose();
            }

            // Assign new command channel reference
            m_clientCommandChannel = value;

            if (m_clientCommandChannel is not null)
            {
                // Attach to desired events on new command channel reference
                m_clientCommandChannel.ConnectionAttempt += ClientCommandChannelConnectionAttempt;
                m_clientCommandChannel.ConnectionEstablished += ClientCommandChannelConnectionEstablished;
                m_clientCommandChannel.ConnectionException += ClientCommandChannelConnectionException;
                m_clientCommandChannel.ConnectionTerminated += ClientCommandChannelConnectionTerminated;
                m_clientCommandChannel.ReceiveData += ClientCommandChannelReceiveData;
                m_clientCommandChannel.ReceiveDataException += ClientCommandChannelReceiveDataException;
                m_clientCommandChannel.SendDataException += ClientCommandChannelSendDataException;
            }
        }
    }

    /// <summary>
    /// Gets or sets reference to <see cref="TcpServer"/> command channel, attaching and/or detaching to events as needed.
    /// </summary>
    /// <remarks>
    /// This handles reverse connectivity operations.
    /// </remarks>
    protected IServer? ServerCommandChannel
    {
        get => m_serverCommandChannel;
        set
        {
            if (m_serverCommandChannel is not null)
            {
                // Detach from events on existing command channel reference
                m_serverCommandChannel.ClientConnected -= ServerCommandChannelClientConnected;
                m_serverCommandChannel.ClientDisconnected -= ServerCommandChannelClientDisconnected;
                m_serverCommandChannel.ClientConnectingException -= ServerCommandChannelClientConnectingException;
                m_serverCommandChannel.ReceiveClientData -= ServerCommandChannelReceiveClientData;
                m_serverCommandChannel.ReceiveClientDataException -= ServerCommandChannelReceiveClientDataException;
                m_serverCommandChannel.SendClientDataException -= ServerCommandChannelSendClientDataException;
                m_serverCommandChannel.ServerStarted -= ServerCommandChannelServerStarted;
                m_serverCommandChannel.ServerStopped -= ServerCommandChannelServerStopped;

                if (m_serverCommandChannel != value)
                    m_serverCommandChannel.Dispose();
            }

            // Assign new command channel reference
            m_serverCommandChannel = value;

            if (m_serverCommandChannel is not null)
            {
                // Attach to desired events on new command channel reference
                m_serverCommandChannel.ClientConnected += ServerCommandChannelClientConnected;
                m_serverCommandChannel.ClientDisconnected += ServerCommandChannelClientDisconnected;
                m_serverCommandChannel.ClientConnectingException += ServerCommandChannelClientConnectingException;
                m_serverCommandChannel.ReceiveClientData += ServerCommandChannelReceiveClientData;
                m_serverCommandChannel.ReceiveClientDataException += ServerCommandChannelReceiveClientDataException;
                m_serverCommandChannel.SendClientDataException += ServerCommandChannelSendClientDataException;
                m_serverCommandChannel.ServerStarted += ServerCommandChannelServerStarted;
                m_serverCommandChannel.ServerStopped += ServerCommandChannelServerStopped;
            }
        }
    }

    /// <summary>
    /// Gets the total number of measurements processed through this data publisher over the lifetime of the subscriber.
    /// </summary>
    public long LifetimeMeasurements { get; private set; }

    /// <summary>
    /// Gets the minimum value of the measurements per second calculation.
    /// </summary>
    public long MinimumMeasurementsPerSecond { get; private set; }

    /// <summary>
    /// Gets the maximum value of the measurements per second calculation.
    /// </summary>
    public long MaximumMeasurementsPerSecond { get; private set; }

    /// <summary>
    /// Gets the average value of the measurements per second calculation.
    /// </summary>
    public long AverageMeasurementsPerSecond => m_measurementsPerSecondCount == 0L ? 0L : m_totalMeasurementsPerSecond / m_measurementsPerSecondCount;

    /// <summary>
    /// Gets the minimum latency calculated over the full lifetime of the subscriber.
    /// </summary>
    public int LifetimeMinimumLatency => (int)Ticks.ToMilliseconds(m_lifetimeMinimumLatency);

    /// <summary>
    /// Gets the maximum latency calculated over the full lifetime of the subscriber.
    /// </summary>
    public int LifetimeMaximumLatency => (int)Ticks.ToMilliseconds(m_lifetimeMaximumLatency);

    /// <summary>
    /// Gets the average latency calculated over the full lifetime of the subscriber.
    /// </summary>
    public int LifetimeAverageLatency => m_lifetimeLatencyMeasurements == 0 ? -1 : (int)Ticks.ToMilliseconds(m_lifetimeTotalLatency / m_lifetimeLatencyMeasurements);

    /// <summary>
    /// Gets real-time as determined by either the local clock or the latest measurement received.
    /// </summary>
    protected Ticks RealTime => UseLocalClockAsRealTime ? DateTime.UtcNow.Ticks : m_realTime;

    #endregion

    #region [ Methods ]

    /// <summary>
    /// Releases the unmanaged resources used by the <see cref="DataSubscriber"/> object and optionally releases the managed resources.
    /// </summary>
    /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
    protected override void Dispose(bool disposing)
    {
        if (m_disposed)
            return;

        try
        {
            if (!disposing)
                return;

            DataLossInterval = 0.0D;
            ClientCommandChannel = null;
            ServerCommandChannel = null;
            DataChannel = null;

            if (m_dataGapRecoverer is not null)
            {
                m_dataGapRecoverer.RecoveredMeasurements -= DataGapRecoverer_RecoveredMeasurements;
                m_dataGapRecoverer.StatusMessage -= DataGapRecoverer_StatusMessage;
                m_dataGapRecoverer.ProcessException -= DataGapRecoverer_ProcessException;
                m_dataGapRecoverer.Dispose();
                m_dataGapRecoverer = null;
            }

            if (m_runTimeLog is not null)
            {
                m_runTimeLog.ProcessException -= RunTimeLog_ProcessException;
                m_runTimeLog.Dispose();
                m_runTimeLog = null;
            }

            if (m_subscribedDevicesTimer is not null)
            {
                m_subscribedDevicesTimer.Elapsed -= SubscribedDevicesTimer_Elapsed;
                m_subscribedDevicesTimer.Dispose();
                m_subscribedDevicesTimer = null;
            }
        }
        finally
        {
            m_disposed = true; // Prevent duplicate dispose.
            base.Dispose(disposing); // Call base class Dispose().
        }
    }

    /// <summary>
    /// Initializes <see cref="DataSubscriber"/>.
    /// </summary>
    public override void Initialize()
    {
        base.Initialize();

        Dictionary<string, string> settings = Settings;

        // See if user has opted for different operational modes
        if (settings.TryGetValue(nameof(OperationalModes), out string? setting) && Enum.TryParse(setting, true, out OperationalModes operationalModes))
            OperationalModes = operationalModes;

        // Set the security mode if explicitly defined
        if (!settings.TryGetValue(nameof(SecurityMode), out setting) || !Enum.TryParse(setting, true, out SecurityMode securityMode))
            securityMode = SecurityMode.None;

        SecurityMode = securityMode;

        // Apply any version override (e.g., to downgrade to older version)
        if (settings.TryGetValue(nameof(Version), out setting) && int.TryParse(setting, out int value) && value < 32)
            Version = value;

        // Apply gateway compression mode to operational mode flags
        if (settings.TryGetValue(nameof(CompressionModes), out setting) && Enum.TryParse(setting, true, out CompressionModes compressionModes))
            CompressionModes = compressionModes;

        // Check if output measurements should be filtered to only those belonging to the subscriber
        FilterOutputMeasurements = !settings.TryGetValue(nameof(FilterOutputMeasurements), out setting) || setting.ParseBoolean();

        // If no output measurements have been defined when filter output measurements is true, initialize outputs to an empty array,
        // this will ensure that by default subscribed measurements are only those associated with the subscriber
        if (FilterOutputMeasurements && (OutputMeasurements is null || OutputMeasurements.Length == 0))
        {
            List<IMeasurement> outputMeasurements = [];

            foreach (Guid signalID in GetDeviceMeasurements())
            {
                MeasurementKey key = MeasurementKey.LookUpBySignalID(signalID);

                if (key != MeasurementKey.Undefined)
                    outputMeasurements.Add(new Measurement { Metadata = key.Metadata });
            }

            OutputMeasurements = outputMeasurements.ToArray();
        }

        // Check if the subscriber supports real-time and historical processing
        SupportsRealTimeProcessing = !settings.TryGetValue(nameof(SupportsRealTimeProcessing), out setting) || setting.ParseBoolean();
        m_supportsTemporalProcessing = settings.TryGetValue(nameof(SupportsTemporalProcessing), out setting) && setting.ParseBoolean();

        // Check if measurements for this connection should be marked as "internal" - i.e., owned and allowed for proxy
        if (settings.TryGetValue(nameof(Internal), out setting))
            Internal = setting.ParseBoolean();

        // Check if user has explicitly defined the ReceiveInternalMetadata flag
        if (settings.TryGetValue(nameof(ReceiveInternalMetadata), out setting))
            ReceiveInternalMetadata = setting.ParseBoolean();

        // Check if user has explicitly defined the ReceiveExternalMetadata flag
        if (settings.TryGetValue(nameof(ReceiveExternalMetadata), out setting))
            ReceiveExternalMetadata = setting.ParseBoolean();

        // Check if user has explicitly defined the MutualSubscription flag
        if (settings.TryGetValue(nameof(MutualSubscription), out setting) && setting.ParseBoolean())
        {
            MutualSubscription = true;
            ReceiveInternalMetadata = true;
            ReceiveExternalMetadata = false;
            FilterOutputMeasurements = false;
        }

        // Check if user has defined a meta-data synchronization timeout
        if (settings.TryGetValue(nameof(MetadataSynchronizationTimeout), out setting) && int.TryParse(setting, out int metadataSynchronizationTimeout))
            MetadataSynchronizationTimeout = metadataSynchronizationTimeout;

        // Check if user has defined a flag for using a transaction during meta-data synchronization
        if (settings.TryGetValue(nameof(UseTransactionForMetadata), out setting))
            UseTransactionForMetadata = setting.ParseBoolean();

        // Check if user has defined a flag for using identity inserts during meta-data synchronization
        if (settings.TryGetValue(nameof(UseIdentityInsertsForMetadata), out setting))
            UseIdentityInsertsForMetadata = setting.ParseBoolean();

        // Check if user has defined a flag for auto-deletion of CALC signals during meta-data synchronization
        if (settings.TryGetValue(nameof(AutoDeleteCalculatedMeasurements), out setting))
            AutoDeleteCalculatedMeasurements = setting.ParseBoolean();

        // Check if user has defined a flag for auto-deletion of ALRM signals during meta-data synchronization
        if (settings.TryGetValue(nameof(AutoDeleteAlarmMeasurements), out setting))
            AutoDeleteAlarmMeasurements = setting.ParseBoolean();

        // Check if user has defined a flag for synchronizing independent devices during meta-data synchronization
        if (settings.TryGetValue(nameof(SyncIndependentDevices), out setting))
            SyncIndependentDevices = setting.ParseBoolean();

        // Check if user has defined a flag for auto-enabling independently synced devices
        if (settings.TryGetValue(nameof(AutoEnableIndependentlySyncedDevices), out setting))
            AutoEnableIndependentlySyncedDevices = setting.ParseBoolean();

        // Check if user wants to request that publisher use millisecond resolution to conserve bandwidth
    #pragma warning disable CS0618 // Type or member is obsolete
        UseMillisecondResolution = !settings.TryGetValue(nameof(UseMillisecondResolution), out setting) || setting.ParseBoolean();
    #pragma warning restore CS0618

        // Check if user has defined any meta-data filter expressions
        if (settings.TryGetValue(nameof(MetadataFilters), out setting))
            MetadataFilters = setting;

        // Define auto connect setting
        if (settings.TryGetValue(nameof(AutoConnect), out setting))
        {
            AutoConnect = setting.ParseBoolean();

            if (AutoConnect)
                AutoSynchronizeMetadata = true;
        }

        // Define the maximum allowed exceptions before resetting the connection
        if (settings.TryGetValue(nameof(AllowedParsingExceptions), out setting))
            AllowedParsingExceptions = int.Parse(setting);

        // Define the window of time over which parsing exceptions are tolerated
        if (settings.TryGetValue(nameof(ParsingExceptionWindow), out setting))
            ParsingExceptionWindow = Ticks.FromSeconds(double.Parse(setting));

        // Check if synchronize meta-data is explicitly enabled or disabled
        if (settings.TryGetValue(nameof(AutoSynchronizeMetadata), out setting))
            AutoSynchronizeMetadata = setting.ParseBoolean();
        else if (settings.TryGetValue("synchronizeMetadata", out setting))
            AutoSynchronizeMetadata = setting.ParseBoolean();

        // Determine if source name prefixes should be applied during metadata synchronization
        if (settings.TryGetValue(nameof(UseSourcePrefixNames), out setting))
            UseSourcePrefixNames = setting.ParseBoolean();

        // Define data loss interval
        if (settings.TryGetValue(nameof(DataLossInterval), out setting) && double.TryParse(setting, out double interval))
            DataLossInterval = interval;

        // Define buffer size
        if (!settings.TryGetValue("bufferSize", out setting) || !int.TryParse(setting, out int bufferSize))
            bufferSize = DataPublisher.DefaultBufferSize;

        if (settings.TryGetValue(nameof(UseLocalClockAsRealTime), out setting))
            UseLocalClockAsRealTime = setting.ParseBoolean();

        // Handle throttled subscription options
        if (settings.TryGetValue(nameof(Throttled), out setting))
            Throttled = setting.ParseBoolean();

        if (settings.TryGetValue(nameof(PublishInterval), out setting) && double.TryParse(setting, out interval))
            PublishInterval = interval;

        if (AutoConnect)
        {
            // Connect to local events when automatically engaging connection cycle
            ConnectionAuthenticated += DataSubscriber_ConnectionAuthenticated;
            MetaDataReceived += DataSubscriber_MetaDataReceived;

            // Update output measurements to include "subscribed" points
            UpdateOutputMeasurements(true);
        }
        else if (AutoSynchronizeMetadata)
        {
            // Output measurements do not include "subscribed" points,
            // but should still be filtered if applicable
            TryFilterOutputMeasurements();
        }

        // Attempt to retrieve any defined command channel settings
        Dictionary<string, string> commandChannelSettings = settings.TryGetValue("commandChannel", out string? commandChannelConnectionString) ? commandChannelConnectionString.ParseKeyValuePairs() : settings;

        if (string.IsNullOrWhiteSpace(commandChannelConnectionString))
            commandChannelConnectionString = ConnectionString;

        bool serverBasedConnection = !commandChannelSettings.TryGetValue("server", out string? server) || string.IsNullOrWhiteSpace(server);

    #if !NET
        if (settings.TryGetValue(nameof(UseSimpleTcpClient), out setting))
            UseSimpleTcpClient = setting.ParseBoolean();
    #endif

        if (securityMode == SecurityMode.TLS)
        {
            bool checkCertificateRevocation;

            if (!commandChannelSettings.TryGetValue("localCertificate", out string? localCertificate) || !File.Exists(localCertificate))
                localCertificate = GetLocalCertificate();

            if (!commandChannelSettings.TryGetValue("remoteCertificate", out string? remoteCertificate) || !RemoteCertificateExists(ref remoteCertificate))
                throw new ArgumentException("The \"remoteCertificate\" setting must be defined and certificate file must exist when using TLS security mode.");

            if (!commandChannelSettings.TryGetValue("validPolicyErrors", out setting) || !Enum.TryParse(setting, out SslPolicyErrors validPolicyErrors))
                validPolicyErrors = SslPolicyErrors.None;

            if (!commandChannelSettings.TryGetValue("validChainFlags", out setting) || !Enum.TryParse(setting, out X509ChainStatusFlags validChainFlags))
                validChainFlags = X509ChainStatusFlags.NoError;

            if (commandChannelSettings.TryGetValue("checkCertificateRevocation", out setting) && !string.IsNullOrWhiteSpace(setting))
                checkCertificateRevocation = setting.ParseBoolean();
            else
                checkCertificateRevocation = true;

            SimpleCertificateChecker certificateChecker = new();

            // Set up certificate checker
            certificateChecker.TrustedCertificates.Add(new X509Certificate2(FilePath.GetAbsolutePath(remoteCertificate)));
            certificateChecker.ValidPolicyErrors = validPolicyErrors;
            certificateChecker.ValidChainFlags = validChainFlags;

            if (serverBasedConnection)
            {
                // Create a new TLS server
                TlsServer commandChannel = new()
                {
                    ConfigurationString = commandChannelConnectionString,
                    PayloadAware = true,
                    PayloadMarker = null,
                    PayloadEndianOrder = EndianOrder.BigEndian,
                    MaxClientConnections = 1, // Subscriber can only serve a single publisher
                    CertificateFile = localCertificate,
                    CheckCertificateRevocation = checkCertificateRevocation,
                    CertificateChecker = certificateChecker,
                    RequireClientCertificate = true,
                    ReceiveBufferSize = bufferSize,
                    SendBufferSize = bufferSize,
                    NoDelay = true,
                #if !NET
                    PersistSettings = false,
                #endif
                };

                // Assign command channel server reference and attach to needed events
                ServerCommandChannel = commandChannel;
            }
            else
            {
                // Create a new TLS client
                TlsClient commandChannel = new()
                {
                    ConnectionString = commandChannelConnectionString,
                    PayloadAware = true,
                    PayloadMarker = null,
                    PayloadEndianOrder = EndianOrder.BigEndian,
                    MaxConnectionAttempts = 1,
                    CertificateFile = FilePath.GetAbsolutePath(localCertificate!),
                    CheckCertificateRevocation = checkCertificateRevocation,
                    CertificateChecker = certificateChecker,
                    ReceiveBufferSize = bufferSize,
                    SendBufferSize = bufferSize,
                    NoDelay = true,
                #if !NET
                    PersistSettings = false,
                #endif
                };

                // Assign command channel client reference and attach to needed events
                ClientCommandChannel = commandChannel;
            }
        }
        else
        {
            if (serverBasedConnection)
            {
                // Create a new TCP server
                TcpServer commandChannel = new()
                {
                    ConfigurationString = commandChannelConnectionString,
                    PayloadAware = true,
                    PayloadMarker = null,
                    PayloadEndianOrder = EndianOrder.BigEndian,
                    MaxClientConnections = 1, // Subscriber can only serve a single publisher
                    ReceiveBufferSize = bufferSize,
                    SendBufferSize = bufferSize,
                    NoDelay = true,
                #if !NET
                    PersistSettings = false,
                #endif
                };

                // Assign command channel server reference and attach to needed events
                ServerCommandChannel = commandChannel;
            }
            else
            {
            #if !NET
                if (UseSimpleTcpClient)
                {
                    // Create a new simple TCP client
                    TcpSimpleClient commandChannel = new()
                    {
                        ConnectionString = commandChannelConnectionString,
                        PayloadAware = true,
                        PayloadMarker = null,
                        PayloadEndianOrder = EndianOrder.BigEndian,
                        PersistSettings = false,
                        MaxConnectionAttempts = 1,
                        ReceiveBufferSize = bufferSize,
                        SendBufferSize = bufferSize,
                        NoDelay = true
                    };

                    // Assign command channel client reference and attach to needed events
                    ClientCommandChannel = commandChannel;
                }
                else
                {
            #endif
                // Create a new TCP client
                TcpClient commandChannel = new()
                {
                    ConnectionString = commandChannelConnectionString,
                    PayloadAware = true,
                    PayloadMarker = null,
                    PayloadEndianOrder = EndianOrder.BigEndian,
                    MaxConnectionAttempts = 1,
                    ReceiveBufferSize = bufferSize,
                    SendBufferSize = bufferSize,
                    NoDelay = true,
                #if !NET
                    PersistSettings = false,
                #endif
                };

                // Assign command channel client reference and attach to needed events
                ClientCommandChannel = commandChannel;
            #if !NET
                }
            #endif
            }
        }

        if (commandChannelSettings.TryGetValue("DataChannelLocalPort", out setting) && ushort.TryParse(setting, out ushort dataChannelLocalPort))
        {
            settings["commandChannel"] = commandChannelConnectionString;
            settings["port"] = dataChannelLocalPort.ToString();
        }

        // Check for simplified compression setup flag
        if (settings.TryGetValue("compression", out setting) && setting.ParseBoolean())
        {
            CompressionModes |= CompressionModes.TSSC | CompressionModes.GZip;
            OperationalModes |= OperationalModes.CompressPayloadData | OperationalModes.CompressMetadata | OperationalModes.CompressSignalIndexCache;
        }

        // Get logging path, if any has been defined
        if (settings.TryGetValue(nameof(LoggingPath), out setting))
        {
            setting = FilePath.GetDirectoryName(FilePath.GetAbsolutePath(setting));

            if (Directory.Exists(setting))
                m_loggingPath = setting;
            else
                OnStatusMessage(MessageLevel.Info, $"Logging path \"{setting}\" not found, defaulting to \"{FilePath.GetAbsolutePath("")}\"...", flags: MessageFlags.UsageIssue);
        }

        // Initialize data gap recovery processing, if requested
        if (settings.TryGetValue("dataGapRecovery", out setting))
        {
            if (m_clientCommandChannel is null)
            {
                m_dataGapRecoveryEnabled = false;
                OnStatusMessage(MessageLevel.Warning, "Cannot use data gap recovery operations with a server-based data subscriber configuration. Data gap recovery will not be enabled.", "Data Subscriber Initialization", MessageFlags.UsageIssue);
            }
            else
            {
                // Make sure setting exists to allow user to by-pass data gap recovery at a configuration level
            #if NET
                SettingsSection systemSettings = ConfigSettings.Instance[ConfigSettings.SystemSettingsCategory];
                object? dataGapRecoveryEnabledSetting = systemSettings["DataGapRecoveryEnabled"];
                bool dataGapRecoveryEnabled = (dataGapRecoveryEnabledSetting?.ToString() ?? "false").ParseBoolean();
            #else
                ConfigurationFile configFile = ConfigurationFile.Current;
                CategorizedSettingsElementCollection systemSettings = configFile.Settings["systemSettings"];
                CategorizedSettingsElement dataGapRecoveryEnabledSetting = systemSettings["DataGapRecoveryEnabled"];
                bool dataGapRecoveryEnabled = dataGapRecoveryEnabledSetting is not null && dataGapRecoveryEnabledSetting.ValueAsBoolean();
            #endif

                // See if this node should process phasor source validation
                if (dataGapRecoveryEnabled)
                {
                    // Example connection string for data gap recovery:
                    //  dataGapRecovery={enabled=true; recoveryStartDelay=10.0; minimumRecoverySpan=0.0; maximumRecoverySpan=3600.0}
                    Dictionary<string, string> dataGapSettings = setting.ParseKeyValuePairs();

                    if (dataGapSettings.TryGetValue("enabled", out setting) && setting.ParseBoolean())
                    {
                        // Remove dataGapRecovery connection setting from command channel connection string, if defined there.
                        // This will prevent any recursive data gap recovery operations from being established:
                        Dictionary<string, string> connectionSettings = m_clientCommandChannel.ConnectionString.ParseKeyValuePairs();
                        connectionSettings.Remove("dataGapRecovery");
                        connectionSettings.Remove("synchronizeMetadata");
                        connectionSettings.Remove(nameof(AutoConnect));
                        connectionSettings.Remove(nameof(AutoSynchronizeMetadata));
                        connectionSettings.Remove(nameof(OutputMeasurements));
                        connectionSettings.Remove(nameof(BypassStatistics));
                        connectionSettings.Remove(nameof(LoggingPath));

                        if (dataGapSettings.ContainsKey("server"))
                            connectionSettings.Remove("server");

                        // Note that the data gap recoverer will connect on the same command channel port as
                        // the real-time subscriber (TCP only)
                        m_dataGapRecoveryEnabled = true;

                        m_dataGapRecoverer = new DataGapRecoverer
                        {
                            SourceConnectionName = Name,
                            DataSource = DataSource,
                            ConnectionString = string.Join("; ", $"autoConnect=false; synchronizeMetadata=false{(string.IsNullOrWhiteSpace(m_loggingPath) ? "" : "; loggingPath=" + m_loggingPath)}", dataGapSettings.JoinKeyValuePairs(), connectionSettings.JoinKeyValuePairs()),
                            FilterExpression = this.OutputMeasurementKeys().Select(key => key.SignalID.ToString()).ToDelimitedString(';')
                        };

                        m_dataGapRecoverer.RecoveredMeasurements += DataGapRecoverer_RecoveredMeasurements;
                        m_dataGapRecoverer.StatusMessage += DataGapRecoverer_StatusMessage;
                        m_dataGapRecoverer.ProcessException += DataGapRecoverer_ProcessException;
                        m_dataGapRecoverer.Initialize();
                    }
                    else
                    {
                        m_dataGapRecoveryEnabled = false;
                    }
                }
            }
        }
        else
        {
            m_dataGapRecoveryEnabled = false;
        }

        if (settings.TryGetValue(nameof(BypassStatistics), out setting) && setting.ParseBoolean())
        {
            BypassStatistics = true;
        }
        else
        {
            void statisticsCalculated(object? sender, EventArgs args)
            {
                ResetMeasurementsPerSecondCounters();
            }

            StatisticsEngine.Register(this, "Subscriber", "SUB");
            StatisticsEngine.Calculated += statisticsCalculated;
            Disposed += (_, _) => StatisticsEngine.Calculated -= statisticsCalculated;
        }

        if (PersistConnectionForMetadata)
        {
            m_clientCommandChannel?.ConnectAsync();
            m_serverCommandChannel?.Start();
        }

        Initialized = true;
    }

    // Initialize (or reinitialize) the output measurements associated with the data subscriber.
    // Returns true if output measurements were updated, otherwise false if they remain the same.
    private bool UpdateOutputMeasurements(bool initialCall = false)
    {
        IMeasurement[]? originalOutputMeasurements = OutputMeasurements;

        // Reapply output measurements if reinitializing - this way filter expressions and/or sourceIDs
        // will be reapplied. This can be important after a meta-data refresh which may have added new
        // measurements that could now be applicable as desired output measurements.
        if (!initialCall)
        {
            if (Settings.TryGetValue("outputMeasurements", out string? setting))
                OutputMeasurements = ParseOutputMeasurements(DataSource, true, setting);

        #pragma warning disable CA2245
            OutputSourceIDs = OutputSourceIDs;
        #pragma warning restore CA2245
        }

        // If active measurements are defined, attempt to defined desired subscription points from there
        if ((SecurityMode == SecurityMode.TLS || FilterOutputMeasurements) && DataSource is not null && DataSource.Tables.Contains("ActiveMeasurements"))
        {
            try
            {
                // Filter to points associated with this subscriber that have been requested for subscription, are enabled and not owned locally
                DataRow[] filteredRows = DataSource.Tables["ActiveMeasurements"]!.Select("Subscribed <> 0");
                List<IMeasurement> subscribedMeasurements = [];

                foreach (DataRow row in filteredRows)
                {
                    // Create a new measurement for the provided field level information
                    Measurement measurement = new();

                    // Parse primary measurement identifier
                    Guid signalID = row["SignalID"].ToNonNullString(Guid.Empty.ToString()).ConvertToType<Guid>();

                    // Set measurement key if defined
                    MeasurementKey key = MeasurementKey.LookUpOrCreate(signalID, row["ID"].ToString());
                    measurement.Metadata = key.Metadata;
                    subscribedMeasurements.Add(measurement);
                }

                // Combine subscribed output measurement with any existing output measurement and return unique set
                if (subscribedMeasurements.Count > 0)
                    OutputMeasurements = OutputMeasurements is null ? subscribedMeasurements.ToArray() : subscribedMeasurements.Concat(OutputMeasurements).Distinct().ToArray();
            }
            catch (Exception ex)
            {
                // Errors here may not be catastrophic, this simply limits the auto-assignment of input measurement keys desired for subscription
                OnProcessException(MessageLevel.Warning, new InvalidOperationException($"Failed to apply subscribed measurements to subscription filter: {ex.Message}", ex));
            }
        }

        // Ensure that we are not attempting to subscribe to
        // measurements that we know cannot be published
        TryFilterOutputMeasurements();

        // Determine if output measurements have changed
        return originalOutputMeasurements?.CompareTo(OutputMeasurements, false) != 0;
    }

    // When synchronizing meta-data, the publisher sends meta-data for all possible signals we can subscribe to.
    // Here we check each signal defined in OutputMeasurements to determine whether that signal was defined in
    // the published meta-data rather than blindly attempting to subscribe to all signals.
    private void TryFilterOutputMeasurements()
    {
        if (!FilterOutputMeasurements)
            return;

        try
        {
            HashSet<Guid> measurementIDSet = GetDeviceMeasurements();

            if (measurementIDSet.Count == 0 || OutputMeasurements is null)
            {
                OutputMeasurements = [];
                return;
            }

            OutputMeasurements = OutputMeasurements.Where(measurement => measurementIDSet.Contains(measurement.ID)).ToArray();
        }
        catch (Exception ex)
        {
            OnProcessException(MessageLevel.Warning, new InvalidOperationException($"Error when filtering output measurements by device ID: {ex.Message}", ex));
        }
    }

    private HashSet<Guid> GetDeviceMeasurements()
    {
        if (DataSource is null || !DataSource.Tables.Contains("ActiveMeasurements"))
            return [];

        Guid signalID = Guid.Empty;

        IEnumerable<Guid> measurementIDs = DataSource.Tables["ActiveMeasurements"]!
            .Select($"Convert(DeviceID, 'System.String') = '{ID}'")
            .Where(row => Guid.TryParse(row["SignalID"].ToNonNullString(), out signalID))
            .Select(_ => signalID);

        return [..measurementIDs];
    }

    /// <summary>
    /// Subscribes (or re-subscribes) to a data publisher for a set of data points.
    /// </summary>
    /// <param name="info">Configuration object that defines the subscription.</param>
    /// <returns><c>true</c> if subscribe transmission was successful; otherwise <c>false</c>.</returns>
    public bool Subscribe(SubscriptionInfo info)
    {
        StringBuilder connectionString = new();
        AssemblyInfo assemblyInfo = new(typeof(DataSubscriber).Assembly);

        connectionString.Append($"throttled={info.Throttled};");
        connectionString.Append($"publishInterval={info.PublishInterval};");
        connectionString.Append($"includeTime={info.IncludeTime};");
        connectionString.Append($"lagTime={info.LagTime};");
        connectionString.Append($"leadTime={info.LeadTime};");
        connectionString.Append($"useLocalClockAsRealTime={info.UseLocalClockAsRealTime};");
        connectionString.Append($"processingInterval={info.ProcessingInterval};");
        connectionString.Append($"useMillisecondResolution={info.UseMillisecondResolution};");
        connectionString.Append($"requestNaNValueFilter={info.RequestNaNValueFilter};");

        Version version = assemblyInfo.Version ?? new Version(0, 0, 0);
    #if NET
        const string SourceLib = nameof(Gemstone);
    #else
        const string SourceLib = nameof(GSF);
    #endif
        connectionString.Append($"assemblyInfo={{source=STTP {SourceLib} Library ({assemblyInfo.Name}.dll); version={version.Major}.{version.Minor}.{version.Build}; updatedOn={assemblyInfo.BuildDate:yyyy-MM-dd HH:mm:ss} }};");

        if (!string.IsNullOrWhiteSpace(info.FilterExpression))
            connectionString.Append($"filterExpression={{{info.FilterExpression}}};");

        if (info.UdpDataChannel)
            connectionString.Append($"dataChannel={{localport={info.DataChannelLocalPort}}};");

        if (!string.IsNullOrWhiteSpace(info.StartTime))
            connectionString.Append($"startTimeConstraint={info.StartTime};");

        if (!string.IsNullOrWhiteSpace(info.StopTime))
            connectionString.Append($"stopTimeConstraint={info.StopTime};");

        if (!string.IsNullOrWhiteSpace(info.ConstraintParameters))
            connectionString.Append($"timeConstraintParameters={info.ConstraintParameters};");

        if (!string.IsNullOrWhiteSpace(info.ExtraConnectionStringParameters))
            connectionString.Append($"{info.ExtraConnectionStringParameters};");

        // Make sure not to monitor for data loss any faster than down-sample time on throttled connections - additionally
        // you will want to make sure data stream monitor is twice lag-time to allow time for initial points to arrive.
        if (info.Throttled && m_dataStreamMonitor is not null && m_dataStreamMonitor.Interval / 1000.0D < info.LagTime)
            m_dataStreamMonitor.Interval = (int)(2.0D * info.LagTime * 1000.0D);

        // Set millisecond resolution member variable for compact measurement parsing
    #pragma warning disable 618
        UseMillisecondResolution = info.UseMillisecondResolution;
    #pragma warning restore 618

        return Subscribe(info.UseCompactMeasurementFormat, connectionString.ToString());
    }

    /// <summary>
    /// Subscribes (or re-subscribes) to a data publisher for an unsynchronized set of data points.
    /// </summary>
    /// <param name="compactFormat">Boolean value that determines if the compact measurement format should be used. Set to <c>false</c> for full fidelity measurement serialization; otherwise set to <c>true</c> for bandwidth conservation.</param>
    /// <param name="throttled">Boolean value that determines if data should be throttled at a set transmission interval or sent on change.</param>
    /// <param name="filterExpression">Filtering expression that defines the measurements that are being subscribed.</param>
    /// <param name="dataChannel">Desired UDP return data channel connection string to use for data packet transmission. Set to <c>null</c> to use TCP channel for data transmission.</param>
    /// <param name="includeTime">Boolean value that determines if time is a necessary component in streaming data.</param>
    /// <param name="lagTime">When <paramref name="throttled"/> is <c>true</c>, defines the data transmission speed in seconds (can be sub-second).</param>
    /// <param name="leadTime">When <paramref name="throttled"/> is <c>true</c>, defines the allowed time deviation tolerance to real-time in seconds (can be sub-second).</param>
    /// <param name="useLocalClockAsRealTime">When <paramref name="throttled"/> is <c>true</c>, defines boolean value that determines whether to use the local clock time as real-time. Set to <c>false</c> to use latest received measurement timestamp as real-time.</param>
    /// <param name="startTime">Defines a relative or exact start time for the temporal constraint to use for historical playback.</param>
    /// <param name="stopTime">Defines a relative or exact stop time for the temporal constraint to use for historical playback.</param>
    /// <param name="constraintParameters">Defines any temporal parameters related to the constraint to use for historical playback.</param>
    /// <param name="processingInterval">Defines the desired processing interval milliseconds, i.e., historical play back speed, to use when temporal constraints are defined.</param>
    /// <param name="publishInterval">Defines the interval, in seconds, at which data should be published when using a throttled subscription.</param>
    /// <returns><c>true</c> if subscribe transmission was successful; otherwise <c>false</c>.</returns>
    /// <remarks>
    /// <para>
    /// When the <paramref name="startTime"/> or <paramref name="stopTime"/> temporal processing constraints are defined (i.e., not <c>null</c>), this
    /// specifies the start and stop time over which the subscriber session will process data. Passing in <c>null</c> for the <paramref name="startTime"/>
    /// and <paramref name="stopTime"/> specifies the subscriber session will process data in standard, i.e., real-time, operation.
    /// </para>
    /// <para>
    /// Except for the values of -1 and 0, the <paramref name="processingInterval"/> value specifies the desired historical playback data
    /// processing interval in milliseconds. This is basically a delay, or timer interval, over which to process data. Setting this value
    /// to -1 means to use the default processing interval while setting the value to 0 means to process data as fast as possible.
    /// </para>
    /// <para>
    /// The <paramref name="startTime"/> and <paramref name="stopTime"/> parameters can be specified in one of the
    /// following formats:
    /// <list type="table">
    ///     <listheader>
    ///         <term>Time Format</term>
    ///         <description>Format Description</description>
    ///     </listheader>
    ///     <item>
    ///         <term>12-30-2000 23:59:59.033</term>
    ///         <description>Absolute date and time.</description>
    ///     </item>
    ///     <item>
    ///         <term>*</term>
    ///         <description>Evaluates to <see cref="DateTime.UtcNow"/>.</description>
    ///     </item>
    ///     <item>
    ///         <term>*-20s</term>
    ///         <description>Evaluates to 20 seconds before <see cref="DateTime.UtcNow"/>.</description>
    ///     </item>
    ///     <item>
    ///         <term>*-10m</term>
    ///         <description>Evaluates to 10 minutes before <see cref="DateTime.UtcNow"/>.</description>
    ///     </item>
    ///     <item>
    ///         <term>*-1h</term>
    ///         <description>Evaluates to 1 hour before <see cref="DateTime.UtcNow"/>.</description>
    ///     </item>
    ///     <item>
    ///         <term>*-1d</term>
    ///         <description>Evaluates to 1 day before <see cref="DateTime.UtcNow"/>.</description>
    ///     </item>
    /// </list>
    /// </para>
    /// </remarks>
    [Obsolete("Preferred method uses SubscriptionInfo object to subscribe.", false)]
    public virtual bool Subscribe(bool compactFormat, bool throttled, string filterExpression, string? dataChannel = null, bool includeTime = true, double lagTime = 10.0D, double leadTime = 5.0D, bool useLocalClockAsRealTime = false, string? startTime = null, string? stopTime = null, string? constraintParameters = null, int processingInterval = -1, double publishInterval = -1.0D)
    {
        int port = 0;

        if (!string.IsNullOrWhiteSpace(dataChannel))
        {
            Dictionary<string, string> settings = dataChannel.ParseKeyValuePairs();

            if (settings.TryGetValue("port", out string? setting) || settings.TryGetValue("localPort", out setting))
        #pragma warning disable CA1806
                int.TryParse(setting, out port);
        #pragma warning restore CA1806
        }

        return Subscribe(new SubscriptionInfo
        {
            UseCompactMeasurementFormat = compactFormat,
            Throttled = throttled,
            PublishInterval = publishInterval,
            UdpDataChannel = port > 0,
            DataChannelLocalPort = port,
            FilterExpression = filterExpression,
            IncludeTime = includeTime,
            LagTime = lagTime,
            LeadTime = leadTime,
            UseLocalClockAsRealTime = useLocalClockAsRealTime,
            StartTime = startTime,
            StopTime = stopTime,
            ConstraintParameters = constraintParameters,
            ProcessingInterval = processingInterval,
            UseMillisecondResolution = UseMillisecondResolution // When used as adapter, use configured option
        });
    }

    /// <summary>
    /// Subscribes (or re-subscribes) to a data publisher for a set of data points.
    /// </summary>
    /// <param name="compactFormat">Boolean value that determines if the compact measurement format should be used. Set to <c>false</c> for full fidelity measurement serialization; otherwise set to <c>true</c> for bandwidth conservation.</param>
    /// <param name="connectionString">Connection string that defines required and optional parameters for the subscription.</param>
    /// <returns><c>true</c> if subscribe transmission was successful; otherwise <c>false</c>.</returns>
    public virtual bool Subscribe(bool compactFormat, string connectionString)
    {
        bool success = false;

        if (!string.IsNullOrWhiteSpace(connectionString))
        {
            try
            {
                // Parse connection string to see if it contains a data channel definition
                Dictionary<string, string> settings = connectionString.ParseKeyValuePairs();
                UdpClient? dataChannel = null;

                // Track specified time inclusion for later deserialization
                m_includeTime = !settings.TryGetValue("includeTime", out string? setting) || setting.ParseBoolean();

                settings.TryGetValue("dataChannel", out setting);

                if (!string.IsNullOrWhiteSpace(setting))
                {
                    if ((CompressionModes & CompressionModes.TSSC) > 0)
                    {
                        // TSSC is a stateful compression algorithm which will not reliably support UDP
                        OnStatusMessage(MessageLevel.Warning, "Cannot use TSSC compression mode with UDP - special compression mode disabled");

                        // Disable TSSC compression processing
                        CompressionModes &= ~CompressionModes.TSSC;
                    }

                    dataChannel = new UdpClient(setting)
                    {
                        ReceiveBufferSize = ushort.MaxValue,
                        MaxConnectionAttempts = -1
                    };

                    dataChannel.ConnectAsync();
                }

                // Assign data channel client reference and attach to needed events
                DataChannel = dataChannel;

                // Setup subscription packet
                using BlockAllocatedMemoryStream buffer = new();
                DataPacketFlags flags = DataPacketFlags.NoFlags;

                if (compactFormat)
                    flags |= DataPacketFlags.Compact;

                // Write data packet flags into buffer
                buffer.WriteByte((byte)flags);

                // Get encoded bytes of connection string
                byte[] bytes = Encoding.GetBytes(connectionString);

                // Write encoded connection string length into buffer
                buffer.Write(BigEndian.GetBytes(bytes.Length), 0, 4);

                // Encode connection string into buffer
                buffer.Write(bytes, 0, bytes.Length);

                // Send subscribe server command with associated command buffer
                success = SendServerCommand(ServerCommand.Subscribe, buffer.ToArray());
            }
            catch (Exception ex)
            {
                OnProcessException(MessageLevel.Error, new InvalidOperationException($"Exception occurred while trying to make publisher subscription: {ex.Message}", ex));
            }
        }
        else
        {
            OnProcessException(MessageLevel.Error, new InvalidOperationException("Cannot make publisher subscription without a connection string."));
        }

        return success;
    }

    /// <summary>
    /// Subscribes to a data publisher based on currently configured adapter settings.
    /// </summary>
    /// <returns><c>true</c> if subscribe command was sent successfully; otherwise <c>false</c>.</returns>
    [AdapterCommand("Subscribes to data publisher.")]
    [EditorBrowsable(EditorBrowsableState.Advanced)] // Method exists for remote console execution
    public virtual bool Subscribe()
    {
        return SubscribeToOutputMeasurements(!m_metadataRefreshPending);
    }

    /// <summary>
    /// Unsubscribes from a data publisher.
    /// </summary>
    /// <returns><c>true</c> if unsubscribe command was sent successfully; otherwise <c>false</c>.</returns>
    [AdapterCommand("Unsubscribes from data publisher.")]
    public virtual bool Unsubscribe()
    {
        return SendServerCommand(ServerCommand.Unsubscribe);
    }

    /// <summary>
    /// Returns the measurements signal IDs that were authorized after the last successful subscription request.
    /// </summary>
    [AdapterCommand("Gets authorized signal IDs from last subscription request.")]
    [Label("Get Authorized Signal IDs")]
    public virtual Guid[] GetAuthorizedSignalIDs()
    {
        lock (m_signalIndexCacheLock)
            return m_signalIndexCache?[m_cacheIndex] is null ? [] : m_signalIndexCache[m_cacheIndex]!.AuthorizedSignalIDs;
    }

    /// <summary>
    /// Returns the measurements signal IDs that were unauthorized after the last successful subscription request.
    /// </summary>
    [AdapterCommand("Gets unauthorized signal IDs from last subscription request.")]
    [Label("Get Unauthorized SignalIDs")]
    public virtual Guid[] GetUnauthorizedSignalIDs()
    {
        lock (m_signalIndexCacheLock)
            return m_signalIndexCache?[m_cacheIndex] is null ? [] : m_signalIndexCache[m_cacheIndex]!.UnauthorizedSignalIDs;
    }

    /// <summary>
    /// Resets the counters for the lifetime statistics without interrupting the adapter's operations.
    /// </summary>
    [AdapterCommand("Resets the counters for the lifetime statistics without interrupting the adapter's operations.")]
    [Label("Reset Lifetime Counters")]
    public virtual void ResetLifetimeCounters()
    {
        LifetimeMeasurements = 0L;
        TotalBytesReceived = 0L;
        m_lifetimeTotalLatency = 0L;
        m_lifetimeMinimumLatency = 0L;
        m_lifetimeMaximumLatency = 0L;
        m_lifetimeLatencyMeasurements = 0L;
    }

    /// <summary>
    /// Initiate a meta-data refresh.
    /// </summary>
    [AdapterCommand("Initiates a meta-data refresh.")]
    [Label("Refresh Metadata")]
    public virtual void RefreshMetadata()
    {
        SendServerCommand(ServerCommand.MetaDataRefresh, MetadataFilters);
    }

    /// <summary>
    /// Log a data gap for data gap recovery.
    /// </summary>
    /// <param name="timeString">The string representing the data gap.</param>
    [AdapterCommand("Logs a data gap for data gap recovery.")]
    [Label("Log Data Gap")]
    [Parameter(nameof(timeString), "Time String", "The string representing the data gap.")]
    public virtual void LogDataGap(string timeString)
    {
        DateTimeOffset end = default;
        string[] split = timeString.Split(';');

        if (!m_dataGapRecoveryEnabled)
            throw new InvalidOperationException("Data gap recovery is not enabled.");

        if (split.Length != 2)
            throw new FormatException("Invalid format for time string - ex: 2014-03-27 02:10:47.566;2014-03-27 02:10:59.733");

        string startTime = split[0];
        string endTime = split[1];

        bool parserSuccessful =
            DateTimeOffset.TryParse(startTime, CultureInfo.CurrentCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AllowInnerWhite, out DateTimeOffset start) &&
            DateTimeOffset.TryParse(endTime, CultureInfo.CurrentCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AllowInnerWhite, out end);

        if (!parserSuccessful)
            throw new FormatException("Invalid format for time string - ex: 2014-03-27 02:10:47.566;2014-03-27 02:10:59.733");

        m_dataGapRecoverer?.LogDataGap(start, end, true);
    }

    /// <summary>
    /// Remove a data gap from data gap recovery.
    /// </summary>
    /// <param name="timeString">The string representing the data gap.</param>
    [AdapterCommand("Removes a data gap from data gap recovery.")]
    [Label("Remove Data Gap")]
    [Parameter(nameof(timeString), "Time String", "The string representing the data gap.")]
    public virtual string RemoveDataGap(string timeString)
    {
        DateTimeOffset end = default;
        string[] split = timeString.Split(';');

        if (!m_dataGapRecoveryEnabled)
            throw new InvalidOperationException("Data gap recovery is not enabled.");

        if (split.Length != 2)
            throw new FormatException("Invalid format for time string - ex: 2014-03-27 02:10:47.566;2014-03-27 02:10:59.733");

        string startTime = split[0];
        string endTime = split[1];

        bool parserSuccessful =
            DateTimeOffset.TryParse(startTime, CultureInfo.CurrentCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AllowInnerWhite, out DateTimeOffset start) &&
            DateTimeOffset.TryParse(endTime, CultureInfo.CurrentCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AllowInnerWhite, out end);

        if (!parserSuccessful)
            throw new FormatException("Invalid format for time string - ex: 2014-03-27 02:10:47.566;2014-03-27 02:10:59.733");

        if (m_dataGapRecoverer?.RemoveDataGap(start, end) ?? false)
            return "Data gap successfully removed.";

        return "Data gap not found.";
    }

    /// <summary>
    /// Displays the contents of the outage log.
    /// </summary>
    /// <returns>The contents of the outage log.</returns>
    [AdapterCommand("Displays data gaps queued for data gap recovery.")]
    [Label("Dump Outage Log")]
    public virtual string DumpOutageLog()
    {
        if (m_dataGapRecoveryEnabled && m_dataGapRecoverer is not null)
            return Environment.NewLine + m_dataGapRecoverer.DumpOutageLog();

        throw new InvalidOperationException("Data gap recovery not enabled");
    }

    /// <summary>
    /// Gets the status of the temporal <see cref="DataSubscriber"/> used by the data gap recovery module.
    /// </summary>
    /// <returns>Status of the temporal <see cref="DataSubscriber"/> used by the data gap recovery module.</returns>
    [AdapterCommand("Gets the status of the temporal subscription used by the data gap recovery module.")]
    [Label("Get Data Gap Recovery Subscription Status")]
    public virtual string GetDataGapRecoverySubscriptionStatus()
    {
        if (m_dataGapRecoveryEnabled && m_dataGapRecoverer is not null)
            return m_dataGapRecoverer.TemporalSubscriptionStatus;

        return "Data gap recovery not enabled";
    }

    /// <summary>
    /// Spawn meta-data synchronization.
    /// </summary>
    /// <param name="metadata"><see cref="DataSet"/> to use for synchronization.</param>
    /// <remarks>
    /// This method makes sure only one meta-data synchronization happens at a time.
    /// </remarks>
    public void SynchronizeMetadata(DataSet metadata)
    {
        try
        {
            m_receivedMetadata = metadata;
        #if NET
            m_synchronizeMetadataOperation.RunAsync();
        #else
            m_synchronizeMetadataOperation.RunOnceAsync();
        #endif
        }
        catch (Exception ex)
        {
            // Process exception for logging
            OnProcessException(MessageLevel.Warning, new InvalidOperationException($"Failed to queue meta-data synchronization: {ex.Message}", ex));
        }
    }

    /// <summary>
    /// Sends a server command to the publisher connection with associated <paramref name="message"/> data.
    /// </summary>
    /// <param name="commandCode"><see cref="ServerCommand"/> to send.</param>
    /// <param name="message">String based command data to send to server.</param>
    /// <returns><c>true</c> if <paramref name="commandCode"/> transmission was successful; otherwise <c>false</c>.</returns>
    public virtual bool SendServerCommand(ServerCommand commandCode, string? message)
    {
        if (string.IsNullOrWhiteSpace(message))
            return SendServerCommand(commandCode);

        using BlockAllocatedMemoryStream buffer = new();
        byte[] bytes = Encoding.GetBytes(message);

        buffer.Write(BigEndian.GetBytes(bytes.Length), 0, 4);
        buffer.Write(bytes, 0, bytes.Length);

        return SendServerCommand(commandCode, buffer.ToArray());
    }

    /// <summary>
    /// Sends a server command to the publisher connection.
    /// </summary>
    /// <param name="commandCode"><see cref="ServerCommand"/> to send.</param>
    /// <param name="data">Optional command data to send.</param>
    /// <returns><c>true</c> if <paramref name="commandCode"/> transmission was successful; otherwise <c>false</c>.</returns>
    public virtual bool SendServerCommand(ServerCommand commandCode, byte[]? data = null)
    {
        if (m_clientCommandChannel?.CurrentState == ClientState.Connected || m_serverCommandChannel?.CurrentState == ServerState.Running && m_activeClientID != Guid.Empty)
        {
            try
            {
                using BlockAllocatedMemoryStream commandPacket = new();
                // Write command code into command packet
                commandPacket.WriteByte((byte)commandCode);

                // Write command buffer into command packet
                if (data is not null && data.Length > 0)
                    commandPacket.Write(data, 0, data.Length);

                // Send command packet to publisher
                m_clientCommandChannel?.SendAsync(commandPacket.ToArray(), 0, (int)commandPacket.Length);
                m_serverCommandChannel?.SendToAsync(m_activeClientID, commandPacket.ToArray(), 0, (int)commandPacket.Length);
                m_metadataRefreshPending = commandCode == ServerCommand.MetaDataRefresh;

                return true;
            }
            catch (Exception ex)
            {
                OnProcessException(MessageLevel.Error, new InvalidOperationException($"Exception occurred while trying to send server command \"{commandCode}\" to publisher: {ex.Message}", ex));
            }
        }
        else
            OnProcessException(MessageLevel.Error, new InvalidOperationException($"Subscriber is currently unconnected. Cannot send server command \"{commandCode}\" to publisher."));

        return false;
    }

    /// <summary>
    /// Attempts to connect to this <see cref="DataSubscriber"/>.
    /// </summary>
    protected override void AttemptConnection()
    {
        if (!this.TemporalConstraintIsDefined() && !SupportsRealTimeProcessing)
            return;

        long now = UseLocalClockAsRealTime ? DateTime.UtcNow.Ticks : 0L;
        List<DeviceStatisticsHelper<SubscribedDevice>>? statisticsHelpers = m_statisticsHelpers;

    #if NET
        m_registerStatisticsOperation.RunAsync();
    #else
        m_registerStatisticsOperation.RunOnceAsync();
    #endif
        m_expectedBufferBlockSequenceNumber = 0u;
        m_commandChannelConnectionAttempts = 0;
        m_dataChannelConnectionAttempts = 0;

        m_subscribed = false;
        m_keyIVs = null;
        TotalBytesReceived = 0L;
        m_monitoredBytesReceived = 0L;
        m_lastBytesReceived = 0;
        m_lastReceivedAt = DateTime.MinValue;

        if (!PersistConnectionForMetadata)
        {
            m_clientCommandChannel?.ConnectAsync();
            m_serverCommandChannel?.Start();
        }
        else
        {
            OnConnected();
        }

        if (PersistConnectionForMetadata && CommandChannelConnected)
            SubscribeToOutputMeasurements(true);

        if (UseLocalClockAsRealTime && m_subscribedDevicesTimer is null)
        {
            m_subscribedDevicesTimer = Common.TimerScheduler.CreateTimer(1000);
            m_subscribedDevicesTimer.Elapsed += SubscribedDevicesTimer_Elapsed;
        }

        if (statisticsHelpers is not null)
        {
            m_realTime = 0L;
            m_lastStatisticsHelperUpdate = 0L;

            foreach (DeviceStatisticsHelper<SubscribedDevice> statisticsHelper in statisticsHelpers)
                statisticsHelper.Reset(now);
        }

        if (UseLocalClockAsRealTime)
            m_subscribedDevicesTimer?.Start();
    }

    /// <summary>
    /// Attempts to disconnect from this <see cref="DataSubscriber"/>.
    /// </summary>
    protected override void AttemptDisconnection()
    {
        // Unregister device statistics
    #if NET
        m_registerStatisticsOperation.RunAsync();
    #else
        m_registerStatisticsOperation.RunOnceAsync();
    #endif

        // Stop data stream monitor
        if (m_dataStreamMonitor is not null)
            m_dataStreamMonitor.Enabled = false;

        // Disconnect command channel
        if (!PersistConnectionForMetadata)
        {
            m_clientCommandChannel?.Disconnect();
            m_serverCommandChannel?.Stop();
        }

        m_activeClientID = Guid.Empty;
        m_subscribedDevicesTimer?.Stop();
        m_metadataRefreshPending = false;
    }

    /// <summary>
    /// Gets a short one-line status of this <see cref="DataSubscriber"/>.
    /// </summary>
    /// <param name="maxLength">Maximum length of the status message.</param>
    /// <returns>Text of the status message.</returns>
    public override string GetShortStatus(int maxLength)
    {
        if (m_clientCommandChannel?.CurrentState == ClientState.Connected)
            return $"Subscriber connection has received {ProcessedMeasurements:N0} data points".CenterText(maxLength);

        if (m_serverCommandChannel?.CurrentState == ServerState.Running && m_activeClientID != Guid.Empty)
            return $"Subscriber server-based connection has received {ProcessedMeasurements:N0} points".CenterText(maxLength);

        return "Subscriber is not connected.".CenterText(maxLength);
    }

    /// <summary>
    /// Get message from string based response.
    /// </summary>
    /// <param name="buffer">Response buffer.</param>
    /// <param name="startIndex">Start index of response message.</param>
    /// <param name="length">Length of response message.</param>
    /// <returns>Decoded response string.</returns>
    protected string InterpretResponseMessage(byte[] buffer, int startIndex, int length)
    {
        return Encoding.GetString(buffer, startIndex, length);
    }

    // Restarts the subscriber.
    private void Restart()
    {
        try
        {
            base.Start();
        }
        catch (Exception ex)
        {
            OnProcessException(MessageLevel.Warning, ex);
        }
    }

    private void ProcessServerResponse(byte[]? buffer, int length)
    {
        // Currently this work is done on the async socket completion thread, make sure work to be done is timely and if the response processing
        // is coming in via the command channel and needs to send a command back to the server, it should be done on a separate thread...
        if (buffer is null || length <= 0)
            return;

        int startIndex = 0;

        while (startIndex < length)
        {
            try
            {
                Dictionary<Guid, DeviceStatisticsHelper<SubscribedDevice>>? subscribedDevicesLookup;
                DeviceStatisticsHelper<SubscribedDevice>? statisticsHelper;

                ServerResponse responseCode = (ServerResponse)buffer[startIndex];
                ServerCommand commandCode = (ServerCommand)buffer[startIndex + 1];
                int responseLength = BigEndian.ToInt32(buffer, startIndex + 2);
                int responseIndex = startIndex + DataPublisher.ClientResponseHeaderSize;
                byte[][][] keyIVs;

                startIndex = responseIndex + responseLength;

                // Disconnect any established UDP data channel upon successful unsubscribe
                if (commandCode == ServerCommand.Unsubscribe && responseCode == ServerResponse.Succeeded)
                    DataChannel = null;

                if (!IsUserCommand(commandCode))
                    OnReceivedServerResponse(responseCode, commandCode);
                else
                    OnReceivedUserCommandResponse(commandCode, responseCode, buffer, responseIndex, responseLength);

                switch (responseCode)
                {
                    case ServerResponse.Succeeded:
                        switch (commandCode)
                        {
                            case ServerCommand.Subscribe:
                                OnStatusMessage(MessageLevel.Info, $"Success code received in response to server command \"{commandCode}\": {InterpretResponseMessage(buffer, responseIndex, responseLength)}");
                                m_subscribed = true;
                                break;
                            case ServerCommand.Unsubscribe:
                                OnStatusMessage(MessageLevel.Info, $"Success code received in response to server command \"{commandCode}\": {InterpretResponseMessage(buffer, responseIndex, responseLength)}");
                                m_subscribed = false;
                                if (m_dataStreamMonitor is not null)
                                    m_dataStreamMonitor.Enabled = false;
                                break;
                            case ServerCommand.RotateCipherKeys:
                                OnStatusMessage(MessageLevel.Info, $"Success code received in response to server command \"{commandCode}\": {InterpretResponseMessage(buffer, responseIndex, responseLength)}");
                                break;
                            case ServerCommand.MetaDataRefresh:
                                OnStatusMessage(MessageLevel.Info, $"Success code received in response to server command \"{commandCode}\": latest meta-data received.");
                                OnMetaDataReceived(DeserializeMetadata(buffer.BlockCopy(responseIndex, responseLength)));
                                m_metadataRefreshPending = false;
                                break;
                        }

                        break;
                    case ServerResponse.Failed:
                        OnStatusMessage(MessageLevel.Info, $"Failure code received in response to server command \"{commandCode}\": {InterpretResponseMessage(buffer, responseIndex, responseLength)}");

                        if (commandCode == ServerCommand.MetaDataRefresh)
                            m_metadataRefreshPending = false;
                        break;
                    case ServerResponse.DataPacket:
                    {
                        long now = DateTime.UtcNow.Ticks;

                        // Deserialize data packet
                        List<IMeasurement> measurements = [];
                        Ticks timestamp = default;

                        if (TotalBytesReceived == 0)
                        {
                            // At the point when data is being received, data monitor should be enabled
                            if (!(m_dataStreamMonitor?.Enabled ?? false))
                                m_dataStreamMonitor!.Enabled = true;

                            // Establish run-time log for subscriber
                            if (AutoConnect || m_dataGapRecoveryEnabled)
                            {
                                if (m_runTimeLog is null)
                                {
                                    m_runTimeLog = new RunTimeLog { FileName = GetLoggingPath($"{Name}_RunTimeLog.txt") };
                                    m_runTimeLog.ProcessException += RunTimeLog_ProcessException;
                                    m_runTimeLog.Initialize();
                                }
                                else
                                {
                                    // Mark the start of any data transmissions
                                    m_runTimeLog.StartTime = DateTimeOffset.UtcNow;
                                    m_runTimeLog.Enabled = true;
                                }
                            }

                            // The duration between last disconnection and start of data transmissions
                            // represents a gap in data - if data gap recovery is enabled, we log
                            // this as a gap for recovery:
                            if (m_dataGapRecoveryEnabled)
                                m_dataGapRecoverer?.LogDataGap(m_runTimeLog!.StopTime, DateTimeOffset.UtcNow);
                        }

                        // Track total data packet bytes received from any channel
                        TotalBytesReceived += m_lastBytesReceived;
                        m_monitoredBytesReceived += m_lastBytesReceived;

                        // Get data packet flags
                        DataPacketFlags flags = (DataPacketFlags)buffer[responseIndex];
                        responseIndex++;

                        SignalIndexCache? signalIndexCache;
                        bool compactMeasurementFormat = (byte)(flags & DataPacketFlags.Compact) > 0;
                        bool compressedPayload = (byte)(flags & DataPacketFlags.Compressed) > 0;
                        int cipherIndex = (flags & DataPacketFlags.CipherIndex) > 0 ? 1 : 0;
                        int cacheIndex = (flags & DataPacketFlags.CacheIndex) > 0 ? 1 : 0;
                        byte[] packet = buffer;
                        int packetLength = responseLength - 1;

                        lock (m_signalIndexCacheLock)
                            signalIndexCache = m_signalIndexCache?[cacheIndex];

                        // Decrypt data packet payload if keys are available
                        if (m_keyIVs is not null)
                        {
                            // Get a local copy of volatile keyIVs reference since this can change at any time
                            keyIVs = m_keyIVs;

                            // Decrypt payload portion of data packet
                            packet = Common.SymmetricAlgorithm.Decrypt(packet, responseIndex, packetLength, keyIVs[cipherIndex][0], keyIVs[cipherIndex][1]);
                            responseIndex = 0;
                            packetLength = packet.Length;
                        }

                        // Deserialize number of measurements that follow
                        int count = BigEndian.ToInt32(packet, responseIndex);
                        responseIndex += 4;
                        packetLength -= 4;

                        if (compressedPayload)
                        {
                            if (CompressionModes.HasFlag(CompressionModes.TSSC))
                            {
                                if (signalIndexCache is not null)
                                {
                                    try
                                    {
                                        // Decompress TSSC serialized measurements from payload
                                        ParseTSSCMeasurements(packet, packetLength, signalIndexCache, ref responseIndex, measurements);
                                    }
                                    catch (Exception ex)
                                    {
                                        OnProcessException(MessageLevel.Error, new InvalidOperationException($"Decompression failure: (Decoded {measurements.Count} of {count} measurements) - {ex.Message}", ex));
                                    }
                                }
                            }
                            else
                            {
                                OnProcessException(MessageLevel.Error, new InvalidOperationException("Decompression failure: Unexpected compression type in use - STTP currently only supports TSSC payload compression"));
                            }
                        }
                        else
                        {
                            // Deserialize measurements
                            for (int i = 0; i < count; i++)
                            {
                                if (!compactMeasurementFormat)
                                {
                                #if NET
                                    throw new NotSupportedException("Full measurement format not supported in Gemstone STTP implementation");
                                #else
                                    // Deserialize full measurement format
                                    SerializableMeasurement measurement = new(Encoding);
                                    responseIndex += measurement.ParseBinaryImage(packet, responseIndex, length - responseIndex);
                                    measurements.Add(measurement);
                                #endif
                                }
                                // ReSharper disable once RedundantIfElseBlock
                                else if (signalIndexCache is not null)
                                {
                                #pragma warning disable 618
                                    bool useMillisecondResolution = UseMillisecondResolution;
                                #pragma warning restore 618

                                    // Deserialize compact measurement format
                                    CompactMeasurement measurement = new(signalIndexCache, m_includeTime, m_baseTimeOffsets, m_timeIndex, useMillisecondResolution);
                                    responseIndex += measurement.ParseBinaryImage(packet, responseIndex, length - responseIndex);

                                    // Apply timestamp from frame if not included in transmission
                                    if (!measurement.IncludeTime)
                                        measurement.Timestamp = timestamp;

                                    measurements.Add(measurement);
                                }
                                else if (m_lastMissingCacheWarning + MissingCacheWarningInterval < now)
                                {
                                    // Warning message for missing signal index cache
                                    if (m_lastMissingCacheWarning != 0L)
                                        OnStatusMessage(MessageLevel.Error, "Signal index cache has not arrived. No compact measurements can be parsed.");

                                    m_lastMissingCacheWarning = now;
                                }
                            }
                        }

                        // Calculate statistics
                        subscribedDevicesLookup = m_subscribedDevicesLookup;
                        statisticsHelper = null;

                        if (subscribedDevicesLookup is not null)
                        {
                            IEnumerable<IGrouping<DeviceStatisticsHelper<SubscribedDevice>?, IMeasurement>> deviceGroups = measurements
                                .Where(measurement => subscribedDevicesLookup.TryGetValue(measurement.ID, out statisticsHelper))
                                .Select(measurement => Tuple.Create(statisticsHelper, measurement))
                                .ToList()
                                .GroupBy(tuple => tuple.Item1, tuple => tuple.Item2);

                            foreach (IGrouping<DeviceStatisticsHelper<SubscribedDevice>?, IMeasurement> deviceGroup in deviceGroups)
                            {
                                statisticsHelper = deviceGroup.Key;

                                foreach (IGrouping<Ticks, IMeasurement> frame in deviceGroup.GroupBy(measurement => measurement.Timestamp))
                                {
                                    // Determine the number of measurements received with valid values
                                    const MeasurementStateFlags ErrorFlags = MeasurementStateFlags.BadData | MeasurementStateFlags.BadTime | MeasurementStateFlags.SystemError;

                                    static bool hasError(MeasurementStateFlags stateFlags)
                                    {
                                        return (stateFlags & ErrorFlags) != MeasurementStateFlags.Normal;
                                    }

                                    int measurementsReceived = frame.Count(measurement => !double.IsNaN(measurement.Value));
                                    int measurementsWithError = frame.Count(measurement => !double.IsNaN(measurement.Value) && hasError(measurement.StateFlags));

                                    IMeasurement? statusFlags = null;
                                    IMeasurement? frequency = null;
                                    IMeasurement? deltaFrequency = null;

                                    // Attempt to update real-time
                                    if (!UseLocalClockAsRealTime && frame.Key > m_realTime)
                                        m_realTime = frame.Key;

                                    // Search the frame for status flags, frequency, and delta frequency
                                    foreach (IMeasurement measurement in frame)
                                    {
                                        if (measurement.ID == statisticsHelper?.Device.StatusFlagsID)
                                            statusFlags = measurement;
                                        else if (measurement.ID == statisticsHelper?.Device.FrequencyID)
                                            frequency = measurement;
                                        else if (measurement.ID == statisticsHelper?.Device.DeltaFrequencyID)
                                            deltaFrequency = measurement;
                                    }

                                    // If we are receiving status flags for this device,
                                    // count the data quality, time quality, and device errors
                                    if (statusFlags is not null)
                                    {
                                        if (statisticsHelper is not null)
                                        {
                                            uint commonStatusFlags = (uint)statusFlags.Value;

                                            if ((commonStatusFlags & (uint)Bits.Bit19) > 0)
                                                statisticsHelper.Device.DataQualityErrors++;

                                            if ((commonStatusFlags & (uint)Bits.Bit18) > 0)
                                                statisticsHelper.Device.TimeQualityErrors++;

                                            if ((commonStatusFlags & (uint)Bits.Bit16) > 0)
                                                statisticsHelper.Device.DeviceErrors++;
                                        }

                                        measurementsReceived--;

                                        if (hasError(statusFlags.StateFlags))
                                            measurementsWithError--;
                                    }

                                    // Zero is not a valid value for frequency.
                                    // If frequency is zero, invalidate both frequency and delta frequency
                                    if (frequency is not null)
                                    {
                                        if (!this.TemporalConstraintIsDefined())
                                            statisticsHelper?.MarkDeviceTimestamp(frequency.Timestamp);

                                        if (frequency.Value == 0.0D)
                                        {
                                            if (deltaFrequency is null || double.IsNaN(deltaFrequency.Value))
                                                measurementsReceived--;
                                            else
                                                measurementsReceived -= 2;

                                            if (hasError(frequency.StateFlags))
                                            {
                                                if (deltaFrequency is null || double.IsNaN(deltaFrequency.Value))
                                                    measurementsWithError--;
                                                else
                                                    measurementsWithError -= 2;
                                            }
                                        }
                                    }

                                    // Track the number of measurements received
                                    statisticsHelper?.AddToMeasurementsReceived(measurementsReceived);
                                    statisticsHelper?.AddToMeasurementsWithError(measurementsWithError);
                                }
                            }
                        }

                        OnNewMeasurements(measurements);

                        // Gather statistics on received data
                        DateTime timeReceived = RealTime;

                        if (!UseLocalClockAsRealTime && timeReceived.Ticks - m_lastStatisticsHelperUpdate > Ticks.PerSecond)
                        {
                            UpdateStatisticsHelpers();
                            m_lastStatisticsHelperUpdate = m_realTime;
                        }

                        LifetimeMeasurements += measurements.Count;
                        UpdateMeasurementsPerSecond(timeReceived, measurements.Count);

                        foreach (IMeasurement measurement in measurements)
                        {
                            long latency = timeReceived.Ticks - (long)measurement.Timestamp;

                            // Throw out latencies that exceed one hour as invalid
                            if (Math.Abs(latency) > Time.SecondsPerHour * Ticks.PerSecond)
                                continue;

                            if (m_lifetimeMinimumLatency > latency || m_lifetimeMinimumLatency == 0)
                                m_lifetimeMinimumLatency = latency;

                            if (m_lifetimeMaximumLatency < latency || m_lifetimeMaximumLatency == 0)
                                m_lifetimeMaximumLatency = latency;

                            m_lifetimeTotalLatency += latency;
                            m_lifetimeLatencyMeasurements++;
                        }

                        break;
                    }
                    case ServerResponse.BufferBlock:
                    {
                        // Buffer block received - wrap as a buffer block measurement and expose back to consumer
                        uint sequenceNumber = BigEndian.ToUInt32(buffer, responseIndex);
                        int bufferCacheIndex = (int)(sequenceNumber - m_expectedBufferBlockSequenceNumber);
                        int signalCacheIndex = Version > 1 ? buffer[responseIndex + 4] : 0;

                        // Check if this buffer block has already been processed (e.g., mistaken retransmission due to timeout)
                        if (bufferCacheIndex >= 0 && (bufferCacheIndex >= m_bufferBlockCache.Count || m_bufferBlockCache[bufferCacheIndex] is null))
                        {
                            // Send confirmation that buffer block is received
                            SendServerCommand(ServerCommand.ConfirmBufferBlock, buffer.BlockCopy(responseIndex, 4));

                            if (Version > 1)
                                responseIndex += 1;

                            // Get measurement key from signal index cache
                            int signalIndex = BigEndian.ToInt32(buffer, responseIndex + 4);

                            SignalIndexCache? signalIndexCache;

                            lock (m_signalIndexCacheLock)
                                signalIndexCache = m_signalIndexCache?[signalCacheIndex];

                            if (signalIndexCache is null || !signalIndexCache.Reference.TryGetValue(signalIndex, out MeasurementKey? measurementKey))
                                throw new InvalidOperationException($"Failed to find associated signal identification for runtime ID {signalIndex}");

                            // Skip the sequence number and signal index when creating the buffer block measurement
                            BufferBlockMeasurement bufferBlockMeasurement = new(buffer, responseIndex + 8, responseLength - 8)
                            {
                                Metadata = measurementKey.Metadata
                            };

                            // Determine if this is the next buffer block in the sequence
                            if (sequenceNumber == m_expectedBufferBlockSequenceNumber)
                            {
                                List<IMeasurement> bufferBlockMeasurements = [];
                                int i;

                                // Add the buffer block measurement to the list of measurements to be published
                                bufferBlockMeasurements.Add(bufferBlockMeasurement);
                                m_expectedBufferBlockSequenceNumber++;

                                // Add cached buffer block measurements to the list of measurements to be published
                                for (i = 1; i < m_bufferBlockCache.Count; i++)
                                {
                                    if (m_bufferBlockCache[i] is null)
                                        break;

                                    bufferBlockMeasurements.Add(m_bufferBlockCache[i]!);
                                    m_expectedBufferBlockSequenceNumber++;
                                }

                                // Remove published measurements from the buffer block queue
                                if (m_bufferBlockCache.Count > 0)
                                    m_bufferBlockCache.RemoveRange(0, i);

                                // Publish measurements
                                OnNewMeasurements(bufferBlockMeasurements);
                            }
                            else
                            {
                                // Ensure that the list has at least as many
                                // elements as it needs to cache this measurement
                                for (int i = m_bufferBlockCache.Count; i <= bufferCacheIndex; i++)
                                    m_bufferBlockCache.Add(null);

                                // Insert this buffer block into the proper location in the list
                                m_bufferBlockCache[bufferCacheIndex] = bufferBlockMeasurement;
                            }
                        }

                        LifetimeMeasurements += 1;
                        UpdateMeasurementsPerSecond(DateTime.UtcNow, 1);
                        break;
                    }
                    case ServerResponse.DataStartTime:
                        // Raise data start time event
                        OnDataStartTime(BigEndian.ToInt64(buffer, responseIndex));
                        break;
                    case ServerResponse.ProcessingComplete:
                        // Raise input processing completed event
                        OnProcessingComplete(InterpretResponseMessage(buffer, responseIndex, responseLength));
                        break;
                    case ServerResponse.UpdateSignalIndexCache:
                    {
                        int version = Version;
                        int cacheIndex = 0;

                        // Get active cache index
                        if (version > 1)
                            cacheIndex = buffer[responseIndex++];

                        // Deserialize new signal index cache
                        SignalIndexCache remoteSignalIndexCache = DeserializeSignalIndexCache(buffer.BlockCopy(responseIndex, responseLength));
                        SignalIndexCache signalIndexCache = new(DataSource, remoteSignalIndexCache);

                        lock (m_signalIndexCacheLock)
                        {
                            Interlocked.CompareExchange(ref m_signalIndexCache, new SignalIndexCache[version > 1 ? 2 : 1], null);

                            m_signalIndexCache[cacheIndex] = signalIndexCache;
                            m_remoteSignalIndexCache = remoteSignalIndexCache;
                            m_cacheIndex = cacheIndex;
                        }

                        if (version > 1)
                            SendServerCommand(ServerCommand.ConfirmSignalIndexCache);

                        FixExpectedMeasurementCounts();
                        break;
                    }
                    case ServerResponse.UpdateBaseTimes:
                        // Get active time index
                        m_timeIndex = BigEndian.ToInt32(buffer, responseIndex);
                        responseIndex += 4;

                        // Deserialize new base time offsets
                        m_baseTimeOffsets = [BigEndian.ToInt64(buffer, responseIndex), BigEndian.ToInt64(buffer, responseIndex + 8)];
                        break;
                    case ServerResponse.UpdateCipherKeys:
                        // Move past active cipher index (not currently used anywhere else)
                        responseIndex++;

                        // Extract remaining response
                        byte[] bytes = buffer.BlockCopy(responseIndex, responseLength - 1);

                        // Deserialize new cipher keys
                        keyIVs = new byte[2][][];
                        keyIVs[EvenKey] = new byte[2][];
                        keyIVs[OddKey] = new byte[2][];

                        int index = 0;

                        // Read even key size
                        int bufferLen = BigEndian.ToInt32(bytes, index);
                        index = 4;

                        // Read even key
                        keyIVs[EvenKey][KeyIndex] = new byte[bufferLen];
                        Buffer.BlockCopy(bytes, index, keyIVs[EvenKey][KeyIndex], 0, bufferLen);
                        index += bufferLen;

                        // Read even initialization vector size
                        bufferLen = BigEndian.ToInt32(bytes, index);
                        index += 4;

                        // Read even initialization vector
                        keyIVs[EvenKey][IVIndex] = new byte[bufferLen];
                        Buffer.BlockCopy(bytes, index, keyIVs[EvenKey][IVIndex], 0, bufferLen);
                        index += bufferLen;

                        // Read odd key size
                        bufferLen = BigEndian.ToInt32(bytes, index);
                        index += 4;

                        // Read odd key
                        keyIVs[OddKey][KeyIndex] = new byte[bufferLen];
                        Buffer.BlockCopy(bytes, index, keyIVs[OddKey][KeyIndex], 0, bufferLen);
                        index += bufferLen;

                        // Read odd initialization vector size
                        bufferLen = BigEndian.ToInt32(bytes, index);
                        index += 4;

                        // Read odd initialization vector
                        keyIVs[OddKey][IVIndex] = new byte[bufferLen];
                        Buffer.BlockCopy(bytes, index, keyIVs[OddKey][IVIndex], 0, bufferLen);
                        //index += bufferLen;

                        // Exchange keys
                        m_keyIVs = keyIVs;

                        OnStatusMessage(MessageLevel.Info, "Successfully established new cipher keys for data packet transmissions.");
                        break;
                    case ServerResponse.Notify:
                        // Skip the 4-byte hash
                        string message = Encoding.GetString(buffer, responseIndex + 4, responseLength - 4);

                        // Display notification
                        OnStatusMessage(MessageLevel.Info, $"NOTIFICATION: {message}");
                        OnNotificationReceived(message);

                        // Send confirmation of receipt of the notification
                        SendServerCommand(ServerCommand.ConfirmNotification, buffer.BlockCopy(responseIndex, 4));
                        break;
                    case ServerResponse.ConfigurationChanged:
                        OnStatusMessage(MessageLevel.Info, "Received notification from publisher that configuration has changed.");
                        OnServerConfigurationChanged();

                        // Initiate meta-data refresh when publisher configuration has changed - we only do this
                        // for automatic connections since API style connections have to manually initiate a
                        // meta-data refresh. API style connection should attach to server configuration changed
                        // event and request meta-data refresh to complete automated cycle.
                        if (AutoConnect && AutoSynchronizeMetadata)
                            SendServerCommand(ServerCommand.MetaDataRefresh, MetadataFilters);
                        break;
                }
            }
            catch (Exception ex)
            {
                OnProcessException(MessageLevel.Error, new InvalidOperationException($"Failed to process publisher response packet due to exception: {ex.Message}", ex));
            }
        }
    }

    private void ParseTSSCMeasurements(byte[] buffer, int packetLength, SignalIndexCache signalIndexCache, ref int responseIndex, List<IMeasurement> measurements)
    {
        TsscDecoder? decoder = signalIndexCache.TsscDecoder;
        bool newDecoder = false;

        // Use TSSC compression to decompress measurements                                            
        if (decoder is null)
        {
            decoder = signalIndexCache.TsscDecoder = new TsscDecoder();
            decoder.SequenceNumber = 0;
            newDecoder = true;
        }

        if (buffer[responseIndex] != 85)
            throw new Exception($"TSSC version not recognized: {buffer[responseIndex]}");

        responseIndex++;

        int sequenceNumber = BigEndian.ToUInt16(buffer, responseIndex);
        responseIndex += 2;

        if (sequenceNumber == 0)
        {
            if (!newDecoder)
            {
                if (decoder.SequenceNumber > 0)
                    OnStatusMessage(MessageLevel.Info, $"TSSC algorithm reset before sequence number: {decoder.SequenceNumber}", "TSSC");

                decoder = signalIndexCache.TsscDecoder = new TsscDecoder();
                decoder.SequenceNumber = 0;
            }
        }

        if (decoder.SequenceNumber != sequenceNumber)
        {
            OnProcessException(MessageLevel.Warning, new InvalidDataException($"TSSC is out of sequence. Expecting: {decoder.SequenceNumber}, Received: {sequenceNumber} -- resetting connection."));
            Start();
            return;
        }

        decoder.SetBuffer(buffer, responseIndex, packetLength - 3);

        while (decoder.TryGetMeasurement(out int id, out long time, out uint quality, out float value))
        {
            if (!signalIndexCache.Reference.TryGetValue(id, out MeasurementKey? key) /* || key is null */)
                continue;

            Measurement measurement = new()
            {
                Metadata = key.Metadata,
                Timestamp = time,
                StateFlags = (MeasurementStateFlags)quality,
                Value = value
            };

            measurements.Add(measurement);
        }

        decoder.SequenceNumber++;

        // Do not increment to 0 on roll-over
        if (decoder.SequenceNumber == 0)
            decoder.SequenceNumber = 1;
    }

    private static bool IsUserCommand(ServerCommand command)
    {
        return s_userCommands.Contains(command);
    }

    // Handles auto-connection subscription initialization
    private void StartSubscription()
    {
        SubscribeToOutputMeasurements(!AutoSynchronizeMetadata);

        // Initiate meta-data refresh
        if (AutoSynchronizeMetadata && !this.TemporalConstraintIsDefined())
            SendServerCommand(ServerCommand.MetaDataRefresh, MetadataFilters);
    }

    private bool SubscribeToOutputMeasurements(bool metaDataRefreshCompleted)
    {
        StringBuilder filterExpression = new();
        string? dataChannel = null;
        string? startTimeConstraint = null;
        string? stopTimeConstraint = null;
        int processingInterval = -1;

        // If TCP command channel is defined separately, then base connection string defines data channel
        if (Settings.ContainsKey("commandChannel"))
            dataChannel = ConnectionString;

        if (this.TemporalConstraintIsDefined())
        {
            startTimeConstraint = StartTimeConstraint.ToString("yyyy-MM-dd HH:mm:ss.fffffff");
            stopTimeConstraint = StopTimeConstraint.ToString("yyyy-MM-dd HH:mm:ss.fffffff");
            processingInterval = ProcessingInterval;
        }

        MeasurementKey[]? outputMeasurementKeys = AutoStart
            ? this.OutputMeasurementKeys()
            : RequestedOutputMeasurementKeys;

        if (outputMeasurementKeys is not null && outputMeasurementKeys.Length > 0)
        {
            // TODO: Handle "continued" subscribe operations so connection string size can be fixed
            foreach (MeasurementKey measurementKey in outputMeasurementKeys)
            {
                if (filterExpression.Length > 0)
                    filterExpression.Append(';');

                // Subscribe by associated Guid...
                filterExpression.Append(measurementKey.SignalID);
            }

            // Start unsynchronized subscription
        #pragma warning disable 618
            return Subscribe(true, Throttled, filterExpression.ToString(), dataChannel, startTime: startTimeConstraint, stopTime: stopTimeConstraint, processingInterval: processingInterval, publishInterval: PublishInterval);
        #pragma warning restore 618
        }

        Unsubscribe();

        if (AutoStart && metaDataRefreshCompleted)
            OnStatusMessage(MessageLevel.Error, "No measurements are currently defined for subscription.");

        return false;
    }

    /// <summary>
    /// Handles meta-data synchronization to local system.
    /// </summary>
    /// <remarks>
    /// This function should only be initiated from call to <see cref="SynchronizeMetadata(DataSet)"/> to make
    /// sure only one meta-data synchronization happens at once. Users can override this method to customize
    /// process of meta-data synchronization.
    /// </remarks>
    protected virtual void SynchronizeMetadata()
    {
        bool dataMonitoringEnabled = false;

        // TODO: This function is complex and very closely tied to the current time-series data schema - perhaps it should be moved outside this class and referenced
        // TODO: as a delegate that can be assigned and called to allow other schemas as well. DataPublisher is already very flexible in what data it can deliver.
        try
        {
            DataSet? metadata = m_receivedMetadata;

            // Only perform database synchronization if meta-data has changed since last update
            if (!SynchronizedMetadataChanged(metadata))
                return;

            if (metadata is null)
            {
                OnStatusMessage(MessageLevel.Error, "Meta-data synchronization was not performed, deserialized dataset was empty.");
                return;
            }

            // Reset data stream monitor while meta-data synchronization is in progress
            if (m_dataStreamMonitor?.Enabled ?? false)
            {
                m_dataStreamMonitor.Enabled = false;
                dataMonitoringEnabled = true;
            }

            // Track total meta-data synchronization process time
            Ticks startTime = DateTime.UtcNow.Ticks;
            DateTime latestUpdateTime = DateTime.MinValue;

            // Open the configuration database using settings found in the config file
        #if NET
            using (AdoDataConnection database = new(ConfigSettings.Default))
            using (DbCommand command = database.Connection.CreateCommand())
            {
                DbTransaction? transaction = null;
        #else
            using (AdoDataConnection database = new("systemSettings"))
            using (IDbCommand command = database.Connection.CreateCommand())
            {
                IDbTransaction? transaction = null;
        #endif
                if (UseTransactionForMetadata)
                    transaction = database.Connection.BeginTransaction(database.DefaultIsolationLevel);

                try
                {
                    if (transaction is not null)
                        command.Transaction = transaction;

                    // Query the actual record ID based on the known run-time ID for this subscriber device
                    object? sourceID = ExecuteScalar(command, $"SELECT SourceID FROM Runtime WHERE ID = {ID} AND SourceTable='Device'");

                    if (sourceID is null || sourceID == DBNull.Value)
                        return;

                    int parentID = Convert.ToInt32(sourceID);

                    // Validate that the subscriber device is marked as a concentrator (we are about to associate children devices with it)
                    if (!(ExecuteScalar(command, $"SELECT IsConcentrator FROM Device WHERE ID = {parentID}")?.ToString() ?? "false").ParseBoolean())
                        ExecuteNonQuery(command, $"UPDATE Device SET IsConcentrator = 1 WHERE ID = {parentID}");

                    // Get any historian associated with the subscriber device
                    object? historianID = ExecuteScalar(command, $"SELECT HistorianID FROM Device WHERE ID = {parentID}");

                #if !NET
                    // Determine the active node ID - we cache this since this value won't change for the lifetime of this class
                    if (m_nodeID == Guid.Empty)
                        m_nodeID = Guid.Parse(ExecuteScalar(command, $"SELECT NodeID FROM IaonInputAdapter WHERE ID = {(int)ID}")?.ToString() ?? Guid.Empty.ToString());

                    // Determine the protocol record auto-inc ID value for STTP - this value is also cached since it shouldn't change for the lifetime of this class
                    if (m_sttpProtocolID == 0)
                        m_sttpProtocolID = int.Parse(ExecuteScalar(command, "SELECT ID FROM Protocol WHERE Acronym='STTP'")?.ToString() ?? "0");
                #endif

                    // Ascertain total number of actions required for all meta-data synchronization so some level feed back can be provided on progress
                    InitSyncProgress(metadata.Tables.Cast<DataTable>().Select(dataTable => (long)dataTable.Rows.Count).Sum() + 3);

                    // Prefix all children devices with the name of the parent since the same device names could appear in different connections (helps keep device names unique)
                    string sourcePrefix = UseSourcePrefixNames ? $"{Name}!" : "";
                    Dictionary<string, int> deviceIDs = new(StringComparer.OrdinalIgnoreCase);
                    DateTime updateTime;
                    string deviceAcronym;
                    int deviceID;

                    // Check to see if data for the "DeviceDetail" table was included in the meta-data
                    if (metadata.Tables.Contains("DeviceDetail"))
                    {
                        DataTable deviceDetail = metadata.Tables["DeviceDetail"]!;
                        DataRow[] deviceRows;

                        // Define SQL statement to query if this device is already defined (this should always be based on the unique guid-based device ID)
                        string deviceExistsSql = database.ParameterizedQueryString("SELECT COUNT(*) FROM Device WHERE UniqueID = {0}", "uniqueID");

                    #if NET
                        //                                                                             0         1            2        3     4               5         6          7         8            9                 --              10        --
                        // Define SQL statement to insert new device record
                        string insertDeviceSql = database.ParameterizedQueryString("INSERT INTO Device(ParentID, HistorianID, Acronym, Name, OriginalSource, AccessID, Longitude, Latitude, ContactList, ConnectionString, IsConcentrator, Internal, Enabled) " +
                                                                                   "VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, 0, {10}, " + (SyncIndependentDevices ? AutoEnableIndependentlySyncedDevices ? "1" : "0" : "1") + ")",
                                                                                   "parentID", "historianID", "acronym", "name", "originalSource", "accessID", "longitude", "latitude", "contactList", "connectionString", "internal");

                        // Define SQL statement to update existing device record
                        string updateDeviceSql = database.ParameterizedQueryString("UPDATE Device SET Acronym = {0}, Name = {1}, OriginalSource = {2}, HistorianID = {3}, AccessID = {4}, Longitude = {5}, Latitude = {6}, ContactList = {7}, Internal = {8} WHERE UniqueID = {9}",
                            "acronym", "name", "originalSource", "historianID", "accessID", "longitude", "latitude", "contactList", "internal", "uniqueID");

                        string updateDeviceWithConnectionStringSql = database.ParameterizedQueryString("UPDATE Device SET Acronym = {0}, Name = {1}, OriginalSource = {2}, HistorianID = {3}, AccessID = {4}, Longitude = {5}, Latitude = {6}, ContactList = {7}, ConnectionString = {8}, Internal = {9} WHERE UniqueID = {10}",
                            "acronym", "name", "originalSource", "historianID", "accessID", "longitude", "latitude", "contactList", "connectionString", "internal", "uniqueID");
                    #else
                        // Define SQL statement to insert new device record
                        string insertDeviceSql = database.ParameterizedQueryString("INSERT INTO Device(NodeID, ParentID, HistorianID, Acronym, Name, ProtocolID, FramesPerSecond, OriginalSource, AccessID, Longitude, Latitude, ContactList, ConnectionString, IsConcentrator, Enabled) " +
                                                                                   "VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, 0, " + (SyncIndependentDevices ? AutoEnableIndependentlySyncedDevices ? "1" : "0" : "1") + ")",
                                                                                   "nodeID", "parentID", "historianID", "acronym", "name", "protocolID", "framesPerSecond", "originalSource", "accessID", "longitude", "latitude", "contactList", "connectionString");

                        // Define SQL statement to update existing device record
                        string updateDeviceSql = database.ParameterizedQueryString("UPDATE Device SET Acronym = {0}, Name = {1}, OriginalSource = {2}, ProtocolID = {3}, FramesPerSecond = {4}, HistorianID = {5}, AccessID = {6}, Longitude = {7}, Latitude = {8}, ContactList = {9} WHERE UniqueID = {10}",
                            "acronym", "name", "originalSource", "protocolID", "framesPerSecond", "historianID", "accessID", "longitude", "latitude", "contactList", "uniqueID");

                        string updateDeviceWithConnectionStringSql = database.ParameterizedQueryString("UPDATE Device SET Acronym = {0}, Name = {1}, OriginalSource = {2}, ProtocolID = {3}, FramesPerSecond = {4}, HistorianID = {5}, AccessID = {6}, Longitude = {7}, Latitude = {8}, ContactList = {9}, ConnectionString = {10} WHERE UniqueID = {11}",
                            "acronym", "name", "originalSource", "protocolID", "framesPerSecond", "historianID", "accessID", "longitude", "latitude", "contactList", "connectionString", "uniqueID");
                    #endif

                        // Define SQL statement to update device's guid-based unique ID after insert
                        string updateDeviceUniqueIDSql = database.ParameterizedQueryString("UPDATE Device SET UniqueID = {0} WHERE Acronym = {1}", "uniqueID", "acronym");

                        // Define SQL statement to query if a device can be safely updated
                        string deviceParentRestriction = SyncIndependentDevices ? "OriginalSource <> {1}" : "(ParentID <> {1} OR ParentID IS NULL)";
                        string deviceIsUpdateableSql = database.ParameterizedQueryString("SELECT COUNT(*) FROM Device WHERE UniqueID = {0} AND " + deviceParentRestriction, "uniqueID", "parentID");

                        // Define SQL statement to retrieve device's auto-inc ID based on its unique guid-based ID
                        string queryDeviceIDSql = database.ParameterizedQueryString("SELECT ID FROM Device WHERE UniqueID = {0}", "uniqueID");

                        // Define SQL statement to retrieve all unique device ID's for the current parent to check for mismatches
                        string queryUniqueDeviceIDsSql = database.ParameterizedQueryString($"SELECT UniqueID FROM Device WHERE {(SyncIndependentDevices ? "OriginalSource" : "ParentID")} = {{0}}", "parentID");

                        // Define SQL statement to remove device records that no longer exist in the meta-data
                        string deleteDeviceSql = database.ParameterizedQueryString("DELETE FROM Device WHERE UniqueID = {0}", "uniqueID");

                        // Determine which device rows should be synchronized based on operational mode flags
                        if (ReceiveInternalMetadata && ReceiveExternalMetadata || MutualSubscription)
                            deviceRows = deviceDetail.Select();
                        else if (ReceiveInternalMetadata)
                            deviceRows = deviceDetail.Select("OriginalSource IS NULL");
                        else if (ReceiveExternalMetadata)
                            deviceRows = deviceDetail.Select("OriginalSource IS NOT NULL");
                        else
                            deviceRows = [];

                        // Check existence of optional meta-data fields
                        DataColumnCollection deviceDetailColumns = deviceDetail.Columns;
                        bool accessIDFieldExists = deviceDetailColumns.Contains("AccessID");
                        bool longitudeFieldExists = deviceDetailColumns.Contains("Longitude");
                        bool latitudeFieldExists = deviceDetailColumns.Contains("Latitude");
                        bool companyAcronymFieldExists = deviceDetailColumns.Contains("CompanyAcronym");
                        bool protocolNameFieldExists = deviceDetailColumns.Contains("ProtocolName");
                        bool vendorAcronymFieldExists = deviceDetailColumns.Contains("VendorAcronym");
                        bool vendorDeviceNameFieldExists = deviceDetailColumns.Contains("VendorDeviceName");
                        bool interconnectionNameFieldExists = deviceDetailColumns.Contains("InterconnectionName");
                        bool updatedOnFieldExists = deviceDetailColumns.Contains("UpdatedOn");
                        bool connectionStringFieldExists = deviceDetailColumns.Contains("ConnectionString");
                        bool framesPerSecondFieldExists = deviceDetailColumns.Contains("FramesPerSecond");
                        object parentIDValue = SyncIndependentDevices ? parentID.ToString() : parentID;
                        int accessID = 0;

                        List<Guid> uniqueIDs = deviceRows
                            .Select(deviceRow => deviceRow.ConvertGuidField("UniqueID"))
                            .ToList();

                        // Remove any device records associated with this subscriber that no longer exist in the meta-data
                        if (uniqueIDs.Count > 0)
                        {
                            // ReSharper disable once AccessToDisposedClosure
                            IEnumerable<Guid> retiredUniqueIDs =
                                RetrieveData(database, command, queryUniqueDeviceIDsSql, parentIDValue)
                                    .Select()
                                    .Select(deviceRow => deviceRow.ConvertGuidField("UniqueID"))
                                    .Except(uniqueIDs);

                            foreach (Guid retiredUniqueID in retiredUniqueIDs)
                                ExecuteNonQuery(command, deleteDeviceSql, database.Guid(retiredUniqueID));

                            UpdateSyncProgress();
                        }

                        foreach (DataRow row in deviceRows)
                        {
                            Guid uniqueID = row.ConvertGuidField("UniqueID");
                            bool recordNeedsUpdating;

                            // Determine if record has changed since last synchronization
                            if (updatedOnFieldExists)
                            {
                                try
                                {
                                    updateTime = Convert.ToDateTime(row["UpdatedOn"]);
                                    recordNeedsUpdating = updateTime > m_lastMetaDataRefreshTime;

                                    if (updateTime > latestUpdateTime)
                                        latestUpdateTime = updateTime;
                                }
                                catch
                                {
                                    recordNeedsUpdating = true;
                                }
                            }
                            else
                            {
                                recordNeedsUpdating = true;
                            }

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
                                int protocolID = m_sttpProtocolID;
                             #endif

                                // If we are synchronizing independent devices, we need to determine the protocol ID for the device
                                // based on the protocol name defined in the meta-data
                                if (SyncIndependentDevices && !string.IsNullOrWhiteSpace(protocolName))
                                {
                                #if NET
                                    Dictionary<string, string> settings = connectionString.ParseKeyValuePairs();
                                    settings["phasorProtocol"] = protocolName;
                                    connectionString = settings.JoinKeyValuePairs();
                                #else
                                    string queryProtocolIDSql = database.ParameterizedQueryString("SELECT ID FROM Protocol WHERE Name = {0}", "protocolName");
                                    object? protocolIDValue = ExecuteScalar(command, queryProtocolIDSql, protocolName);

                                    if (protocolIDValue is not null && protocolIDValue is not DBNull)
                                        protocolID = Convert.ToInt32(protocolIDValue);

                                    if (protocolID == 0)
                                        protocolID = m_sttpProtocolID;
                                #endif
                                }

                                // For mutual subscriptions where this subscription is owner (i.e., internal is true), we only sync devices that we did not provide
                                if (!MutualSubscription || !Internal || string.IsNullOrEmpty(row.Field<string>("OriginalSource")))
                                {
                                    // Gateway is assuming ownership of the device records when the "internal" flag is true - this means the device's measurements can be forwarded to another party. From a device record perspective,
                                    // ownership is inferred by setting 'OriginalSource' to null. When gateway doesn't own device records (i.e., the "internal" flag is false), this means the device's measurements can only be consumed
                                    // locally - from a device record perspective this means the 'OriginalSource' field is set to the acronym of the PDC or PMU that generated the source measurements. This field allows a mirrored source
                                    // restriction to be implemented later to ensure all devices in an output protocol came from the same original source connection, if desired.
                                    object originalSource = SyncIndependentDevices ? parentID.ToString() :
                                        Internal ? DBNull.Value :
                                        string.IsNullOrEmpty(row.Field<string>("ParentAcronym")) ? sourcePrefix + row.Field<string>("Acronym") :
                                        sourcePrefix + row.Field<string>("ParentAcronym");

                                    // Determine if device record already exists
                                    if (Convert.ToInt32(ExecuteScalar(command, deviceExistsSql, database.Guid(uniqueID))) == 0)
                                    {
                                    #if NET
                                        // Insert new device record
                                        ExecuteNonQuery(command, insertDeviceSql, SyncIndependentDevices ? DBNull.Value : parentID,
                                            historianID, sourcePrefix + row.Field<string>("Acronym"), row.Field<string>("Name"), originalSource,
                                            accessID, longitude, latitude, contactList.JoinKeyValuePairs(), connectionString, database.Bool(Internal));
                                    #else
                                        // Insert new device record
                                        ExecuteNonQuery(command, insertDeviceSql, database.Guid(m_nodeID), SyncIndependentDevices ? DBNull.Value : parentID,
                                            historianID, sourcePrefix + row.Field<string>("Acronym"), row.Field<string>("Name"), protocolID,
                                            framesPerSecondFieldExists ? row.ConvertField<int>("FramesPerSecond") : 30, originalSource, accessID,
                                            longitude, latitude, contactList.JoinKeyValuePairs(), connectionString);
                                    #endif

                                        // Guids are normally auto-generated during insert - after insertion update the Guid so that it matches the source data. Most of the database
                                        // scripts have triggers that support properly assigning the Guid during an insert, but this code ensures the Guid will always get assigned.
                                        ExecuteNonQuery(command, updateDeviceUniqueIDSql, database.Guid(uniqueID), sourcePrefix + row.Field<string>("Acronym"));
                                    }
                                    else if (recordNeedsUpdating)
                                    {
                                        // Perform safety check to preserve device records which are not safe to overwrite (e.g., device already exists locally as part of another connection)
                                        if (Convert.ToInt32(ExecuteScalar(command, deviceIsUpdateableSql, database.Guid(uniqueID), parentIDValue)) > 0)
                                            continue;

                                    #if NET
                                        // Update existing device record
                                        if (connectionStringFieldExists)
                                            ExecuteNonQuery(command, updateDeviceWithConnectionStringSql, sourcePrefix + row.Field<string>("Acronym"), row.Field<string>("Name"),
                                                originalSource, historianID, accessID, longitude, latitude, contactList.JoinKeyValuePairs(), connectionString, database.Bool(Internal), database.Guid(uniqueID));
                                        else
                                            ExecuteNonQuery(command, updateDeviceSql, sourcePrefix + row.Field<string>("Acronym"), row.Field<string>("Name"),
                                                originalSource, historianID, accessID, longitude, latitude, contactList.JoinKeyValuePairs(), database.Bool(Internal), database.Guid(uniqueID));
                                    #else
                                        // Update existing device record
                                        if (connectionStringFieldExists)
                                            ExecuteNonQuery(command, updateDeviceWithConnectionStringSql, sourcePrefix + row.Field<string>("Acronym"), row.Field<string>("Name"),
                                                originalSource, protocolID, framesPerSecondFieldExists ? row.ConvertField<int>("FramesPerSecond") : 30, historianID, accessID, longitude, latitude, contactList.JoinKeyValuePairs(), connectionString, database.Guid(uniqueID));
                                        else
                                            ExecuteNonQuery(command, updateDeviceSql, sourcePrefix + row.Field<string>("Acronym"), row.Field<string>("Name"),
                                                originalSource, protocolID, framesPerSecondFieldExists ? row.ConvertField<int>("FramesPerSecond") : 30, historianID, accessID, longitude, latitude, contactList.JoinKeyValuePairs(), database.Guid(uniqueID));
                                    #endif
                                    }
                                }
                            }

                            // Capture local device ID auto-inc value for measurement association
                            deviceIDs[row.Field<string>("Acronym")!] = Convert.ToInt32(ExecuteScalar(command, queryDeviceIDSql, database.Guid(uniqueID)));

                            // Periodically notify user about synchronization progress
                            UpdateSyncProgress();
                        }
                    }

                    // Check to see if data for the "MeasurementDetail" table was included in the meta-data
                    if (metadata.Tables.Contains("MeasurementDetail"))
                    {
                        DataTable measurementDetail = metadata.Tables["MeasurementDetail"]!;
                        List<Guid> signalIDs = [];
                        DataRow[] measurementRows;

                        // Define SQL statement to query if this measurement is already defined (this should always be based on the unique signal ID Guid)
                        string measurementExistsSql = database.ParameterizedQueryString("SELECT COUNT(*) FROM Measurement WHERE SignalID = {0}", "signalID");

                        // Define SQL statement to query if this measurement is already defined (this will be used before identity insert)
                        string identityMeasurementExistsSql = database.ParameterizedQueryString("SELECT COUNT(*) FROM Measurement WHERE PointID = {0}", "pointID");

                        // Define SQL statement to insert new measurement record
                        string insertMeasurementSql = database.ParameterizedQueryString("INSERT INTO Measurement(DeviceID, HistorianID, PointTag, AlternateTag, SignalTypeID, PhasorSourceIndex, SignalReference, Description, Internal, Subscribed, Enabled) " +
                                                                                        "VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, 0, 1)", "deviceID", "historianID", "pointTag", "tempAlternateTagID", "signalTypeID", "phasorSourceIndex", "signalReference", "description", "internal");

                        // Define SQL statement to insert new measurement record
                        string identityInsertMeasurementSql = database.ParameterizedQueryString("INSERT INTO Measurement(PointID, DeviceID, HistorianID, PointTag, AlternateTag, SignalTypeID, PhasorSourceIndex, SignalReference, Description, Internal, Subscribed, Enabled) " +
                                                                                                "VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, 0, 1)", "pointID", "deviceID", "historianID", "pointTag", "tempAlternateTagID", "signalTypeID", "phasorSourceIndex", "signalReference", "description", "internal");

                        // Define SQL statement to update measurement's signal ID after insert, restoring original signal ID and alternate tag from meta-data
                        string updateMeasurementSignalIDSql = database.ParameterizedQueryString("UPDATE Measurement SET SignalID = {0}, AlternateTag = {1} WHERE AlternateTag = {2}", "signalID", "alternateTag", "tempAlternateTagID");

                        // Define SQL statement to update existing measurement record
                        string updateMeasurementSql = database.ParameterizedQueryString("UPDATE Measurement SET HistorianID = {0}, PointTag = {1}, AlternateTag = {2}, SignalTypeID = {3}, PhasorSourceIndex = {4}, SignalReference = {5}, Description = {6}, Internal = {7} WHERE SignalID = {8}",
                            "historianID", "pointTag", "alternateTag", "signalTypeID", "phasorSourceIndex", "signalReference", "description", "internal", "signalID");

                        // Define SQL statement to update existing measurement record
                        string identityUpdateMeasurementSql = database.ParameterizedQueryString("UPDATE Measurement SET DeviceID = {0}, HistorianID = {1}, PointTag = {2}, AlternateTag = {3}, SignalTypeID = {4}, PhasorSourceIndex = {5}, SignalReference = {6}, Description = {7}, Internal = {8}, Subscribed = 0, Enabled = 1, SignalID = {9} WHERE PointID = {10}",
                            "deviceID", "historianID", "pointTag", "tempAlternateTagID", "signalTypeID", "phasorSourceIndex", "signalReference", "description", "internal", "signalID", "pointID");

                        // Define SQL statement to retrieve all measurement signal ID's for the current parent to check for mismatches - note that we use the ActiveMeasurements view
                        // since it associates measurements with their top-most parent runtime device ID, this allows us to easily query all measurements for the parent device
                        string queryMeasurementSignalIDsSql = database.ParameterizedQueryString("SELECT SignalID FROM ActiveMeasurement WHERE DeviceID = {0}", "deviceID");

                        // Define SQL statement to retrieve measurement's associated device ID, i.e., actual record ID, based on measurement's signal ID
                        string queryMeasurementDeviceIDSql = database.ParameterizedQueryString("SELECT DeviceID FROM Measurement WHERE SignalID = {0}", "signalID");

                        // Load signal type ID's from local database associated with their acronym for proper signal type translation
                        Dictionary<string, int> signalTypeIDs = new(StringComparer.OrdinalIgnoreCase);

                        string? signalTypeAcronym;

                        foreach (DataRow row in RetrieveData(database, command, "SELECT ID, Acronym FROM SignalType").Rows)
                        {
                            signalTypeAcronym = row.Field<string>("Acronym");

                            if (!string.IsNullOrWhiteSpace(signalTypeAcronym))
                                signalTypeIDs[signalTypeAcronym] = row.ConvertField<int>("ID");
                        }

                        // Define local signal type ID deletion exclusion set
                        string deleteCondition = "";

                        if (MutualSubscription && !Internal)
                        {
                            // For mutual subscriptions where this subscription is renter (i.e., internal is false), do not delete measurements that are locally owned
                            deleteCondition = " AND Internal == 0";
                        }
                        else
                        {
                            List<int> excludedSignalTypeIDs = [];

                            // We are intentionally ignoring CALC and ALRM signals during measurement deletion since if you have subscribed to a device and subsequently created local
                            // calculations and alarms associated with this device, these signals are locally owned and not part of the publisher subscription stream. As a result any
                            // CALC or ALRM measurements that are created at source and then removed could be orphaned in subscriber. The best fix would be to have a simple flag that
                            // clearly designates that a measurement was created locally and is not part of the remote synchronization set.
                            if (!AutoDeleteCalculatedMeasurements && signalTypeIDs.TryGetValue("CALC", out int signalTypeID))
                                excludedSignalTypeIDs.Add(signalTypeID);

                            if (!AutoDeleteAlarmMeasurements && signalTypeIDs.TryGetValue("ALRM", out signalTypeID))
                                excludedSignalTypeIDs.Add(signalTypeID);

                            if (excludedSignalTypeIDs.Count > 0)
                                deleteCondition = $" AND NOT SignalTypeID IN ({excludedSignalTypeIDs.ToDelimitedString(',')})";
                        }

                        // Define SQL statement to remove device records that no longer exist in the meta-data
                        string deleteMeasurementSql = database.ParameterizedQueryString($"DELETE FROM Measurement WHERE SignalID = {{0}}{deleteCondition}", "signalID");

                        // Determine which measurement rows should be synchronized based on operational mode flags
                        if (ReceiveInternalMetadata && ReceiveExternalMetadata)
                            measurementRows = measurementDetail.Select();
                        else if (ReceiveInternalMetadata)
                            measurementRows = measurementDetail.Select("Internal <> 0");
                        else if (ReceiveExternalMetadata)
                            measurementRows = measurementDetail.Select("Internal = 0");
                        else
                            measurementRows = [];

                        // Check existence of optional meta-data fields
                        DataColumnCollection measurementDetailColumns = measurementDetail.Columns;
                        bool phasorSourceIndexFieldExists = measurementDetailColumns.Contains("PhasorSourceIndex");
                        bool updatedOnFieldExists = measurementDetailColumns.Contains("UpdatedOn");
                        bool alternateTagFieldExists = measurementDetailColumns.Contains("AlternateTag");

                        object phasorSourceIndex = DBNull.Value;
                        object alternateTag = DBNull.Value;

                        if (UseIdentityInsertsForMetadata && database.IsSQLServer)
                            ExecuteNonQuery(command, "SET IDENTITY_INSERT Measurement ON");

                        try
                        {
                            foreach (DataRow row in measurementRows)
                            {
                                bool recordNeedsUpdating;

                                // Determine if record has changed since last synchronization
                                if (updatedOnFieldExists)
                                {
                                    try
                                    {
                                        updateTime = Convert.ToDateTime(row["UpdatedOn"]);
                                        recordNeedsUpdating = updateTime > m_lastMetaDataRefreshTime;

                                        if (updateTime > latestUpdateTime)
                                            latestUpdateTime = updateTime;
                                    }
                                    catch
                                    {
                                        recordNeedsUpdating = true;
                                    }
                                }
                                else
                                {
                                    recordNeedsUpdating = true;
                                }

                                // Get device and signal type acronyms
                                deviceAcronym = row.Field<string>("DeviceAcronym") ?? string.Empty;
                                signalTypeAcronym = row.Field<string>("SignalAcronym") ?? string.Empty;

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
                                if (!string.IsNullOrWhiteSpace(deviceAcronym) && deviceIDs.ContainsKey(deviceAcronym) && !string.IsNullOrWhiteSpace(signalTypeAcronym) && signalTypeIDs.ContainsKey(signalTypeAcronym))
                                {
                                    Guid signalID = row.ConvertGuidField("SignalID");

                                    // Track unique measurement signal Guids in this meta-data session, we'll need to remove any old associated measurements that no longer exist
                                    signalIDs.Add(signalID);

                                    // Prefix the tag name with the "updated" device name
                                    string pointTag = sourcePrefix + row.Field<string>("PointTag");

                                    // Look up associated device ID (local DB auto-inc)
                                    deviceID = deviceIDs[deviceAcronym];

                                    // Determine if measurement record already exists
                                    if (Convert.ToInt32(ExecuteScalar(command, measurementExistsSql, database.Guid(signalID))) == 0)
                                    {
                                        string temporaryAlternateTagID = Guid.NewGuid().ToString();

                                        // Insert new measurement record
                                        if (UseIdentityInsertsForMetadata && MeasurementKey.TryParse(row.Field<string>("ID")!, out MeasurementKey measurementKey))
                                        {
                                            long pointID = (long)measurementKey.ID;

                                            if (Convert.ToInt32(ExecuteScalar(command, identityMeasurementExistsSql, pointID)) == 0)
                                                ExecuteNonQuery(command, identityInsertMeasurementSql, pointID, deviceID, historianID, pointTag, temporaryAlternateTagID, signalTypeIDs[signalTypeAcronym], phasorSourceIndex, sourcePrefix + row.Field<string>("SignalReference"), row.Field<string>("Description") ?? string.Empty, database.Bool(Internal));
                                            else
                                                ExecuteNonQuery(command, identityUpdateMeasurementSql, deviceID, historianID, pointTag, temporaryAlternateTagID, signalTypeIDs[signalTypeAcronym], phasorSourceIndex, sourcePrefix + row.Field<string>("SignalReference"), row.Field<string>("Description") ?? string.Empty, database.Bool(Internal), database.Guid(signalID), pointID);
                                        }
                                        else
                                        {
                                            ExecuteNonQuery(command, insertMeasurementSql, deviceID, historianID, pointTag, temporaryAlternateTagID, signalTypeIDs[signalTypeAcronym], phasorSourceIndex, sourcePrefix + row.Field<string>("SignalReference"), row.Field<string>("Description") ?? string.Empty, database.Bool(Internal));
                                        }

                                        // Guids are normally auto-generated during insert - after insertion update the Guid so that it matches the source data. Most of the database
                                        // scripts have triggers that support properly assigning the Guid during an insert, but this code ensures the Guid will always get assigned.
                                        // TODO: Ensure database schemas define an index on the AlternateTag field to optimize this update
                                        ExecuteNonQuery(command, updateMeasurementSignalIDSql, database.Guid(signalID), alternateTag, temporaryAlternateTagID);
                                    }
                                    else if (recordNeedsUpdating)
                                    {
                                        // Update existing measurement record. Note that this update assumes that measurements will remain associated with a static source device.
                                        ExecuteNonQuery(command, updateMeasurementSql, historianID, pointTag, alternateTag, signalTypeIDs[signalTypeAcronym], phasorSourceIndex, sourcePrefix + row.Field<string>("SignalReference"), row.Field<string>("Description") ?? string.Empty, database.Bool(Internal), database.Guid(signalID));
                                    }
                                }

                                // Periodically notify user about synchronization progress
                                UpdateSyncProgress();
                            }
                        }
                        finally
                        {
                            if (UseIdentityInsertsForMetadata && database.IsSQLServer)
                                ExecuteNonQuery(command, "SET IDENTITY_INSERT Measurement OFF");
                        }

                        // Remove any measurement records associated with existing devices in this session but no longer exist in the meta-data
                        if (signalIDs.Count > 0)
                        {
                            // Sort signal ID list so that binary search can be used for quick lookups
                            signalIDs.Sort();

                            // Query all the guid-based signal ID's for all measurement records associated with the parent device using run-time ID
                            DataTable measurementSignalIDs = RetrieveData(database, command, queryMeasurementSignalIDsSql, (int)ID);

                            // Walk through each database record and see if the measurement exists in the provided meta-data
                            foreach (DataRow measurementRow in measurementSignalIDs.Rows)
                            {
                                Guid signalID = measurementRow.ConvertGuidField("SignalID");

                                // Remove any measurements in the database that are associated with received devices and do not exist in the meta-data
                                if (signalIDs.BinarySearch(signalID) >= 0)
                                    continue;

                                // Measurement was not in the meta-data, get the measurement's actual record based ID for its associated device
                                object? measurementDeviceID = ExecuteScalar(command, queryMeasurementDeviceIDSql, database.Guid(signalID));

                                // If the unknown measurement is directly associated with a device that exists in the meta-data it is assumed that this measurement
                                // was removed from the publishing system and no longer exists therefore we remove it from the local measurement cache. If the user
                                // needs custom local measurements associated with a remote device, they should be associated with the parent device only.
                                if (measurementDeviceID is not null && measurementDeviceID is not DBNull && deviceIDs.ContainsValue(Convert.ToInt32(measurementDeviceID)))
                                    ExecuteNonQuery(command, deleteMeasurementSql, database.Guid(signalID));
                            }

                            UpdateSyncProgress();
                        }
                    }

                    // Check to see if data for the "PhasorDetail" table was included in the meta-data
                    if (metadata.Tables.Contains("PhasorDetail"))
                    {
                    #if NET
                        const string PrimaryVoltageID = "PrimaryVoltageID";
                        const string DestinationPhasorID = "DestinationPhasorID";
                    #else
                        const string PrimaryVoltageID = "DestinationPhasorID";
                        const string DestinationPhasorID = "PrimaryVoltageID";
                    #endif

                        DataTable phasorDetail = metadata.Tables["PhasorDetail"]!;
                        Dictionary<int, List<int>> definedSourceIndices = new();
                        Dictionary<int, int> metadataToDatabaseIDMap = new();
                        Dictionary<int, int> sourceToDestinationIDMap = new();

                        // Phasor data is normally only needed so that the user can properly generate a mirrored IEEE C37.118 output stream from the source data.
                        // This is necessary since, in this protocol, the phasors are described (i.e., labeled) as a unit (i.e., as a complex number) instead of
                        // as two distinct angle and magnitude measurements.

                        // Define SQL statement to query if phasor record is already defined (no Guid is defined for these simple label records)
                        string phasorExistsSql = database.ParameterizedQueryString("SELECT COUNT(*) FROM Phasor WHERE DeviceID = {0} AND SourceIndex = {1}", "deviceID", "sourceIndex");

                    #if NET
                        // Define SQL statement to insert new phasor record
                        string insertPhasorSql = database.ParameterizedQueryString("INSERT INTO Phasor(DeviceID, Label, Type, Phase, SourceIndex, Internal) VALUES ({0}, {1}, {2}, {3}, {4}, {5})", "deviceID", "label", "type", "phase", "sourceIndex", "internal");

                        // Define SQL statement to update existing phasor record
                        string updatePhasorSql = database.ParameterizedQueryString("UPDATE Phasor SET Label = {0}, Type = {1}, Phase = {2}, Internal = {3} WHERE DeviceID = {4} AND SourceIndex = {5}", "label", "type", "phase", "internal", "deviceID", "sourceIndex");
                    #else
                        // Define SQL statement to insert new phasor record
                        string insertPhasorSql = database.ParameterizedQueryString("INSERT INTO Phasor(DeviceID, Label, Type, Phase, SourceIndex) VALUES ({0}, {1}, {2}, {3}, {4})", "deviceID", "label", "type", "phase", "sourceIndex");

                        // Define SQL statement to update existing phasor record
                        string updatePhasorSql = database.ParameterizedQueryString("UPDATE Phasor SET Label = {0}, Type = {1}, Phase = {2} WHERE DeviceID = {3} AND SourceIndex = {4}", "label", "type", "phase", "deviceID", "sourceIndex");
                    #endif

                        // Define SQL statement to delete a phasor record
                        string deletePhasorSql = database.ParameterizedQueryString("DELETE FROM Phasor WHERE DeviceID = {0}", "deviceID");

                        // Define SQL statement to query phasor record ID
                        string queryPhasorIDSql = database.ParameterizedQueryString("SELECT ID FROM Phasor WHERE DeviceID = {0} AND SourceIndex = {1}", "deviceID", "sourceIndex");

                        // Define SQL statement to update destinationPhasorID field of existing phasor record
                        string updatePrimaryVoltageIDSql = database.ParameterizedQueryString($"UPDATE Phasor SET {PrimaryVoltageID} = {{0}} WHERE ID = {{1}}", "primaryVoltageID", "id");

                        // Define SQL statement to update phasor BaseKV
                        string updatePhasorBaseKVSql = database.ParameterizedQueryString("UPDATE Phasor SET BaseKV = {0} WHERE DeviceID = {1} AND SourceIndex = {2}", "baseKV", "deviceID", "sourceIndex");

                        // Check existence of optional meta-data fields
                        DataColumnCollection phasorDetailColumns = phasorDetail.Columns;
                        bool phasorIDFieldExists = phasorDetailColumns.Contains("ID");
                        bool primaryVoltageIDFieldExists = phasorDetailColumns.Contains(PrimaryVoltageID) || phasorDetailColumns.Contains(DestinationPhasorID);
                        bool baseKVFieldExists = phasorDetailColumns.Contains("BaseKV");

                        foreach (DataRow row in phasorDetail.Rows)
                        {
                            // Get device acronym
                            deviceAcronym = row.Field<string>("DeviceAcronym") ?? string.Empty;

                            // Make sure we have an associated device already defined for the phasor record
                            // ReSharper disable once CanSimplifyDictionaryLookupWithTryGetValue
                            if (!string.IsNullOrWhiteSpace(deviceAcronym) && deviceIDs.ContainsKey(deviceAcronym))
                            {
                                bool recordNeedsUpdating;

                                // Determine if record has changed since last synchronization
                                try
                                {
                                    updateTime = Convert.ToDateTime(row["UpdatedOn"]);
                                    recordNeedsUpdating = updateTime > m_lastMetaDataRefreshTime;

                                    if (updateTime > latestUpdateTime)
                                        latestUpdateTime = updateTime;
                                }
                                catch
                                {
                                    recordNeedsUpdating = true;
                                }

                                deviceID = deviceIDs[deviceAcronym];

                                int sourceIndex = row.ConvertField<int>("SourceIndex");
                                bool updateRecord = false;

                                // Determine if phasor record already exists
                                if (Convert.ToInt32(ExecuteScalar(command, phasorExistsSql, deviceID, sourceIndex)) == 0)
                                {
                                #if NET
                                    // Insert new phasor record
                                    ExecuteNonQuery(command, insertPhasorSql, deviceID, row.Field<string>("Label") ?? "undefined", (row.Field<string>("Type") ?? "V").TruncateLeft(1), (row.Field<string>("Phase") ?? "+").TruncateLeft(1), sourceIndex, database.Bool(Internal));
                                #else
                                    // Insert new phasor record
                                    ExecuteNonQuery(command, insertPhasorSql, deviceID, row.Field<string>("Label") ?? "undefined", (row.Field<string>("Type") ?? "V").TruncateLeft(1), (row.Field<string>("Phase") ?? "+").TruncateLeft(1), sourceIndex);
                                #endif
                                    updateRecord = true;
                                }
                                else if (recordNeedsUpdating)
                                {
                                #if NET
                                    // Update existing phasor record
                                    ExecuteNonQuery(command, updatePhasorSql, row.Field<string>("Label") ?? "undefined", (row.Field<string>("Type") ?? "V").TruncateLeft(1), (row.Field<string>("Phase") ?? "+").TruncateLeft(1), database.Bool(Internal), deviceID, sourceIndex);
                                #else
                                    // Update existing phasor record
                                    ExecuteNonQuery(command, updatePhasorSql, row.Field<string>("Label") ?? "undefined", (row.Field<string>("Type") ?? "V").TruncateLeft(1), (row.Field<string>("Phase") ?? "+").TruncateLeft(1), deviceID, sourceIndex);
                                #endif
                                    updateRecord = true;
                                }

                                if (updateRecord && baseKVFieldExists)
                                    ExecuteNonQuery(command, updatePhasorBaseKVSql, row.ConvertField<int>("BaseKV"), deviceID, sourceIndex);

                                if (phasorIDFieldExists && primaryVoltageIDFieldExists)
                                {
                                    int sourcePhasorID = row.ConvertField<int>("ID");

                                    // Using ConvertNullableField extension since publisher could use SQLite database in which case
                                    // all integers would arrive in data set as longs and need to be converted back to integers
                                    int? destinationPhasorID = row.ConvertNullableField<int>(phasorDetailColumns.Contains(PrimaryVoltageID) ? 
                                        PrimaryVoltageID : 
                                        DestinationPhasorID);

                                    if (destinationPhasorID.HasValue)
                                        sourceToDestinationIDMap[sourcePhasorID] = destinationPhasorID.Value;

                                    // Map all metadata phasor IDs to associated local database phasor IDs
                                    metadataToDatabaseIDMap[sourcePhasorID] = Convert.ToInt32(ExecuteScalar(command, queryPhasorIDSql, deviceID, sourceIndex));
                                }

                                // Track defined phasors for each device
                                definedSourceIndices.GetOrAdd(deviceID, _ => []).Add(sourceIndex);
                            }

                            // Periodically notify user about synchronization progress
                            UpdateSyncProgress();
                        }

                        // Once all phasor records have been processed, handle updating of destination phasor IDs
                        foreach (KeyValuePair<int, int> item in sourceToDestinationIDMap)
                        {
                            if (metadataToDatabaseIDMap.TryGetValue(item.Key, out int sourcePhasorID) && metadataToDatabaseIDMap.TryGetValue(item.Value, out int destinationPhasorID))
                                ExecuteNonQuery(command, updatePrimaryVoltageIDSql, destinationPhasorID, sourcePhasorID);
                        }

                        // For mutual subscriptions where this subscription is owner (i.e., internal is true), do not delete any phasor data - it will be managed by owner only
                        if (!MutualSubscription || !Internal)
                        {
                            // Remove any phasor records associated with existing devices in this session but no longer exist in the meta-data
                            foreach (int id in deviceIDs.Values)
                            {
                                string deleteSql = definedSourceIndices.TryGetValue(id, out List<int>? sourceIndices) ? $"{deletePhasorSql} AND SourceIndex NOT IN ({string.Join(",", sourceIndices)})" : deletePhasorSql;

                                ExecuteNonQuery(command, deleteSql, id);
                            }
                        }
                    }

                    transaction?.Commit();

                    // Update local in-memory synchronized meta-data cache
                    m_synchronizedMetadata = metadata;
                }
                catch (Exception ex)
                {
                    OnProcessException(MessageLevel.Error, new InvalidOperationException($"Failed to synchronize meta-data to local cache: {ex.Message}", ex));

                    if (transaction is not null)
                    {
                        try
                        {
                            transaction.Rollback();
                        }
                        catch (Exception rollbackException)
                        {
                            OnProcessException(MessageLevel.Error, new InvalidOperationException($"Failed to roll back database transaction due to exception: {rollbackException.Message}", rollbackException));
                        }
                    }

                    return;
                }
                finally
                {
                    transaction?.Dispose();
                }
            }

            // New signals may have been defined, take original remote signal index cache and apply changes
            if (m_remoteSignalIndexCache is not null && m_signalIndexCache is not null)
            {
                SignalIndexCache? originalReference, remoteSignalIndexCache;

                lock (m_signalIndexCacheLock)
                {
                    originalReference = m_signalIndexCache[m_cacheIndex];
                    remoteSignalIndexCache = m_remoteSignalIndexCache;
                }

                SignalIndexCache signalIndexCache = new(DataSource, remoteSignalIndexCache);

                if (signalIndexCache.Reference.Count > 0)
                {
                    lock (m_signalIndexCacheLock)
                    {
                        if (ReferenceEquals(originalReference, m_signalIndexCache[m_cacheIndex]))
                        {
                            signalIndexCache.TsscDecoder = m_signalIndexCache[m_cacheIndex]!.TsscDecoder;
                            m_signalIndexCache[m_cacheIndex] = signalIndexCache;
                        }
                    }
                }
            }

            m_lastMetaDataRefreshTime = latestUpdateTime > DateTime.MinValue ? latestUpdateTime : DateTime.UtcNow;

            OnStatusMessage(MessageLevel.Info, $"Meta-data synchronization completed successfully in {(DateTime.UtcNow.Ticks - startTime).ToElapsedTimeString(2)}");

            // Send notification that system configuration has changed
            OnConfigurationChanged();
        }
        catch (Exception ex)
        {
            OnProcessException(MessageLevel.Error, new InvalidOperationException($"Failed to synchronize meta-data to local cache: {ex.Message}", ex));
        }
        finally
        {
            // Restart data stream monitor after meta-data synchronization if it was originally enabled
            if (dataMonitoringEnabled && m_dataStreamMonitor is not null)
                m_dataStreamMonitor.Enabled = true;
        }
    }

    // Since Gemstone data extensions moved position of timeout parameter to a more logical ordinal location so that it will not
    // conflict with parameters, we establish overloads for .NET core / framework versions of needed data extension methods
#if NET
    private DataTable RetrieveData(AdoDataConnection _, DbCommand command, string sql, params object?[] parameters)
    {
        return command.RetrieveData(MetadataSynchronizationTimeout, sql, parameters);
    }

    private void ExecuteNonQuery(DbCommand command, string sql, params object?[] parameters)
    {
        command.ExecuteNonQuery(MetadataSynchronizationTimeout, sql, parameters);
    }

    private object? ExecuteScalar(DbCommand command, string sql, params object?[] parameters)
    {
        return command.ExecuteScalar(MetadataSynchronizationTimeout, sql, parameters);
    }
#else
    private DataTable RetrieveData(AdoDataConnection database, IDbCommand command, string sql, params object?[] parameters)
    {
        return command.RetrieveData(database.AdapterType, sql, MetadataSynchronizationTimeout, parameters);
    }

    private void ExecuteNonQuery(IDbCommand command, string sql, params object?[] parameters)
    {
        command.ExecuteNonQuery(sql, MetadataSynchronizationTimeout, parameters);
    }

    private object? ExecuteScalar(IDbCommand command, string sql, params object?[] parameters)
    {
        return command.ExecuteScalar(sql, MetadataSynchronizationTimeout, parameters);
    }
#endif

    private void InitSyncProgress(long totalActions)
    {
        m_syncProgressTotalActions = totalActions;
        m_syncProgressActionsCount = 0;
        m_syncProgressLastMessage = DateTime.UtcNow.Ticks;
    }

    private void UpdateSyncProgress()
    {
        m_syncProgressActionsCount++;

        // We update user on progress every 15 seconds or at 100% complete
        if (DateTime.UtcNow.Ticks - m_syncProgressLastMessage < 150000000 && m_syncProgressActionsCount < m_syncProgressTotalActions)
            return;

        OnStatusMessage(MessageLevel.Info, $"Meta-data synchronization is {m_syncProgressActionsCount / (double)m_syncProgressTotalActions:0.0%} complete...");
        m_syncProgressLastMessage = DateTime.UtcNow.Ticks;
    }

    private SignalIndexCache DeserializeSignalIndexCache(byte[] buffer)
    {
        CompressionModes compressionModes = (CompressionModes)(m_operationalModes & OperationalModes.CompressionModeMask);
        bool compressSignalIndexCache = (m_operationalModes & OperationalModes.CompressSignalIndexCache) > 0;
        GZipStream? inflater = null;

        if (compressSignalIndexCache && compressionModes.HasFlag(CompressionModes.GZip))
        {
            try
            {
                using MemoryStream compressedData = new(buffer);
                inflater = new GZipStream(compressedData, CompressionMode.Decompress, true);
                buffer = inflater.ReadStream();
            }
            finally
            {
                inflater?.Close();
            }
        }

        SignalIndexCache deserializedCache = new() { Encoding = Encoding };
        deserializedCache.ParseBinaryImage(buffer, 0, buffer.Length);

        return deserializedCache;
    }

    private DataSet DeserializeMetadata(byte[] buffer)
    {
        CompressionModes compressionModes = (CompressionModes)(m_operationalModes & OperationalModes.CompressionModeMask);
        bool compressMetadata = (m_operationalModes & OperationalModes.CompressMetadata) > 0;
        Ticks startTime = DateTime.UtcNow.Ticks;
        DataSet deserializedMetadata;
        GZipStream? inflater = null;

        if (compressMetadata && compressionModes.HasFlag(CompressionModes.GZip))
        {
            try
            {
                // Insert compressed data into compressed buffer
                using MemoryStream compressedData = new(buffer);
                inflater = new GZipStream(compressedData, CompressionMode.Decompress, true);
                buffer = inflater.ReadStream();
            }
            finally
            {
                inflater?.Close();
            }
        }

        // Copy decompressed data into encoded buffer
        using (MemoryStream encodedData = new(buffer))
        using (XmlTextReader xmlReader = new(encodedData))
        {
            // Read encoded data into data set as XML
            deserializedMetadata = new DataSet();
            deserializedMetadata.ReadXml(xmlReader, XmlReadMode.ReadSchema);
        }

        long rowCount = deserializedMetadata.Tables.Cast<DataTable>().Select(dataTable => (long)dataTable.Rows.Count).Sum();

        if (rowCount > 0)
        {
            Time elapsedTime = (DateTime.UtcNow.Ticks - startTime).ToSeconds();
            OnStatusMessage(MessageLevel.Info, $"Received a total of {rowCount:N0} records spanning {deserializedMetadata.Tables.Count:N0} tables of meta-data that was {(compressMetadata ? "uncompressed and " : "")}deserialized in {elapsedTime.ToString(3)}...");
        }

        return deserializedMetadata;
    }

    private static Encoding GetCharacterEncoding(OperationalEncoding operationalEncoding)
    {
        Encoding encoding = operationalEncoding switch
        {
            OperationalEncoding.UTF16LE => Encoding.Unicode,
            OperationalEncoding.UTF16BE => Encoding.BigEndianUnicode,
            OperationalEncoding.UTF8 => Encoding.UTF8,
            _ => throw new InvalidOperationException($"Unsupported encoding detected: {operationalEncoding}")
        };

        return encoding;
    }

    // Socket exception handler
    private bool HandleSocketException(Exception? ex)
    {
        // WSAECONNABORTED and WSAECONNRESET are common errors after a client disconnect,
        // if they happen for other reasons, make sure disconnect procedure is handled
        if (ex is SocketException { ErrorCode: 10053 or 10054 })
        {
            DisconnectClient();
            return true;
        }

        if (ex is not null)
            HandleSocketException(ex.InnerException);

        return false;
    }

    // Disconnect client, restarting if disconnect was not intentional
    private void DisconnectClient()
    {
        // Mark end of any data transmission in run-time log
        if (m_runTimeLog is not null && m_runTimeLog.Enabled)
        {
            m_runTimeLog.StopTime = DateTimeOffset.UtcNow;
            m_runTimeLog.Enabled = false;
        }

        // Stop data gap recovery operations
        if (m_dataGapRecoveryEnabled && m_dataGapRecoverer is not null)
        {
            try
            {
                m_dataGapRecoverer.Enabled = false;
            }
            catch (Exception ex)
            {
                OnProcessException(MessageLevel.Warning, new InvalidOperationException($"Exception while attempting to flush data gap recoverer log: {ex.Message}", ex));
            }
        }

        DataChannel = null;
        m_metadataRefreshPending = false;

        if (m_serverCommandChannel is null)
        {
            // If user didn't initiate disconnect, restart the connection cycle
            if (Enabled)
                Start();
        }
        else
        {
            if (m_activeClientID != Guid.Empty)
                m_serverCommandChannel.DisconnectOne(m_activeClientID);

            // When subscriber is in server mode, the server does not need to be restarted, but we
            // will reset client statistics - this is a server of "one" client, the publisher
            m_activeClientID = Guid.Empty;
            m_subscribedDevicesTimer?.Stop();
            m_metadataRefreshPending = false;
            m_expectedBufferBlockSequenceNumber = 0u;
            m_subscribed = false;
            m_keyIVs = null;
            TotalBytesReceived = 0L;
            m_lastBytesReceived = 0;
            m_lastReceivedAt = DateTime.MinValue;
        }
    }

    // Gets the socket instance used by this client
    private Socket? GetCommandChannelSocket()
    {
        Guid clientID = m_serverCommandChannel?.ClientIDs.FirstOrDefault() ?? Guid.Empty;

        return m_serverCommandChannel switch
        {
            TcpServer tcpServerCommandChannel when tcpServerCommandChannel.TryGetClient(clientID, out TransportProvider<Socket>? tcpProvider) => tcpProvider?.Provider,
            TlsServer tlsServerCommandChannel when tlsServerCommandChannel.TryGetClient(clientID, out TransportProvider<TlsServer.TlsSocket>? tlsProvider) => tlsProvider?.Provider?.Socket,
#if NET
            _ => (m_clientCommandChannel as TcpClient)?.Client
#else
            _ => (m_clientCommandChannel as TcpClient)?.Client ?? (m_clientCommandChannel as TcpSimpleClient)?.Client
#endif
        };
    }

    private void HandleDeviceStatisticsRegistration()
    {
        if (BypassStatistics)
            return;

        if (Enabled)
            RegisterDeviceStatistics();
        else
            UnregisterDeviceStatistics();
    }

    private void RegisterDeviceStatistics()
    {
        long now = UseLocalClockAsRealTime ? DateTime.UtcNow.Ticks : 0L;

        try
        {
            DataSet? dataSource = DataSource;

            if (dataSource is null || !dataSource.Tables.Contains("InputStreamDevices"))
            {
                if (m_statisticsHelpers is not null)
                {
                    foreach (DeviceStatisticsHelper<SubscribedDevice> statisticsHelper in m_statisticsHelpers)
                        statisticsHelper.Device.Dispose();
                }

                m_statisticsHelpers = [];
                m_subscribedDevicesLookup = new Dictionary<Guid, DeviceStatisticsHelper<SubscribedDevice>>();
            }
            else
            {
                Dictionary<Guid, DeviceStatisticsHelper<SubscribedDevice>> subscribedDevicesLookup = new();
                List<DeviceStatisticsHelper<SubscribedDevice>> subscribedDevices = [];
                HashSet<string> subscribedDeviceNames = [];
                HashSet<string> definedDeviceNames = [];

                foreach (DataRow deviceRow in dataSource.Tables["InputStreamDevices"]!.Select($"ParentID = {ID}"))
                    definedDeviceNames.Add($"LOCAL${deviceRow["Acronym"].ToNonNullString()}");

                if (m_statisticsHelpers is not null)
                {
                    foreach (DeviceStatisticsHelper<SubscribedDevice> statisticsHelper in m_statisticsHelpers)
                    {
                        if (definedDeviceNames.Contains(statisticsHelper.Device.Name))
                        {
                            subscribedDevices.Add(statisticsHelper);
                            subscribedDeviceNames.Add(statisticsHelper.Device.Name);
                        }
                        else
                        {
                            statisticsHelper.Device.Dispose();
                        }
                    }
                }

                foreach (string definedDeviceName in definedDeviceNames)
                {
                    if (subscribedDeviceNames.Contains(definedDeviceName))
                        continue;

                    DeviceStatisticsHelper<SubscribedDevice> statisticsHelper = new(new SubscribedDevice(definedDeviceName));
                    subscribedDevices.Add(statisticsHelper);
                    statisticsHelper.Reset(now);
                }

                if (dataSource.Tables.Contains("ActiveMeasurements"))
                {
                    ActiveMeasurementsTableLookup measurementLookup = DataSourceLookups.ActiveMeasurements(dataSource);

                    foreach (DeviceStatisticsHelper<SubscribedDevice> statisticsHelper in subscribedDevices)
                    {
                        string deviceName = Regex.Replace(statisticsHelper.Device.Name, @"^LOCAL\$", "");

                        foreach (DataRow measurementRow in measurementLookup.LookupByDeviceNameNoStat(deviceName))
                        {
                            if (Guid.TryParse(measurementRow["SignalID"].ToNonNullString(), out Guid signalID))
                            {
                                // In some rare cases duplicate signal ID's have been encountered (likely bad configuration),
                                // as a result we use a GetOrAdd instead of an Add
                                subscribedDevicesLookup.GetOrAdd(signalID, statisticsHelper);

                                switch (measurementRow["SignalType"].ToNonNullString())
                                {
                                    case "FLAG":
                                        statisticsHelper.Device.StatusFlagsID = signalID;
                                        break;

                                    case "FREQ":
                                        statisticsHelper.Device.FrequencyID = signalID;
                                        break;

                                    case "DFDT":
                                        statisticsHelper.Device.DeltaFrequencyID = signalID;
                                        break;
                                }
                            }
                        }
                    }
                }

                m_subscribedDevicesLookup = subscribedDevicesLookup;
                m_statisticsHelpers = subscribedDevices;
            }

            FixExpectedMeasurementCounts();
        }
        catch (Exception ex)
        {
            OnProcessException(MessageLevel.Info, new InvalidOperationException($"Unable to register device statistics due to exception: {ex.Message}", ex));
        }
    }

    private void UnregisterDeviceStatistics()
    {
        try
        {
            if (m_statisticsHelpers is null)
                return;

            foreach (DeviceStatisticsHelper<SubscribedDevice> statisticsHelper in m_statisticsHelpers)
                statisticsHelper.Device.Dispose();

            m_statisticsHelpers = [];
            m_subscribedDevicesLookup = new Dictionary<Guid, DeviceStatisticsHelper<SubscribedDevice>>();
        }
        catch (Exception ex)
        {
            OnProcessException(MessageLevel.Info, new InvalidOperationException($"Unable to unregister device statistics due to exception: {ex.Message}", ex));
        }
    }

    private void FixExpectedMeasurementCounts()
    {
        Dictionary<Guid, DeviceStatisticsHelper<SubscribedDevice>>? subscribedDevicesLookup = m_subscribedDevicesLookup;
        List<DeviceStatisticsHelper<SubscribedDevice>>? statisticsHelpers = m_statisticsHelpers;
        DataSet? dataSource = DataSource;
        SignalIndexCache? signalIndexCache;
        DataTable measurementTable;

        lock (m_signalIndexCacheLock)
            signalIndexCache = m_signalIndexCache?[m_cacheIndex];

        try
        {
            if (statisticsHelpers is null || subscribedDevicesLookup is null)
                return;

            if (signalIndexCache is null)
                return;

            if (dataSource is null || !dataSource.Tables.Contains("ActiveMeasurements"))
                return;

            measurementTable = dataSource.Tables["ActiveMeasurements"]!;

            if (!measurementTable.Columns.Contains("FramesPerSecond"))
                return;

            // Get expected measurement counts
            IEnumerable<IGrouping<DeviceStatisticsHelper<SubscribedDevice>, Guid>> groups = signalIndexCache.AuthorizedSignalIDs
                .Where(signalID => subscribedDevicesLookup.TryGetValue(signalID, out _))
                .Select(signalID => Tuple.Create(subscribedDevicesLookup[signalID], signalID))
                .ToList()
                .GroupBy(tuple => tuple.Item1, tuple => tuple.Item2);

            foreach (IGrouping<DeviceStatisticsHelper<SubscribedDevice>, Guid> group in groups)
            {
                int[] frameRates = group
                    .Select(signalID => GetFramesPerSecond(measurementTable, signalID))
                    .Where(frameRate => frameRate != 0)
                    .ToArray();

                group.Key.Device.MeasurementsDefined = frameRates.Length;
                group.Key.ExpectedMeasurementsPerSecond = frameRates.Sum();
            }
        }
        catch (Exception ex)
        {
            OnProcessException(MessageLevel.Info, new InvalidOperationException($"Unable to set expected measurement counts for gathering statistics due to exception: {ex.Message}", ex));
        }
    }

    private static int GetFramesPerSecond(DataTable measurementTable, Guid signalID)
    {
        DataRow? row = measurementTable.Select($"SignalID = '{signalID}'").FirstOrDefault();

        if (row is null)
            return 0;

        return row.Field<string>("SignalType")?.ToUpperInvariant() switch
        {
            "FLAG" => 0,
            "STAT" => 0,
            "CALC" => 0,
            "ALRM" => 0,
            "QUAL" => 0,
            _ => row.ConvertField<int>("FramesPerSecond")
        };
    }

    // This method is called when connection has been authenticated
    private void DataSubscriber_ConnectionAuthenticated(object? sender, EventArgs e)
    {
        if (AutoConnect && Enabled)
            StartSubscription();
    }

    // This method is called then new meta-data has been received
    private void DataSubscriber_MetaDataReceived(object? sender, EventArgs<DataSet> e)
    {
        try
        {
            // We handle synchronization on a separate thread since this process may be lengthy
            if (AutoSynchronizeMetadata)
                SynchronizeMetadata(e.Argument);
        }
        catch (Exception ex)
        {
            // Process exception for logging
            OnProcessException(MessageLevel.Error, new InvalidOperationException($"Failed to queue meta-data synchronization due to exception: {ex.Message}", ex));
        }
    }

    /// <summary>
    /// Raises the <see cref="ConnectionEstablished"/> event.
    /// </summary>
    protected virtual void OnConnectionEstablished()
    {
        try
        {
            ConnectionEstablished?.Invoke(this, EventArgs.Empty);
            m_lastMissingCacheWarning = 0L;
        }
        catch (Exception ex)
        {
            // We protect our code from consumer thrown exceptions
            OnProcessException(MessageLevel.Info, new InvalidOperationException($"Exception in consumer handler for ConnectionEstablished event: {ex.Message}", ex), "ConsumerEventException");
        }
    }

    /// <summary>
    /// Raises the <see cref="ConnectionTerminated"/> event.
    /// </summary>
    protected virtual void OnConnectionTerminated()
    {
        try
        {
            ConnectionTerminated?.Invoke(this, EventArgs.Empty);
        }
        catch (Exception ex)
        {
            // We protect our code from consumer thrown exceptions
            OnProcessException(MessageLevel.Info, new InvalidOperationException($"Exception in consumer handler for ConnectionTerminated event: {ex.Message}", ex), "ConsumerEventException");
        }
    }

    /// <summary>
    /// Raises the <see cref="ConnectionAuthenticated"/> event.
    /// </summary>
    protected virtual void OnConnectionAuthenticated()
    {
        try
        {
            ConnectionAuthenticated?.Invoke(this, EventArgs.Empty);
        }
        catch (Exception ex)
        {
            // We protect our code from consumer thrown exceptions
            OnProcessException(MessageLevel.Info, new InvalidOperationException($"Exception in consumer handler for ConnectionAuthenticated event: {ex.Message}", ex), "ConsumerEventException");
        }
    }

    /// <summary>
    /// Raises the <see cref="ReceivedServerResponse"/> event.
    /// </summary>
    /// <param name="responseCode">Response received from the server.</param>
    /// <param name="commandCode">Command that the server responded to.</param>
    protected virtual void OnReceivedServerResponse(ServerResponse responseCode, ServerCommand commandCode)
    {
        try
        {
            ReceivedServerResponse?.Invoke(this, new EventArgs<ServerResponse, ServerCommand>(responseCode, commandCode));
        }
        catch (Exception ex)
        {
            // We protect our code from consumer thrown exceptions
            OnProcessException(MessageLevel.Info, new InvalidOperationException($"Exception in consumer handler for ReceivedServerResponse event: {ex.Message}", ex), "ConsumerEventException");
        }
    }

    /// <summary>
    /// Raises the <see cref="ReceivedUserCommandResponse"/> event.
    /// </summary>
    /// <param name="command">The code for the user command.</param>
    /// <param name="response">The code for the server's response.</param>
    /// <param name="buffer">Buffer containing the message from the server.</param>
    /// <param name="startIndex">Index into the buffer used to skip the header.</param>
    /// <param name="length">The length of the message in the buffer, including the header.</param>
    protected virtual void OnReceivedUserCommandResponse(ServerCommand command, ServerResponse response, byte[] buffer, int startIndex, int length)
    {
        if (ReceivedUserCommandResponse is null)
            return;

        try
        {
            UserCommandArgs args = new(command, response, buffer, startIndex, length);
            ReceivedUserCommandResponse?.Invoke(this, args);
        }
        catch (Exception ex)
        {
            // We protect our code from consumer thrown exceptions
            OnProcessException(MessageLevel.Info, new InvalidOperationException($"Exception in consumer handler for UserCommandResponse event: {ex.Message}", ex), "ConsumerEventException");
        }
    }

    /// <summary>
    /// Raises the <see cref="MetaDataReceived"/> event.
    /// </summary>
    /// <param name="metadata">Meta-data <see cref="DataSet"/> instance to send to client subscription.</param>
    protected virtual void OnMetaDataReceived(DataSet metadata)
    {
        try
        {
            MetaDataReceived?.Invoke(this, new EventArgs<DataSet>(metadata));
        }
        catch (Exception ex)
        {
            // We protect our code from consumer thrown exceptions
            OnProcessException(MessageLevel.Info, new InvalidOperationException($"Exception in consumer handler for MetaDataReceived event: {ex.Message}", ex), "ConsumerEventException");
        }
    }

    /// <summary>
    /// Raises the <see cref="DataStartTime"/> event.
    /// </summary>
    /// <param name="startTime">Start time, in <see cref="Ticks"/>, of first measurement transmitted.</param>
    protected virtual void OnDataStartTime(Ticks startTime)
    {
        try
        {
            DataStartTime?.Invoke(this, new EventArgs<Ticks>(startTime));
        }
        catch (Exception ex)
        {
            // We protect our code from consumer thrown exceptions
            OnProcessException(MessageLevel.Info, new InvalidOperationException($"Exception in consumer handler for DataStartTime event: {ex.Message}", ex), "ConsumerEventException");
        }
    }

    /// <summary>
    /// Raises the <see cref="ProcessingComplete"/> event.
    /// </summary>
    /// <param name="source">Type name of adapter that sent the processing completed notification.</param>
    protected virtual void OnProcessingComplete(string source)
    {
        try
        {
            ProcessingComplete?.Invoke(this, new EventArgs<string>(source));

            // Also raise base class event in case this event has been subscribed
            OnProcessingComplete();
        }
        catch (Exception ex)
        {
            // We protect our code from consumer thrown exceptions
            OnProcessException(MessageLevel.Info, new InvalidOperationException($"Exception in consumer handler for ProcessingComplete event: {ex.Message}", ex), "ConsumerEventException");
        }
    }

    /// <summary>
    /// Raises the <see cref="NotificationReceived"/> event.
    /// </summary>
    /// <param name="message">Message for the notification.</param>
    protected virtual void OnNotificationReceived(string message)
    {
        try
        {
            NotificationReceived?.Invoke(this, new EventArgs<string>(message));
        }
        catch (Exception ex)
        {
            // We protect our code from consumer thrown exceptions
            OnProcessException(MessageLevel.Info, new InvalidOperationException($"Exception in consumer handler for NotificationReceived event: {ex.Message}", ex), "ConsumerEventException");
        }
    }

    /// <summary>
    /// Raises the <see cref="ServerConfigurationChanged"/> event.
    /// </summary>
    protected virtual void OnServerConfigurationChanged()
    {
        try
        {
            ServerConfigurationChanged?.Invoke(this, EventArgs.Empty);
        }
        catch (Exception ex)
        {
            // We protect our code from consumer thrown exceptions
            OnProcessException(MessageLevel.Info, new InvalidOperationException($"Exception in consumer handler for ServerConfigurationChanged event: {ex.Message}", ex), "ConsumerEventException");
        }
    }

    /// <summary>
    /// Raises <see cref="AdapterBase.ProcessException"/> event.
    /// </summary>
    /// <param name="level">The <see cref="MessageLevel"/> to assign to this message</param>
    /// <param name="exception">Processing <see cref="Exception"/>.</param>
    /// <param name="eventName">A fixed string to classify this event; defaults to <c>null</c>.</param>
    /// <param name="flags"><see cref="MessageFlags"/> to use, if any; defaults to <see cref="MessageFlags.None"/>.</param>
    protected override void OnProcessException(MessageLevel level, Exception exception, string? eventName = null, MessageFlags flags = MessageFlags.None)
    {
        base.OnProcessException(level, exception, eventName, flags);

        // Just in case Log Message Suppression was turned on, turn it off so this code can raise messages
        using IDisposable messages = Logger.OverrideSuppressLogMessages();

        if (DateTime.UtcNow.Ticks - m_lastParsingExceptionTime > ParsingExceptionWindow)
        {
            // Exception window has passed since last exception, so we reset counters
            m_lastParsingExceptionTime = DateTime.UtcNow.Ticks;
            m_parsingExceptionCount = 0;
        }

        m_parsingExceptionCount++;

        if (m_parsingExceptionCount <= AllowedParsingExceptions)
            return;

        try
        {
            // When the parsing exception threshold has been exceeded, connection is restarted
            Start();
        }
        catch (Exception ex)
        {
            base.OnProcessException(MessageLevel.Warning, new InvalidOperationException($"Error while restarting subscriber connection due to excessive exceptions: {ex.Message}", ex), "DataSubscriber", MessageFlags.UsageIssue);
        }
        finally
        {
            // Notify consumer of parsing exception threshold deviation
            OnExceededParsingExceptionThreshold();
            m_lastParsingExceptionTime = 0;
            m_parsingExceptionCount = 0;
        }
    }

    /// <summary>
    /// Raises the <see cref="ExceededParsingExceptionThreshold"/> event.
    /// </summary>
    private void OnExceededParsingExceptionThreshold()
    {
        ExceededParsingExceptionThreshold?.Invoke(this, EventArgs.Empty);
    }

    // Updates the measurements per second counters after receiving another set of measurements.
    private void UpdateMeasurementsPerSecond(DateTime now, int measurementCount)
    {
        long secondsSinceEpoch = now.Ticks / Ticks.PerSecond;

        if (secondsSinceEpoch > m_lastSecondsSinceEpoch)
        {
            if (m_measurementsInSecond < MinimumMeasurementsPerSecond || MinimumMeasurementsPerSecond == 0L)
                MinimumMeasurementsPerSecond = m_measurementsInSecond;

            if (m_measurementsInSecond > MaximumMeasurementsPerSecond || MaximumMeasurementsPerSecond == 0L)
                MaximumMeasurementsPerSecond = m_measurementsInSecond;

            m_totalMeasurementsPerSecond += m_measurementsInSecond;
            m_measurementsPerSecondCount++;
            m_measurementsInSecond = 0L;

            m_lastSecondsSinceEpoch = secondsSinceEpoch;
        }

        m_measurementsInSecond += measurementCount;
    }

    // Resets the measurements per second counters after reading the values from the last calculation interval.
    private void ResetMeasurementsPerSecondCounters()
    {
        MinimumMeasurementsPerSecond = 0L;
        MaximumMeasurementsPerSecond = 0L;
        m_totalMeasurementsPerSecond = 0L;
        m_measurementsPerSecondCount = 0L;
    }

    private void UpdateStatisticsHelpers()
    {
        List<DeviceStatisticsHelper<SubscribedDevice>>? statisticsHelpers = m_statisticsHelpers;

        if (statisticsHelpers is null)
            return;

        long now = RealTime;

        foreach (DeviceStatisticsHelper<SubscribedDevice> statisticsHelper in statisticsHelpers)
        {
            statisticsHelper.Update(now);

            // FUTURE: Missing data detection could be complex. For example, no need to continue logging data outages for devices that are offline - but how to detect?
            // If data channel is UDP, measurements are missing for time span and data gap recovery enabled, request missing
            //if m_dataChannel is not null && m_dataGapRecoveryEnabled && m_dataGapRecoverer is not null && m_lastMeasurementCheck > 0 &&
            //    statisticsHelper.Device.MeasurementsExpected - statisticsHelper.Device.MeasurementsReceived > m_minimumMissingMeasurementThresholdID
            //    m_dataGapRecoverer.LogDataGap(m_lastMeasurementCheck - Ticks.FromSeconds(m_transmissionDelayTimeAdjustment), now);
        }

        //m_lastMeasurementCheck = now;
    }

    private void SubscribedDevicesTimer_Elapsed(object? sender, EventArgs<DateTime> elapsedEventArgs)
    {
        UpdateStatisticsHelpers();
    }

    private bool SynchronizedMetadataChanged(DataSet? newSynchronizedMetadata)
    {
        try
        {
            return !DataSetEqualityComparer.Default.Equals(m_synchronizedMetadata, newSynchronizedMetadata);
        }
        catch
        {
            return true;
        }
    }

    /// <summary>
    /// Gets file path for any defined logging path.
    /// </summary>
    /// <param name="filePath">Path to acquire within logging path.</param>
    /// <returns>File path within any defined logging path.</returns>
    protected string GetLoggingPath(string filePath)
    {
        return string.IsNullOrWhiteSpace(m_loggingPath) ? FilePath.GetAbsolutePath(filePath) : Path.Combine(m_loggingPath, filePath);
    }

    private void DataStreamMonitor_Elapsed(object? sender, EventArgs<DateTime> e)
    {
        bool dataReceived = m_monitoredBytesReceived > 0;

        if (m_dataChannel is null && m_metadataRefreshPending)
        {
            if (m_lastReceivedAt > DateTime.MinValue)
                dataReceived = (DateTime.UtcNow - m_lastReceivedAt).Seconds < DataLossInterval;
        }

        if (!dataReceived)
        {
            // If we've received no data in the last time-span, we restart connect cycle...
            m_dataStreamMonitor!.Enabled = false;
            OnStatusMessage(MessageLevel.Info, $"{Environment.NewLine}No data received in {m_dataStreamMonitor.Interval / 1000.0D:0.0} seconds, restarting connect cycle...{Environment.NewLine}", "Connection Issues");

            ThreadPool.QueueUserWorkItem(_ =>
            {
                if (m_serverCommandChannel is null)
                    Restart();
                else
                    DisconnectClient();
            });
        }

        // Reset bytes received bytes being monitored
        m_monitoredBytesReceived = 0L;
    }

    private void RunTimeLog_ProcessException(object? sender, EventArgs<Exception> e)
    {
        OnProcessException(MessageLevel.Info, e.Argument);
    }

    private void DataGapRecoverer_RecoveredMeasurements(object? sender, EventArgs<ICollection<IMeasurement>> e)
    {
        OnNewMeasurements(e.Argument);
    }

    private void DataGapRecoverer_StatusMessage(object? sender, EventArgs<string> e)
    {
        OnStatusMessage(MessageLevel.Info, "[DataGapRecoverer] " + e.Argument);
    }

    private void DataGapRecoverer_ProcessException(object? sender, EventArgs<Exception> e)
    {
        OnProcessException(MessageLevel.Warning, new InvalidOperationException("[DataGapRecoverer] " + e.Argument.Message, e.Argument.InnerException));
    }

    #region [ Client Command Channel Event Handlers ]

    private void ClientCommandChannelConnectionEstablished(object? sender, EventArgs e)
    {
        // Define operational modes as soon as possible
        SendServerCommand(ServerCommand.DefineOperationalModes, BigEndian.GetBytes((uint)m_operationalModes));

        // Notify input adapter base that asynchronous connection succeeded
        if (!PersistConnectionForMetadata)
            OnConnected();
        else
            SendServerCommand(ServerCommand.MetaDataRefresh, MetadataFilters);

        // Notify consumer that connection was successfully established
        OnConnectionEstablished();

        OnStatusMessage(MessageLevel.Info, m_serverCommandChannel is null ? "Data subscriber command channel connection to publisher was established." : "Data subscriber server-based command channel established a new client connection from the publisher.");

        if (AutoConnect && Enabled)
            StartSubscription();

        if (m_dataGapRecoveryEnabled && m_dataGapRecoverer is not null)
            m_dataGapRecoverer.Enabled = true;
    }

    private void ClientCommandChannelConnectionTerminated(object? sender, EventArgs e)
    {
        OnConnectionTerminated();

        OnStatusMessage(MessageLevel.Info, m_serverCommandChannel is null ? "Data subscriber command channel connection to publisher was terminated." : "Data subscriber server-based command channel client connection from the publisher was terminated.");

        DisconnectClient();
    }

    private void ClientCommandChannelConnectionException(object? sender, EventArgs<Exception> e)
    {
        Exception ex = e.Argument;
        OnProcessException(MessageLevel.Info, new ConnectionException($"Data subscriber encountered an exception while attempting command channel publisher connection: {ex.Message}", ex));
    }

    private void ClientCommandChannelConnectionAttempt(object? sender, EventArgs e)
    {
        // Inject a short delay between multiple connection attempts
        if (m_commandChannelConnectionAttempts > 0)
            Thread.Sleep(2000);

        OnStatusMessage(MessageLevel.Info, "Attempting command channel connection to publisher...");
        m_commandChannelConnectionAttempts++;
    }

    private void ClientCommandChannelSendDataException(object? sender, EventArgs<Exception> e)
    {
        Exception ex = e.Argument;

        if (!HandleSocketException(ex) && ex is not ObjectDisposedException)
            OnProcessException(MessageLevel.Info, new InvalidOperationException($"Data subscriber encountered an exception while sending command channel data to publisher connection: {ex.Message}", ex));
    }

    private void ClientCommandChannelReceiveData(object? sender, EventArgs<int> e)
    {
        try
        {
            int length = e.Argument;
            byte[] buffer = new byte[length];

            m_lastBytesReceived = length;
            m_lastReceivedAt = DateTime.UtcNow;

            m_clientCommandChannel?.Read(buffer, 0, length);
            m_serverCommandChannel?.Read(m_activeClientID, buffer, 0, length);

            ProcessServerResponse(buffer, length);
        }
        catch (Exception ex)
        {
            OnProcessException(MessageLevel.Info, ex);
        }
    }

    private void ClientCommandChannelReceiveDataException(object? sender, EventArgs<Exception> e)
    {
        Exception ex = e.Argument;

        if (!HandleSocketException(ex) && ex is not ObjectDisposedException)
            OnProcessException(MessageLevel.Info, new InvalidOperationException($"Data subscriber encountered an exception while receiving command channel data from publisher connection: {ex.Message}", ex));
    }

    #endregion

    #region [ Server Command Channel Event Handlers ]

    private void ServerCommandChannelReceiveClientData(object? sender, EventArgs<Guid, int> e)
    {
        ClientCommandChannelReceiveData(sender, new EventArgs<int>(e.Argument2));
    }

    private void ServerCommandChannelClientConnected(object? sender, EventArgs<Guid> e)
    {
        m_activeClientID = e.Argument;

        // Reset all connection stats when new publisher-client connects - this is equivalent
        // to a normal client-based subscriber establishing a new connection to the publisher
        List<DeviceStatisticsHelper<SubscribedDevice>>? statisticsHelpers = m_statisticsHelpers;

        if (statisticsHelpers is not null)
        {
            long now = UseLocalClockAsRealTime ? DateTime.UtcNow.Ticks : 0L;
            m_realTime = 0L;
            m_lastStatisticsHelperUpdate = 0L;

            foreach (DeviceStatisticsHelper<SubscribedDevice> statisticsHelper in statisticsHelpers)
                statisticsHelper.Reset(now);
        }

        if (UseLocalClockAsRealTime)
            m_subscribedDevicesTimer?.Start();

        ClientCommandChannelConnectionEstablished(sender, EventArgs.Empty);
    }

    private void ServerCommandChannelClientDisconnected(object? sender, EventArgs<Guid> e)
    {
        ClientCommandChannelConnectionTerminated(sender, EventArgs.Empty);
    }

    private void ServerCommandChannelClientConnectingException(object? sender, EventArgs<Exception> e)
    {
        Exception ex = e.Argument;
        OnProcessException(MessageLevel.Info, new ConnectionException($"Data subscriber encountered an exception while connecting client-based publisher to the command channel: {ex.Message}", ex));
    }

    private void ServerCommandChannelServerStarted(object? sender, EventArgs e)
    {
        OnStatusMessage(MessageLevel.Info, "Data subscriber server-based command channel started.");
    }

    private void ServerCommandChannelServerStopped(object? sender, EventArgs e)
    {
        if (Enabled)
        {
            OnStatusMessage(MessageLevel.Info, "Data subscriber server-based command channel was unexpectedly terminated, restarting...");

            Action restartServerCommandChannel = () =>
            {
                try
                {
                    m_serverCommandChannel?.Start();
                }
                catch (Exception ex)
                {
                    OnProcessException(MessageLevel.Warning, new InvalidOperationException($"Failed to restart data subscriber server-based command channel: {ex.Message}", ex));
                }
            };

            // We must wait for command channel to completely shutdown before trying to restart...
            restartServerCommandChannel.DelayAndExecute(2000);
        }
        else
        {
            OnStatusMessage(MessageLevel.Info, "Data subscriber server-based command channel stopped.");
        }
    }

    private void ServerCommandChannelSendClientDataException(object? sender, EventArgs<Guid, Exception> e)
    {
        Exception ex = e.Argument2;

        if (HandleSocketException(ex))
            return;

        if (ex is not NullReferenceException && ex is not ObjectDisposedException)
            OnProcessException(MessageLevel.Info, new InvalidOperationException($"Data subscriber encountered an exception while sending command channel data to client-based publisher connection: {ex.Message}", ex));

        DisconnectClient();
    }

    private void ServerCommandChannelReceiveClientDataException(object? sender, EventArgs<Guid, Exception> e)
    {
        Exception ex = e.Argument2;

        if (HandleSocketException(ex))
            return;

        if (ex is not NullReferenceException && ex is not ObjectDisposedException)
            OnProcessException(MessageLevel.Info, new InvalidOperationException($"Data subscriber encountered an exception while receiving command channel data from client-based publisher connection: {ex.Message}", ex));

        DisconnectClient();
    }

    #endregion

    #region [ Data Channel Event Handlers ]

    private void DataChannelConnectionException(object? sender, EventArgs<Exception> e)
    {
        Exception ex = e.Argument;
        OnProcessException(MessageLevel.Info, new ConnectionException($"Data subscriber encountered an exception while attempting to establish UDP data channel connection: {ex.Message}", ex));
    }

    private void DataChannelConnectionAttempt(object? sender, EventArgs e)
    {
        // Inject a short delay between multiple connection attempts
        if (m_dataChannelConnectionAttempts > 0)
            Thread.Sleep(2000);

        OnStatusMessage(MessageLevel.Info, "Attempting to establish data channel connection to publisher...");
        m_dataChannelConnectionAttempts++;
    }

    private void DataChannelReceiveData(object? sender, EventArgs<int> e)
    {
        try
        {
            int length = e.Argument;
            byte[] buffer = new byte[length];

            m_lastBytesReceived = length;

            m_dataChannel?.Read(buffer, 0, length);
            ProcessServerResponse(buffer, length);
        }
        catch (Exception ex)
        {
            OnProcessException(MessageLevel.Info, ex);
        }
    }

    private void DataChannelReceiveDataException(object? sender, EventArgs<Exception> e)
    {
        Exception ex = e.Argument;

        if (!HandleSocketException(ex) && ex is not ObjectDisposedException)
            OnProcessException(MessageLevel.Info, new InvalidOperationException($"Data subscriber encountered an exception while receiving UDP data from publisher connection: {ex.Message}", ex));
    }

    #endregion

#endregion

    #region [ Static ]

    // Static Properties
    private static readonly ServerCommand[] s_userCommands =
    [
        ServerCommand.UserCommand00,
        ServerCommand.UserCommand01,
        ServerCommand.UserCommand02,
        ServerCommand.UserCommand03,
        ServerCommand.UserCommand04,
        ServerCommand.UserCommand05,
        ServerCommand.UserCommand06,
        ServerCommand.UserCommand07,
        ServerCommand.UserCommand08,
        ServerCommand.UserCommand09,
        ServerCommand.UserCommand10,
        ServerCommand.UserCommand11,
        ServerCommand.UserCommand12,
        ServerCommand.UserCommand13,
        ServerCommand.UserCommand14,
        ServerCommand.UserCommand15
    ];

    // Static Methods

    /// <summary>
    /// Gets the path to the local certificate from the configuration file.
    /// </summary>
    /// <returns>Path to the local certificate from the configuration file.</returns>
    internal static string? GetLocalCertificate()
    {
        try
        {
            // ReSharper disable once JoinDeclarationAndInitializer
            // ReSharper disable once RedundantAssignment
            string localCertificate = null!;

#if NET
            localCertificate = ConfigSettings.Default[ConfigSettings.SystemSettingsCategory]["LocalCertificate"];
#else
            CategorizedSettingsElement localCertificateElement = ConfigurationFile.Current.Settings["systemSettings"]["LocalCertificate"];
        
            if (localCertificateElement is not null)
                localCertificate = localCertificateElement.Value;
#endif

            if (localCertificate is null || !File.Exists(FilePath.GetAbsolutePath(localCertificate)))
                throw new InvalidOperationException("Unable to find local certificate. Local certificate file must exist when using TLS security mode.");

            return localCertificate;
        }
        catch (Exception ex)
        {
            Logger.SwallowException(ex, nameof(GetLocalCertificate));
            return null;
        }
    }

    /// <summary>
    /// Checks if the specified certificate exists, updating path if needed.
    /// </summary>
    /// <param name="remoteCertificate">Reference certificate.</param>
    /// <returns><c>true</c> if certificate exists; otherwise, <c>false</c>.</returns>
    internal static bool RemoteCertificateExists(ref string remoteCertificate)
    {
        try
        {
            if (File.Exists(FilePath.GetAbsolutePath(remoteCertificate)))
                return true;

#if NET
            string remoteCertificatePath = ConfigSettings.Default[ConfigSettings.SystemSettingsCategory]["RemoteCertificatesPath"];

            if (string.IsNullOrWhiteSpace(remoteCertificatePath))
                return false;
#else
            CategorizedSettingsElement remoteCertificateElement = ConfigurationFile.Current.Settings["systemSettings"]["RemoteCertificatesPath"];

            if (remoteCertificateElement is null)
                return false;

            string remoteCertificatePath = remoteCertificateElement.Value;
#endif

            remoteCertificate = Path.Combine(remoteCertificatePath, remoteCertificate);

            return File.Exists(FilePath.GetAbsolutePath(remoteCertificate));
        }
        catch (Exception ex)
        {
            Logger.SwallowException(ex, nameof(RemoteCertificateExists));
            return false;
        }
    }

    #endregion
}