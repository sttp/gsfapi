//******************************************************************************************************
//  UnsynchronizedClientSubscription.cs - Gbtc
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
//  06/24/2011 - Ritchie
//       Generated original version of source code.
//  12/20/2012 - Starlynn Danyelle Gilliam
//       Modified Header.
//
//******************************************************************************************************
// ReSharper disable ArrangeObjectCreationWhenTypeNotEvident
// ReSharper disable PossibleMultipleEnumeration

using sttp.tssc;

namespace sttp;

/// <summary>
/// Represents an unsynchronized client subscription to the <see cref="DataPublisher" />.
/// </summary>
internal class SubscriberAdapter : FacileActionAdapterBase, IClientSubscription
{
    #region [ Members ]

    // Nested Types
    private class Concentrator : ConcentratorBase
    {
        // Delegate to process time-sorted measurements
        public required Action<ICollection<IMeasurement>> ProcessMeasurements { get; init; }

        protected override void PublishFrame(IFrame frame, int index)
        {
            ProcessMeasurements(frame.Measurements.Values);
        }
    }

    // Events

    /// <summary>
    /// Indicates that a buffer block needed to be retransmitted because
    /// it was previously sent, but no confirmation was received.
    /// </summary>
    public event EventHandler? BufferBlockRetransmission;

    /// <summary>
    /// Indicates to the host that processing for an input adapter (via temporal session) has completed.
    /// </summary>
    /// <remarks>
    /// This event is expected to only be raised when an input adapter has been designed to process
    /// a finite amount of data, e.g., reading a historical range of data during temporal processing.
    /// </remarks>
    public event EventHandler<EventArgs<IClientSubscription, EventArgs>>? ProcessingComplete;

    // Fields
    private readonly DataPublisher m_parent;
    private readonly SubscriberConnection m_connection;
    private Concentrator? m_concentrator;
    private volatile bool m_usePayloadCompression;
    private volatile bool m_useCompactMeasurementFormat;
    private readonly CompressionModes m_compressionModes;
    private bool m_resetTsscEncoder;
    private TsscEncoder? m_tsscEncoder;
    private readonly Lock m_tsscSyncLock;
    private byte[] m_tsscWorkingBuffer = null!;
    private ushort m_tsscSequenceNumber;
    private long m_lastPublishTime;
    private double m_publishInterval;
    private bool m_includeTime;
    private bool m_useMillisecondResolution;
    private bool m_isNaNFiltered;
    private volatile long[]? m_baseTimeOffsets;
    private volatile int m_timeIndex;
    private SharedTimer? m_baseTimeRotationTimer;
    private volatile bool m_startTimeSent;
    private IaonSession? m_iaonSession;

    private readonly List<byte[]?> m_bufferBlockCache;
    private readonly Lock m_bufferBlockCacheLock;
    private uint m_bufferBlockSequenceNumber;
    private uint m_expectedBufferBlockConfirmationNumber;
    private SharedTimer m_bufferBlockRetransmissionTimer = null!;
    private double m_bufferBlockRetransmissionTimeout;

    private bool m_disposed;

#endregion

    #region [ Constructors ]

    /// <summary>
    /// Creates a new <see cref="SubscriberAdapter"/>.
    /// </summary>
    /// <param name="parent">Reference to parent.</param>
    /// <param name="clientID"><see cref="Guid"/> based client connection ID.</param>
    /// <param name="subscriberID"><see cref="Guid"/> based subscriber ID.</param>
    /// <param name="compressionModes"><see cref="CompressionModes"/> requested by client.</param>
    public SubscriberAdapter(DataPublisher parent, Guid clientID, Guid subscriberID, CompressionModes compressionModes)
    {
        m_parent = parent;
        ClientID = clientID;
        SubscriberID = subscriberID;
        m_compressionModes = compressionModes;
        m_bufferBlockCache = [];
        m_bufferBlockCacheLock = new();
        m_tsscSyncLock = new();
        m_parent.ClientConnections.TryGetValue(ClientID, out SubscriberConnection? connection);
        m_connection = connection ?? throw new NullReferenceException("Subscriber adapter failed to find associated connection");
        m_connection.SignalIndexCache = new SignalIndexCache { SubscriberID = subscriberID };
    }

    #endregion

    #region [ Properties ]

    /// <summary>
    /// Gets name of the action adapter.
    /// </summary>
    public override string Name
    {
        get => base.Name;
        set
        {
            base.Name = value;
            Log.InitialStackMessages = Log.InitialStackMessages.Union("AdapterName", GetType().Name, "HostName", value);
        }
    }

    /// <summary>
    /// Gets the <see cref="Guid"/> client TCP connection identifier of this <see cref="SubscriberAdapter"/>.
    /// </summary>
    public Guid ClientID { get; }

    /// <summary>
    /// Gets the <see cref="Guid"/> based subscriber ID of this <see cref="SubscriberAdapter"/>.
    /// </summary>
    public Guid SubscriberID { get; }

    /// <summary>
    /// Gets the input filter requested by the subscriber when establishing this <see cref="SubscriberAdapter"/>.
    /// </summary>
    public string? RequestedInputFilter { get; private set; }

    /// <summary>
    /// Gets or sets flag that determines if payload compression should be enabled in data packets of this <see cref="SubscriberAdapter"/>.
    /// </summary>
    public bool UsePayloadCompression
    {
        get => m_usePayloadCompression;
        set
        {
            m_usePayloadCompression = value;

            if (m_usePayloadCompression)
                m_useCompactMeasurementFormat = true;
        }
    }

    /// <summary>
    /// Gets or sets flag that determines if the compact measurement format should be used in data packets of this <see cref="SubscriberAdapter"/>.
    /// </summary>
    public bool UseCompactMeasurementFormat
    {
        get => m_useCompactMeasurementFormat;
        set => m_useCompactMeasurementFormat = value || m_usePayloadCompression;
    }

    /// <summary>
    /// Gets size of timestamp in bytes.
    /// </summary>
    public int TimestampSize
    {
        get
        {
            if (!m_useCompactMeasurementFormat)
                return 8;

            if (!m_includeTime)
                return 0;

            if (!m_parent.UseBaseTimeOffsets)
                return 8;

            return !m_useMillisecondResolution ? 4 : 2;
        }
    }

    /// <summary>
    /// Gets or sets the desired processing interval, in milliseconds, for the adapter.
    /// </summary>
    /// <remarks>
    /// Except for the values of -1 and 0, this value specifies the desired processing interval for data, i.e.,
    /// basically a delay, or timer interval, over which to process data. A value of -1 means to use the default
    /// processing interval while a value of 0 means to process data as fast as possible.
    /// </remarks>
    public override int ProcessingInterval
    {
        get => base.ProcessingInterval;
        set
        {
            base.ProcessingInterval = value;

            // Update processing interval in private temporal session, if defined
            if (m_iaonSession?.AllAdapters is not null)
                m_iaonSession.AllAdapters.ProcessingInterval = value;
        }
    }

    /// <summary>
    /// Gets or sets primary keys of input measurements the <see cref="SubscriberAdapter"/> expects, if any.
    /// </summary>
    /// <remarks>
    /// We override method so assignment can be synchronized such that dynamic updates won't interfere
    /// with filtering in <see cref="QueueMeasurementsForProcessing"/>.
    /// </remarks>
    public override MeasurementKey[]? InputMeasurementKeys
    {
        get => base.InputMeasurementKeys;
        set
        {
            lock (this)
            {
                // Update signal index cache unless "detaching" from real-time
                if (value is not null && !(value.Length == 1 && value[0] == MeasurementKey.Undefined) && 
                    value.Length > 0 && !new HashSet<MeasurementKey>(base.InputMeasurementKeys ?? []).SetEquals(value))
                {
                    // Safe: no lock required for signal index cache here
                    Guid[] authorizedSignalIDs = m_parent.UpdateSignalIndexCache(ClientID, m_connection.SignalIndexCache, value);

                    if (DataSource is not null && m_connection.SignalIndexCache is not null)
                        value = ParseInputMeasurementKeys(DataSource, false, string.Join("; ", authorizedSignalIDs));
                }

                base.InputMeasurementKeys = value;
            }
        }
    }

    /// <summary>
    /// Gets or sets flag that determines if data should be pre-processed before publication as time-sorted.
    /// </summary>
    public bool TimeSortedPublication { get; set; }

    /// <summary>
    /// Gets the flag indicating if this adapter supports temporal processing.
    /// </summary>
    /// <remarks>
    /// Although this adapter provisions support for temporal processing by proxying historical data to a remote sink, the adapter
    /// does not need to be automatically engaged within an actual temporal <see cref="IaonSession"/>, therefore this method returns
    /// <c>false</c> to make sure the adapter doesn't get automatically instantiated within a temporal session.
    /// </remarks>
    public override bool SupportsTemporalProcessing => false;

    /// <summary>
    /// Gets a formatted message describing the status of this <see cref="SubscriberAdapter"/>.
    /// </summary>
    public override string Status
    {
        get
        {
            StringBuilder status = new();

            status.Append(m_connection.Status);
            status.AppendLine();

            status.Append(base.Status);

            if (m_iaonSession is not null)
                status.Append(m_iaonSession.Status);

            if (m_concentrator is not null)
                status.Append(m_concentrator.Status);

            return status.ToString();
        }
    }

    /// <summary>
    /// Gets the status of the active temporal session, if any.
    /// </summary>
    public string? TemporalSessionStatus => m_iaonSession?.Status;

    int IClientSubscription.CompressionStrength { get; set; } = 31;

#if NET
    object? IClientSubscription.SignalIndexCache => null;
#else
    GSF.TimeSeries.Transport.SignalIndexCache IClientSubscription.SignalIndexCache => null;
#endif

    #endregion

    #region [ Methods ]

    /// <summary>
    /// Releases the unmanaged resources used by the <see cref="SubscriberAdapter"/> object and optionally releases the managed resources.
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

            // Dispose base time rotation timer
            if (m_baseTimeRotationTimer is not null)
            {
                m_baseTimeRotationTimer.Dispose();
                m_baseTimeRotationTimer = null;
            }

            // Dispose Iaon session
            this.DisposeTemporalSession(ref m_iaonSession);
        }
        finally
        {
            m_disposed = true;          // Prevent duplicate dispose.
            base.Dispose(disposing);    // Call base class Dispose().
        }
    }

    /// <summary>
    /// Initializes <see cref="SubscriberAdapter"/>.
    /// </summary>
    public override void Initialize()
    {
        Dictionary<string, string> settings = Settings;
        MeasurementKey[] inputMeasurementKeys;

        if (settings.TryGetValue(nameof(SubscriptionInfo.FilterExpression), out string? setting))
        {
            // IMPORTANT: The allowSelect argument of ParseInputMeasurementKeys must be null
            //            in order to prevent SQL injection via the subscription filter expression
            inputMeasurementKeys = ParseInputMeasurementKeys(DataSource, false, setting);
            RequestedInputFilter = setting;
        }
        else
        {
            inputMeasurementKeys = [];
            RequestedInputFilter = null;
        }

        // IMPORTANT: We need to remove the setting before calling base.Initialize()
        //            or else we will still be subject to SQL injection
        settings.Remove(nameof(InputMeasurementKeys));

        if (settings.TryGetValue(nameof(SubscriptionInfo.Throttled), out setting))
            TrackLatestMeasurements = setting.ParseBoolean();

        base.Initialize();

        if (settings.TryGetValue(nameof(TimeSortedPublication), out setting))
            TimeSortedPublication = setting.ParseBoolean();

        if (TimeSortedPublication)
        {
            if (!settings.TryGetValue(nameof(DataPublisher.TimeSortedSamplingRate), out setting) || !int.TryParse(setting, out int samplingRate))
                throw new InvalidOperationException($"{nameof(DataPublisher.TimeSortedSamplingRate)} must be defined when {nameof(TimeSortedPublication)} is enabled.");

            if (!settings.TryGetValue(nameof(DataPublisher.TimeSortedLagTime), out setting) || !double.TryParse(setting, out double lagTime))
                lagTime = DataPublisher.DefaultTimeSortedLagTime;

            if (!settings.TryGetValue(nameof(DataPublisher.TimeSortedLeadTime), out setting) || !double.TryParse(setting, out double leadTime))
                leadTime = DataPublisher.DefaultTimeSortedLeadTime;

            m_concentrator = new Concentrator
            {
                FramesPerSecond = samplingRate,
                LagTime = lagTime,
                LeadTime = leadTime,
                ProcessMeasurements = ProcessMeasurements
            };

            m_concentrator.Start();
        }

        // Set the InputMeasurementKeys property after calling base.Initialize()
        // so that the base class does not overwrite our setting
        InputMeasurementKeys = inputMeasurementKeys;

        if (!settings.TryGetValue(nameof(SubscriptionInfo.PublishInterval), out setting) || !double.TryParse(setting, out m_publishInterval))
            m_publishInterval = -1;

        m_includeTime = !settings.TryGetValue(nameof(SubscriptionInfo.IncludeTime), out setting) || setting.ParseBoolean();
        m_useMillisecondResolution = settings.TryGetValue(nameof(SubscriptionInfo.UseMillisecondResolution), out setting) && setting.ParseBoolean();

        if (settings.TryGetValue(nameof(SubscriptionInfo.RequestNaNValueFilter), out setting))
            m_isNaNFiltered = m_parent.AllowNaNValueFilter && setting.ParseBoolean();
        else
            m_isNaNFiltered = false;

        m_bufferBlockRetransmissionTimeout = settings.TryGetValue("bufferBlockRetransmissionTimeout", out setting) ? double.Parse(setting) : 5.0D;

        if (m_parent.UseBaseTimeOffsets && m_includeTime)
        {
            m_baseTimeRotationTimer = Common.TimerScheduler.CreateTimer(m_useMillisecondResolution ? 60000 : 420000);
            m_baseTimeRotationTimer.AutoReset = true;
            m_baseTimeRotationTimer.Elapsed += BaseTimeRotationTimer_Elapsed;
        }

        m_bufferBlockRetransmissionTimer = Common.TimerScheduler.CreateTimer((int)(m_bufferBlockRetransmissionTimeout * 1000.0D));
        m_bufferBlockRetransmissionTimer.AutoReset = false;
        m_bufferBlockRetransmissionTimer.Elapsed += BufferBlockRetransmissionTimer_Elapsed;

        // Handle temporal session initialization
        if (this.TemporalConstraintIsDefined())
            m_iaonSession = this.CreateTemporalSession();
    }

    /// <summary>
    /// Starts the <see cref="SubscriberAdapter"/> or restarts it if it is already running.
    /// </summary>
    public override void Start()
    {
        if (!Enabled)
            m_startTimeSent = false;

        // Reset compressor on successful re-subscription
        if (m_connection.Version == 1)
        {
            lock (m_tsscSyncLock)
                m_resetTsscEncoder = true;
        }

        base.Start();

        if (m_baseTimeRotationTimer is not null && m_includeTime && m_useCompactMeasurementFormat)
            m_baseTimeRotationTimer.Start();
    }

    /// <summary>
    /// Stops the <see cref="SubscriberAdapter"/>.
    /// </summary>	
    public override void Stop()
    {
        base.Stop();

        if (m_baseTimeRotationTimer is null)
            return;

        m_baseTimeRotationTimer.Stop();
        m_baseTimeOffsets = null;
    }

    /// <summary>
    /// Gets a short one-line status of this <see cref="SubscriberAdapter"/>.
    /// </summary>
    /// <param name="maxLength">Maximum number of available characters for display.</param>
    /// <returns>A short one-line summary of the current status of this <see cref="AdapterBase"/>.</returns>
    public override string GetShortStatus(int maxLength)
    {
        int inputCount = 0, outputCount = 0;

        if (InputMeasurementKeys is not null)
            inputCount = InputMeasurementKeys.Length;

        if (OutputMeasurements is not null)
            outputCount = OutputMeasurements.Length;

        return $"Total input measurements: {inputCount:N0}, total output measurements: {outputCount:N0}".PadLeft(maxLength);
    }

    /// <summary>
    /// Queues a collection of measurements for processing.
    /// </summary>
    /// <param name="measurements">Collection of measurements to queue for processing.</param>
    /// <remarks>
    /// Measurements are filtered against the defined <see cref="InputMeasurementKeys"/> so we override method
    /// so that dynamic updates to keys will be synchronized with filtering to prevent interference.
    /// </remarks>
    // IMPORTANT: TSSC is sensitive to order - always make sure this function gets called sequentially, concurrent
    // calls to this function can cause TSSC parsing to get out of sequence and /fail
    public override void QueueMeasurementsForProcessing(IEnumerable<IMeasurement>? measurements)
    {
        if (measurements is null)
            return;

        if (!m_startTimeSent && measurements.Any())
        {
            m_startTimeSent = true;

            IMeasurement? measurement = measurements.FirstOrDefault();
            Ticks timestamp = 0;

            if (measurement is not null)
                timestamp = measurement.Timestamp;

            m_parent.SendDataStartTime(ClientID, timestamp);
        }

        if (m_isNaNFiltered)
            measurements = measurements.Where(measurement => !double.IsNaN(measurement.Value));

        if (!measurements.Any() || !Enabled)
            return;

        if (TrackLatestMeasurements)
        {
            // Keep track of latest measurements
            base.QueueMeasurementsForProcessing(measurements);

            double publishInterval = m_publishInterval > 0 ? m_publishInterval : LagTime;

            if (DateTime.UtcNow.Ticks <= m_lastPublishTime + Ticks.FromSeconds(publishInterval))
                return;

            List<IMeasurement> currentMeasurements = [];

            // Create a new set of measurements that represent the latest known values setting value to NaN if it is old
            foreach (TemporalMeasurement measurement in LatestMeasurements)
            {
                MeasurementStateFlags timeQuality = measurement.Timestamp.TimeIsValid(RealTime, measurement.LagTime, measurement.LeadTime)
                    ? MeasurementStateFlags.Normal
                    : MeasurementStateFlags.BadTime;

                Measurement newMeasurement = new()
                {
                    Metadata = measurement.Metadata,
                    Value = measurement.Value,
                    Timestamp = measurement.Timestamp,
                    StateFlags = measurement.StateFlags | timeQuality
                };

                currentMeasurements.Add(newMeasurement);
            }

            ProcessMeasurements(currentMeasurements);
        }
        else
        {
            if (TimeSortedPublication)
                m_concentrator!.SortMeasurements(measurements);
            else
                ProcessMeasurements(measurements);
        }
    }

    /// <summary>
    /// Handles the confirmation message received from the
    /// subscriber to indicate that a buffer block was received.
    /// </summary>
    /// <param name="sequenceNumber">The sequence number of the buffer block.</param>
    /// <returns>A list of buffer block sequence numbers for blocks that need to be retransmitted.</returns>
    public void ConfirmBufferBlock(uint sequenceNumber)
    {
        // We are still receiving confirmations,
        // so stop the retransmission timer
        m_bufferBlockRetransmissionTimer.Stop();

        lock (m_bufferBlockCacheLock)
        {
            // Find the buffer block's location in the cache
            int sequenceIndex = (int)(sequenceNumber - m_expectedBufferBlockConfirmationNumber);

            if (sequenceIndex >= 0 && sequenceIndex < m_bufferBlockCache.Count && m_bufferBlockCache[sequenceIndex] is not null)
            {
                // Remove the confirmed block from the cache
                m_bufferBlockCache[sequenceIndex] = null;

                if (sequenceNumber == m_expectedBufferBlockConfirmationNumber)
                {
                    // Get the number of elements to trim from the start of the cache
                    int removalCount = m_bufferBlockCache.TakeWhile(m => m is null).Count();

                    // Trim the cache
                    m_bufferBlockCache.RemoveRange(0, removalCount);

                    // Increase the expected confirmation number
                    m_expectedBufferBlockConfirmationNumber += (uint)removalCount;
                }
                else
                {
                    // Retransmit if confirmations are received out of order
                    for (int i = 0; i < sequenceIndex; i++)
                    {
                        if (m_bufferBlockCache[i] is null)
                            continue;

                        m_parent?.SendClientResponse(ClientID, ServerResponse.BufferBlock, ServerCommand.Subscribe, m_bufferBlockCache[i]);
                        OnBufferBlockRetransmission();
                    }
                }
            }

            // If there are any objects lingering in the
            // cache, start the retransmission timer
            if (m_bufferBlockCache.Count > 0)
                m_bufferBlockRetransmissionTimer.Start();
        }
    }

    public void ConfirmSignalIndexCache(Guid clientID)
    {
        lock (m_connection.CacheUpdateLock)
        {
            // Swap over to next signal index cache
            if (m_connection.NextSignalIndexCache is not null)
            {
                OnStatusMessage(MessageLevel.Info, $"Received confirmation of signal index cache update for subscriber {clientID}. Transitioning from cache index {m_connection.CurrentCacheIndex} with {m_connection.SignalIndexCache?.Reference.Count ?? 0:N0} records to cache index {m_connection.NextCacheIndex} with {m_connection.NextSignalIndexCache?.Reference.Count ?? 0:N0} records...", nameof(ConfirmSignalIndexCache));

                m_connection.SignalIndexCache = m_connection.NextSignalIndexCache;
                m_connection.SignalIndexCache!.SubscriberID = SubscriberID;
                m_connection.CurrentCacheIndex = m_connection.NextCacheIndex;
                m_connection.NextSignalIndexCache = null;

                lock (m_tsscSyncLock)
                {
                    m_resetTsscEncoder = true;
                }
            }
        }

        // Check for any pending signal index cache update
        ThreadPool.QueueUserWorkItem(_ =>
        {
            try
            {
                Guid[] authorizedSignalIDs;

                lock (m_connection.PendingCacheUpdateLock)
                {
                    if (m_connection.PendingSignalIndexCache is null)
                        return;

                    SignalIndexCache nextSignalIndexCache = m_connection.PendingSignalIndexCache;
                    m_connection.PendingSignalIndexCache = null;

                    OnStatusMessage(MessageLevel.Info, $"Applying pending signal cache update for subscriber {clientID} with {nextSignalIndexCache.Reference.Count:N0} records...", nameof(ConfirmSignalIndexCache));
                    authorizedSignalIDs = m_parent.UpdateSignalIndexCache(ClientID, nextSignalIndexCache, InputMeasurementKeys);
                }

                if (DataSource is not null)
                    base.InputMeasurementKeys = ParseInputMeasurementKeys(DataSource, false, string.Join("; ", authorizedSignalIDs));
            }
            catch (Exception ex)
            {
                Logger.SwallowException(ex);
            }
        });
    }

    private void ProcessMeasurements(IEnumerable<IMeasurement> measurements)
    {
        if (m_usePayloadCompression && m_compressionModes.HasFlag(CompressionModes.TSSC))
        {
            ProcessTSSCMeasurements(measurements);
            return;
        }

        // Includes data packet flags and measurement count
        const int PacketHeaderSize = DataPublisher.ClientResponseHeaderSize + 5;

        try
        {
            if (!Enabled)
                return;

            List<IBinaryMeasurement> packet = [];
            int packetSize = PacketHeaderSize;

            //usePayloadCompression = m_usePayloadCompression;
            bool useCompactMeasurementFormat = m_useCompactMeasurementFormat;
            SignalIndexCache? signalIndexCache;
            int currentCacheIndex;

            lock (m_connection.CacheUpdateLock)
            {
                signalIndexCache = m_connection.SignalIndexCache;
                currentCacheIndex = m_connection.CurrentCacheIndex;
            }

            if (signalIndexCache is null)
                return;

            foreach (IMeasurement measurement in measurements)
            {
                if (measurement is BufferBlockMeasurement { Buffer: not null } bufferBlockMeasurement)
                {
                    // Still sending buffer block measurements to client; we are expecting
                    // confirmations which will indicate whether retransmission is necessary,
                    // so we will restart the retransmission timer
                    m_bufferBlockRetransmissionTimer.Stop();

                    // Handle buffer block measurements as a special case - this can be any kind of data,
                    // measurement subscriber will need to know how to interpret buffer
                    byte[] bufferBlock = new byte[6 + bufferBlockMeasurement.Length + (m_connection.Version > 1 ? 4 : 0)];
                    int index = 0;

                    // Prepend sequence number
                    index += BigEndian.CopyBytes(m_bufferBlockSequenceNumber, bufferBlock, index);
                    m_bufferBlockSequenceNumber++;

                    if (m_connection.Version > 1)
                        bufferBlock[index++] = (byte)currentCacheIndex;

                    // Copy signal index into buffer
                    int bufferBlockSignalIndex = signalIndexCache.GetSignalIndex(bufferBlockMeasurement.Key);
                    index += BigEndian.CopyBytes(bufferBlockSignalIndex, bufferBlock, index);

                    // Append measurement data and send
                    Buffer.BlockCopy(bufferBlockMeasurement.Buffer, 0, bufferBlock, index, bufferBlockMeasurement.Length);
                    m_parent.SendClientResponse(ClientID, ServerResponse.BufferBlock, ServerCommand.Subscribe, bufferBlock);

                    lock (m_bufferBlockCacheLock)
                    {
                        // Cache buffer block for retransmission
                        m_bufferBlockCache.Add(bufferBlock);
                    }

                    // Start the retransmission timer in case we never receive a confirmation
                    m_bufferBlockRetransmissionTimer.Start();
                }
                else
                {
                    // Serialize the current measurement.
                    IBinaryMeasurement binaryMeasurement = useCompactMeasurementFormat ?
                        new CompactMeasurement(measurement, signalIndexCache, m_includeTime, m_baseTimeOffsets, m_timeIndex, m_useMillisecondResolution) :
                        throw new InvalidOperationException("Full measurement serialization not supported.");

                    // Determine the size of the measurement in bytes.
                    int binaryLength = binaryMeasurement.BinaryLength;

                    // If the current measurement will not fit in the packet based on the max
                    // packet size, process the current packet and start a new packet.
                    if (packetSize + binaryLength > m_parent.MaxPacketSize)
                    {
                        ProcessBinaryMeasurements(packet, useCompactMeasurementFormat, currentCacheIndex);
                        packet.Clear();
                        packetSize = PacketHeaderSize;
                    }

                    // Add the current measurement to the packet.
                    packet.Add(binaryMeasurement);
                    packetSize += binaryLength;
                }
            }

            // Process the remaining measurements.
            if (packet.Count > 0)
                ProcessBinaryMeasurements(packet, useCompactMeasurementFormat, currentCacheIndex);

            IncrementProcessedMeasurements(measurements.Count());

            // Update latency statistics
            m_parent.UpdateLatencyStatistics(measurements.Select(m => (long)(m_lastPublishTime - m.Timestamp)));
        }
        catch (Exception ex)
        {
            string message = $"Error processing measurements: {ex.Message}";
            OnProcessException(MessageLevel.Info, new InvalidOperationException(message, ex));
        }
    }

    private void ProcessBinaryMeasurements(IEnumerable<IBinaryMeasurement> measurements, bool useCompactMeasurementFormat, int currentCacheIndex)
    {
        // Create working buffer
        using BlockAllocatedMemoryStream workingBuffer = new();

        // Serialize data packet flags into response
        DataPacketFlags flags = DataPacketFlags.NoFlags; // No flags means bit is cleared, i.e., unsynchronized

        if (useCompactMeasurementFormat)
            flags |= DataPacketFlags.Compact;

        if (currentCacheIndex > 0)
            flags |= DataPacketFlags.CacheIndex;

        workingBuffer.WriteByte((byte)flags);

        // No frame level timestamp is serialized into the data packet since all data is unsynchronized and essentially
        // published upon receipt, however timestamps are optionally included in the serialized measurements.

        // Serialize total number of measurement values to follow
        workingBuffer.Write(BigEndian.GetBytes(measurements.Count()), 0, 4);

        // Serialize measurements to data buffer
        foreach (IBinaryMeasurement measurement in measurements)
            measurement.CopyBinaryImageToStream(workingBuffer);

        // Publish data packet to client
        m_parent.SendClientResponse(ClientID, ServerResponse.DataPacket, ServerCommand.Subscribe, workingBuffer.ToArray());

        // Track last publication time
        m_lastPublishTime = DateTime.UtcNow.Ticks;
    }

    private void ProcessTSSCMeasurements(IEnumerable<IMeasurement> measurements)
    {
        SignalIndexCache? signalIndexCache;
        int currentCacheIndex;

        lock (m_connection.CacheUpdateLock)
        {
            signalIndexCache = m_connection.SignalIndexCache;
            currentCacheIndex = m_connection.CurrentCacheIndex;

            // Cache not available while initializing
            if (signalIndexCache is null || signalIndexCache.Reference.Count == 0)
                return;
        }

        lock (m_tsscSyncLock)
        {
            try
            {
                if (!Enabled)
                    return;

                if (m_tsscEncoder is null || m_resetTsscEncoder)
                {
                    m_resetTsscEncoder = false;
                    m_tsscEncoder = new TsscEncoder();
                    m_tsscWorkingBuffer = new byte[32 * 1024];

                    OnStatusMessage(MessageLevel.Info, $"TSSC algorithm reset before sequence number: {m_tsscSequenceNumber}", "TSSC");
                    m_tsscSequenceNumber = 0;
                }

                m_tsscEncoder.SetBuffer(m_tsscWorkingBuffer, 0, m_tsscWorkingBuffer.Length);

                int count = 0;

                foreach (IMeasurement measurement in measurements)
                {
                    int index = signalIndexCache.GetSignalIndex(measurement.Key);

                    if (index == int.MaxValue) 
                        continue; // Ignore unmapped signal
                    
                    if (!m_tsscEncoder.TryAddMeasurement(index, measurement.Timestamp.Value, (uint)measurement.StateFlags, (float)measurement.AdjustedValue))
                    {
                        SendTSSCPayload(count, currentCacheIndex);
                        count = 0;
                        m_tsscEncoder.SetBuffer(m_tsscWorkingBuffer, 0, m_tsscWorkingBuffer.Length);

                        // This will always succeed
                        m_tsscEncoder.TryAddMeasurement(index, measurement.Timestamp.Value, (uint)measurement.StateFlags, (float)measurement.AdjustedValue);
                    }

                    count++;
                }

                if (count > 0)
                    SendTSSCPayload(count, currentCacheIndex);

                IncrementProcessedMeasurements(measurements.Count());

                // Update latency statistics
                m_parent.UpdateLatencyStatistics(measurements.Select(m => (long)(m_lastPublishTime - m.Timestamp)));
            }
            catch (Exception ex)
            {
                string message = $"Error processing measurements: {ex.Message}";
                OnProcessException(MessageLevel.Info, new InvalidOperationException(message, ex));
            }
        }
    }

    private void SendTSSCPayload(int count, int currentCacheIndex)
    {
        if (m_tsscEncoder is null)
            return;

        int length = m_tsscEncoder.FinishBlock();
        byte[] packet = new byte[length + 8];
        DataPacketFlags flags = DataPacketFlags.Compressed;

        if (currentCacheIndex > 0)
            flags |= DataPacketFlags.CacheIndex;

        packet[0] = (byte)flags;

        // Serialize total number of measurement values to follow
        BigEndian.CopyBytes(count, packet, 1);

        packet[1 + 4] = 85; // A version number
            
        BigEndian.CopyBytes(m_tsscSequenceNumber, packet, 5 + 1);

        unchecked { m_tsscSequenceNumber++; }

        if (m_tsscSequenceNumber == 0)
            m_tsscSequenceNumber = 1; // Do not increment to 0 if rolled over

        Array.Copy(m_tsscWorkingBuffer, 0, packet, 8, length);

        m_parent.SendClientResponse(ClientID, ServerResponse.DataPacket, ServerCommand.Subscribe, packet);

        // Track last publication time
        m_lastPublishTime = DateTime.UtcNow.Ticks;
    }

    // Retransmits all buffer blocks for which confirmation has not yet been received
    private void BufferBlockRetransmissionTimer_Elapsed(object? sender, EventArgs<DateTime> e)
    {
        lock (m_bufferBlockCacheLock)
        {
            foreach (byte[]? bufferBlock in m_bufferBlockCache)
            {
                if (bufferBlock is null)
                    continue;

                m_parent.SendClientResponse(ClientID, ServerResponse.BufferBlock, ServerCommand.Subscribe, bufferBlock);
                OnBufferBlockRetransmission();
            }
        }

        // Restart the retransmission timer
        m_bufferBlockRetransmissionTimer.Start();
    }

    // Rotates base time offsets
    private void RotateBaseTimes()
    {
        if (m_baseTimeRotationTimer == null)
            return;

        if (m_baseTimeOffsets is null)
        {
            m_baseTimeOffsets = new long[2];
            m_baseTimeOffsets[0] = RealTime;
            m_baseTimeOffsets[1] = RealTime + m_baseTimeRotationTimer.Interval * Ticks.PerMillisecond;
            m_timeIndex = 0;
        }
        else
        {
            int oldIndex = m_timeIndex;

            // Switch to newer timestamp
            m_timeIndex = oldIndex ^ 1;

            // Now make older timestamp the newer timestamp
            m_baseTimeOffsets[oldIndex] = RealTime + m_baseTimeRotationTimer.Interval * Ticks.PerMillisecond;
        }

        // Since this function will only be called periodically, there is no real benefit
        // to maintaining this memory stream at a member level
        using MemoryStream responsePacket = new();

        responsePacket.Write(BigEndian.GetBytes(m_timeIndex), 0, 4);
        responsePacket.Write(BigEndian.GetBytes(m_baseTimeOffsets[0]), 0, 8);
        responsePacket.Write(BigEndian.GetBytes(m_baseTimeOffsets[1]), 0, 8);

        m_parent.SendClientResponse(ClientID, ServerResponse.UpdateBaseTimes, ServerCommand.Subscribe, responsePacket.ToArray());
    }

    private void OnBufferBlockRetransmission()
    {
        BufferBlockRetransmission?.SafeInvoke(this, EventArgs.Empty);
    }

    private void BaseTimeRotationTimer_Elapsed(object? sender, EventArgs<DateTime> e)
    {
        RotateBaseTimes();
    }

    void IClientSubscription.OnStatusMessage(MessageLevel level, string status, string? eventName, MessageFlags flags)
    {
        OnStatusMessage(level, status, eventName, flags);
    }

    void IClientSubscription.OnProcessException(MessageLevel level, Exception ex, string? eventName, MessageFlags flags)
    {
        OnProcessException(level, ex, eventName, flags);
    }

    // Explicitly implement processing completed event bubbler to satisfy IClientSubscription interface
    void IClientSubscription.OnProcessingCompleted(object? sender, EventArgs e)
    {
        ProcessingComplete?.SafeInvoke(sender, new EventArgs<IClientSubscription, EventArgs>(this, e));
    }

    #endregion
}