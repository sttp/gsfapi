//******************************************************************************************************
//  DataPublisher.cs - Gbtc
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
//  11/15/2010 - Mehulbhai P Thakker
//       Fixed issue when DataSubscriber tries to resubscribe by setting subscriber. Initialized
//       manually in ReceiveClientDataComplete event handler.
//  12/02/2010 - J. Ritchie Carroll
//       Fixed an issue for when DataSubcriber dynamically resubscribes with a different
//       synchronization method (e.g., going from unsynchronized to synchronized)
//  05/26/2011 - J. Ritchie Carroll
//       Implemented subscriber authentication model.
//  02/07/2012 - Mehulbhai Thakkar
//       Modified m_metadataTables to include filter expression and made the list ";" separated.
//  12/20/2012 - Starlynn Danyelle Gilliam
//       Modified Header.
//
//******************************************************************************************************

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Xml;
using GSF;
using GSF.Communication;
using GSF.Configuration;
using GSF.Data;
using GSF.Diagnostics;
using GSF.IO;
using GSF.Net.Security;
using GSF.Security.Cryptography;
using GSF.Threading;
using GSF.TimeSeries;
using GSF.TimeSeries.Adapters;
using GSF.TimeSeries.Statistics;
using GSF.TimeSeries.Transport;
using GSF.Units;
using TcpClient = GSF.Communication.TcpClient;

namespace sttp
{
    #region [ Enumerations ]

    /// <summary>
    /// Security modes used by the <see cref="DataPublisher"/> to secure data sent over the command channel.
    /// </summary>
    public enum SecurityMode
    {
        /// <summary>
        /// No security.
        /// </summary>
        None,

        /// <summary>
        /// Transport Layer Security.
        /// </summary>
        TLS
    }

    /// <summary>
    /// Server commands received by <see cref="DataPublisher"/> and sent by <see cref="DataSubscriber"/>.
    /// </summary>
    /// <remarks>
    /// Solicited server commands will receive a <see cref="ServerResponse.Succeeded"/> or <see cref="ServerResponse.Failed"/>
    /// response code along with an associated success or failure message. Message type for successful responses will be based
    /// on server command - for example, server response for a successful MetaDataRefresh command will return a serialized
    /// <see cref="DataSet"/> of the available server metadata. Message type for failed responses will always be a string of
    /// text representing the error message.
    /// </remarks>
    public enum ServerCommand : byte
    {
        /// <summary>
        /// Meta data refresh command.
        /// </summary>
        /// <remarks>
        /// Requests that server send an updated set of metadata so client can refresh its point list.
        /// Successful return message type will be a <see cref="DataSet"/> containing server device and measurement metadata.
        /// Received device list should be defined as children of the "parent" server device connection similar to the way
        /// PMUs are defined as children of a parent PDC device connection.
        /// Devices and measurements contain unique Guids that should be used to key metadata updates in local repository.
        /// Optional string based message can follow command that should represent client requested meta-data filtering
        /// expressions, e.g.: "FILTER MeasurementDetail WHERE SignalType &lt;&gt; 'STAT'"
        /// </remarks>
        MetaDataRefresh = 0x01,

        /// <summary>
        /// Subscribe command.
        /// </summary>
        /// <remarks>
        /// Requests a subscription of streaming data from server based on connection string that follows.
        /// It will not be necessary to stop an existing subscription before requesting a new one.
        /// Successful return message type will be string indicating total number of allowed points.
        /// Client should wait for UpdateSignalIndexCache and UpdateBaseTime response codes before attempting
        /// to parse data when using the compact measurement format.
        /// </remarks>
        Subscribe = 0x02,

        /// <summary>
        /// Unsubscribe command.
        /// </summary>
        /// <remarks>
        /// Requests that server stop sending streaming data to the client and cancel the current subscription.
        /// </remarks>
        Unsubscribe = 0x03,

        /// <summary>
        /// Rotate cipher keys.
        /// </summary>
        /// <remarks>
        /// Manually requests that server send a new set of cipher keys for data packet encryption.
        /// </remarks>
        RotateCipherKeys = 0x04,

        /// <summary>
        /// Update processing interval.
        /// </summary>
        /// <remarks>
        /// Manually requests server to update the processing interval with the following specified value.
        /// </remarks>
        UpdateProcessingInterval = 0x05,

        /// <summary>
        /// Define operational modes for subscriber connection.
        /// </summary>
        /// <remarks>
        /// As soon as connection is established, requests that server set operational
        /// modes that affect how the subscriber and publisher will communicate.
        /// </remarks>
        DefineOperationalModes = 0x06,

        /// <summary>
        /// Confirm receipt of a notification.
        /// </summary>
        /// <remarks>
        /// This message is sent in response to <see cref="ServerResponse.Notify"/>.
        /// </remarks>
        ConfirmNotification = 0x07,

        /// <summary>
        /// Confirm receipt of a buffer block measurement.
        /// </summary>
        /// <remarks>
        /// This message is sent in response to <see cref="ServerResponse.BufferBlock"/>.
        /// </remarks>
        ConfirmBufferBlock = 0x08,

        /// <summary>
        /// Provides measurements to the publisher over the command channel.
        /// </summary>
        /// <remarks>
        /// Allows for unsolicited publication of measurement data to the server
        /// so that consumers of data can also provide data to other consumers.
        /// </remarks>
        PublishCommandMeasurements = 0x09,

        /// <summary>
        /// Code for handling user-defined commands.
        /// </summary>
        UserCommand00 = 0xD0,

        /// <summary>
        /// Code for handling user-defined commands.
        /// </summary>
        UserCommand01 = 0xD1,

        /// <summary>
        /// Code for handling user-defined commands.
        /// </summary>
        UserCommand02 = 0xD2,

        /// <summary>
        /// Code for handling user-defined commands.
        /// </summary>
        UserCommand03 = 0xD3,

        /// <summary>
        /// Code for handling user-defined commands.
        /// </summary>
        UserCommand04 = 0xD4,

        /// <summary>
        /// Code for handling user-defined commands.
        /// </summary>
        UserCommand05 = 0xD5,

        /// <summary>
        /// Code for handling user-defined commands.
        /// </summary>
        UserCommand06 = 0xD6,

        /// <summary>
        /// Code for handling user-defined commands.
        /// </summary>
        UserCommand07 = 0xD7,

        /// <summary>
        /// Code for handling user-defined commands.
        /// </summary>
        UserCommand08 = 0xD8,

        /// <summary>
        /// Code for handling user-defined commands.
        /// </summary>
        UserCommand09 = 0xD9,

        /// <summary>
        /// Code for handling user-defined commands.
        /// </summary>
        UserCommand10 = 0xDA,

        /// <summary>
        /// Code for handling user-defined commands.
        /// </summary>
        UserCommand11 = 0xDB,

        /// <summary>
        /// Code for handling user-defined commands.
        /// </summary>
        UserCommand12 = 0xDC,

        /// <summary>
        /// Code for handling user-defined commands.
        /// </summary>
        UserCommand13 = 0xDD,

        /// <summary>
        /// Code for handling user-defined commands.
        /// </summary>
        UserCommand14 = 0xDE,

        /// <summary>
        /// Code for handling user-defined commands.
        /// </summary>
        UserCommand15 = 0xDF
    }

    /// <summary>
    /// Server responses sent by <see cref="DataPublisher"/> and received by <see cref="DataSubscriber"/>.
    /// </summary>
    public enum ServerResponse : byte
    {
        // Although the server commands and responses will be on two different paths, the response enumeration values
        // are defined as distinct from the command values to make it easier to identify codes from a wire analysis.

        /// <summary>
        /// Command succeeded response.
        /// </summary>
        /// <remarks>
        /// Informs client that its solicited server command succeeded, original command and success message follow.
        /// </remarks>
        Succeeded = 0x80,

        /// <summary>
        /// Command failed response.
        /// </summary>
        /// <remarks>
        /// Informs client that its solicited server command failed, original command and failure message follow.
        /// </remarks>
        Failed = 0x81,

        /// <summary>
        /// Data packet response.
        /// </summary>
        /// <remarks>
        /// Unsolicited response informs client that a data packet follows.
        /// </remarks>
        DataPacket = 0x82,

        /// <summary>
        /// Update signal index cache response.
        /// </summary>
        /// <remarks>
        /// Unsolicited response requests that client update its runtime signal index cache with the one that follows.
        /// </remarks>
        UpdateSignalIndexCache = 0x83,

        /// <summary>
        /// Update runtime base-timestamp offsets response.
        /// </summary>
        /// <remarks>
        /// Unsolicited response requests that client update its runtime base-timestamp offsets with those that follow.
        /// </remarks>
        UpdateBaseTimes = 0x84,

        /// <summary>
        /// Update runtime cipher keys response.
        /// </summary>
        /// <remarks>
        /// Response, solicited or unsolicited, requests that client update its runtime data cipher keys with those that follow.
        /// </remarks>
        UpdateCipherKeys = 0x85,

        /// <summary>
        /// Data start time response packet.
        /// </summary>
        /// <remarks>
        /// Unsolicited response provides the start time of data being processed from the first measurement.
        /// </remarks>
        DataStartTime = 0x86,

        /// <summary>
        /// Processing complete notification.
        /// </summary>
        /// <remarks>
        /// Unsolicited response provides notification that input processing has completed, typically via temporal constraint.
        /// </remarks>
        ProcessingComplete = 0x87,

        /// <summary>
        /// Buffer block response.
        /// </summary>
        /// <remarks>
        /// Unsolicited response informs client that a raw buffer block follows.
        /// </remarks>
        BufferBlock = 0x88,

        /// <summary>
        /// Notify response.
        /// </summary>
        /// <remarks>
        /// Unsolicited response provides a notification message to the client.
        /// </remarks>
        Notify = 0x89,

        /// <summary>
        /// Configuration changed response.
        /// </summary>
        /// <remarks>
        /// Unsolicited response provides a notification that the publisher's source configuration has changed and that
        /// client may want to request a meta-data refresh.
        /// </remarks>
        ConfigurationChanged = 0x8A,

        /// <summary>
        /// Code for handling user-defined responses.
        /// </summary>
        UserResponse00 = 0xE0,

        /// <summary>
        /// Code for handling user-defined responses.
        /// </summary>
        UserResponse01 = 0xE1,

        /// <summary>
        /// Code for handling user-defined responses.
        /// </summary>
        UserResponse02 = 0xE2,

        /// <summary>
        /// Code for handling user-defined responses.
        /// </summary>
        UserResponse03 = 0xE3,

        /// <summary>
        /// Code for handling user-defined responses.
        /// </summary>
        UserResponse04 = 0xE4,

        /// <summary>
        /// Code for handling user-defined responses.
        /// </summary>
        UserResponse05 = 0xE5,

        /// <summary>
        /// Code for handling user-defined responses.
        /// </summary>
        UserResponse06 = 0xE6,

        /// <summary>
        /// Code for handling user-defined responses.
        /// </summary>
        UserResponse07 = 0xE7,

        /// <summary>
        /// Code for handling user-defined responses.
        /// </summary>
        UserResponse08 = 0xE8,

        /// <summary>
        /// Code for handling user-defined responses.
        /// </summary>
        UserResponse09 = 0xE9,

        /// <summary>
        /// Code for handling user-defined responses.
        /// </summary>
        UserResponse10 = 0xEA,

        /// <summary>
        /// Code for handling user-defined responses.
        /// </summary>
        UserResponse11 = 0xEB,

        /// <summary>
        /// Code for handling user-defined responses.
        /// </summary>
        UserResponse12 = 0xEC,

        /// <summary>
        /// Code for handling user-defined responses.
        /// </summary>
        UserResponse13 = 0xED,

        /// <summary>
        /// Code for handling user-defined responses.
        /// </summary>
        UserResponse14 = 0xEE,

        /// <summary>
        /// Code for handling user-defined responses.
        /// </summary>
        UserResponse15 = 0xEF,

        /// <summary>
        /// No operation keep-alive ping.
        /// </summary>
        /// <remarks>
        /// The command channel can remain quiet for some time, this command allows a period test of client connectivity.
        /// </remarks>
        NoOP = 0xFF
    }

    /// <summary>
    /// <see cref="DataPublisher"/> data packet flags.
    /// </summary>
    [Flags]
    public enum DataPacketFlags : byte
    {
        /// <summary>
        /// Determines if serialized measurement is compact.
        /// </summary>
        /// <remarks>
        /// Bit set = compact, bit clear = full fidelity.
        /// </remarks>
        Compact = (byte)Bits.Bit01,
        /// <summary>
        /// Determines which cipher index to use when encrypting data packet.
        /// </summary>
        /// <remarks>
        /// Bit set = use odd cipher index (i.e., 1), bit clear = use even cipher index (i.e., 0).
        /// </remarks>
        CipherIndex = (byte)Bits.Bit02,
        /// <summary>
        /// Determines if data packet payload is compressed.
        /// </summary>
        /// <remarks>
        /// Bit set = payload compressed, bit clear = payload normal.
        /// </remarks>
        Compressed = (byte)Bits.Bit03,
        /// <summary>
        /// No flags set.
        /// </summary>
        /// <remarks>
        /// This would represent unsynchronized, full fidelity measurement data packets.
        /// </remarks>
        NoFlags = (Byte)Bits.Nil
    }

    /// <summary>
    /// Operational modes that affect how <see cref="DataPublisher"/> and <see cref="DataSubscriber"/> communicate.
    /// </summary>
    /// <remarks>
    /// Operational modes are sent from a subscriber to a publisher to request operational behaviors for the
    /// connection, as a result the operation modes must be sent before any other command. The publisher may
    /// silently refuse some requests (e.g., compression) based on its configuration. Operational modes only
    /// apply to fundamental protocol control.
    /// </remarks>
    [Flags]
    public enum OperationalModes : uint
    {
        /// <summary>
        /// Mask to get version number of protocol.
        /// </summary>
        /// <remarks>
        /// Version number is currently set to 1.
        /// </remarks>
        VersionMask = (uint)(Bits.Bit04 | Bits.Bit03 | Bits.Bit02 | Bits.Bit01 | Bits.Bit00),
        /// <summary>
        /// Mask to get mode of compression.
        /// </summary>
        /// <remarks>
        /// GZip and TSSC compression are the only modes currently supported. Remaining bits are
        /// reserved for future compression modes.
        /// </remarks>
        CompressionModeMask = (uint)(Bits.Bit07 | Bits.Bit06 | Bits.Bit05),
        /// <summary>
        /// Mask to get character encoding used when exchanging messages between publisher and subscriber.
        /// </summary>
        /// <remarks>
        /// 00 = UTF-16, little endian<br/>
        /// 01 = UTF-16, big endian<br/>
        /// 10 = UTF-8<br/>
        /// 11 = ANSI
        /// </remarks>
        EncodingMask = (uint)(Bits.Bit09 | Bits.Bit08),
        /// <summary>
        /// Determines whether external measurements are exchanged during metadata synchronization.
        /// </summary>
        /// <remarks>
        /// Bit set = external measurements are exchanged, bit clear = no external measurements are exchanged
        /// </remarks>
        ReceiveExternalMetadata = (uint)Bits.Bit25,
        /// <summary>
        /// Determines whether internal measurements are exchanged during metadata synchronization.
        /// </summary>
        /// <remarks>
        /// Bit set = internal measurements are exchanged, bit clear = no internal measurements are exchanged
        /// </remarks>
        ReceiveInternalMetadata = (uint)Bits.Bit26,
        /// <summary>
        /// Determines whether payload data is compressed when exchanging between publisher and subscriber.
        /// </summary>
        /// <remarks>
        /// Bit set = compress, bit clear = no compression
        /// </remarks>
        CompressPayloadData = (uint)Bits.Bit29,
        /// <summary>
        /// Determines whether the signal index cache is compressed when exchanging between publisher and subscriber.
        /// </summary>
        /// <remarks>
        /// Bit set = compress, bit clear = no compression
        /// </remarks>
        CompressSignalIndexCache = (uint)Bits.Bit30,
        /// <summary>
        /// Determines whether metadata is compressed when exchanging between publisher and subscriber.
        /// </summary>
        /// <remarks>
        /// Bit set = compress, bit clear = no compression
        /// </remarks>
        CompressMetadata = (uint)Bits.Bit31,
        /// <summary>
        /// No flags set.
        /// </summary>
        /// <remarks>
        /// This would represent protocol version 0,
        /// UTF-16 little endian character encoding,
        /// .NET serialization and no compression.
        /// </remarks>
        NoFlags = (uint)Bits.Nil
    }

    /// <summary>
    /// Enumeration for character encodings supported by the Streaming Telemetry Transport Protocol.
    /// </summary>
    public enum OperationalEncoding : uint
    {
        /// <summary>
        /// UTF-16, little endian
        /// </summary>
        UTF16LE = (uint)Bits.Nil,
        /// <summary>
        /// UTF-16, big endian
        /// </summary>
        UTF16BE = (uint)Bits.Bit08,
        /// <summary>
        /// UTF-8
        /// </summary>
        UTF8 = (uint)Bits.Bit09
    }

    /// <summary>
    /// Enumeration for compression modes supported by the Streaming Telemetry Transport Protocol.
    /// </summary>
    [Flags]
    public enum CompressionModes : uint
    {
        /// <summary>
        /// GZip compression
        /// </summary>
        GZip = (uint)Bits.Bit05,
        /// <summary>
        /// TSSC compression
        /// </summary>
        TSSC = (uint)Bits.Bit06,
        /// <summary>
        /// No compression
        /// </summary>
        None = (uint)Bits.Nil
    }

    #endregion

    /// <summary>
    /// Represents a data publishing server that allows multiple connections for data subscriptions.
    /// </summary>
    [Description("STTP Publisher: server component that allows STTP-style gateway subscription connections.")]
    [EditorBrowsable(EditorBrowsableState.Always)]
    public class DataPublisher : ActionAdapterCollection, IOptimizedRoutingConsumer
    {
        #region [ Members ]

        // Nested Types

        private sealed class LatestMeasurementCache : FacileActionAdapterBase
        {
            public LatestMeasurementCache(string connectionString)
            {
                ConnectionString = connectionString;
            }

            public override DataSet DataSource
            {
                get => base.DataSource;
                set
                {
                    base.DataSource = value;

                    if (Initialized)
                        UpdateInputMeasurementKeys();
                }
            }

            public override string Name
            {
                get => "LatestMeasurementCache";
                set => base.Name = value;
            }

            public override bool SupportsTemporalProcessing => false;

            public override string GetShortStatus(int maxLength)
            {
                return "LatestMeasurementCache happily exists. :)";
            }

            private void UpdateInputMeasurementKeys()
            {
                if (Settings.TryGetValue("inputMeasurementKeys", out string inputMeasurementKeys))
                    InputMeasurementKeys = ParseInputMeasurementKeys(DataSource, true, inputMeasurementKeys);
            }
        }

        // Events

        /// <summary>
        /// Indicates that a new client has connected to the publisher.
        /// </summary>
        /// <remarks>
        /// <see cref="EventArgs{T1, T2, T3}.Argument1"/> is the <see cref="Guid"/> based subscriber ID.<br/>
        /// <see cref="EventArgs{T1, T2, T3}.Argument2"/> is the connection identification (e.g., IP and DNS name, if available).<br/>
        /// <see cref="EventArgs{T1, T2, T3}.Argument3"/> is the subscriber information as reported by the client.
        /// </remarks>
        public event EventHandler<EventArgs<Guid, string, string>> ClientConnected;

        /// <summary>
        /// Indicates to the host that processing for an input adapter (via temporal session) has completed.
        /// </summary>
        /// <remarks>
        /// This event is expected to only be raised when an input adapter has been designed to process
        /// a finite amount of data, e.g., reading a historical range of data during temporal processing.
        /// </remarks>
        public event EventHandler ProcessingComplete;

        // Constants

        /// <summary>
        /// Default value for <see cref="SecurityMode"/>.
        /// </summary>
        public const SecurityMode DefaultSecurityMode = SecurityMode.None;

        /// <summary>
        /// Default value for <see cref="EncryptPayload"/>.
        /// </summary>
        public const bool DefaultEncryptPayload = false;

        /// <summary>
        /// Default value for <see cref="SharedDatabase"/>.
        /// </summary>
        public const bool DefaultSharedDatabase = false;

        /// <summary>
        /// Default value for <see cref="AllowPayloadCompression"/>.
        /// </summary>
        public const bool DefaultAllowPayloadCompression = true;

        /// <summary>
        /// Default value for <see cref="AllowMetadataRefresh"/>.
        /// </summary>
        public const bool DefaultAllowMetadataRefresh = true;

        /// <summary>
        /// Default value for <see cref="AllowNaNValueFilter"/>.
        /// </summary>
        public const bool DefaultAllowNaNValueFilter = true;

        /// <summary>
        /// Default value for <see cref="ForceNaNValueFilter"/>.
        /// </summary>
        public const bool DefaultForceNaNValueFilter = false;

        /// <summary>
        /// Default value for <see cref="UseBaseTimeOffsets"/>.
        /// </summary>
        public const bool DefaultUseBaseTimeOffsets = true;

        /// <summary>
        /// Default value for <see cref="CipherKeyRotationPeriod"/>.
        /// </summary>
        public const double DefaultCipherKeyRotationPeriod = 60000.0D;

        /// <summary>
        /// Default value for <see cref="MetadataTables"/>.
        /// </summary>
        public const string DefaultMetadataTables =
            "SELECT NodeID, UniqueID, OriginalSource, IsConcentrator, Acronym, Name, AccessID, ParentAcronym, ProtocolName, FramesPerSecond, CompanyAcronym, VendorAcronym, VendorDeviceName, Longitude, Latitude, InterconnectionName, ContactList, Enabled, UpdatedOn FROM DeviceDetail WHERE IsConcentrator = 0;" +
            "SELECT DeviceAcronym, ID, SignalID, PointTag, SignalReference, SignalAcronym, PhasorSourceIndex, Description, Internal, Enabled, UpdatedOn FROM MeasurementDetail;" +
            "SELECT ID, DeviceAcronym, Label, Type, Phase, DestinationPhasorID, SourceIndex, BaseKV, UpdatedOn FROM PhasorDetail;" +
            "SELECT VersionNumber FROM SchemaVersion";

        /// <summary>
        /// Maximum packet size before software fragmentation of payload.
        /// </summary>
        public const int MaxPacketSize = ushort.MaxValue / 2;

        /// <summary>
        /// Size of client response header in bytes.
        /// </summary>
        /// <remarks>
        /// Header consists of response byte, in-response-to server command byte, 4-byte int representing payload length.
        /// </remarks>
        public const int ClientResponseHeaderSize = 6;

        // Length of random salt prefix
        internal const int CipherSaltLength = 8;

        // Fields
        private IServer m_serverCommandChannel;
        private IClient m_clientCommandChannel;
        private CertificatePolicyChecker m_certificateChecker;
        private Dictionary<X509Certificate, DataRow> m_subscriberIdentities;
        private ConcurrentDictionary<Guid, SubscriberConnection> m_clientConnections;
        private readonly ConcurrentDictionary<Guid, IServer> m_clientPublicationChannels;
        private readonly Dictionary<Guid, Dictionary<int, string>> m_clientNotifications;
        private readonly object m_clientNotificationsLock;
        private SharedTimer m_cipherKeyRotationTimer;
        private long m_commandChannelConnectionAttempts;
        private Guid m_proxyClientID;
        private RoutingTables m_routingTables;
        private string m_metadataTables;
        private string m_cachedMeasurementExpression;
        private SecurityMode m_securityMode;
        private bool m_encryptPayload;
        private bool m_sharedDatabase;
        private bool m_allowPayloadCompression;
        private bool m_allowMetadataRefresh;
        private bool m_allowNaNValueFilter;
        private bool m_forceNaNValueFilter;
        private bool m_useBaseTimeOffsets;
        private int m_measurementReportingInterval;

        private long m_totalBytesSent;
        private long m_lifetimeMeasurements;
        private long m_minimumMeasurementsPerSecond;
        private long m_maximumMeasurementsPerSecond;
        private long m_totalMeasurementsPerSecond;
        private long m_measurementsPerSecondCount;
        private long m_measurementsInSecond;
        private long m_lastSecondsSinceEpoch;
        private long m_lifetimeTotalLatency;
        private long m_lifetimeMinimumLatency;
        private long m_lifetimeMaximumLatency;
        private long m_lifetimeLatencyMeasurements;
        private long m_bufferBlockRetransmissions;
        private bool m_disposed;

        #endregion

        #region [ Constructors ]

        /// <summary>
        /// Creates a new <see cref="DataPublisher"/>.
        /// </summary>
        public DataPublisher()
        {
            base.Name = "Data Publisher Collection";
            base.DataMember = "[internal]";

            m_clientConnections = new ConcurrentDictionary<Guid, SubscriberConnection>();
            m_clientPublicationChannels = new ConcurrentDictionary<Guid, IServer>();
            m_clientNotifications = new Dictionary<Guid, Dictionary<int, string>>();
            m_clientNotificationsLock = new object();
            m_securityMode = DefaultSecurityMode;
            m_encryptPayload = DefaultEncryptPayload;
            m_sharedDatabase = DefaultSharedDatabase;
            m_allowPayloadCompression = DefaultAllowPayloadCompression;
            m_allowMetadataRefresh = DefaultAllowMetadataRefresh;
            m_allowNaNValueFilter = DefaultAllowNaNValueFilter;
            m_forceNaNValueFilter = DefaultForceNaNValueFilter;
            m_useBaseTimeOffsets = DefaultUseBaseTimeOffsets;
            m_metadataTables = DefaultMetadataTables;

            using (Logger.AppendStackMessages("HostAdapter", "Data Publisher Collection"))
            {
                switch (OptimizationOptions.DefaultRoutingMethod)
                {
                    case OptimizationOptions.RoutingMethod.HighLatencyLowCpu:
                        m_routingTables = new RoutingTables(new RouteMappingHighLatencyLowCpu());
                        break;
                    default:
                        m_routingTables = new RoutingTables();
                        break;
                }
            }
            m_routingTables.ActionAdapters = this;
            m_routingTables.StatusMessage += m_routingTables_StatusMessage;
            m_routingTables.ProcessException += m_routingTables_ProcessException;

            // Setup a timer for rotating cipher keys
            m_cipherKeyRotationTimer = Common.TimerScheduler.CreateTimer((int)DefaultCipherKeyRotationPeriod);
            m_cipherKeyRotationTimer.AutoReset = true;
            m_cipherKeyRotationTimer.Enabled = false;
            m_cipherKeyRotationTimer.Elapsed += m_cipherKeyRotationTimer_Elapsed;
        }

        #endregion

        #region [ Properties ]

        /// <summary>
        /// Gets or sets the security mode of the <see cref="DataPublisher"/>'s command channel.
        /// </summary>
        [ConnectionStringParameter,
        Description("Define the security mode used for communications over the command channel."),
        DefaultValue(DefaultSecurityMode)]
        public SecurityMode SecurityMode
        {
            get => m_securityMode;
            set => m_securityMode = value;
        }

        /// <summary>
        /// Gets or sets flag that determines whether data sent over the data channel should be encrypted.
        /// </summary>
        [ConnectionStringParameter,
        Description("Define the flag that determines whether data sent over the data channel should be encrypted. This value is only relevant when SecurityMode is TLS."),
        DefaultValue(DefaultEncryptPayload)]
        public bool EncryptPayload
        {
            get => m_encryptPayload;
            set
            {
                m_encryptPayload = value;

                // Start cipher key rotation timer when encrypting payload
                if ((object)m_cipherKeyRotationTimer != null)
                    m_cipherKeyRotationTimer.Enabled = value;
            }
        }

        /// <summary>
        /// Gets or sets flag that indicates whether this publisher is publishing
        /// data that this node subscribed to from another node in a shared database.
        /// </summary>
        [ConnectionStringParameter,
        Description("Define the flag that indicates whether this publisher is publishing data that this node subscribed to from another node in a shared database."),
        DefaultValue(DefaultSharedDatabase)]
        public bool SharedDatabase
        {
            get => m_sharedDatabase;
            set => m_sharedDatabase = value;
        }

        /// <summary>
        /// Gets or sets flag that indicates if this publisher will allow payload compression when requested by subscribers.
        /// </summary>
        [ConnectionStringParameter,
        Description("Define the flag that indicates if this publisher will allow payload compression when requested by subscribers."),
        DefaultValue(DefaultAllowPayloadCompression)]
        public bool AllowPayloadCompression
        {
            get => m_allowPayloadCompression;
            set => m_allowPayloadCompression = value;
        }

        /// <summary>
        /// Gets or sets flag that indicates if this publisher will allow synchronized subscriptions when requested by subscribers.
        /// </summary>
        [ConnectionStringParameter,
        Description("Define the flag that indicates if this publisher will allow metadata refresh commands when requested by subscribers."),
        DefaultValue(DefaultAllowMetadataRefresh)]
        public bool AllowMetadataRefresh
        {
            get => m_allowMetadataRefresh;
            set => m_allowMetadataRefresh = value;
        }

        /// <summary>
        /// Gets or sets flag that indicates if this publisher will allow filtering of data which is not a number.
        /// </summary>
        [ConnectionStringParameter,
        Description("Define the flag that indicates if this publisher will allow filtering of data which is not a number."),
        DefaultValue(DefaultAllowNaNValueFilter)]
        public bool AllowNaNValueFilter
        {
            get => m_allowNaNValueFilter;
            set => m_allowNaNValueFilter = value;
        }

        /// <summary>
        /// Gets or sets flag that indicates if this publisher will force filtering of data which is not a number.
        /// </summary>
        [ConnectionStringParameter,
        Description("Define the flag that indicates if this publisher will force filtering of data which is not a number."),
        DefaultValue(DefaultForceNaNValueFilter)]
        public bool ForceNaNValueFilter
        {
            get => m_forceNaNValueFilter;
            set => m_forceNaNValueFilter = value;
        }

        /// <summary>
        /// Gets or sets flag that determines whether to use base time offsets to decrease the size of compact measurements.
        /// </summary>
        [ConnectionStringParameter,
        Description("Define the flag that determines whether to use base time offsets to decrease the size of compact measurements."),
        DefaultValue(DefaultUseBaseTimeOffsets)]
        public bool UseBaseTimeOffsets
        {
            get => m_useBaseTimeOffsets;
            set => m_useBaseTimeOffsets = value;
        }

        /// <summary>
        /// Gets or sets the cipher key rotation period.
        /// </summary>
        [ConnectionStringParameter,
        Description("Define the period, in milliseconds, over which new cipher keys will be provided to subscribers when EncryptPayload is true."),
        DefaultValue(DefaultCipherKeyRotationPeriod)]
        public double CipherKeyRotationPeriod
        {
            get
            {
                if ((object)m_cipherKeyRotationTimer != null)
                    return m_cipherKeyRotationTimer.Interval;

                return double.NaN;
            }
            set
            {
                if (value < 1000.0D)
                    throw new ArgumentOutOfRangeException(nameof(value), "Cipher key rotation period should not be set to less than 1000 milliseconds.");

                if ((object)m_cipherKeyRotationTimer != null)
                    m_cipherKeyRotationTimer.Interval = (int)value;

                throw new ArgumentException("Cannot assign new cipher rotation period, timer is not defined.");
            }
        }

        /// <summary>
        /// Gets or sets the set of measurements which are cached by the data
        /// publisher to be published to subscribers immediately upon subscription.
        /// </summary>
        [ConnectionStringParameter,
        DefaultValue(""),
        Description("Defines the set of measurements to be cached and sent to subscribers immediately upon subscription."),
        CustomConfigurationEditor("GSF.TimeSeries.UI.WPF.dll", "GSF.TimeSeries.UI.Editors.MeasurementEditor")]
        public string CachedMeasurementExpression
        {
            get => m_cachedMeasurementExpression;
            set => m_cachedMeasurementExpression = value;
        }

        /// <summary>
        /// Gets or sets the measurement reporting interval.
        /// </summary>
        /// <remarks>
        /// This is used to determined how many measurements should be processed before reporting status.
        /// </remarks>
        [ConnectionStringParameter,
        DefaultValue(AdapterBase.DefaultMeasurementReportingInterval),
        Description("Defines the measurement reporting interval used to determined how many measurements should be processed, per subscriber, before reporting status.")]
        public int MeasurementReportingInterval
        {
            get => m_measurementReportingInterval;
            set => m_measurementReportingInterval = value;
        }

        /// <summary>
        /// Gets or sets <see cref="DataSet"/> based data source used to load each <see cref="IAdapter"/>.
        /// Updates to this property will cascade to all items in this <see cref="AdapterCollectionBase{T}"/>.
        /// </summary>
        public override DataSet DataSource
        {
            get => base.DataSource;
            set
            {
                if (DataSourceChanged(value))
                {
                    base.DataSource = value;

                    UpdateRights();
                    UpdateCertificateChecker();
                    UpdateClientNotifications();
                    UpdateLatestMeasurementCache();
                    NotifyClientsOfConfigurationChange();
                }
            }
        }

        /// <summary>
        /// Gets the status of this <see cref="DataPublisher"/>.
        /// </summary>
        /// <remarks>
        /// Derived classes should provide current status information about the adapter for display purposes.
        /// </remarks>
        public override string Status
        {
            get
            {
                StringBuilder status = new StringBuilder();

                status.Append(m_serverCommandChannel?.Status);
                status.Append(m_clientCommandChannel?.Status);
                status.Append(base.Status);
                status.AppendFormat("        Reporting interval: {0:N0} per subscriber", MeasurementReportingInterval);
                status.AppendLine();
                status.AppendFormat("  Buffer block retransmits: {0:N0}", m_bufferBlockRetransmissions);
                status.AppendLine();

                return status.ToString();
            }
        }

        /// <summary>
        /// Gets or sets the name of this <see cref="DataPublisher"/>.
        /// </summary>
        /// <remarks>
        /// The assigned name is used as the settings category when persisting the TCP server settings.
        /// </remarks>
        public override string Name
        {
            get => base.Name;
            set
            {
                IPersistSettings commandChannel = m_serverCommandChannel as IPersistSettings ?? m_clientCommandChannel as IPersistSettings;
                base.Name = value.ToUpper();

                if (commandChannel != null)
                    commandChannel.SettingsCategory = value.Replace("!", "").ToLower();
            }
        }

        /// <summary>
        /// Gets or sets semi-colon separated list of SQL select statements used to create data for meta-data exchange.
        /// </summary>
        [ConnectionStringParameter]
        [Description("Semi-colon separated list of SQL select statements used to create data for meta-data exchange.")]
        [DefaultValue(DefaultMetadataTables)]
        public string MetadataTables
        {
            get => m_metadataTables;
            set => m_metadataTables = value;
        }

        /// <summary>
        /// Gets flag that determines if <see cref="DataPublisher"/> subscriptions
        /// are automatically initialized when they are added to the collection.
        /// </summary>
        protected override bool AutoInitialize => false;

        /// <summary>
        /// Gets dictionary of connected clients.
        /// </summary>
        protected internal ConcurrentDictionary<Guid, SubscriberConnection> ClientConnections => m_clientConnections;

        /// <summary>
        /// Gets or sets reference to <see cref="TcpServer"/> command channel, attaching and/or detaching to events as needed.
        /// </summary>
        protected IServer ServerCommandChannel
        {
            get => m_serverCommandChannel;
            set
            {
                if ((object)m_serverCommandChannel != null)
                {
                    // Detach from events on existing command channel reference
                    m_serverCommandChannel.ClientConnected -= ServerCommandChannelClientConnected;
                    m_serverCommandChannel.ClientDisconnected -= ServerCommandChannelClientDisconnected;
                    m_serverCommandChannel.ClientConnectingException -= ServerCommandChannelClientConnectingException;
                    m_serverCommandChannel.ReceiveClientDataComplete -= ServerCommandChannelReceiveClientDataComplete;
                    m_serverCommandChannel.ReceiveClientDataException -= ServerCommandChannelReceiveClientDataException;
                    m_serverCommandChannel.SendClientDataException -= ServerCommandChannelSendClientDataException;
                    m_serverCommandChannel.ServerStarted -= ServerCommandChannelServerStarted;
                    m_serverCommandChannel.ServerStopped -= ServerCommandChannelServerStopped;

                    if (m_serverCommandChannel != value)
                        m_serverCommandChannel.Dispose();
                }

                // Assign new command channel reference
                m_serverCommandChannel = value;

                if ((object)m_serverCommandChannel != null)
                {
                    // Attach to desired events on new command channel reference
                    m_serverCommandChannel.ClientConnected += ServerCommandChannelClientConnected;
                    m_serverCommandChannel.ClientDisconnected += ServerCommandChannelClientDisconnected;
                    m_serverCommandChannel.ClientConnectingException += ServerCommandChannelClientConnectingException;
                    m_serverCommandChannel.ReceiveClientDataComplete += ServerCommandChannelReceiveClientDataComplete;
                    m_serverCommandChannel.ReceiveClientDataException += ServerCommandChannelReceiveClientDataException;
                    m_serverCommandChannel.SendClientDataException += ServerCommandChannelSendClientDataException;
                    m_serverCommandChannel.ServerStarted += ServerCommandChannelServerStarted;
                    m_serverCommandChannel.ServerStopped += ServerCommandChannelServerStopped;
                }
            }
        }

        /// <summary>
        /// Gets or sets reference to <see cref="TcpClient"/> command channel, attaching and/or detaching to events as needed.
        /// </summary>
        /// <remarks>
        /// This handles reverse connectivity operations.
        /// </remarks>
        protected IClient ClientCommandChannel
        {
            get => m_clientCommandChannel;
            set
            {
                if ((object)m_clientCommandChannel != null)
                {
                    // Detach from events on existing command channel reference
                    m_clientCommandChannel.ConnectionAttempt -= ClientCommandChannelConnectionAttempt;
                    m_clientCommandChannel.ConnectionEstablished -= ClientCommandChannelConnectionEstablished;
                    m_clientCommandChannel.ConnectionException -= ClientCommandChannelConnectionException;
                    m_clientCommandChannel.ConnectionTerminated -= ClientCommandChannelConnectionTerminated;
                    m_clientCommandChannel.ReceiveDataComplete -= ClientCommandChannelReceiveDataComplete;
                    m_clientCommandChannel.ReceiveDataException -= ClientCommandChannelReceiveDataException;
                    m_clientCommandChannel.SendDataException -= ClientCommandChannelSendDataException;

                    if (m_clientCommandChannel != value)
                        m_clientCommandChannel.Dispose();
                }

                // Assign new command channel reference
                m_clientCommandChannel = value;

                if ((object)m_clientCommandChannel != null)
                {
                    // Attach to desired events on new command channel reference
                    m_clientCommandChannel.ConnectionAttempt += ClientCommandChannelConnectionAttempt;
                    m_clientCommandChannel.ConnectionEstablished += ClientCommandChannelConnectionEstablished;
                    m_clientCommandChannel.ConnectionException += ClientCommandChannelConnectionException;
                    m_clientCommandChannel.ConnectionTerminated += ClientCommandChannelConnectionTerminated;
                    m_clientCommandChannel.ReceiveDataComplete += ClientCommandChannelReceiveDataComplete;
                    m_clientCommandChannel.ReceiveDataException += ClientCommandChannelReceiveDataException;
                    m_clientCommandChannel.SendDataException += ClientCommandChannelSendDataException;
                }
            }
        }

        /// <summary>
        /// Gets flag indicating if publisher is connected and listening.
        /// </summary>
        public bool IsConnected => m_serverCommandChannel?.Enabled ?? m_clientCommandChannel?.Enabled ?? false;

        /// <summary>
        /// Gets the total number of buffer block retransmissions on all subscriptions over the lifetime of the publisher.
        /// </summary>
        public long BufferBlockRetransmissions => m_bufferBlockRetransmissions;

        /// <summary>
        /// Gets the total number of bytes sent to clients of this data publisher.
        /// </summary>
        public long TotalBytesSent => m_totalBytesSent;

        /// <summary>
        /// Gets the total number of measurements processed through this data publisher over the lifetime of the publisher.
        /// </summary>
        public long LifetimeMeasurements => m_lifetimeMeasurements;

        /// <summary>
        /// Gets the minimum value of the measurements per second calculation.
        /// </summary>
        public long MinimumMeasurementsPerSecond => m_minimumMeasurementsPerSecond;

        /// <summary>
        /// Gets the maximum value of the measurements per second calculation.
        /// </summary>
        public long MaximumMeasurementsPerSecond => m_maximumMeasurementsPerSecond;

        /// <summary>
        /// Gets the average value of the measurements per second calculation.
        /// </summary>
        public long AverageMeasurementsPerSecond
        {
            get
            {
                if (m_measurementsPerSecondCount == 0L)
                    return 0L;

                return m_totalMeasurementsPerSecond / m_measurementsPerSecondCount;
            }
        }

        /// <summary>
        /// Gets the minimum latency calculated over the full lifetime of the publisher.
        /// </summary>
        public int LifetimeMinimumLatency => (int)Ticks.ToMilliseconds(m_lifetimeMinimumLatency);

        /// <summary>
        /// Gets the maximum latency calculated over the full lifetime of the publisher.
        /// </summary>
        public int LifetimeMaximumLatency => (int)Ticks.ToMilliseconds(m_lifetimeMaximumLatency);

        /// <summary>
        /// Gets the average latency calculated over the full lifetime of the publisher.
        /// </summary>
        public int LifetimeAverageLatency
        {
            get
            {
                if (m_lifetimeLatencyMeasurements == 0)
                    return -1;

                return (int)Ticks.ToMilliseconds(m_lifetimeTotalLatency / m_lifetimeLatencyMeasurements);
            }
        }

        #endregion

        #region [ Methods ]

        /// <summary>
        /// Releases the unmanaged resources used by the <see cref="DataPublisher"/> object and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
        protected override void Dispose(bool disposing)
        {
            if (!m_disposed)
            {
                try
                {
                    if (disposing)
                    {
                        ServerCommandChannel = null;

                        if ((object)m_clientConnections != null)
                            m_clientConnections.Values.AsParallel().ForAll(cc => cc.Dispose());

                        m_clientConnections = null;

                        if ((object)m_routingTables != null)
                        {
                            m_routingTables.StatusMessage -= m_routingTables_StatusMessage;
                            m_routingTables.ProcessException -= m_routingTables_ProcessException;
                            m_routingTables.Dispose();
                        }
                        m_routingTables = null;

                        // Dispose the cipher key rotation timer
                        if ((object)m_cipherKeyRotationTimer != null)
                        {
                            m_cipherKeyRotationTimer.Elapsed -= m_cipherKeyRotationTimer_Elapsed;
                            m_cipherKeyRotationTimer.Dispose();
                        }
                        m_cipherKeyRotationTimer = null;
                    }
                }
                finally
                {
                    m_disposed = true;          // Prevent duplicate dispose.
                    base.Dispose(disposing);    // Call base class Dispose().
                }
            }
        }

        /// <summary>
        /// Initializes <see cref="DataPublisher"/>.
        /// </summary>
        public override void Initialize()
        {
            // We don't call base class initialize since it tries to auto-load adapters from the defined
            // data member - instead, the data publisher dynamically creates adapters upon request
            Initialized = false;

            Clear();

            Dictionary<string, string> settings = Settings;

            // Check flag that will determine if subscriber payloads should be encrypted by default
            if (settings.TryGetValue("encryptPayload", out string setting))
                m_encryptPayload = setting.ParseBoolean();

            // Check flag that indicates whether publisher is publishing data
            // that its node subscribed to from another node in a shared database
            if (settings.TryGetValue("sharedDatabase", out setting))
                m_sharedDatabase = setting.ParseBoolean();

            // Extract custom metadata table expressions if provided
            if (settings.TryGetValue("metadataTables", out setting) && !string.IsNullOrWhiteSpace(setting))
                m_metadataTables = setting;

            // Check flag to see if payload compression is allowed
            if (settings.TryGetValue("allowPayloadCompression", out setting))
                m_allowPayloadCompression = setting.ParseBoolean();

            // Check flag to see if metadata refresh commands are allowed
            if (settings.TryGetValue("allowMetadataRefresh", out setting))
                m_allowMetadataRefresh = setting.ParseBoolean();

            // Check flag to see if NaN value filtering is allowed
            if (settings.TryGetValue("allowNaNValueFilter", out setting))
                m_allowNaNValueFilter = setting.ParseBoolean();

            // Check flag to see if NaN value filtering is forced
            if (settings.TryGetValue("forceNaNValueFilter", out setting))
                m_forceNaNValueFilter = setting.ParseBoolean();

            if (settings.TryGetValue("useBaseTimeOffsets", out setting))
                m_useBaseTimeOffsets = setting.ParseBoolean();

            if (settings.TryGetValue("measurementReportingInterval", out setting))
                MeasurementReportingInterval = int.Parse(setting);
            else
                MeasurementReportingInterval = AdapterBase.DefaultMeasurementReportingInterval;

            // Get user specified period for cipher key rotation
            if (settings.TryGetValue("cipherKeyRotationPeriod", out setting) && double.TryParse(setting, out double period))
                CipherKeyRotationPeriod = period;

            // Get security mode used for the command channel
            if (settings.TryGetValue("securityMode", out setting))
                m_securityMode = (SecurityMode)Enum.Parse(typeof(SecurityMode), setting);

            if (settings.TryGetValue("cachedMeasurementExpression", out m_cachedMeasurementExpression))
            {
                // Create adapter for caching measurements that have a slower refresh interval
                LatestMeasurementCache cache = new LatestMeasurementCache($"trackLatestMeasurements=true;lagTime=60;leadTime=60;inputMeasurementKeys={{{m_cachedMeasurementExpression}}}");

                // Set up its data source first
                cache.DataSource = DataSource;

                // Add the cache as a child to the data publisher
                Add(cache);

                // AutoInitialize is false for the DataPublisher,
                // so we must initialize manually
                cache.Initialize();
                cache.Initialized = true;

                // Update measurement reporting interval post-initialization
                cache.MeasurementReportingInterval = MeasurementReportingInterval;

                // Trigger routing table calculation to route cached measurements to the cache
                m_routingTables.CalculateRoutingTables(null);

                // Start tracking cached measurements
                cache.Start();
            }

            Dictionary<string, string> commandChannelSettings;
            bool clientBasedConnection = false;

            // Attempt to retrieve any defined command channel settings
            if (settings.TryGetValue("commandChannel", out string commandChannelConnectionString))
                commandChannelSettings = commandChannelConnectionString.ParseKeyValuePairs();
            else
                commandChannelSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            if (commandChannelSettings.TryGetValue("server", out string server))
                clientBasedConnection = !string.IsNullOrWhiteSpace(server);

            if (!commandChannelSettings.TryGetValue("bufferSize", out setting) || !int.TryParse(setting, out int bufferSize))
                bufferSize = ClientBase.DefaultReceiveBufferSize;

            if (m_securityMode == SecurityMode.TLS)
            {
                // Create certificate checker for publisher
                m_certificateChecker = new CertificatePolicyChecker();
                m_subscriberIdentities = new Dictionary<X509Certificate, DataRow>();
                UpdateCertificateChecker();

                if (clientBasedConnection)
                {
                    bool checkCertificateRevocation;

                    if (!commandChannelSettings.TryGetValue("localCertificate", out string localCertificate) || !File.Exists(localCertificate))
                        localCertificate = DataSubscriber.GetLocalCertificate();

                    if (commandChannelSettings.TryGetValue("remoteCertificate", out setting))
                        OnStatusMessage(MessageLevel.Warning, $"Requested override of remoteCertificate \"{setting}\" ignored. System will use configured subscriber settings.", "DataPublisher TlsClient Initialization", MessageFlags.UsageIssue);

                    if (commandChannelSettings.TryGetValue("validPolicyErrors", out setting))
                        OnStatusMessage(MessageLevel.Warning, $"Requested override of validPolicyErrors \"{setting}\" ignored. System will use configured subscriber settings.", "DataPublisher TlsClient Initialization", MessageFlags.UsageIssue);

                    if (commandChannelSettings.TryGetValue("validChainFlags", out setting))
                        OnStatusMessage(MessageLevel.Warning, $"Requested override of validChainFlags \"{setting}\" ignored. System will use configured subscriber settings.", "DataPublisher TlsClient Initialization", MessageFlags.UsageIssue);

                    if (commandChannelSettings.TryGetValue("checkCertificateRevocation", out setting) && !string.IsNullOrWhiteSpace(setting))
                        checkCertificateRevocation = setting.ParseBoolean();
                    else
                        checkCertificateRevocation = true;

                    // Create a new TLS client
                    TlsClient commandChannel = new TlsClient();

                    // Initialize default settings
                    commandChannel.PayloadAware = true;
                    commandChannel.PayloadMarker = null;
                    commandChannel.PayloadEndianOrder = EndianOrder.BigEndian;
                    commandChannel.PersistSettings = false;
                    commandChannel.MaxConnectionAttempts = 1;
                    commandChannel.CertificateFile = FilePath.GetAbsolutePath(localCertificate);
                    commandChannel.CheckCertificateRevocation = checkCertificateRevocation;
                    commandChannel.CertificateChecker = m_certificateChecker;
                    commandChannel.ReceiveBufferSize = bufferSize;
                    commandChannel.SendBufferSize = bufferSize;
                    commandChannel.NoDelay = true;

                    // Assign command channel client reference and attach to needed events
                    ClientCommandChannel = commandChannel;
                }
                else
                {
                    // Create a new TLS server
                    TlsServer commandChannel = new TlsServer();

                    // Initialize default settings
                    commandChannel.SettingsCategory = Name.Replace("!", "").ToLower();
                    commandChannel.ConfigurationString = "port=6165";
                    commandChannel.PayloadAware = true;
                    commandChannel.PayloadMarker = null;
                    commandChannel.PayloadEndianOrder = EndianOrder.BigEndian;
                    commandChannel.RequireClientCertificate = true;
                    commandChannel.CertificateChecker = m_certificateChecker;
                    commandChannel.PersistSettings = true;
                    commandChannel.NoDelay = true;

                    // Assign command channel server reference and attach to needed events
                    ServerCommandChannel = commandChannel;
                }
            }
            else
            {
                if (clientBasedConnection)
                {
                    // Create a new TCP client
                    TcpClient commandChannel = new TcpClient();

                    // Initialize default settings
                    commandChannel.PayloadAware = true;
                    commandChannel.PayloadMarker = null;
                    commandChannel.PayloadEndianOrder = EndianOrder.BigEndian;
                    commandChannel.PersistSettings = false;
                    commandChannel.MaxConnectionAttempts = -1; // Reconnects not handled by base class
                    commandChannel.ReceiveBufferSize = bufferSize;
                    commandChannel.SendBufferSize = bufferSize;
                    commandChannel.NoDelay = true;

                    // Assign command channel client reference and attach to needed events
                    ClientCommandChannel = commandChannel;
                }
                else
                {
                    // Create a new TCP server
                    TcpServer commandChannel = new TcpServer();

                    // Initialize default settings
                    commandChannel.SettingsCategory = Name.Replace("!", "").ToLower();
                    commandChannel.ConfigurationString = "port=6165";
                    commandChannel.PayloadAware = true;
                    commandChannel.PayloadMarker = null;
                    commandChannel.PayloadEndianOrder = EndianOrder.BigEndian;
                    commandChannel.PersistSettings = true;
                    commandChannel.NoDelay = true;

                    // Assign command channel server reference and attach to needed events
                    ServerCommandChannel = commandChannel;
                }
            }

            if (clientBasedConnection)
            {
                // Set client-based connection string
                m_clientCommandChannel.ConnectionString = commandChannelConnectionString;
            }
            else
            {
                // Initialize TCP server - this will load persisted settings
                m_serverCommandChannel.Initialize();

                // Allow user to override persisted server settings by specifying a command channel setting
                if (!string.IsNullOrWhiteSpace(commandChannelConnectionString))
                    m_serverCommandChannel.ConfigurationString = commandChannelConnectionString;
            }

            // Start cipher key rotation timer when encrypting payload
            if (m_encryptPayload && (object)m_cipherKeyRotationTimer != null)
                m_cipherKeyRotationTimer.Start();

            // Register publisher with the statistics engine
            StatisticsEngine.Register(this, "Publisher", "PUB");
            StatisticsEngine.Calculated += (sender, args) => ResetMeasurementsPerSecondCounters();

            Initialized = true;
        }

        /// <summary>
        /// Queues a collection of measurements for processing to each <see cref="IActionAdapter"/> connected to this <see cref="DataPublisher"/>.
        /// </summary>
        /// <param name="measurements">Measurements to queue for processing.</param>
        public override void QueueMeasurementsForProcessing(IEnumerable<IMeasurement> measurements)
        {
            int measurementCount;

            IList<IMeasurement> measurementList = measurements as IList<IMeasurement> ?? measurements.ToList();
            m_routingTables.InjectMeasurements(this, new EventArgs<ICollection<IMeasurement>>(measurementList));

            measurementCount = measurementList.Count;
            m_lifetimeMeasurements += measurementCount;
            UpdateMeasurementsPerSecond(measurementCount);
        }


        RoutingPassthroughMethod IOptimizedRoutingConsumer.GetRoutingPassthroughMethods()
        {
            return new RoutingPassthroughMethod(QueueMeasurementsForProcessing);
        }

        /// <summary>
        /// Queues a collection of measurements for processing to each <see cref="IActionAdapter"/> connected to this <see cref="DataPublisher"/>.
        /// </summary>
        /// <param name="measurements">Measurements to queue for processing.</param>
        private void QueueMeasurementsForProcessing(List<IMeasurement> measurements)
        {
            int measurementCount;

            m_routingTables.InjectMeasurements(this, new EventArgs<ICollection<IMeasurement>>(measurements));

            measurementCount = measurements.Count;
            m_lifetimeMeasurements += measurementCount;
            UpdateMeasurementsPerSecond(measurementCount);
        }

        /// <summary>
        /// Establish <see cref="DataPublisher"/> and start listening for client connections.
        /// </summary>
        public override void Start()
        {
            if (Enabled)
                return;

            base.Start();

            m_serverCommandChannel?.Start();
            m_clientCommandChannel?.ConnectAsync();
        }

        /// <summary>
        /// Terminate <see cref="DataPublisher"/> and stop listening for client connections.
        /// </summary>
        public override void Stop()
        {
            base.Stop();
            
            m_serverCommandChannel?.Stop();
            m_clientCommandChannel?.Disconnect();
        }

        /// <summary>
        /// Gets a short one-line status of this <see cref="DataPublisher"/>.
        /// </summary>
        /// <param name="maxLength">Maximum number of available characters for display.</param>
        /// <returns>A short one-line summary of the current status of the <see cref="DataPublisher"/>.</returns>
        public override string GetShortStatus(int maxLength)
        {
            if ((object)m_serverCommandChannel != null)
                return $"Publishing data to {m_serverCommandChannel.ClientIDs.Length} clients.".CenterText(maxLength);

            if ((object)m_clientCommandChannel != null)
                return "Publishing data to a single client through a client-based connection.".CenterText(maxLength);

            return "Currently not connected".CenterText(maxLength);
        }

        /// <summary>
        /// Enumerates connected clients.
        /// </summary>
        [AdapterCommand("Enumerates connected clients.", "Administrator", "Editor", "Viewer")]
        public virtual void EnumerateClients()
        {
            OnStatusMessage(MessageLevel.Info, EnumerateClients(false));
        }

        /// <summary>
        /// Enumerates connected clients with active temporal sessions.
        /// </summary>
        [AdapterCommand("Enumerates connected clients with active temporal sessions.", "Administrator", "Editor", "Viewer")]
        public virtual void EnumerateTemporalClients()
        {
            OnStatusMessage(MessageLevel.Info, EnumerateClients(true));
        }

        private string EnumerateClients(bool filterToTemporalSessions)
        {
            StringBuilder clientEnumeration = new StringBuilder();
            Guid[] clientIDs = (Guid[])m_serverCommandChannel?.ClientIDs.Clone() ?? new[] { m_proxyClientID };
            bool hasActiveTemporalSession;

            if (filterToTemporalSessions)
                clientEnumeration.AppendFormat("\r\nIndices for connected clients with active temporal sessions:\r\n\r\n");
            else
                clientEnumeration.AppendFormat("\r\nIndices for {0} connected clients:\r\n\r\n", clientIDs.Length);

            for (int i = 0; i < clientIDs.Length; i++)
            {
                if (m_clientConnections.TryGetValue(clientIDs[i], out SubscriberConnection connection) && (object)connection != null && (object)connection.Subscription != null)
                {
                    hasActiveTemporalSession = connection.Subscription.TemporalConstraintIsDefined();

                    if (!filterToTemporalSessions || hasActiveTemporalSession)
                    {
                        clientEnumeration.Append(
                            $"  {i.ToString().PadLeft(3)} - {connection.ConnectionID}\r\n" +
                            $"          {connection.SubscriberInfo}\r\n" + GetOperationalModes(connection) +
                            $"          Active Temporal Session = {(hasActiveTemporalSession ? "Yes" : "No")}\r\n\r\n");
                    }
                }
            }

            // Return enumeration
            return clientEnumeration.ToString();
        }

        private string GetOperationalModes(SubscriberConnection connection)
        {
            StringBuilder description = new StringBuilder();
            OperationalModes operationalModes = connection.OperationalModes;
            CompressionModes compressionModes = (CompressionModes)(operationalModes & OperationalModes.CompressionModeMask);
            bool tsscEnabled = (compressionModes & CompressionModes.TSSC) > 0;
            bool gzipEnabled = (compressionModes & CompressionModes.GZip) > 0;

            if ((operationalModes & OperationalModes.CompressPayloadData) > 0 && tsscEnabled)
            {
                description.Append("          CompressPayloadData[TSSC]\r\n");
            }
            else
            {
                if (connection.Subscription.UseCompactMeasurementFormat)
                    description.Append("          CompactPayloadData[");
                else
                    description.Append("          FullSizePayloadData[");

                description.Append($"{connection.Subscription.TimestampSize}-byte Timestamps]\r\n");
            }

            if ((operationalModes & OperationalModes.CompressSignalIndexCache) > 0 && gzipEnabled)
                description.Append("          CompressSignalIndexCache\r\n");

            if ((operationalModes & OperationalModes.CompressMetadata) > 0 && gzipEnabled)
                description.Append("          CompressMetadata\r\n");

            if ((operationalModes & OperationalModes.ReceiveExternalMetadata) > 0)
                description.Append("          ReceiveExternalMetadata\r\n");

            if ((operationalModes & OperationalModes.ReceiveInternalMetadata) > 0)
                description.Append("          ReceiveInternalMetadata\r\n");

            return description.ToString();
        }

        /// <summary>
        /// Rotates cipher keys for specified client connection.
        /// </summary>
        /// <param name="clientIndex">Enumerated index for client connection.</param>
        [AdapterCommand("Rotates cipher keys for client connection using its enumerated index.", "Administrator")]
        public virtual void RotateCipherKeys(int clientIndex)
        {
            Guid clientID = Guid.Empty;
            bool success = true;

            try
            {
                clientID = m_serverCommandChannel?.ClientIDs[clientIndex] ?? m_proxyClientID;
            }
            catch
            {
                success = false;
                OnStatusMessage(MessageLevel.Error, $"Failed to find connected client with enumerated index {clientIndex}");
            }

            if (success)
            {
                if (m_clientConnections.TryGetValue(clientID, out SubscriberConnection connection))
                    connection.RotateCipherKeys();
                else
                    OnStatusMessage(MessageLevel.Error, $"Failed to find connected client {clientID}");
            }
        }

        /// <summary>
        /// Gets subscriber information for specified client connection.
        /// </summary>
        /// <param name="clientIndex">Enumerated index for client connection.</param>
        [AdapterCommand("Gets subscriber information for client connection using its enumerated index.", "Administrator", "Editor", "Viewer")]
        public virtual string GetSubscriberInfo(int clientIndex)
        {
            Guid clientID = Guid.Empty;
            bool success = true;

            try
            {
                clientID = m_serverCommandChannel?.ClientIDs[clientIndex] ?? m_proxyClientID;
            }
            catch
            {
                success = false;
                OnStatusMessage(MessageLevel.Error, $"Failed to find connected client with enumerated index {clientIndex}");
            }

            if (success)
            {
                if (m_clientConnections.TryGetValue(clientID, out SubscriberConnection connection))
                    return connection.SubscriberInfo;

                OnStatusMessage(MessageLevel.Error, $"Failed to find connected client {clientID}");
            }

            return "";
        }

        /// <summary>
        /// Gets temporal status for a specified client connection.
        /// </summary>
        /// <param name="clientIndex">Enumerated index for client connection.</param>
        [AdapterCommand("Gets temporal status for a subscriber, if any, using its enumerated index.", "Administrator", "Editor", "Viewer")]
        public virtual string GetTemporalStatus(int clientIndex)
        {
            Guid clientID = Guid.Empty;
            bool success = true;

            try
            {
                clientID = m_serverCommandChannel?.ClientIDs[clientIndex] ?? m_proxyClientID;
            }
            catch
            {
                success = false;
                OnStatusMessage(MessageLevel.Error, $"Failed to find connected client with enumerated index {clientIndex}");
            }

            if (success)
            {
                if (m_clientConnections.TryGetValue(clientID, out SubscriberConnection connection))
                {
                    string temporalStatus = null;

                    if ((object)connection.Subscription != null)
                    {
                        if (connection.Subscription.TemporalConstraintIsDefined())
                            temporalStatus = connection.Subscription.TemporalSessionStatus;
                        else
                            temporalStatus = "Subscription does not have an active temporal session.";
                    }

                    if (string.IsNullOrWhiteSpace(temporalStatus))
                        temporalStatus = "Temporal session status is unavailable.";

                    return temporalStatus;
                }

                OnStatusMessage(MessageLevel.Error, $"Failed to find connected client {clientID}");
            }

            return "";
        }

        /// <summary>
        /// Gets the local certificate currently in use by the data publisher.
        /// </summary>
        /// <returns>The local certificate file read directly from the certificate file as an array of bytes.</returns>
        [AdapterCommand("Gets the local certificate currently in use by the data publisher.", "Administrator", "Editor")]
        public virtual byte[] GetLocalCertificate()
        {
            TlsServer commandChannel;

            commandChannel = m_serverCommandChannel as TlsServer;

            if ((object)commandChannel == null)
                throw new InvalidOperationException("Certificates can only be exported in TLS security mode with a server-based command channel.");

            return File.ReadAllBytes(FilePath.GetAbsolutePath(commandChannel.CertificateFile));
        }

        /// <summary>
        /// Imports a certificate to the trusted certificates path.
        /// </summary>
        /// <param name="fileName">The file name to give to the certificate when imported.</param>
        /// <param name="certificateData">The data to be written to the certificate file.</param>
        /// <returns>The local path on the server where the file was written.</returns>
        [AdapterCommand("Imports a certificate to the trusted certificates path.", "Administrator", "Editor")]
        public virtual string ImportCertificate(string fileName, byte[] certificateData)
        {
            TlsServer commandChannel;
            string trustedCertificatesPath;
            string filePath;

            commandChannel = m_serverCommandChannel as TlsServer;

            if ((object)commandChannel == null)
                throw new InvalidOperationException("Certificates can only be imported in TLS security mode with a server-based command channel.");

            trustedCertificatesPath = FilePath.GetAbsolutePath(commandChannel.TrustedCertificatesPath);
            filePath = Path.Combine(trustedCertificatesPath, fileName);
            filePath = FilePath.GetUniqueFilePathWithBinarySearch(filePath);

            if (!Directory.Exists(trustedCertificatesPath))
                Directory.CreateDirectory(trustedCertificatesPath);

            File.WriteAllBytes(filePath, certificateData);

            return filePath;
        }

        /// <summary>
        /// Gets subscriber status for specified subscriber ID.
        /// </summary>
        /// <param name="subscriberID">Guid based subscriber ID for client connection.</param>
        [AdapterCommand("Gets subscriber status for client connection using its subscriber ID.", "Administrator", "Editor", "Viewer")]
        public virtual Tuple<Guid, bool, string> GetSubscriberStatus(Guid subscriberID)
        {
            return new Tuple<Guid, bool, string>(subscriberID, GetConnectionProperty(subscriberID, cc => cc.IsConnected), GetConnectionProperty(subscriberID, cc => cc.SubscriberInfo));
        }

        /// <summary>
        /// Resets the counters for the lifetime statistics without interrupting the adapter's operations.
        /// </summary>
        [AdapterCommand("Resets the counters for the lifetime statistics without interrupting the adapter's operations.", "Administrator", "Editor")]
        public virtual void ResetLifetimeCounters()
        {
            m_lifetimeMeasurements = 0L;
            m_totalBytesSent = 0L;
            m_lifetimeTotalLatency = 0L;
            m_lifetimeMinimumLatency = 0L;
            m_lifetimeMaximumLatency = 0L;
            m_lifetimeLatencyMeasurements = 0L;
        }

        /// <summary>
        /// Sends a notification to all subscribers.
        /// </summary>
        /// <param name="message">The message to be sent.</param>
        [AdapterCommand("Sends a notification to all subscribers.", "Administrator", "Editor")]
        public virtual void SendNotification(string message)
        {
            string notification = $"[{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff}] {message}";

            lock (m_clientNotificationsLock)
            {
                foreach (Dictionary<int, string> notifications in m_clientNotifications.Values)
                    notifications.Add(notification.GetHashCode(), notification);

                SerializeClientNotifications();
                SendAllNotifications();
            }

            OnStatusMessage(MessageLevel.Info, "Sent notification: {0}", message);
        }

        /// <summary>
        /// Updates signal index cache based on input measurement keys.
        /// </summary>
        /// <param name="clientID">Client ID of connection over which to update signal index cache.</param>
        /// <param name="signalIndexCache">New signal index cache.</param>
        /// <param name="inputMeasurementKeys">Subscribed measurement keys.</param>
        public void UpdateSignalIndexCache(Guid clientID, SignalIndexCache signalIndexCache, MeasurementKey[] inputMeasurementKeys)
        {
            ConcurrentDictionary<int, MeasurementKey> reference = new ConcurrentDictionary<int, MeasurementKey>();
            List<Guid> unauthorizedKeys = new List<Guid>();
            int index = 0;
            Guid signalID;

            byte[] serializedSignalIndexCache;

            if ((object)inputMeasurementKeys != null)
            {
                Func<Guid, bool> hasRightsFunc = m_securityMode == SecurityMode.TLS ?
                    new SubscriberRightsLookup(DataSource, signalIndexCache.SubscriberID).HasRightsFunc :
                    _ => true;

                // We will now go through the client's requested keys and see which ones are authorized for subscription,
                // this information will be available through the returned signal index cache which will also define
                // a runtime index optimization for the allowed measurements.
                foreach (MeasurementKey key in inputMeasurementKeys)
                {
                    signalID = key.SignalID;

                    // Validate that subscriber has rights to this signal
                    if (signalID != Guid.Empty && hasRightsFunc(signalID))
                        reference.TryAdd(index++, key);
                    else
                        unauthorizedKeys.Add(key.SignalID);
                }
            }

            signalIndexCache.Reference = reference;
            signalIndexCache.UnauthorizedSignalIDs = unauthorizedKeys.ToArray();
            serializedSignalIndexCache = SerializeSignalIndexCache(clientID, signalIndexCache);

            // Send client updated signal index cache
            if (m_clientConnections.TryGetValue(clientID, out SubscriberConnection connection) && connection.IsSubscribed)
                SendClientResponse(clientID, ServerResponse.UpdateSignalIndexCache, ServerCommand.Subscribe, serializedSignalIndexCache);
        }

        /// <summary>
        /// Updates the latest measurement cache when the
        /// set of cached measurements may have changed.
        /// </summary>
        protected void UpdateLatestMeasurementCache()
        {
            try
            {
                if (Settings.TryGetValue("cachedMeasurementExpression", out string cachedMeasurementExpression))
                {
                    if (TryGetAdapterByName("LatestMeasurementCache", out IActionAdapter cache))
                    {
                        cache.InputMeasurementKeys = AdapterBase.ParseInputMeasurementKeys(DataSource, true, cachedMeasurementExpression);
                        m_routingTables.CalculateRoutingTables(null);
                    }
                }
            }
            catch (Exception ex)
            {
                OnProcessException(MessageLevel.Info, new InvalidOperationException($"Failed to update latest measurement cache: {ex.Message}", ex));
            }
        }

        /// <summary>
        /// Updates each subscription's inputs based on
        /// possible updates to that subscriber's rights.
        /// </summary>
        protected void UpdateRights()
        {
            foreach (SubscriberConnection connection in m_clientConnections.Values)
                UpdateRights(connection);

            m_routingTables.CalculateRoutingTables(null);
        }

        private void UpdateClientNotifications()
        {
            try
            {
                lock (m_clientNotificationsLock)
                {
                    m_clientNotifications.Clear();

                    if (DataSource.Tables.Contains("Subscribers"))
                    {
                        foreach (DataRow row in DataSource.Tables["Subscribers"].Rows)
                        {
                            if (Guid.TryParse(row["ID"].ToNonNullString(), out Guid subscriberID))
                                m_clientNotifications.Add(subscriberID, new Dictionary<int, string>());
                        }
                    }

                    DeserializeClientNotifications();
                }
            }
            catch (Exception ex)
            {
                OnProcessException(MessageLevel.Info, new InvalidOperationException($"Failed to initialize client notification dictionary: {ex.Message}", ex));
            }
        }

        private void NotifyClientsOfConfigurationChange()
        {
            // Make sure publisher allows meta-data refresh, no need to notify clients of configuration change if they can't receive updates
            if (m_allowMetadataRefresh)
            {
                // This can be a lazy notification so we queue up work and return quickly
                ThreadPool.QueueUserWorkItem(state =>
                {
                    // Make a copy of client connection enumeration with ToArray() in case connections are added or dropped during notification
                    foreach (SubscriberConnection connection in m_clientConnections.Values)
                        SendClientResponse(connection.ClientID, ServerResponse.ConfigurationChanged, ServerCommand.Subscribe);
                });
            }
        }

        private void SerializeClientNotifications()
        {
            string notificationsFileName = FilePath.GetAbsolutePath($"{Name}Notifications.txt");

            // Delete existing file for re-serialization
            if (File.Exists(notificationsFileName))
                File.Delete(notificationsFileName);

            using (FileStream fileStream = File.OpenWrite(notificationsFileName))
            using (TextWriter writer = new StreamWriter(fileStream))
            {
                foreach (KeyValuePair<Guid, Dictionary<int, string>> pair in m_clientNotifications)
                {
                    foreach (string notification in pair.Value.Values)
                        writer.WriteLine("{0},{1}", pair.Key, notification);
                }
            }
        }

        private void DeserializeClientNotifications()
        {
            string notificationsFileName = FilePath.GetAbsolutePath($"{Name}Notifications.txt");
            string notification;

            if (File.Exists(notificationsFileName))
            {
                using (FileStream fileStream = File.OpenRead(notificationsFileName))
                using (TextReader reader = new StreamReader(fileStream))
                {
                    string line = reader.ReadLine();

                    while ((object)line != null)
                    {
                        int separatorIndex = line.IndexOf(',');

                        if (Guid.TryParse(line.Substring(0, separatorIndex), out Guid subscriberID))
                        {
                            if (m_clientNotifications.TryGetValue(subscriberID, out Dictionary<int, string> notifications))
                            {
                                notification = line.Substring(separatorIndex + 1);
                                notifications.Add(notification.GetHashCode(), notification);
                            }
                        }

                        line = reader.ReadLine();
                    }
                }
            }
        }

        private void SendAllNotifications()
        {
            foreach (SubscriberConnection connection in m_clientConnections.Values)
                SendNotifications(connection);
        }

        private void SendNotifications(SubscriberConnection connection)
        {
            byte[] hash;
            byte[] message;

            using (BlockAllocatedMemoryStream buffer = new BlockAllocatedMemoryStream())
            {
                if (m_clientNotifications.TryGetValue(connection.SubscriberID, out Dictionary<int, string> notifications))
                {
                    foreach (KeyValuePair<int, string> pair in notifications)
                    {
                        hash = BigEndian.GetBytes(pair.Key);
                        message = connection.Encoding.GetBytes(pair.Value);

                        buffer.Write(hash, 0, hash.Length);
                        buffer.Write(message, 0, message.Length);
                        SendClientResponse(connection.ClientID, ServerResponse.Notify, ServerCommand.Subscribe, buffer.ToArray());
                        buffer.Position = 0;
                    }
                }
            }
        }

        /// <summary>
        /// Gets the text encoding associated with a particular client.
        /// </summary>
        /// <param name="clientID">ID of client.</param>
        /// <returns>Text encoding associated with a particular client.</returns>
        protected internal Encoding GetClientEncoding(Guid clientID)
        {
            if (m_clientConnections.TryGetValue(clientID, out SubscriberConnection connection))
            {
                Encoding clientEncoding = connection.Encoding;

                if ((object)clientEncoding != null)
                    return clientEncoding;
            }

            // Default to Unicode
            return Encoding.Unicode;
        }

        /// <summary>
        /// Sends the start time of the first measurement in a connection transmission.
        /// </summary>
        /// <param name="clientID">ID of client to send response.</param>
        /// <param name="startTime">Start time, in <see cref="Ticks"/>, of first measurement transmitted.</param>
        protected internal virtual bool SendDataStartTime(Guid clientID, Ticks startTime)
        {
            bool result = SendClientResponse(clientID, ServerResponse.DataStartTime, ServerCommand.Subscribe, BigEndian.GetBytes((long)startTime));

            if (m_clientConnections.TryGetValue(clientID, out SubscriberConnection connection))
                OnStatusMessage(MessageLevel.Info, $"Start time sent to {connection.ConnectionID}.");

            return result;
        }

        // Handle input processing complete notifications

        /// <summary>
        /// Sends response back to specified client.
        /// </summary>
        /// <param name="clientID">ID of client to send response.</param>
        /// <param name="response">Server response.</param>
        /// <param name="command">In response to command.</param>
        /// <returns><c>true</c> if send was successful; otherwise <c>false</c>.</returns>
        protected internal virtual bool SendClientResponse(Guid clientID, ServerResponse response, ServerCommand command)
        {
            return SendClientResponse(clientID, response, command, (byte[])null);
        }

        /// <summary>
        /// Sends response back to specified client with a message.
        /// </summary>
        /// <param name="clientID">ID of client to send response.</param>
        /// <param name="response">Server response.</param>
        /// <param name="command">In response to command.</param>
        /// <param name="status">Status message to return.</param>
        /// <returns><c>true</c> if send was successful; otherwise <c>false</c>.</returns>
        protected internal virtual bool SendClientResponse(Guid clientID, ServerResponse response, ServerCommand command, string status)
        {
            if ((object)status != null)
                return SendClientResponse(clientID, response, command, GetClientEncoding(clientID).GetBytes(status));

            return SendClientResponse(clientID, response, command);
        }

        /// <summary>
        /// Sends response back to specified client with a formatted message.
        /// </summary>
        /// <param name="clientID">ID of client to send response.</param>
        /// <param name="response">Server response.</param>
        /// <param name="command">In response to command.</param>
        /// <param name="formattedStatus">Formatted status message to return.</param>
        /// <param name="args">Arguments for <paramref name="formattedStatus"/>.</param>
        /// <returns><c>true</c> if send was successful; otherwise <c>false</c>.</returns>
        protected internal virtual bool SendClientResponse(Guid clientID, ServerResponse response, ServerCommand command, string formattedStatus, params object[] args)
        {
            if (!string.IsNullOrWhiteSpace(formattedStatus))
                return SendClientResponse(clientID, response, command, GetClientEncoding(clientID).GetBytes(string.Format(formattedStatus, args)));

            return SendClientResponse(clientID, response, command);
        }

        /// <summary>
        /// Sends response back to specified client with attached data.
        /// </summary>
        /// <param name="clientID">ID of client to send response.</param>
        /// <param name="response">Server response.</param>
        /// <param name="command">In response to command.</param>
        /// <param name="data">Data to return to client; null if none.</param>
        /// <returns><c>true</c> if send was successful; otherwise <c>false</c>.</returns>
        protected internal virtual bool SendClientResponse(Guid clientID, ServerResponse response, ServerCommand command, byte[] data)
        {
            return SendClientResponse(clientID, (byte)response, (byte)command, data);
        }

        /// <summary>
        /// Updates latency statistics based on the collection of latencies passed into the method.
        /// </summary>
        /// <param name="latencies">The latencies of the measurements sent by the publisher.</param>
        protected internal virtual void UpdateLatencyStatistics(IEnumerable<long> latencies)
        {
            foreach (long latency in latencies)
            {
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
        }

        // Attempts to get the subscriber for the given client based on that client's X.509 certificate.
        private void TryFindClientDetails(SubscriberConnection connection)
        {
            TlsServer serverCommandChannel = m_serverCommandChannel as TlsServer;
            X509Certificate remoteCertificate;

            // If connection is not TLS, there is no X.509 certificate
            if ((object)serverCommandChannel == null)
            {
                TlsClient clientCommandChannel = m_clientCommandChannel as TlsClient;

                if ((object)clientCommandChannel == null)
                    return;

                // Get remote certificate and corresponding trusted certificate
                remoteCertificate = clientCommandChannel.SslStream.RemoteCertificate;
            }
            else
            {
                // If connection is not found, cannot get X.509 certificate
                if (!serverCommandChannel.TryGetClient(connection.ClientID, out TransportProvider<TlsServer.TlsSocket> client))
                    return;

                // Get remote certificate and corresponding trusted certificate
                remoteCertificate = client.Provider.SslStream.RemoteCertificate;
            }

            if ((object)remoteCertificate == null)
                return;

            if (m_subscriberIdentities.TryGetValue(m_certificateChecker.GetTrustedCertificate(remoteCertificate), out DataRow subscriber))
            {
                // Load client details from subscriber identity
                connection.SubscriberID = Guid.Parse(subscriber["ID"].ToNonNullString(Guid.Empty.ToString()).Trim());
                connection.SubscriberAcronym = subscriber["Acronym"].ToNonNullString().Trim();
                connection.SubscriberName = subscriber["Name"].ToNonNullString().Trim();
                connection.ValidIPAddresses = ParseAddressList(subscriber["ValidIPAddresses"].ToNonNullString());
            }
        }

        // Parses a list of IP addresses.
        private List<IPAddress> ParseAddressList(string addressList)
        {
            string[] splitList = addressList.Split(';', ',');
            List<IPAddress> ipAddressList = new List<IPAddress>();
            string dualStackAddress;

            foreach (string address in splitList)
            {
                // Attempt to parse the IP address
                if (!IPAddress.TryParse(address.Trim(), out IPAddress ipAddress))
                    continue;

                // Add the parsed address to the list
                ipAddressList.Add(ipAddress);

                // IPv4 addresses may connect as an IPv6 dual-stack equivalent,
                // so attempt to add that equivalent address to the list as well
                if (ipAddress.AddressFamily == AddressFamily.InterNetwork)
                {
                    dualStackAddress = $"::ffff:{address.Trim()}";

                    if (IPAddress.TryParse(dualStackAddress, out ipAddress))
                        ipAddressList.Add(ipAddress);
                }
            }

            return ipAddressList;
        }

        // Update certificate validation routine.
        private void UpdateCertificateChecker()
        {
            try
            {
                CertificatePolicy policy;
                string remoteCertificateFile;
                X509Certificate certificate;

                if ((object)m_certificateChecker == null || (object)m_subscriberIdentities == null || m_securityMode != SecurityMode.TLS)
                    return;

                m_certificateChecker.DistrustAll();
                m_subscriberIdentities.Clear();

                foreach (DataRow subscriber in DataSource.Tables["Subscribers"].Select("Enabled <> 0"))
                {
                    try
                    {
                        policy = new CertificatePolicy();
                        remoteCertificateFile = subscriber["RemoteCertificateFile"].ToNonNullString();

                        if (Enum.TryParse(subscriber["ValidPolicyErrors"].ToNonNullString(), out SslPolicyErrors validPolicyErrors))
                            policy.ValidPolicyErrors = validPolicyErrors;

                        if (Enum.TryParse(subscriber["ValidChainFlags"].ToNonNullString(), out X509ChainStatusFlags validChainFlags))
                            policy.ValidChainFlags = validChainFlags;

                        if (File.Exists(remoteCertificateFile))
                        {
                            certificate = new X509Certificate2(remoteCertificateFile);
                            m_certificateChecker.Trust(certificate, policy);
                            m_subscriberIdentities.Add(certificate, subscriber);
                        }
                    }
                    catch (Exception ex)
                    {
                        OnProcessException(MessageLevel.Error, new InvalidOperationException($"Failed to add subscriber \"{subscriber["Acronym"].ToNonNullNorEmptyString("[UNKNOWN]")}\" certificate to trusted certificates: {ex.Message}", ex), flags: MessageFlags.SecurityMessage);
                    }
                }
            }
            catch (Exception ex)
            {
                OnProcessException(MessageLevel.Error, new InvalidOperationException($"Failed to update certificate checker: {ex.Message}", ex), flags: MessageFlags.SecurityMessage);
            }
        }

        // Update rights for the given subscription.
        private void UpdateRights(SubscriberConnection connection)
        {
            if ((object)connection == null)
                return;

            try
            {
                SubscriberAdapter subscription = connection.Subscription;
                MeasurementKey[] requestedInputs;
                HashSet<MeasurementKey> authorizedSignals;
                string message;

                // Determine if the connection has been disabled or removed - make sure to set authenticated to false if necessary
                if ((object)DataSource != null && DataSource.Tables.Contains("Subscribers") &&
                    !DataSource.Tables["Subscribers"].Select($"ID = '{connection.SubscriberID}' AND Enabled <> 0").Any())
                    connection.Authenticated = false;

                if ((object)subscription != null)
                {
                    // It is important here that "SELECT" not be allowed in parsing the input measurement keys expression since this key comes
                    // from the remote subscription - this will prevent possible SQL injection attacks.
                    requestedInputs = AdapterBase.ParseInputMeasurementKeys(DataSource, false, subscription.RequestedInputFilter);
                    authorizedSignals = new HashSet<MeasurementKey>();

                    Func<Guid, bool> hasRightsFunc = m_securityMode == SecurityMode.TLS ?
                        new SubscriberRightsLookup(DataSource, subscription.SubscriberID).HasRightsFunc :
                        _ => true;

                    foreach (MeasurementKey input in requestedInputs)
                    {
                        if (hasRightsFunc(input.SignalID))
                            authorizedSignals.Add(input);
                    }

                    if (!authorizedSignals.SetEquals(subscription.InputMeasurementKeys))
                    {
                        // Update the subscription associated with this connection based on newly acquired or revoked rights
                        message = $"Update to authorized signals caused subscription to change. Now subscribed to {authorizedSignals.Count} signals.";
                        subscription.InputMeasurementKeys = authorizedSignals.ToArray();
                        SendClientResponse(subscription.ClientID, ServerResponse.Succeeded, ServerCommand.Subscribe, message);
                    }
                }
            }
            catch (Exception ex)
            {
                OnProcessException(MessageLevel.Error, new InvalidOperationException($"Failed to update authorized signal rights for \"{connection.ConnectionID}\" - connection will be terminated: {ex.Message}", ex), flags: MessageFlags.SecurityMessage);

                // If we can't assign rights, terminate connection
                ThreadPool.QueueUserWorkItem(DisconnectClient, connection.ClientID);
            }
        }

        // Send binary response packet to client
        private bool SendClientResponse(Guid clientID, byte responseCode, byte commandCode, byte[] data)
        {
            bool success = false;

            // Attempt to lookup associated client connection
            if (m_clientConnections.TryGetValue(clientID, out SubscriberConnection connection) && (object)connection != null && !connection.ClientNotFoundExceptionOccurred)
            {
                try
                {
                    // Create a new working buffer
                    using (BlockAllocatedMemoryStream workingBuffer = new BlockAllocatedMemoryStream())
                    {
                        bool dataPacketResponse = responseCode == (byte)ServerResponse.DataPacket;
                        bool useDataChannel = dataPacketResponse || responseCode == (byte)ServerResponse.BufferBlock;

                        // Add response code
                        workingBuffer.WriteByte(responseCode);

                        // Add original in response to command code
                        workingBuffer.WriteByte(commandCode);

                        if ((object)data == null || data.Length == 0)
                        {
                            // Add zero sized data buffer to response packet
                            workingBuffer.Write(ZeroLengthBytes, 0, 4);
                        }
                        else
                        {
                            // If response is for a data packet and a connection key is defined, encrypt the data packet payload
                            if (dataPacketResponse && (object)connection.KeyIVs != null)
                            {
                                // Get a local copy of volatile keyIVs and cipher index since these can change at any time
                                byte[][][] keyIVs = connection.KeyIVs;
                                int cipherIndex = connection.CipherIndex;

                                // Reserve space for size of data buffer to go into response packet
                                workingBuffer.Write(ZeroLengthBytes, 0, 4);

                                // Get data packet flags
                                DataPacketFlags flags = (DataPacketFlags)data[0];

                                // Encode current cipher index into data packet flags
                                if (cipherIndex > 0)
                                    flags |= DataPacketFlags.CipherIndex;

                                // Write data packet flags into response packet
                                workingBuffer.WriteByte((byte)flags);

                                // Copy source data payload into a memory stream
                                MemoryStream sourceData = new MemoryStream(data, 1, data.Length - 1);

                                // Encrypt payload portion of data packet and copy into the response packet
                                Common.SymmetricAlgorithm.Encrypt(sourceData, workingBuffer, keyIVs[cipherIndex][0], keyIVs[cipherIndex][1]);

                                // Calculate length of encrypted data payload
                                int payloadLength = (int)workingBuffer.Length - 6;

                                // Move the response packet position back to the packet size reservation
                                workingBuffer.Seek(2, SeekOrigin.Begin);

                                // Add the actual size of payload length to response packet
                                workingBuffer.Write(BigEndian.GetBytes(payloadLength), 0, 4);
                            }
                            else
                            {
                                // Add size of data buffer to response packet
                                workingBuffer.Write(BigEndian.GetBytes(data.Length), 0, 4);

                                // Add data buffer
                                workingBuffer.Write(data, 0, data.Length);
                            }
                        }

                        if (m_clientCommandChannel == null)
                        {
                            IServer publishChannel;

                            // Data packets and buffer blocks can be published on a UDP data channel, so check for this...
                            if (useDataChannel)
                                publishChannel = m_clientPublicationChannels.GetOrAdd(clientID, id => connection.ServerPublishChannel);
                            else
                                publishChannel = m_serverCommandChannel;

                            // Send response packet
                            if (publishChannel?.CurrentState == ServerState.Running)
                            {
                                byte[] responseData = workingBuffer.ToArray();

                                if (publishChannel is UdpServer)
                                    publishChannel.MulticastAsync(responseData, 0, responseData.Length);
                                else
                                    publishChannel.SendToAsync(clientID, responseData, 0, responseData.Length);

                                m_totalBytesSent += responseData.Length;
                                success = true;
                            }
                        }
                        else
                        {
                            // Send client-based response packet
                            if (m_clientCommandChannel.CurrentState == ClientState.Connected)
                            {
                                byte[] responseData = workingBuffer.ToArray();

                                m_clientCommandChannel.SendAsync(responseData, 0, responseData.Length);

                                m_totalBytesSent += responseData.Length;
                                success = true;
                            }
                        }
                    }
                }
                catch (ObjectDisposedException)
                {
                    // This happens when there is still data to be sent to a disconnected client - we can safely ignore this exception
                }
                catch (NullReferenceException)
                {
                    // This happens when there is still data to be sent to a disconnected client - we can safely ignore this exception
                }
                catch (SocketException ex)
                {
                    if (!HandleSocketException(clientID, ex))
                        OnProcessException(MessageLevel.Info, new InvalidOperationException($"Failed to send response packet to client due to exception: {ex.Message}", ex));
                }
                catch (InvalidOperationException ex)
                {
                    // Could still be processing threads with client data after client has been disconnected, this can be safely ignored
                    if (ex.Message.StartsWith("No client found") && !connection.IsConnected)
                        connection.ClientNotFoundExceptionOccurred = true;
                    else
                        OnProcessException(MessageLevel.Info, new InvalidOperationException($"Failed to send response packet to client due to exception: {ex.Message}", ex));
                }
                catch (Exception ex)
                {
                    OnProcessException(MessageLevel.Info, new InvalidOperationException($"Failed to send response packet to client due to exception: {ex.Message}", ex));
                }
            }

            return success;
        }

        // Socket exception handler
        private bool HandleSocketException(Guid clientID, Exception ex)
        {
            SocketException socketException = ex as SocketException;

            if ((object)socketException != null)
            {
                // WSAECONNABORTED and WSAECONNRESET are common errors after a client disconnect,
                // if they happen for other reasons, make sure disconnect procedure is handled
                if (socketException.ErrorCode == 10053 || socketException.ErrorCode == 10054)
                {
                    try
                    {
                        ThreadPool.QueueUserWorkItem(DisconnectClient, clientID);
                    }
                    catch (Exception queueException)
                    {
                        // Process exception for logging
                        OnProcessException(MessageLevel.Info, new InvalidOperationException($"Failed to queue client disconnect due to exception: {queueException.Message}", queueException));
                    }

                    return true;
                }
            }

            if ((object)ex != null)
                HandleSocketException(clientID, ex.InnerException);

            return false;
        }

        // Disconnect client - this should be called from non-blocking thread (e.g., thread pool)
        private void DisconnectClient(object state)
        {
            m_commandChannelConnectionAttempts = 0;

            try
            {
                Guid clientID = (Guid)state;

                RemoveClientSubscription(clientID);

                if (m_clientConnections.TryRemove(clientID, out SubscriberConnection connection))
                {
                    connection.Dispose();

                    if (clientID == m_proxyClientID)
                        OnStatusMessage(MessageLevel.Info, "Data publisher client-based connection disconnected from subscriber.");
                    else
                        OnStatusMessage(MessageLevel.Info, "Client disconnected from command channel.");
                }

                m_clientPublicationChannels.TryRemove(clientID, out _);
            }
            catch (Exception ex)
            {
                OnProcessException(MessageLevel.Info, new InvalidOperationException($"Encountered an exception while processing client disconnect: {ex.Message}", ex));
            }
        }

        // Remove client subscription
        private void RemoveClientSubscription(Guid clientID)
        {
            lock (this)
            {
                if (TryGetClientSubscription(clientID, out SubscriberAdapter clientSubscription))
                {
                    clientSubscription.Stop();
                    Remove(clientSubscription);

                    try
                    {
                        // Notify system that subscriber disconnected therefore demanded measurements may have changed
                        ThreadPool.QueueUserWorkItem(NotifyHostOfSubscriptionRemoval);
                    }
                    catch (Exception ex)
                    {
                        // Process exception for logging
                        OnProcessException(MessageLevel.Warning, new InvalidOperationException($"Failed to queue notification of subscription removal due to exception: {ex.Message}", ex));
                    }
                }
            }
        }

        // Handle notification on input measurement key change
        private void NotifyHostOfSubscriptionRemoval(object state)
        {
            OnInputMeasurementKeysUpdated();
        }

        // Attempt to find client subscription
        private bool TryGetClientSubscription(Guid clientID, out SubscriberAdapter subscription)
        {
            // Lookup adapter by its client ID
            if (TryGetAdapter(clientID, GetClientSubscription, out IActionAdapter adapter))
            {
                subscription = (SubscriberAdapter)adapter;
                return true;
            }

            subscription = null;
            return false;
        }

        private bool GetClientSubscription(IActionAdapter item, Guid value)
        {
            SubscriberAdapter subscription = item as SubscriberAdapter;

            if ((object)subscription != null)
                return subscription.ClientID == value;

            return false;
        }

        // Gets specified property from client connection based on subscriber ID
        private TResult GetConnectionProperty<TResult>(Guid subscriberID, Func<SubscriberConnection, TResult> predicate)
        {
            TResult result = default;

            // Lookup client connection by subscriber ID
            SubscriberConnection connection = m_clientConnections.Values.FirstOrDefault(cc => cc.SubscriberID == subscriberID);

            // Extract desired property from client connection using given predicate function
            if ((object)connection != null)
                result = predicate(connection);

            return result;
        }

        /// <summary>
        /// Raises the <see cref="ProcessingComplete"/> event.
        /// </summary>
        protected virtual void OnProcessingComplete()
        {
            try
            {
                ProcessingComplete?.Invoke(this, EventArgs.Empty);
            }
            catch (Exception ex)
            {
                // We protect our code from consumer thrown exceptions
                OnProcessException(MessageLevel.Info, new InvalidOperationException($"Exception in consumer handler for ProcessingComplete event: {ex.Message}", ex), "ConsumerEventException");
            }
        }

        /// <summary>
        /// Raises the <see cref="ClientConnected"/> event.
        /// </summary>
        /// <param name="subscriberID">Subscriber <see cref="Guid"/> (normally <see cref="SubscriberConnection.SubscriberID"/>).</param>
        /// <param name="connectionID">Connection identification (normally <see cref="SubscriberConnection.ConnectionID"/>).</param>
        /// <param name="subscriberInfo">Subscriber information (normally <see cref="SubscriberConnection.SubscriberInfo"/>).</param>
        protected virtual void OnClientConnected(Guid subscriberID, string connectionID, string subscriberInfo)
        {
            try
            {
                ClientConnected?.Invoke(this, new EventArgs<Guid, string, string>(subscriberID, connectionID, subscriberInfo));
            }
            catch (Exception ex)
            {
                // We protect our code from consumer thrown exceptions
                OnProcessException(MessageLevel.Info, new InvalidOperationException($"Exception in consumer handler for ClientConnected event: {ex.Message}", ex), "ConsumerEventException");
            }
        }

        protected internal new void OnStatusMessage(MessageLevel level, string status, string eventName = null, MessageFlags flags = MessageFlags.None)
        {
            base.OnStatusMessage(level, status, eventName, flags);
        }

        protected internal new void OnProcessException(MessageLevel level, Exception exception, string eventName = null, MessageFlags flags = MessageFlags.None)
        {
            base.OnProcessException(level, exception, eventName, flags);
        }

        // Make sure to expose any routing table messages
        private void m_routingTables_StatusMessage(object sender, EventArgs<string> e) => OnStatusMessage(MessageLevel.Info, e.Argument);

        // Make sure to expose any routing table exceptions
        private void m_routingTables_ProcessException(object sender, EventArgs<Exception> e) => OnProcessException(MessageLevel.Warning, e.Argument);

        // Cipher key rotation timer handler
        private void m_cipherKeyRotationTimer_Elapsed(object sender, EventArgs<DateTime> e)
        {
            if ((object)m_clientConnections != null)
            {
                foreach (SubscriberConnection connection in m_clientConnections.Values)
                {
                    if ((object)connection != null && connection.Authenticated)
                        connection.RotateCipherKeys();
                }
            }
        }

        // Determines whether the data in the data source has actually changed when receiving a new data source.
        private bool DataSourceChanged(DataSet newDataSource)
        {
            try
            {
                return !DataSetEqualityComparer.Default.Equals(DataSource, newDataSource);
            }
            catch
            {
                return true;
            }
        }

        #region [ Server Command Request Handlers ]

        // Handles subscribe request
        private void HandleSubscribeRequest(SubscriberConnection connection, byte[] buffer, int startIndex, int length)
        {
            Guid clientID = connection.ClientID;
            SubscriberAdapter subscription;
            string message;

            // Handle subscribe
            try
            {
                // Make sure there is enough buffer for flags and connection string length
                if (length >= 6)
                {
                    // Next byte is the data packet flags
                    DataPacketFlags flags = (DataPacketFlags)buffer[startIndex];
                    startIndex++;

                    bool usePayloadCompression = m_allowPayloadCompression && ((connection.OperationalModes & OperationalModes.CompressPayloadData) > 0);
                    CompressionModes compressionModes = (CompressionModes)(connection.OperationalModes & OperationalModes.CompressionModeMask);
                    bool useCompactMeasurementFormat = (byte)(flags & DataPacketFlags.Compact) > 0;
                    bool addSubscription = false;

                    // Next 4 bytes are an integer representing the length of the connection string that follows
                    int byteLength = BigEndian.ToInt32(buffer, startIndex);
                    startIndex += 4;

                    if (byteLength > 0 && length >= 6 + byteLength)
                    {
                        string connectionString = GetClientEncoding(clientID).GetString(buffer, startIndex, byteLength);
                        //startIndex += byteLength;

                        // Get client subscription
                        if ((object)connection.Subscription == null)
                            TryGetClientSubscription(clientID, out subscription);
                        else
                            subscription = connection.Subscription;

                        if ((object)subscription == null)
                        {
                            // Client subscription not established yet, so we create a new one
                            subscription = new SubscriberAdapter(this, clientID, connection.SubscriberID, compressionModes);
                            addSubscription = true;
                        }

                        // Update connection string settings for GSF adapter syntax:
                        Dictionary<string, string> settings = connectionString.ParseKeyValuePairs();

                        if (settings.TryGetValue("throttled", out string setting))
                            settings["trackLatestMeasurements"] = setting;

                        if (settings.TryGetValue("filterExpression", out setting))
                            settings["inputMeasurementKeys"] = setting;

                        connectionString = settings.JoinKeyValuePairs();

                        // Update client subscription properties
                        subscription.ConnectionString = connectionString;
                        subscription.DataSource = DataSource;

                        // Pass subscriber assembly information to connection, if defined
                        if (subscription.Settings.TryGetValue("assemblyInfo", out setting))
                            connection.SubscriberInfo = setting;

                        // Set up UDP data channel if client has requested this
                        connection.DataChannel = null;

                        if (subscription.Settings.TryGetValue("dataChannel", out setting))
                        {
                            Socket clientSocket = connection.GetCommandChannelSocket();
                            settings = setting.ParseKeyValuePairs();
                            IPEndPoint localEndPoint = null;
                            string networkInterface = "::0";

                            // Make sure return interface matches incoming client connection
                            if ((object)clientSocket != null)
                                localEndPoint = clientSocket.LocalEndPoint as IPEndPoint;

                            if ((object)localEndPoint != null)
                            {
                                networkInterface = localEndPoint.Address.ToString();

                                // Remove dual-stack prefix
                                if (networkInterface.StartsWith("::ffff:", true, CultureInfo.InvariantCulture))
                                    networkInterface = networkInterface.Substring(7);
                            }

                            if (settings.TryGetValue("port", out setting) || settings.TryGetValue("localport", out setting))
                            {
                                if ((compressionModes & CompressionModes.TSSC) > 0)
                                {
                                    // TSSC is a stateful compression algorithm which will not reliably support UDP
                                    OnStatusMessage(MessageLevel.Warning, "Cannot use TSSC compression mode with UDP - special compression mode disabled");

                                    // Disable TSSC compression processing
                                    compressionModes &= ~CompressionModes.TSSC;
                                    connection.OperationalModes &= ~OperationalModes.CompressionModeMask;
                                    connection.OperationalModes |= (OperationalModes)compressionModes;
                                }

                                connection.DataChannel = new UdpServer($"Port=-1; Clients={connection.IPAddress}:{int.Parse(setting)}; interface={networkInterface}");
                                connection.DataChannel.Start();
                            }
                        }

                        // Remove any existing cached publication channel since connection is changing
                        m_clientPublicationChannels.TryRemove(clientID, out IServer _);

                        // Update payload compression state and strength
                        subscription.UsePayloadCompression = usePayloadCompression;

                        // Update measurement serialization format type
                        subscription.UseCompactMeasurementFormat = useCompactMeasurementFormat;

                        // Track subscription in connection information
                        connection.Subscription = subscription;

                        if (addSubscription)
                        {
                            // Adding client subscription to collection will not automatically
                            // initialize it because this class overrides the AutoInitialize property
                            lock (this)
                            {
                                Add(subscription);
                            }

                            // Attach to processing completed notification
                            subscription.BufferBlockRetransmission += subscription_BufferBlockRetransmission;
                            subscription.ProcessingComplete += subscription_ProcessingComplete;
                        }

                        // Make sure temporal support is initialized
                        OnRequestTemporalSupport();

                        // Manually initialize client subscription
                        // Subscribed signals (i.e., input measurement keys) will be parsed from connection string during
                        // initialization of adapter. This should also gracefully handle "resubscribing" which can add and
                        // remove subscribed points since assignment and use of input measurement keys is synchronized
                        // within the client subscription class
                        subscription.Initialize();
                        subscription.Initialized = true;

                        // Update measurement reporting interval post-initialization
                        subscription.MeasurementReportingInterval = MeasurementReportingInterval;

                        // Send updated signal index cache to client with validated rights of the selected input measurement keys
                        byte[] serializedSignalIndexCache = SerializeSignalIndexCache(clientID, subscription.SignalIndexCache);
                        SendClientResponse(clientID, ServerResponse.UpdateSignalIndexCache, ServerCommand.Subscribe, serializedSignalIndexCache);

                        // Send new or updated cipher keys
                        if (connection.Authenticated && m_encryptPayload)
                            connection.RotateCipherKeys();

                        // The subscription adapter must be started before sending
                        // cached measurements or else they will be ignored
                        subscription.Start();

                        // If client has subscribed to any cached measurements, queue them up for the client
                        if (TryGetAdapterByName("LatestMeasurementCache", out IActionAdapter cacheAdapter))
                        {
                            LatestMeasurementCache cache = cacheAdapter as LatestMeasurementCache;

                            if ((object)cache != null)
                            {
                                IEnumerable<IMeasurement> cachedMeasurements = cache.LatestMeasurements.Where(measurement => subscription.InputMeasurementKeys.Any(key => key.SignalID == measurement.ID));
                                subscription.QueueMeasurementsForProcessing(cachedMeasurements);
                            }
                        }

                        // Spawn routing table recalculation after sending cached measurements--
                        // there is a bit of a race condition that could cause the subscriber to
                        // miss some data points that arrive during the routing table calculation,
                        // but data will not be provided out of order (except maybe on resubscribe)
                        OnInputMeasurementKeysUpdated();
                        m_routingTables.CalculateRoutingTables(null);

                        // Notify any direct publisher consumers about the new client connection
                        try
                        {
                            OnClientConnected(connection.SubscriberID, connection.ConnectionID, connection.SubscriberInfo);
                        }
                        catch (Exception ex)
                        {
                            OnProcessException(MessageLevel.Info, new InvalidOperationException($"ClientConnected event handler exception: {ex.Message}", ex));
                        }

                        // Send success response
                        if (subscription.TemporalConstraintIsDefined())
                        {
                            message = $"Client subscribed as {(useCompactMeasurementFormat ? "" : "non-")}compact with a temporal constraint.";
                        }
                        else
                        {
                            if ((object)subscription.InputMeasurementKeys != null)
                                message = $"Client subscribed as {(useCompactMeasurementFormat ? "" : "non-")}compact with {subscription.InputMeasurementKeys.Length} signals.";
                            else
                                message = $"Client subscribed as {(useCompactMeasurementFormat ? "" : "non-")}compact, but no signals were specified. Make sure \"inputMeasurementKeys\" setting is properly defined.";
                        }

                        connection.IsSubscribed = true;
                        SendClientResponse(clientID, ServerResponse.Succeeded, ServerCommand.Subscribe, message);
                        OnStatusMessage(MessageLevel.Info, message);
                    }
                    else
                    {
                        if (byteLength > 0) //-V3022
                            message = "Not enough buffer was provided to parse client data subscription.";
                        else
                            message = "Cannot initialize client data subscription without a connection string.";

                        SendClientResponse(clientID, ServerResponse.Failed, ServerCommand.Subscribe, message);
                        OnProcessException(MessageLevel.Warning, new InvalidOperationException(message));
                    }
                }
                else
                {
                    message = "Not enough buffer was provided to parse client data subscription.";
                    SendClientResponse(clientID, ServerResponse.Failed, ServerCommand.Subscribe, message);
                    OnProcessException(MessageLevel.Warning, new InvalidOperationException(message));
                }
            }
            catch (Exception ex)
            {
                message = $"Failed to process client data subscription due to exception: {ex.Message}";
                SendClientResponse(clientID, ServerResponse.Failed, ServerCommand.Subscribe, message);
                OnProcessException(MessageLevel.Warning, new InvalidOperationException(message, ex));
            }
        }

        // Handles unsubscribe request
        private void HandleUnsubscribeRequest(SubscriberConnection connection)
        {
            Guid clientID = connection.ClientID;

            RemoveClientSubscription(clientID); // This does not disconnect client command channel - nor should it...

            // Detach from processing completed notification
            if ((object)connection.Subscription != null)
            {
                connection.Subscription.BufferBlockRetransmission -= subscription_BufferBlockRetransmission;
                connection.Subscription.ProcessingComplete -= subscription_ProcessingComplete;
            }

            connection.Subscription = null;
            connection.IsSubscribed = false;

            SendClientResponse(clientID, ServerResponse.Succeeded, ServerCommand.Unsubscribe, "Client unsubscribed.");
            OnStatusMessage(MessageLevel.Info, $"{connection.ConnectionID} unsubscribed.");
        }

        /// <summary>
        /// Gets meta-data to return to <see cref="DataSubscriber"/>.
        /// </summary>
        /// <param name="connection">Client connection requesting meta-data.</param>
        /// <param name="filterExpressions">Any meta-data filter expressions requested by client.</param>
        /// <returns>Meta-data to be returned to client.</returns>
        protected virtual DataSet AquireMetadata(SubscriberConnection connection, Dictionary<string, Tuple<string, string, int>> filterExpressions)
        {
            using (AdoDataConnection adoDatabase = new AdoDataConnection("systemSettings"))
            {
                IDbConnection dbConnection = adoDatabase.Connection;
                DataSet metadata = new DataSet();
                DataTable table;
                string sortField;
                int takeCount;

                // Initialize active node ID
                Guid nodeID = Guid.Parse(dbConnection.ExecuteScalar($"SELECT NodeID FROM IaonActionAdapter WHERE ID = {ID}").ToString());

                // Determine whether we're sending internal and external meta-data
                bool sendExternalMetadata = connection.OperationalModes.HasFlag(OperationalModes.ReceiveExternalMetadata);
                bool sendInternalMetadata = connection.OperationalModes.HasFlag(OperationalModes.ReceiveInternalMetadata);

                // Copy key meta-data tables
                foreach (string tableExpression in m_metadataTables.Split(';'))
                {
                    if (string.IsNullOrWhiteSpace(tableExpression))
                        continue;

                    // Query the table or view information from the database
                    table = dbConnection.RetrieveData(adoDatabase.AdapterType, tableExpression);

                    // Remove any expression from table name
                    Match regexMatch = Regex.Match(tableExpression, @"FROM \w+");
                    table.TableName = regexMatch.Value.Split(' ')[1];

                    sortField = "";
                    takeCount = int.MaxValue;

                    // Build filter list
                    List<string> filters = new List<string>();

                    if (table.Columns.Contains("NodeID"))
                        filters.Add($"NodeID = '{nodeID}'");

                    if (table.Columns.Contains("Internal") && !(sendInternalMetadata && sendExternalMetadata))
                        filters.Add($"Internal {(sendExternalMetadata ? "=" : "<>")} 0");

                    if (table.Columns.Contains("OriginalSource") && !(sendInternalMetadata && sendExternalMetadata))
                        filters.Add($"OriginalSource IS {(sendExternalMetadata ? "NOT" : "")} NULL");

                    if (filterExpressions.TryGetValue(table.TableName, out Tuple<string, string, int> filterParameters))
                    {
                        filters.Add($"({filterParameters.Item1})");
                        sortField = filterParameters.Item2;
                        takeCount = filterParameters.Item3;
                    }

                    // Determine whether we need to check subscriber for rights to the data
                    bool checkSubscriberRights = m_securityMode == SecurityMode.TLS && table.Columns.Contains("SignalID");

                    if (m_sharedDatabase || filters.Count == 0 && !checkSubscriberRights)
                    {
                        // Add a copy of the results to the dataset for meta-data exchange
                        metadata.Tables.Add(table.Copy());
                    }
                    else
                    {
                        IEnumerable<DataRow> filteredRows;
                        List<DataRow> filteredRowList;

                        // Make a copy of the table structure
                        metadata.Tables.Add(table.Clone());

                        // Filter in-memory data table down to desired rows
                        filteredRows = table.Select(string.Join(" AND ", filters), sortField);

                        // Reduce data to only what the subscriber has rights to
                        if (checkSubscriberRights)
                        {
                            SubscriberRightsLookup lookup = new SubscriberRightsLookup(DataSource, connection.SubscriberID);
                            filteredRows = filteredRows.Where(row => lookup.HasRights(row.ConvertField<Guid>("SignalID")));
                        }

                        // Apply any maximum row count that user may have specified
                        filteredRowList = filteredRows.Take(takeCount).ToList();

                        if (filteredRowList.Count > 0)
                        {
                            DataTable metadataTable = metadata.Tables[table.TableName];

                            // Manually copy-in each row into table
                            foreach (DataRow row in filteredRowList)
                            {
                                DataRow newRow = metadataTable.NewRow();

                                // Copy each column of data in the current row
                                for (int x = 0; x < table.Columns.Count; x++)
                                {
                                    newRow[x] = row[x];
                                }

                                metadataTable.Rows.Add(newRow);
                            }
                        }
                    }
                }

                // TODO: Although protected against unprovided tables and columns, this post-analysis operation is schema specific. This may need to be moved to an external function and executed via delegate to allow this kind of work for other schemas.

                // Do some post analysis on the meta-data to be delivered to the client, e.g., if a device exists with no associated measurements - don't send the device.
                if (metadata.Tables.Contains("MeasurementDetail") && metadata.Tables["MeasurementDetail"].Columns.Contains("DeviceAcronym") && metadata.Tables.Contains("DeviceDetail") && metadata.Tables["DeviceDetail"].Columns.Contains("Acronym"))
                {
                    List<DataRow> rowsToRemove = new List<DataRow>();
                    string deviceAcronym;
                    int? phasorSourceIndex;

                    // Remove device records where no associated measurements records exist
                    foreach (DataRow row in metadata.Tables["DeviceDetail"].Rows)
                    {
                        deviceAcronym = row["Acronym"].ToNonNullString();

                        if (!string.IsNullOrEmpty(deviceAcronym) && (int)metadata.Tables["MeasurementDetail"].Compute("Count(DeviceAcronym)", $"DeviceAcronym = '{deviceAcronym}'") == 0)
                            rowsToRemove.Add(row);
                    }

                    if (metadata.Tables.Contains("PhasorDetail") && metadata.Tables["PhasorDetail"].Columns.Contains("DeviceAcronym"))
                    {
                        // Remove phasor records where no associated device records exist
                        foreach (DataRow row in metadata.Tables["PhasorDetail"].Rows)
                        {
                            deviceAcronym = row["DeviceAcronym"].ToNonNullString();

                            if (!string.IsNullOrEmpty(deviceAcronym) && (int)metadata.Tables["DeviceDetail"].Compute("Count(Acronym)", $"Acronym = '{deviceAcronym}'") == 0)
                                rowsToRemove.Add(row);
                        }

                        if (metadata.Tables["PhasorDetail"].Columns.Contains("SourceIndex") && metadata.Tables["MeasurementDetail"].Columns.Contains("PhasorSourceIndex"))
                        {
                            // Remove measurement records where no associated phasor records exist
                            foreach (DataRow row in metadata.Tables["MeasurementDetail"].Rows)
                            {
                                deviceAcronym = row["DeviceAcronym"].ToNonNullString();
                                phasorSourceIndex = row.ConvertField<int?>("PhasorSourceIndex");

                                if (!string.IsNullOrEmpty(deviceAcronym) && (object)phasorSourceIndex != null && (int)metadata.Tables["PhasorDetail"].Compute("Count(DeviceAcronym)", $"DeviceAcronym = '{deviceAcronym}' AND SourceIndex = {phasorSourceIndex}") == 0)
                                    rowsToRemove.Add(row);
                            }
                        }
                    }

                    // Remove any unnecessary rows
                    foreach (DataRow row in rowsToRemove)
                        row.Delete();

                }

                return metadata;
            }
        }

        // Handles meta-data refresh request
        private void HandleMetadataRefresh(SubscriberConnection connection, byte[] buffer, int startIndex, int length)
        {
            // Ensure that the subscriber is allowed to request meta-data
            if (!m_allowMetadataRefresh)
                throw new InvalidOperationException("Meta-data refresh has been disallowed by the DataPublisher.");

            OnStatusMessage(MessageLevel.Info, $"Received meta-data refresh request from {connection.ConnectionID}, preparing response...");

            Guid clientID = connection.ClientID;
            Dictionary<string, Tuple<string, string, int>> filterExpressions = new Dictionary<string, Tuple<string, string, int>>(StringComparer.OrdinalIgnoreCase);
            string message;
            Ticks startTime = DateTime.UtcNow.Ticks;

            // Attempt to parse out any subscriber provided meta-data filter expressions
            try
            {
                // Note that these client provided meta-data filter expressions are applied post SQL data retrieval to the limited-capability 
                // DataTable.Select() function against an in-memory DataSet and therefore are not subject to SQL injection attacks
                if (length > 4)
                {
                    int responseLength = BigEndian.ToInt32(buffer, startIndex);
                    startIndex += 4;

                    if (length >= responseLength + 4)
                    {
                        string metadataFilters = GetClientEncoding(clientID).GetString(buffer, startIndex, responseLength);
                        string[] expressions = metadataFilters.Split(';');

                        // Go through each subscriber specified filter expressions
                        foreach (string expression in expressions)
                        {
                            // Attempt to parse filter expression and add it dictionary if successful
                            if (AdapterBase.ParseFilterExpression(expression, out string tableName, out string filterExpression, out string sortField, out int takeCount))
                                filterExpressions.Add(tableName, Tuple.Create(filterExpression, sortField, takeCount));
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                OnProcessException(MessageLevel.Warning, new InvalidOperationException($"Failed to parse subscriber provided meta-data filter expressions: {ex.Message}", ex));
            }

            try
            {
                DataSet metadata = AquireMetadata(connection, filterExpressions);
                byte[] serializedMetadata = SerializeMetadata(clientID, metadata);
                long rowCount = metadata.Tables.Cast<DataTable>().Select(dataTable => (long)dataTable.Rows.Count).Sum();

                if (rowCount > 0)
                {
                    Time elapsedTime = (DateTime.UtcNow.Ticks - startTime).ToSeconds();
                    OnStatusMessage(MessageLevel.Info, $"{rowCount:N0} records spanning {metadata.Tables.Count:N0} tables of meta-data prepared in {elapsedTime.ToString(2)}, sending response to {connection.ConnectionID}...");
                }
                else
                {
                    OnStatusMessage(MessageLevel.Info, $"No meta-data is available, sending an empty response to {connection.ConnectionID}...");
                }

                SendClientResponse(clientID, ServerResponse.Succeeded, ServerCommand.MetaDataRefresh, serializedMetadata);
            }
            catch (Exception ex)
            {
                message = $"Failed to transfer meta-data due to exception: {ex.Message}";
                SendClientResponse(clientID, ServerResponse.Failed, ServerCommand.MetaDataRefresh, message);
                OnProcessException(MessageLevel.Warning, new InvalidOperationException(message, ex));
            }
        }

        // Handles request to update processing interval on client session
        private void HandleUpdateProcessingInterval(SubscriberConnection connection, byte[] buffer, int startIndex, int length)
        {
            Guid clientID = connection.ClientID;
            string message;

            // Make sure there is enough buffer for new processing interval value
            if (length >= 4)
            {
                // Next 4 bytes are an integer representing the new processing interval
                int processingInterval = BigEndian.ToInt32(buffer, startIndex);

                SubscriberAdapter subscription = connection.Subscription;

                if ((object)subscription != null)
                {
                    subscription.ProcessingInterval = processingInterval;
                    SendClientResponse(clientID, ServerResponse.Succeeded, ServerCommand.UpdateProcessingInterval, "New processing interval of {0} assigned.", processingInterval);
                    OnStatusMessage(MessageLevel.Info, $"{connection.ConnectionID} was assigned a new processing interval of {processingInterval}.");
                }
                else
                {
                    message = "Client subscription was not available, could not update processing interval.";
                    SendClientResponse(clientID, ServerResponse.Failed, ServerCommand.UpdateProcessingInterval, message);
                    OnProcessException(MessageLevel.Info, new InvalidOperationException(message));
                }
            }
            else
            {
                message = "Not enough buffer was provided to update client processing interval.";
                SendClientResponse(clientID, ServerResponse.Failed, ServerCommand.UpdateProcessingInterval, message);
                OnProcessException(MessageLevel.Warning, new InvalidOperationException(message));
            }
        }

        // Handle request to define operational modes for client connection
        private void HandleDefineOperationalModes(SubscriberConnection connection, byte[] buffer, int startIndex, int length)
        {
            uint operationalModes;

            if (length >= 4)
            {
                operationalModes = BigEndian.ToUInt32(buffer, startIndex);

                if ((operationalModes & (uint)OperationalModes.VersionMask) != 1u)
                    OnStatusMessage(MessageLevel.Warning, $"Protocol version not supported. Operational modes may not be set correctly for client {connection.ClientID}.", flags: MessageFlags.UsageIssue);

                connection.OperationalModes = (OperationalModes)operationalModes;
            }
        }

        // Handle confirmation of receipt of notification 
        private void HandleConfirmNotification(SubscriberConnection connection, byte[] buffer, int startIndex, int length)
        {
            int hash = BigEndian.ToInt32(buffer, startIndex);

            if (length >= 4)
            {
                lock (m_clientNotificationsLock)
                {
                    if (m_clientNotifications.TryGetValue(connection.SubscriberID, out Dictionary<int, string> notifications))
                    {
                        if (notifications.TryGetValue(hash, out string notification))
                        {
                            notifications.Remove(hash);
                            OnStatusMessage(MessageLevel.Info, $"Subscriber {connection.ConnectionID} confirmed receipt of notification: {notification}.");
                            SerializeClientNotifications();
                        }
                        else
                        {
                            OnStatusMessage(MessageLevel.Info, $"Confirmation for unknown notification received from client {connection.ConnectionID}.");
                        }
                    }
                    else
                    {
                        OnStatusMessage(MessageLevel.Info, "Unsolicited confirmation of notification received.");
                    }
                }
            }
            else
            {
                OnStatusMessage(MessageLevel.Info, "Malformed notification confirmation received.");
            }
        }

        private void HandleConfirmBufferBlock(SubscriberConnection connection, byte[] buffer, int startIndex, int length)
        {
            uint sequenceNumber;

            if (length >= 4)
            {
                sequenceNumber = BigEndian.ToUInt32(buffer, startIndex);
                connection.Subscription.ConfirmBufferBlock(sequenceNumber);
            }
        }

        private void HandlePublishCommandMeasurements(SubscriberConnection connection, byte[] buffer, int startIndex)
        {
            try
            {
                List<IMeasurement> measurements = new List<IMeasurement>();

                int index = startIndex;
                int payloadByteLength = BigEndian.ToInt32(buffer, index);
                index += sizeof(int);

                string dataString = connection.Encoding.GetString(buffer, index, payloadByteLength);
                ConnectionStringParser<SettingAttribute> connectionStringParser = new ConnectionStringParser<SettingAttribute>();

                foreach (string measurementString in dataString.Split(new[] { ";;" }, StringSplitOptions.RemoveEmptyEntries))
                {
                    CommandMeasurement measurement = new CommandMeasurement();
                    connectionStringParser.ParseConnectionString(measurementString, measurement);

                    measurements.Add(new Measurement()
                    {
                        Metadata = MeasurementKey.LookUpBySignalID(measurement.SignalID).Metadata,
                        Timestamp = measurement.Timestamp,
                        Value = measurement.Value,
                        StateFlags = measurement.StateFlags
                    });
                }

                OnNewMeasurements(measurements);
            }
            catch (Exception ex)
            {
                string errorMessage = $"Data packet command failed due to exception: {ex.Message}";
                OnProcessException(MessageLevel.Error, new Exception(errorMessage, ex));
                SendClientResponse(connection.ClientID, ServerResponse.Failed, ServerCommand.PublishCommandMeasurements, errorMessage);
            }
        }

        /// <summary>
        /// Handles custom commands defined by the user of the publisher API.
        /// </summary>
        /// <param name="connection">Object representing the connection to the data subscriber.</param>
        /// <param name="command">The command issued by the subscriber.</param>
        /// <param name="buffer">The buffer containing the entire message from the subscriber.</param>
        /// <param name="startIndex">The index indicating where to start reading from the buffer to skip past the message header.</param>
        /// <param name="length">The total number of bytes in the message, including the header.</param>
        protected virtual void HandleUserCommand(SubscriberConnection connection, ServerCommand command, byte[] buffer, int startIndex, int length)
        {
            OnStatusMessage(MessageLevel.Info, $"Received command code for user-defined command \"{command}\".");
        }

        private byte[] SerializeSignalIndexCache(Guid clientID, SignalIndexCache signalIndexCache)
        {
            byte[] serializedSignalIndexCache = null;

            if (m_clientConnections.TryGetValue(clientID, out SubscriberConnection connection))
            {
                OperationalModes operationalModes = connection.OperationalModes;
                CompressionModes compressionModes = (CompressionModes)(operationalModes & OperationalModes.CompressionModeMask);
                bool compressSignalIndexCache = (operationalModes & OperationalModes.CompressSignalIndexCache) > 0;
                GZipStream deflater = null;

                signalIndexCache.Encoding = GetClientEncoding(clientID);
                serializedSignalIndexCache = new byte[signalIndexCache.BinaryLength];
                signalIndexCache.GenerateBinaryImage(serializedSignalIndexCache, 0);

                if (compressSignalIndexCache && compressionModes.HasFlag(CompressionModes.GZip))
                {
                    try
                    {
                        // Compress serialized signal index cache into compressed data buffer
                        using (BlockAllocatedMemoryStream compressedData = new BlockAllocatedMemoryStream())
                        {
                            deflater = new GZipStream(compressedData, CompressionMode.Compress, true);
                            deflater.Write(serializedSignalIndexCache, 0, serializedSignalIndexCache.Length);
                            deflater.Close();
                            deflater = null;

                            serializedSignalIndexCache = compressedData.ToArray();
                        }
                    }
                    finally
                    {
                        if ((object)deflater != null)
                            deflater.Close();
                    }
                }
            }

            return serializedSignalIndexCache;
        }

        private byte[] SerializeMetadata(Guid clientID, DataSet metadata)
        {
            if (!m_clientConnections.TryGetValue(clientID, out SubscriberConnection connection))
                return null;

            byte[] serializedMetadata;

            OperationalModes operationalModes = connection.OperationalModes;
            CompressionModes compressionModes = (CompressionModes)(operationalModes & OperationalModes.CompressionModeMask);
            bool compressMetadata = (operationalModes & OperationalModes.CompressMetadata) > 0;
            GZipStream deflater = null;

            // Encode XML into encoded data buffer
            using (BlockAllocatedMemoryStream encodedData = new BlockAllocatedMemoryStream())
            using (XmlTextWriter xmlWriter = new XmlTextWriter(encodedData, GetClientEncoding(clientID)))
            {
                metadata.WriteXml(xmlWriter, XmlWriteMode.WriteSchema);
                xmlWriter.Flush();

                // Return result of encoding
                serializedMetadata = encodedData.ToArray();
            }

            if (compressMetadata && compressionModes.HasFlag(CompressionModes.GZip))
            {
                try
                {
                    // Compress serialized metadata into compressed data buffer
                    using (BlockAllocatedMemoryStream compressedData = new BlockAllocatedMemoryStream())
                    {
                        deflater = new GZipStream(compressedData, CompressionMode.Compress, true);
                        deflater.Write(serializedMetadata, 0, serializedMetadata.Length);
                        deflater.Close();
                        deflater = null;

                        // Return result of compression
                        serializedMetadata = compressedData.ToArray();
                    }
                }
                finally
                {
                    if ((object)deflater != null)
                        deflater.Close();
                }
            }

            return serializedMetadata;
        }

        // Updates the measurements per second counters after receiving another set of measurements.
        private void UpdateMeasurementsPerSecond(int measurementCount)
        {
            long secondsSinceEpoch = DateTime.UtcNow.Ticks / Ticks.PerSecond;

            if (secondsSinceEpoch > m_lastSecondsSinceEpoch)
            {
                if (m_measurementsInSecond < m_minimumMeasurementsPerSecond || m_minimumMeasurementsPerSecond == 0L)
                    m_minimumMeasurementsPerSecond = m_measurementsInSecond;

                if (m_measurementsInSecond > m_maximumMeasurementsPerSecond || m_maximumMeasurementsPerSecond == 0L)
                    m_maximumMeasurementsPerSecond = m_measurementsInSecond;

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
            m_minimumMeasurementsPerSecond = 0L;
            m_maximumMeasurementsPerSecond = 0L;
            m_totalMeasurementsPerSecond = 0L;
            m_measurementsPerSecondCount = 0L;
        }

        private void subscription_BufferBlockRetransmission(object sender, EventArgs eventArgs)
        {
            m_bufferBlockRetransmissions++;
        }

        // Bubble up processing complete notifications from subscriptions
        private void subscription_ProcessingComplete(object sender, EventArgs<IClientSubscription, EventArgs> e)
        {
            // Expose notification via data publisher event subscribers
            ProcessingComplete?.Invoke(sender, e.Argument2);

            IClientSubscription subscription = e.Argument1;
            string senderType = (object)sender == null ? "N/A" : sender.GetType().Name;

            // Send direct notification to associated client
            if ((object)subscription != null)
                SendClientResponse(subscription.ClientID, ServerResponse.ProcessingComplete, ServerCommand.Subscribe, senderType);
        }

        #endregion

        #region [ Server Command Channel Event Handlers ]

        private void ServerCommandChannelReceiveClientDataComplete(object sender, EventArgs<Guid, byte[], int> e)
        {
            try
            {
                Guid clientID = e.Argument1;
                byte[] buffer = e.Argument2;
                int length = e.Argument3;
                int index = 0;

                if (length > 0 && (object)buffer != null)
                {
                    string message;
                    byte commandByte = buffer[index];
                    index++;

                    // Attempt to parse solicited server command
                    bool validServerCommand = Enum.TryParse(commandByte.ToString(), out ServerCommand command);

                    // Look up this client connection
                    if (!m_clientConnections.TryGetValue(clientID, out SubscriberConnection connection))
                    {
                        // Received a request from an unknown client, this request is denied
                        OnStatusMessage(MessageLevel.Warning, $"Ignored {length} byte {(validServerCommand ? command.ToString() : "unidentified")} command request received from an unrecognized client: {clientID}", flags: MessageFlags.UsageIssue);
                    }
                    else if (validServerCommand)
                    {
                        switch (command)
                        {
                            case ServerCommand.Subscribe:
                                // Handle subscribe
                                HandleSubscribeRequest(connection, buffer, index, length);
                                break;

                            case ServerCommand.Unsubscribe:
                                // Handle unsubscribe
                                HandleUnsubscribeRequest(connection);
                                break;

                            case ServerCommand.MetaDataRefresh:
                                // Handle meta data refresh (per subscriber request)
                                HandleMetadataRefresh(connection, buffer, index, length);
                                break;

                            case ServerCommand.RotateCipherKeys:
                                // Handle rotation of cipher keys (per subscriber request)
                                connection.RotateCipherKeys();
                                break;

                            case ServerCommand.UpdateProcessingInterval:
                                // Handle request to update processing interval
                                HandleUpdateProcessingInterval(connection, buffer, index, length);
                                break;

                            case ServerCommand.DefineOperationalModes:
                                // Handle request to define operational modes
                                HandleDefineOperationalModes(connection, buffer, index, length);
                                break;

                            case ServerCommand.ConfirmNotification:
                                // Handle confirmation of receipt of notification
                                HandleConfirmNotification(connection, buffer, index, length);
                                break;

                            case ServerCommand.ConfirmBufferBlock:
                                // Handle confirmation of receipt of a buffer block
                                HandleConfirmBufferBlock(connection, buffer, index, length);
                                break;

                            case ServerCommand.PublishCommandMeasurements:
                                // Handle publication of command measurements
                                HandlePublishCommandMeasurements(connection, buffer, index);
                                break;

                            case ServerCommand.UserCommand00:
                            case ServerCommand.UserCommand01:
                            case ServerCommand.UserCommand02:
                            case ServerCommand.UserCommand03:
                            case ServerCommand.UserCommand04:
                            case ServerCommand.UserCommand05:
                            case ServerCommand.UserCommand06:
                            case ServerCommand.UserCommand07:
                            case ServerCommand.UserCommand08:
                            case ServerCommand.UserCommand09:
                            case ServerCommand.UserCommand10:
                            case ServerCommand.UserCommand11:
                            case ServerCommand.UserCommand12:
                            case ServerCommand.UserCommand13:
                            case ServerCommand.UserCommand14:
                            case ServerCommand.UserCommand15:
                                // Handle confirmation of receipt of a user-defined command
                                HandleUserCommand(connection, command, buffer, index, length);
                                break;
                        }
                    }
                    else
                    {
                        // Handle unrecognized commands
                        message = $" sent an unrecognized server command: 0x{commandByte.ToString("X").PadLeft(2, '0')}";
                        SendClientResponse(clientID, (byte)ServerResponse.Failed, commandByte, GetClientEncoding(clientID).GetBytes($"Client{message}"));
                        OnProcessException(MessageLevel.Warning, new InvalidOperationException(connection.ConnectionID + message));
                    }
                }
            }
            catch (Exception ex)
            {
                OnProcessException(MessageLevel.Warning, new InvalidOperationException($"Encountered an exception while processing received client data: {ex.Message}", ex));
            }
        }

        private void ServerCommandChannelClientConnected(object sender, EventArgs<Guid> e)
        {
            Guid clientID = e.Argument;
            SubscriberConnection connection = new SubscriberConnection(this, clientID, m_serverCommandChannel, m_clientCommandChannel);
            
            connection.ClientNotFoundExceptionOccurred = false;

            if (m_securityMode == SecurityMode.TLS)
            {
                TryFindClientDetails(connection);
                connection.Authenticated = connection.ValidIPAddresses.Contains(connection.IPAddress);
            }

            m_clientConnections[clientID] = connection;

            OnStatusMessage(MessageLevel.Info, "Client connected to command channel.");

            if (connection.Authenticated)
            {
                lock (m_clientNotificationsLock)
                {
                    // Send any queued notifications to authenticated client
                    SendNotifications(connection);
                }
            }
            else if (m_securityMode == SecurityMode.TLS)
            {
                string errorMessage = "Unable to authenticate client. Client connected using" + 
                    $" certificate of subscriber \"{connection.SubscriberName}\", however the IP address used ({connection.IPAddress}) was" + 
                    " not found among the list of valid IP addresses.";

                OnProcessException(MessageLevel.Warning, new InvalidOperationException(errorMessage));
            }
        }

        private void ServerCommandChannelClientDisconnected(object sender, EventArgs<Guid> e)
        {
            try
            {
                ThreadPool.QueueUserWorkItem(DisconnectClient, e.Argument);
            }
            catch (Exception ex)
            {
                // Process exception for logging
                OnProcessException(MessageLevel.Info, new InvalidOperationException($"Failed to queue client disconnect due to exception: {ex.Message}", ex));
            }
        }

        private void ServerCommandChannelClientConnectingException(object sender, EventArgs<Exception> e)
        {
            Exception ex = e.Argument;
            OnProcessException(MessageLevel.Info, new ConnectionException($"Data publisher encountered an exception while connecting client to the command channel: {ex.Message}", ex));
        }

        private void ServerCommandChannelServerStarted(object sender, EventArgs e)
        {
            OnStatusMessage(MessageLevel.Info, "Data publisher command channel started.");
        }

        private void ServerCommandChannelServerStopped(object sender, EventArgs e)
        {
            if (Enabled)
            {
                OnStatusMessage(MessageLevel.Info, "Data publisher command channel was unexpectedly terminated, restarting...");

                Action restartServerCommandChannel = () =>
                {
                    try
                    {
                        m_serverCommandChannel.Start();
                    }
                    catch (Exception ex)
                    {
                        OnProcessException(MessageLevel.Warning, new InvalidOperationException($"Failed to restart data publisher command channel: {ex.Message}", ex));
                    }
                };

                // We must wait for command channel to completely shutdown before trying to restart...
                restartServerCommandChannel.DelayAndExecute(2000);
            }
            else
            {
                OnStatusMessage(MessageLevel.Info, "Data publisher command channel stopped.");
            }
        }

        private void ServerCommandChannelSendClientDataException(object sender, EventArgs<Guid, Exception> e)
        {
            Exception ex = e.Argument2;

            if (!HandleSocketException(e.Argument1, ex) && !(ex is NullReferenceException) && !(ex is ObjectDisposedException))
                OnProcessException(MessageLevel.Info, new InvalidOperationException($"Data publisher encountered an exception while sending command channel data to client connection: {ex.Message}", ex));
        }

        private void ServerCommandChannelReceiveClientDataException(object sender, EventArgs<Guid, Exception> e)
        {
            Exception ex = e.Argument2;

            if (!HandleSocketException(e.Argument1, ex) && !(ex is NullReferenceException) && !(ex is ObjectDisposedException))
                OnProcessException(MessageLevel.Info, new InvalidOperationException($"Data publisher encountered an exception while receiving command channel data from client connection: {ex.Message}", ex));
        }

        #endregion

        #region [ Client Command Channel Event Handlers ]

        private void ClientCommandChannelConnectionEstablished(object sender, EventArgs e)
        {
            m_proxyClientID = Guid.NewGuid();
            ServerCommandChannelClientConnected(sender, new EventArgs<Guid>(m_proxyClientID));
        }

        private void ClientCommandChannelConnectionTerminated(object sender, EventArgs e)
        {
            ServerCommandChannelClientDisconnected(sender, new EventArgs<Guid>(m_proxyClientID));
        }

        private void ClientCommandChannelConnectionException(object sender, EventArgs<Exception> e)
        {
            Exception ex = e.Argument;
            OnProcessException(MessageLevel.Info, new ConnectionException($"Data publisher encountered an exception while attempting client-based command channel subscriber connection: {ex.Message}", ex));
        }

        private void ClientCommandChannelConnectionAttempt(object sender, EventArgs e)
        {
            // Inject a short delay between multiple connection attempts
            if (m_commandChannelConnectionAttempts > 0)
                Thread.Sleep(2000);

            OnStatusMessage(MessageLevel.Info, "Attempting client-based command channel connection to subscriber...");
            m_commandChannelConnectionAttempts++;
        }

        private void ClientCommandChannelSendDataException(object sender, EventArgs<Exception> e)
        {
            Exception ex = e.Argument;

            if (!HandleSocketException(m_proxyClientID, ex) && !(ex is ObjectDisposedException))
                OnProcessException(MessageLevel.Info, new InvalidOperationException($"Data publisher encountered an exception while sending client-based command channel data to subscriber connection: {ex.Message}", ex));
        }

        private void ClientCommandChannelReceiveDataComplete(object sender, EventArgs<byte[], int> e)
        {
            ServerCommandChannelReceiveClientDataComplete(sender, new EventArgs<Guid, byte[], int>(m_proxyClientID, e.Argument1, e.Argument2));
        }

        private void ClientCommandChannelReceiveDataException(object sender, EventArgs<Exception> e)
        {
            Exception ex = e.Argument;

            if (!HandleSocketException(m_proxyClientID, ex) && !(ex is ObjectDisposedException))
                OnProcessException(MessageLevel.Info, new InvalidOperationException($"Data publisher encountered an exception while receiving client-based command channel data from subscriber connection: {ex.Message}", ex));
        }

        #endregion

        #endregion

        #region [ Static ]

        // Static Fields

        // Constant zero length integer byte array
        private static readonly byte[] ZeroLengthBytes = { 0, 0, 0, 0 };

        #endregion
    }
}
