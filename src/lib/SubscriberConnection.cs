//******************************************************************************************************
//  ClientConnection.cs - Gbtc
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
//  06/24/2011 - J. Ritchie Carroll
//       Generated original version of source code.
//  12/20/2012 - Starlynn Danyelle Gilliam
//       Modified Header.
//
//******************************************************************************************************

using GSF;
using GSF.Communication;
using GSF.Diagnostics;
using GSF.IO;
using GSF.Threading;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using TcpClient = GSF.Communication.TcpClient;

namespace sttp;

/// <summary>
/// Represents a <see cref="DataSubscriber"/> client connection to the <see cref="DataPublisher"/>.
/// </summary>
public class SubscriberConnection : IProvideStatus, IDisposable
{
    #region [ Members ]

    // Constants
    private const int EvenKey = 0;      // Even key/IV index
    private const int OddKey = 1;       // Odd key/IV index
    private const int KeyIndex = 0;     // Index of cipher key component in keyIV array
    private const int IVIndex = 1;      // Index of initialization vector component in keyIV array

    // Fields
    private DataPublisher m_parent;
    private Guid m_subscriberID;
    private string m_connectionID;
    private readonly string m_hostName;
    private readonly IPAddress m_ipAddress;
    private string m_subscriberInfo;
    private SubscriberAdapter m_subscription;
    private volatile bool m_authenticated;
    private volatile byte[][][] m_keyIVs;
    private volatile int m_cipherIndex;
    private UdpServer m_dataChannel;
    private string m_configurationString;
    private bool m_connectionEstablished;
    private SharedTimer m_pingTimer;
    private SharedTimer m_reconnectTimer;
    private OperationalModes m_operationalModes;
    private Encoding m_encoding;
    private bool m_disposed;

    #endregion

    #region [ Constructors ]

    /// <summary>
    /// Creates a new <see cref="SubscriberConnection"/> instance.
    /// </summary>
    /// <param name="parent">Parent data publisher.</param>
    /// <param name="clientID">Client ID of associated connection.</param>
    /// <param name="serverCommandChannel"><see cref="TcpServer"/> command channel used to lookup connection information.</param>
    /// <param name="clientCommandChannel"><see cref="TcpClient"/> command channel used to lookup connection information.</param>
    public SubscriberConnection(DataPublisher parent, Guid clientID, IServer serverCommandChannel, IClient clientCommandChannel)
    {
        m_parent = parent;
        ClientID = clientID;
        ServerCommandChannel = serverCommandChannel;
        ClientCommandChannel = clientCommandChannel;
        m_subscriberID = clientID;
        m_keyIVs = null;
        m_cipherIndex = 0;
        CacheUpdateLock = new object();
        PendingCacheUpdateLock = new object();

        // Setup ping timer
        m_pingTimer = Common.TimerScheduler.CreateTimer(5000);
        m_pingTimer.AutoReset = true;
        m_pingTimer.Elapsed += PingTimer_Elapsed;
        m_pingTimer.Start();

        // Setup reconnect timer
        m_reconnectTimer = Common.TimerScheduler.CreateTimer(1000);
        m_reconnectTimer.AutoReset = false;
        m_reconnectTimer.Elapsed += ReconnectTimer_Elapsed;

        LookupEndPointInfo(clientID, GetCommandChannelSocket().RemoteEndPoint as IPEndPoint, ref m_ipAddress, ref m_hostName, ref m_connectionID);
    }

    /// <summary>
    /// Releases the unmanaged resources before the <see cref="SubscriberConnection"/> object is reclaimed by <see cref="GC"/>.
    /// </summary>
    ~SubscriberConnection()
    {
        Dispose(false);
    }

    #endregion

    #region [ Properties ]

    /// <summary>
    /// Gets client ID of this <see cref="SubscriberConnection"/>.
    /// </summary>
    public Guid ClientID { get; }

    /// <summary>
    /// Gets the version of this <see cref="SubscriberConnection"/>.
    /// </summary>
    public int Version { get; internal set; }

    /// <summary>
    /// Gets the current signal index cache of this <see cref="SubscriberAdapter"/>.
    /// </summary>
    public SignalIndexCache SignalIndexCache { get; internal set; }

    /// <summary>
    /// Gets the pending Signal Index Cache.
    /// </summary>
    public SignalIndexCache NextSignalIndexCache { get; internal set; }

    /// <summary>
    /// Gets the lock object for updating Signal Index Cache properties.
    /// </summary>
    internal object CacheUpdateLock { get; }

    /// <summary>
    /// Gets the current Signal Index Cache index, i.e., zero or one.
    /// </summary>
    public int CurrentCacheIndex { get; internal set; }

    /// <summary>
    /// Gets the next Signal Index Cache index, i.e., zero or one.
    /// </summary>
    public int NextCacheIndex { get; internal set; }

    /// <summary>
    /// Gets or sets reference to <see cref="UdpServer"/> data channel, attaching to or detaching from events as needed, associated with this <see cref="SubscriberConnection"/>.
    /// </summary>
    public UdpServer DataChannel
    {
        get => m_dataChannel;
        set
        {
            m_connectionEstablished = value is not null;

            if (m_dataChannel is not null)
            {
                // Detach from events on existing data channel reference
                m_dataChannel.ClientConnectingException -= DataChannel_ClientConnectingException;
                m_dataChannel.SendClientDataException -= DataChannel_SendClientDataException;
                m_dataChannel.ServerStarted -= DataChannel_ServerStarted;
                m_dataChannel.ServerStopped -= DataChannel_ServerStopped;

                if (m_dataChannel != value)
                    m_dataChannel.Dispose();
            }

            // Assign new data channel reference
            m_dataChannel = value;

            if (m_dataChannel is not null)
            {
                // Save UDP settings so channel can be reestablished if needed
                m_configurationString = m_dataChannel.ConfigurationString;

                // Attach to events on new data channel reference
                m_dataChannel.ClientConnectingException += DataChannel_ClientConnectingException;
                m_dataChannel.SendClientDataException += DataChannel_SendClientDataException;
                m_dataChannel.ServerStarted += DataChannel_ServerStarted;
                m_dataChannel.ServerStopped += DataChannel_ServerStopped;
            }
        }
    }

    /// <summary>
    /// Gets <see cref="IServer"/> command channel.
    /// </summary>
    public IServer ServerCommandChannel { get; private set; }

    /// <summary>
    /// Gets <see cref="IClient"/> command channel.
    /// </summary>
    public IClient ClientCommandChannel { get; private set; }

    /// <summary>
    /// Gets <see cref="IServer"/> publication channel - that is, data channel if defined otherwise command channel.
    /// </summary>
    public IServer ServerPublishChannel => m_dataChannel ?? ServerCommandChannel;

    /// <summary>
    /// Gets or sets last publish time for subscriber connection.
    /// </summary>
    public Ticks LastPublishTime { get; set; } = DateTime.UtcNow.Ticks;

    /// <summary>
    /// Gets connected state of the associated client socket.
    /// </summary>
    public bool IsConnected
    {
        get
        {
            bool isConnected = false;

            try
            {
                Socket commandChannelSocket = GetCommandChannelSocket();

                if (commandChannelSocket is not null)
                    isConnected = commandChannelSocket.Connected;
            }
            catch
            {
                isConnected = false;
            }

            return isConnected;
        }
    }

    /// <summary>
    /// Gets or sets IsSubscribed state.
    /// </summary>
    public bool IsSubscribed { get; set; }

    /// <summary>
    /// Gets or sets flag that indicates if the socket exception for "No client found for ID [Guid]" has been thrown.
    /// </summary>
    /// <returns>
    /// Since this message might be thrown many times before the communications channel has had a chance to disconnect
    /// the socket, it is best to stop attempting to send data when this error has been encountered.
    /// </returns>
    // Users have encountered issues when a client disconnects where many thousands of exceptions get thrown, every 3ms.
    // This can cause the entire system to become unresponsive and causes all devices to reset (no data).
    // System only recovers when the client disconnect process finally executes as this can take some time to occur.
    public bool ClientNotFoundExceptionOccurred { get; set; }

    /// <summary>
    /// Gets or sets the <see cref="Guid"/> based subscriber ID of this <see cref="SubscriberConnection"/>.
    /// </summary>
    public Guid SubscriberID
    {
        get => m_subscriberID;
        set
        {
            m_subscriberID = value;

            // When connection ID is just a Guid, prefer subscriber ID over client ID when available
            if (m_connectionID.Equals(ClientID.ToString(), StringComparison.OrdinalIgnoreCase) && m_subscriberID != Guid.Empty)
                m_connectionID = m_subscriberID.ToString();
        }
    }

    /// <summary>
    /// Gets or sets the subscriber acronym of this <see cref="SubscriberConnection"/>.
    /// </summary>
    public string SubscriberAcronym { get; set; }

    /// <summary>
    /// Gets or sets the subscriber name of this <see cref="SubscriberConnection"/>.
    /// </summary>
    public string SubscriberName { get; set; }

    /// <summary>
    /// Gets or sets subscriber info for this <see cref="SubscriberConnection"/>.
    /// </summary>
    public string SubscriberInfo
    {
        get => string.IsNullOrWhiteSpace(m_subscriberInfo) ? SubscriberName : m_subscriberInfo;
        set
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                m_subscriberInfo = null;
            }
            else
            {
                Dictionary<string, string> settings = value.ParseKeyValuePairs();

                settings.TryGetValue("source", out string source);
                settings.TryGetValue("version", out string version);
                settings.TryGetValue("updatedOn", out string updatedOn);

                m_subscriberInfo = $"{source.ToNonNullNorWhiteSpace("unknown source")} version {version.ToNonNullNorWhiteSpace("?.?.?.?")} updated on {updatedOn.ToNonNullNorWhiteSpace("undefined date")}";
            }
        }
    }

    /// <summary>
    /// Gets the connection identification of this <see cref="SubscriberConnection"/>.
    /// </summary>
    public string ConnectionID => m_connectionID;

    /// <summary>
    /// Gets or sets authenticated state of this <see cref="SubscriberConnection"/>.
    /// </summary>
    public bool Authenticated
    {
        get => m_authenticated;
        set => m_authenticated = value;
    }

    /// <summary>
    /// Gets active and standby keys and initialization vectors.
    /// </summary>
    public byte[][][] KeyIVs => m_keyIVs;

    /// <summary>
    /// Gets current cipher index.
    /// </summary>
    public int CipherIndex => m_cipherIndex;

    /// <summary>
    /// Gets time of last cipher key update.
    /// </summary>
    public Ticks LastCipherKeyUpdateTime { get; private set; }

    /// <summary>
    /// Gets or sets any pending Signal Index Cache.
    /// </summary>
    /// <remarks>
    /// This cache is for holding any updates while waiting for confirmation of
    /// receipt of signal index cache updates from the data subscriber.
    /// </remarks>
    public SignalIndexCache PendingSignalIndexCache { get; set; }

    /// <summary>
    /// Gets the lock object for updating Signal Index Cache properties.
    /// </summary>
    internal object PendingCacheUpdateLock { get; }

    /// <summary>
    /// Gets or sets the list of valid IP addresses that this client can connect from.
    /// </summary>
    public List<IPAddress> ValidIPAddresses { get; set; }

    /// <summary>
    /// Gets the IP address of the remote client connection.
    /// </summary>
    public IPAddress IPAddress => m_ipAddress;

    /// <summary>
    /// Gets or sets subscription associated with this <see cref="SubscriberConnection"/>.
    /// </summary>
    internal SubscriberAdapter Subscription
    {
        get => m_subscription;
        set
        {
            m_subscription = value;

            if (m_subscription is not null)
                m_subscription.Name = m_hostName;
        }
    }

    /// <summary>
    /// Gets the subscriber name of this <see cref="SubscriberConnection"/>.
    /// </summary>
    public string Name => SubscriberName;

    /// <summary>
    /// Gets or sets a set of flags that define ways in
    /// which the subscriber and publisher communicate.
    /// </summary>
    public OperationalModes OperationalModes
    {
        get => m_operationalModes;
        set
        {
            m_operationalModes = value;

            m_encoding = (OperationalEncoding)(value & OperationalModes.EncodingMask) switch
            {
                OperationalEncoding.UTF16LE => Encoding.Unicode,
                OperationalEncoding.UTF16BE => Encoding.BigEndianUnicode,
                OperationalEncoding.UTF8 => Encoding.UTF8,
                _ => throw new InvalidOperationException($"Unsupported encoding detected: {value}")
            };
        }
    }

    /// <summary>
    /// Character encoding used to send messages to subscriber.
    /// </summary>
    public Encoding Encoding => m_encoding ?? Encoding.Unicode;

    /// <summary>
    /// Gets a formatted message describing the status of this <see cref="SubscriberConnection"/>.
    /// </summary>
    public string Status
    {
        get
        {
            StringBuilder status = new();

            status.AppendLine();
            status.AppendLine($"             Subscriber ID: {m_connectionID}");
            status.AppendLine($"           Subscriber name: {SubscriberName}");
            status.AppendLine($"        Subscriber acronym: {SubscriberAcronym}");
            status.AppendLine($"  Publish channel protocol: {ServerPublishChannel?.TransportProtocol.ToString() ?? "Not configured"}");
            status.AppendLine($"      Data packet security: {(m_parent?.SecurityMode == SecurityMode.TLS && m_dataChannel is null ? "Secured via TLS" : m_keyIVs is null ? "Unencrypted" : "AES Encrypted")}");
            status.AppendLine($"       Current cache index: {CurrentCacheIndex}");
            status.AppendLine($"Signal index cache records: {SignalIndexCache?.Reference?.Count ?? 0:N0}");

            IServer serverCommandChannel = ServerCommandChannel;

            if (serverCommandChannel is not null)
            {
                status.AppendLine();
                status.Append(serverCommandChannel.Status);
            }

            IClient clientCommandChannel = ClientCommandChannel;

            if (clientCommandChannel is not null)
            {
                status.AppendLine();
                status.Append(clientCommandChannel.Status);
            }

            if (m_dataChannel is not null)
            {
                status.AppendLine();
                status.Append(m_dataChannel.Status);
            }

            return status.ToString();
        }
    }

    #endregion

    #region [ Methods ]

    /// <summary>
    /// Releases all the resources used by the <see cref="SubscriberConnection"/> object.
    /// </summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Releases the unmanaged resources used by the <see cref="SubscriberConnection"/> object and optionally releases the managed resources.
    /// </summary>
    /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (m_disposed)
            return;

        try
        {
            if (!disposing)
                return;

            if (m_pingTimer is not null)
            {
                m_pingTimer.Elapsed -= PingTimer_Elapsed;
                m_pingTimer.Dispose();
                m_pingTimer = null;
            }

            if (m_reconnectTimer is not null)
            {
                m_reconnectTimer.Elapsed -= ReconnectTimer_Elapsed;
                m_reconnectTimer.Dispose();
                m_reconnectTimer = null;
            }

            DataChannel = null;
            ServerCommandChannel = null;
            ClientCommandChannel = null;
            m_subscription = null;
            m_parent = null;
        }
        finally
        {
            m_disposed = true;  // Prevent duplicate dispose.
        }
    }

    /// <summary>
    /// Creates or updates cipher keys.
    /// </summary>
    internal void UpdateKeyIVs()
    {
        using (AesManaged symmetricAlgorithm = new())
        {
            symmetricAlgorithm.KeySize = 256;
            symmetricAlgorithm.GenerateKey();
            symmetricAlgorithm.GenerateIV();

            if (m_keyIVs is null)
            {
                // Initialize new key set
                m_keyIVs = new byte[2][][];
                m_keyIVs[EvenKey] = new byte[2][];
                m_keyIVs[OddKey] = new byte[2][];

                m_keyIVs[EvenKey][KeyIndex] = symmetricAlgorithm.Key;
                m_keyIVs[EvenKey][IVIndex] = symmetricAlgorithm.IV;

                symmetricAlgorithm.GenerateKey();
                symmetricAlgorithm.GenerateIV();

                m_keyIVs[OddKey][KeyIndex] = symmetricAlgorithm.Key;
                m_keyIVs[OddKey][IVIndex] = symmetricAlgorithm.IV;

                m_cipherIndex = EvenKey;
            }
            else
            {
                int oldIndex = m_cipherIndex;

                // Generate a new key set for current cipher index
                m_keyIVs[oldIndex][KeyIndex] = symmetricAlgorithm.Key;
                m_keyIVs[oldIndex][IVIndex] = symmetricAlgorithm.IV;

                // Set run-time to the other key set
                m_cipherIndex = oldIndex ^ 1;
            }
        }

        LastCipherKeyUpdateTime = DateTime.UtcNow.Ticks;
    }

    /// <summary>
    /// Rotates or initializes the crypto keys for this <see cref="SubscriberConnection"/>.
    /// </summary>
    public bool RotateCipherKeys()
    {
        // Make sure at least a second has passed before next key rotation
        if ((DateTime.UtcNow.Ticks - LastCipherKeyUpdateTime).ToMilliseconds() >= 1000.0D)
        {
            try
            {
                // Since this function cannot be not called more than once per second there
                // is no real benefit to maintaining these memory streams at a member level
                using (BlockAllocatedMemoryStream response = new())
                {
                    byte[] bytes;

                    // Create or update cipher keys and initialization vectors 
                    UpdateKeyIVs();

                    // Add current cipher index to response
                    response.WriteByte((byte)m_cipherIndex);

                    // Serialize new keys
                    using (BlockAllocatedMemoryStream buffer = new())
                    {
                        // Write even key
                        byte[] bufferLen = BigEndian.GetBytes(m_keyIVs[EvenKey][KeyIndex].Length);
                        buffer.Write(bufferLen, 0, bufferLen.Length);
                        buffer.Write(m_keyIVs[EvenKey][KeyIndex], 0, m_keyIVs[EvenKey][KeyIndex].Length);

                        // Write even initialization vector
                        bufferLen = BigEndian.GetBytes(m_keyIVs[EvenKey][IVIndex].Length);
                        buffer.Write(bufferLen, 0, bufferLen.Length);
                        buffer.Write(m_keyIVs[EvenKey][IVIndex], 0, m_keyIVs[EvenKey][IVIndex].Length);

                        // Write odd key
                        bufferLen = BigEndian.GetBytes(m_keyIVs[OddKey][KeyIndex].Length);
                        buffer.Write(bufferLen, 0, bufferLen.Length);
                        buffer.Write(m_keyIVs[OddKey][KeyIndex], 0, m_keyIVs[OddKey][KeyIndex].Length);

                        // Write odd initialization vector
                        bufferLen = BigEndian.GetBytes(m_keyIVs[OddKey][IVIndex].Length);
                        buffer.Write(bufferLen, 0, bufferLen.Length);
                        buffer.Write(m_keyIVs[OddKey][IVIndex], 0, m_keyIVs[OddKey][IVIndex].Length);

                        // Get bytes from serialized buffer
                        bytes = buffer.ToArray();
                    }

                    // Add serialized key response
                    response.Write(bytes, 0, bytes.Length);

                    // Send cipher key updates
                    m_parent.SendClientResponse(ClientID, ServerResponse.UpdateCipherKeys, ServerCommand.Subscribe, response.ToArray());
                }

                // Send success message
                m_parent.SendClientResponse(ClientID, ServerResponse.Succeeded, ServerCommand.RotateCipherKeys, "New cipher keys established.");
                m_parent.OnStatusMessage(MessageLevel.Info, $"{ConnectionID} cipher keys rotated.");

                return true;
            }
            catch (Exception ex)
            {
                // Send failure message
                m_parent.SendClientResponse(ClientID, ServerResponse.Failed, ServerCommand.RotateCipherKeys, $"Failed to establish new cipher keys: {ex.Message}");
                m_parent.OnStatusMessage(MessageLevel.Warning, $"Failed to establish new cipher keys for {ConnectionID}: {ex.Message}");

                return false;
            }
        }

        m_parent.SendClientResponse(ClientID, ServerResponse.Failed, ServerCommand.RotateCipherKeys, "Cipher key rotation skipped, keys were already rotated within last second.");
        m_parent.OnStatusMessage(MessageLevel.Warning, $"Cipher key rotation skipped for {ConnectionID}, keys were already rotated within last second.");

        return false;
    }

    /// <summary>
    /// Gets the <see cref="Socket"/> instance used by this client
    /// connection to send and receive data over the command channel.
    /// </summary>
    /// <returns>The socket instance used by the client to send and receive data over the command channel.</returns>
    public Socket GetCommandChannelSocket()
    {
        return ServerCommandChannel switch
        {
            TcpServer tcpServerCommandChannel when tcpServerCommandChannel.TryGetClient(ClientID, out TransportProvider<Socket> tcpProvider) => tcpProvider.Provider,
            TlsServer tlsServerCommandChannel when tlsServerCommandChannel.TryGetClient(ClientID, out TransportProvider<TlsServer.TlsSocket> tlsProvider) => tlsProvider.Provider?.Socket,
            _ => (ClientCommandChannel as TcpClient)?.Client ?? (ClientCommandChannel as TcpSimpleClient)?.Client
        };
    }

    // Send a no-op keep-alive ping to make sure the client is still connected
    private void PingTimer_Elapsed(object sender, EventArgs<DateTime> e)
    {
        m_parent.SendClientResponse(ClientID, ServerResponse.NoOP, ServerCommand.Subscribe);
    }

    private void DataChannel_ClientConnectingException(object sender, EventArgs<Exception> e)
    {
        Exception ex = e.Argument;
        m_parent.OnProcessException(MessageLevel.Info, new InvalidOperationException($"Data channel exception occurred while sending client data to \"{m_connectionID}\": {ex.Message}", ex));
    }

    private void DataChannel_SendClientDataException(object sender, EventArgs<Guid, Exception> e)
    {
        Exception ex = e.Argument2;
        m_parent.OnProcessException(MessageLevel.Info, new InvalidOperationException($"Data channel exception occurred while sending client data to \"{m_connectionID}\": {ex.Message}", ex));
    }

    private void DataChannel_ServerStarted(object sender, EventArgs e)
    {
        m_parent.OnStatusMessage(MessageLevel.Info, "Data channel started.");
    }

    private void DataChannel_ServerStopped(object sender, EventArgs e)
    {
        if (m_connectionEstablished)
        {
            m_parent.OnStatusMessage(MessageLevel.Info, "Data channel stopped unexpectedly, restarting data channel...");
            m_reconnectTimer?.Start();
        }
        else
        {
            m_parent.OnStatusMessage(MessageLevel.Info, "Data channel stopped.");
        }
    }

    private void ReconnectTimer_Elapsed(object sender, EventArgs<DateTime> e)
    {
        try
        {
            m_parent.OnStatusMessage(MessageLevel.Info, "Attempting to restart data channel...");
            DataChannel = null;

            UdpServer dataChannel = new(m_configurationString);
            dataChannel.Start();

            DataChannel = dataChannel;
            m_parent.OnStatusMessage(MessageLevel.Info, "Data channel successfully restarted.");
        }
        catch (Exception ex)
        {
            m_parent.OnStatusMessage(MessageLevel.Warning, $"Failed to restart data channel due to exception: {ex.Message}");
            m_reconnectTimer.Start();
        }
    }

    #endregion

    #region [ Static ]

    // Static Methods

    /// <summary>
    /// Looks up and returns a good end-user connection ID for an <see cref="IPEndPoint"/>.
    /// </summary>
    /// <param name="clientID"><see cref="Guid"/> based client ID.</param>
    /// <param name="remoteEndPoint">Remote <see cref="IPEndPoint"/>.</param>
    /// <returns>End-user connection ID for an <see cref="IPEndPoint"/>.</returns>
    public static string GetEndPointConnectionID(Guid clientID, IPEndPoint remoteEndPoint)
    {
        IPAddress ipAddress = IPAddress.None;
        string hostName = null;
        string connectionID = "";
            
        LookupEndPointInfo(clientID, remoteEndPoint, ref ipAddress, ref hostName, ref connectionID);

        return connectionID;
    }

    /// <summary>
    /// Looks up IP, DNS host name and a good end-user connection ID for an <see cref="IPEndPoint"/>.
    /// </summary>
    /// <param name="clientID"><see cref="Guid"/> based client ID.</param>
    /// <param name="remoteEndPoint">Remote <see cref="IPEndPoint"/>.</param>
    /// <param name="ipAddress">Parsed IP address.</param>
    /// <param name="hostName">Looked-up DNS host name.</param>
    /// <param name="connectionID">String based connection identifier for human reference.</param>
    public static void LookupEndPointInfo(Guid clientID, IPEndPoint remoteEndPoint, ref IPAddress ipAddress, ref string hostName, ref string connectionID)
    {
        // Attempt to lookup remote connection identification for logging purposes
        try
        {
            if (remoteEndPoint is not null)
            {
                ipAddress = remoteEndPoint.Address;

                connectionID = remoteEndPoint.AddressFamily == AddressFamily.InterNetworkV6 ?
                    $"[{ipAddress}]:{remoteEndPoint.Port}" :
                    $"{ipAddress}:{remoteEndPoint.Port}";

                try
                {
                    IPHostEntry ipHost = Dns.GetHostEntry(remoteEndPoint.Address);

                    if (!string.IsNullOrWhiteSpace(ipHost.HostName))
                    {
                        hostName = ipHost.HostName;
                        connectionID = $"{hostName} ({connectionID})";
                    }
                }

                // Just ignoring possible DNS lookup failures...
                catch (ArgumentNullException)
                {
                    // The hostNameOrAddress parameter is null. 
                }
                catch (ArgumentOutOfRangeException)
                {
                    // The length of hostNameOrAddress parameter is greater than 255 characters. 
                }
                catch (ArgumentException)
                {
                    // The hostNameOrAddress parameter is an invalid IP address. 
                }
                catch (SocketException)
                {
                    // An error was encountered when resolving the hostNameOrAddress parameter.    
                }
            }
        }
        catch
        {
            // At worst, we'll just use the client GUID for identification
            connectionID = clientID.ToString();
        }

        if (string.IsNullOrWhiteSpace(connectionID))
            connectionID = null;

        if (string.IsNullOrWhiteSpace(hostName))
            hostName = ipAddress is null ? connectionID : ipAddress.ToString();

        ipAddress ??= IPAddress.None;
    }

    #endregion
}