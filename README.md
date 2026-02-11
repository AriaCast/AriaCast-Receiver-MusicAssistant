# AriaCast Receiver for Music Assistant

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54) ![Android](https://img.shields.io/badge/Android-3DDC84?style=for-the-badge&logo=android&logoColor=white)

Stream audio wirelessly from your Android device to Music Assistant players. Think of it as **AirPlay for Android** - cast audio from any app on your phone to any speaker connected to Music Assistant.<br><br>

<img src="https://github.com/user-attachments/assets/8ba869cf-5ee8-4021-90d7-30ad6da3e065" width="45%" />
&nbsp;&nbsp;&nbsp;&nbsp;
<img src="https://github.com/user-attachments/assets/cd89d6e2-bab5-4a36-bdc4-d226d7a3ce71" width="45%" /><br>



## Features

- 🎵 **High-Quality Audio**: 48kHz 16-bit stereo PCM streaming
- 📱 **Easy Discovery**: Automatic server detection via UDP broadcast
- 🎼 **Full Metadata**: Track title, artist, album, artwork, duration, and position
- 🎮 **Remote Control**: Play, pause, next, previous commands from Music Assistant UI
- 🔀 **Flexible Routing**: Stream to any Music Assistant player or group

## Quick Start

### Installation

Clone this repository in the providers folder of your Music Assistant instance

### Configuration

1. Enable **AriaCast Receiver** in Music Assistant settings
2. Configure basic settings:
   - **Server Name**: How it appears in discovery (default: "Music Assistant")
   - **Target Player**: Auto or specific player
   - **Ports**: 12888 (discovery), 12889 (streaming)

### Usage

1. Install the [AriaCast Android app](https://github.com/AirPlr/AriaCast-app)
2. Open the app - it will automatically discover servers on your network
3. Select your Music Assistant server
4. Tap "Start Casting"
5. Play audio from any app - it streams to Music Assistant!

## Configuration Options

| Setting | Default | Description |
|---------|---------|-------------|
| Server Name | AriaCast Speaker | Name shown in client discovery |
| Connected Player | Auto | Target Music Assistant player |
| Streaming Port | 12889 | WebSocket/HTTP port for all endpoints |
| Discovery Port | 12888 | UDP discovery port |
| Allow Player Switching | Yes | Enable manual source selection |

## Protocol Specification

### Endpoints

| Endpoint | Type | Direction | Purpose |
|----------|------|-----------|---------|
| UDP `:12888` | Datagram | App → Server | Discovery broadcast |
| `/audio` | WebSocket | App → Server | PCM audio stream |
| `/control` | WebSocket | Server → App | Media control commands |
| `/metadata` | HTTP POST | App → Server | Track metadata updates |
| `/stats` | WebSocket | Server → App | Buffer statistics |

### 1. UDP Discovery

**App sends:** `DISCOVER_AUDIOCAST` (broadcast to port 12888)

**Server responds:**
\`\`\`json
{
  "server_name": "AriaCast Speaker",
  "ip": "192.168.1.100",
  "port": 12889,
  "samplerate": 48000,
  "channels": 2
}
\`\`\`

### 2. Audio Streaming (`/audio` WebSocket)

**Handshake (Server → App):**
\`\`\`json
{
  "status": "READY",
  "type": "handshake",
  "sampleRate": 48000,
  "channels": 2,
  "sampleWidth": 2,
  "frameSize": 3840
}
\`\`\`

**Audio Data (App → Server):**
- Binary WebSocket frames
- Exactly **3840 bytes** per frame (20ms of audio)
- Format: PCM 16-bit signed little-endian, stereo, 48kHz

### 3. Metadata Updates (`POST /metadata`)

**Request:**
\`\`\`json
{
  "data": {
    "title": "Song Title",
    "artist": "Artist Name",
    "album": "Album Name",
    "artworkUrl": "https://example.com/cover.jpg",
    "durationMs": 180000,
    "positionMs": 45000,
    "isPlaying": true
  }
}
\`\`\`

**Response:** `200 OK`

### 4. Control Commands (`/control` WebSocket)

**Server sends to App:**
\`\`\`json
{"action": "play"}
{"action": "pause"}
{"action": "next"}
{"action": "previous"}
{"action": "toggle"}
{"action": "stop"}
\`\`\`

### 5. Statistics (`/stats` WebSocket)

**Server sends periodically:**
\`\`\`json
{
  "bufferedFrames": 15,
  "droppedFrames": 0,
  "receivedFrames": 12345
}
\`\`\`

## Audio Format

| Parameter | Value |
|-----------|-------|
| Sample Rate | 48000 Hz |
| Channels | 2 (Stereo) |
| Bit Depth | 16-bit signed |
| Encoding | PCM Little-Endian |
| Frame Duration | 20ms |
| Frame Size | 3840 bytes |

## Connection Flow

\`\`\`

1. UDP Discovery                                            
   App broadcasts "DISCOVER_AUDIOCAST" → Server responds    

                              ↓

2. Connection Setup                                         
 Connect to ws://<ip>:12889/audio                       
   • Wait for {"status": "READY"} handshake                 
   • Connect to ws://<ip>:12889/control                     
   • Connect to ws://<ip>:12889/stats                       

                              ↓

3. Streaming                                                
   • Send 3840-byte PCM frames to /audio                    
   • POST metadata changes to /metadata                     
   • Receive control commands from /control                 

\`\`\`

## Troubleshooting

### Server not found in app
1. Ensure both devices are on the same network
2. Check firewall allows UDP port 12888 and TCP port 12889
3. Verify Music Assistant is running with the plugin enabled

### No audio playback
1. Check a Music Assistant player is available
2. Verify player is not already in use by another source
3. Check Music Assistant logs for errors
4. Ensure audio frames are exactly 3840 bytes

### Metadata not showing
1. Verify the app is sending POST requests to `/metadata` in the settings
2. Check Music Assistant logs for metadata-related messages

### Control commands not working
1. Verify the `/control` WebSocket connection is established
2. Check the app is listening for incoming messages
3. Ensure the app has an active media session to control

## Related Projects

- **AriaCast Android App**: [github.com/AirPlr/AriaCast-app](https://github.com/AirPlr/AriaCast-app)
- **AriaCast Standalone Server**: [github.com/AirPlr/Ariacast-server](https://github.com/AirPlr/Ariacast-server)

## Contributing

Report issues with:
- Music Assistant version
- Provider version  
- Client app version
- Debug logs (`--log-level debug`)

## Credits

Built for Music Assistant by the community. Born out of the need for better Android casting integration.



