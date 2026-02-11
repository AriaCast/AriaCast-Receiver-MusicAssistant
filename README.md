# AriaCast Receiver for Music Assistant

![Android](https://img.shields.io/badge/Android-3DDC84?style=for-the-badge&logo=android&logoColor=white)

**Cast audio from any Android app to your Music Assistant speakers - like AirPlay for Android.**

Stream music, podcasts, videos, games - anything playing on your Android device - wirelessly to any speaker or group in your Music Assistant ecosystem.

<img src="https://github.com/user-attachments/assets/8ba869cf-5ee8-4021-90d7-30ad6da3e065" width="45%" />
&nbsp;&nbsp;&nbsp;&nbsp;
<img src="https://github.com/user-attachments/assets/cd89d6e2-bab5-4a36-bdc4-d226d7a3ce71" width="45%" />

---

## ✨ Features

- 🎵 **High-Quality Streaming** - 48kHz 16-bit stereo PCM audio
- 📱 **Zero-Config Discovery** - Servers automatically detected on your network
- 🎼 **Rich Metadata** - Album art, track info, playback position
- 🎮 **Bidirectional Control** - Control playback from both Music Assistant and your phone
- 🔀 **Flexible Routing** - Stream to any player or speaker group
- 🔌 **Universal Compatibility** - Works with any Android audio source

---

## 🚀 Quick Start

### Installation

Install this plugin as a custom provider in Music Assistant:

1. Navigate to your Music Assistant `providers` directory
2. Clone this repository:
   ```bash
   cd providers
   git clone https://github.com/AirPlr/AriaCast-Receiver-MusicAssistant.git
   ```
3. Restart Music Assistant

### Setup

1. **Enable the Plugin**
   - Go to Music Assistant Settings → Providers
   - Find **AriaCast Receiver** and click Enable

2. **Configure Settings** (optional)
   - **Connected Player**: Choose a specific player or leave on Auto
   - **Allow Player Switching**: Enable if you want manual control

3. **Install Android App**
   - Download [AriaCast from GitHub](https://github.com/AirPlr/AriaCast-app)
   - Install on your Android device

### Start Casting

1. Open the AriaCast app on your Android device
2. Your Music Assistant server will appear automatically
3. Tap to connect
4. Start playing audio from any app - it now streams to Music Assistant!

---

## ⚙️ Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| **Connected Player** | Auto | Target Music Assistant player. "Auto" uses currently playing player or first available. |
| **Allow Player Switching** | Yes | Allows you to manually select which player receives the audio stream. |

The plugin automatically configures networking on standard ports (12888 for discovery, 12889 for streaming).

---

## 🔧 How It Works

### Architecture Overview

```
┌──────────────────┐         UDP Discovery        ┌─────────────────────┐
│  Android Device  │ ←────────────────────────────→ │  Music Assistant    │
│  (AriaCast App)  │                                │  (This Plugin)      │
└──────────────────┘                                └─────────────────────┘
         │                                                    │
         │         WebSocket: PCM Audio Stream                │
         ├───────────────────────────────────────────────────→│
         │                                                    │
         │         HTTP POST: Metadata Updates                │
         ├───────────────────────────────────────────────────→│
         │                                                    │
         │      WebSocket: Control Commands (play/pause)      │
         │←───────────────────────────────────────────────────┤
         │                                                    │
         │      WebSocket: Buffer Statistics (optional)       │
         │←───────────────────────────────────────────────────┤
```

### Connection Flow

1. **Discovery Phase**
   - Android app broadcasts `DISCOVER_AUDIOCAST` on UDP port 12888
   - Plugin responds with server info (IP, ports, audio format)

2. **Connection Phase**
   - App connects WebSocket: `/audio` (audio stream)
   - App connects WebSocket: `/control` (receive commands)
   - App connects WebSocket: `/stats` (buffer monitoring)

3. **Streaming Phase**
   - App sends 3840-byte PCM frames (20ms audio chunks)
   - App sends metadata via POST to `/metadata`
   - Plugin forwards audio to selected Music Assistant player
   - Plugin sends control commands back to app when needed

---

## 📡 Technical Specification

### Audio Format

| Parameter | Value |
|-----------|-------|
| Sample Rate | 48000 Hz |
| Channels | 2 (Stereo) |
| Bit Depth | 16-bit signed |
| Encoding | PCM Little-Endian |
| Frame Duration | 20 ms |
| Frame Size | 3840 bytes |

### Network Endpoints

| Endpoint | Type | Port | Purpose |
|----------|------|------|---------|
| UDP Discovery | Datagram | 12888 | Server discovery |
| `/audio` | WebSocket | 12889 | PCM audio streaming |
| `/control` | WebSocket | 12889 | Playback control commands |
| `/metadata` | HTTP POST | 12889 | Track metadata updates |
| `/stats` | WebSocket | 12889 | Buffer statistics |

### Discovery Protocol

**Client broadcasts:**
```
DISCOVER_AUDIOCAST
```

**Server responds (JSON):**
```json
{
  "server_name": "Music Assistant",
  "ip": "192.168.1.100",
  "port": 12889,
  "samplerate": 48000,
  "channels": 2
}
```

### Audio Streaming Protocol

**Initial handshake (Server → Client):**
```json
{
  "status": "READY",
  "type": "handshake",
  "sampleRate": 48000,
  "channels": 2,
  "sampleWidth": 2,
  "frameSize": 3840
}
```

**Audio frames (Client → Server):**
- Binary WebSocket messages
- Exactly 3840 bytes per frame
- Format: PCM 16-bit signed LE, stereo, 48kHz

### Metadata Protocol

**POST /metadata** (JSON)
```json
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
```

### Control Protocol

**WebSocket messages (Server → Client):**
```json
{"action": "play"}
{"action": "pause"}
{"action": "next"}
{"action": "previous"}
{"action": "toggle"}
{"action": "stop"}
```

---

## 🐛 Troubleshooting

### Server Not Appearing in App

- ✅ Verify both devices are on the same network/VLAN
- ✅ Check firewall allows UDP port 12888 (discovery)
- ✅ Check firewall allows TCP port 12889 (streaming)
- ✅ Ensure the plugin is enabled in Music Assistant
- ✅ Try disabling/re-enabling the plugin

### No Audio Playback

- ✅ Verify at least one player is available in Music Assistant
- ✅ Check the selected player isn't already in use
- ✅ Check Music Assistant logs for error messages
- ✅ Test the player with other audio sources
- ✅ Verify network bandwidth is sufficient for 48kHz stereo

### Metadata Not Displaying

- ✅ Check Music Assistant logs for metadata parsing errors
- ✅ Verify the Android app is sending metadata (check app settings)
- ✅ Ensure the artwork URL is publicly accessible

### Playback Controls Not Working

- ✅ Verify the `/control` WebSocket connection is established
- ✅ Check the Android app has an active media session
- ✅ Check Music Assistant logs for control command errors
- ✅ Try disconnecting and reconnecting the stream

### Audio Stuttering/Dropouts

- ✅ Check WiFi signal strength on Android device
- ✅ Verify network isn't congested
- ✅ Check Music Assistant server CPU usage
- ✅ Try reducing distance between device and WiFi access point
- ✅ Disable power saving mode on Android device

---

## 🔗 Related Projects

- **[AriaCast Android App](https://github.com/AirPlr/AriaCast-app)** - The companion mobile application
- **[AriaCast Standalone Server](https://github.com/AirPlr/Ariacast-server)** - Independent server implementation
- **[Music Assistant](https://music-assistant.io/)** - The open source music player ecosystem

---

## 🤝 Contributing

Contributions are welcome! When reporting issues, please include:

- Music Assistant version
- AriaCast Receiver plugin version
- AriaCast Android app version
- Relevant log output (enable debug logging in Music Assistant)
- Steps to reproduce the issue

---

## 📄 License

This project is developed for the Music Assistant ecosystem.

---

## 💙 Credits

Built by the community for the Music Assistant ecosystem. Special thanks to the [AirPlr](https://github.com/AirPlr) team for creating the AriaCast protocol and Android application.

**Maintainers:** Music Assistant Team  
**Protocol:** AirPlr/AriaCast Project



