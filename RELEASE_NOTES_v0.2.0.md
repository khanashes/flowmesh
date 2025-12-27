# FlowMesh v0.2.0 Release Notes

**Release Date:** December 28, 2025

## 🚀 Major Features

### Real-time WebSocket Support
FlowMesh now includes full WebSocket support for real-time updates in the Web UI. The dashboard updates automatically when data changes, eliminating the need for manual refresh.

- **Topic-based Subscriptions**: Subscribe to specific resource updates (streams, queues, replay sessions, KV stores)
- **Automatic Reconnection**: Built-in reconnection logic with exponential backoff
- **Polling Fallback**: Automatically falls back to HTTP polling if WebSocket is unavailable
- **Connection Status Indicator**: Visual indicator in the UI header showing connection status

### KV Dashboard UI
Complete UI for managing KV stores:
- List all KV stores with filtering
- View, edit, and delete keys
- Set TTL (Time To Live) for keys
- Real-time updates when keys are modified

### Enhanced Production Features
- **Authentication**: Token validation in gRPC interceptors with proper AuthContext
- **Tracing**: Configurable sampling strategies (always, probabilistic, rate-based) for OpenTelemetry
- **Checkpoint Recovery**: Full state collection for system recovery after crashes
- **Comprehensive Documentation**: Complete API reference and deployment guides

## 📚 Documentation

- **API Documentation** (`docs/API.md`): Complete reference for all gRPC and HTTP endpoints
- **Deployment Guide** (`docs/DEPLOYMENT.md`): Production deployment instructions
- **WebSocket Testing Guide** (`TESTING_WEBSOCKET.md`): How to test WebSocket functionality

## 🔧 Improvements

- Frontend hooks now use WebSocket for real-time updates
- Better error handling and connection management
- Improved auth interceptor with health check skipping

## 🐛 Fixes

- Fixed replay handler tests for correct stream path format
- Improved WebSocket error handling

## 🔗 Links

- **GitHub**: https://github.com/flowmesh/flowmesh
- **Documentation**: See `docs/` directory
- **Previous Release**: [v0.1.0](RELEASE_NOTES_v0.1.0.md)

## 📊 What's Next

- Node.js SDK
- Multi-node clustering
- Partition replication with Raft/Paxos
- Leader election

---

**Full Changelog**: [v0.1.0...v0.2.0](https://github.com/flowmesh/flowmesh/compare/v0.1.0...v0.2.0)

