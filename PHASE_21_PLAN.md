# Phase 21: Web UI - Stream Dashboard

## Overview
Implement a comprehensive Stream Dashboard in the Web UI that allows users to view, monitor, and manage event streams. This dashboard will provide insights into stream health, consumer group status, and offset management.

## Goals
- Provide a user-friendly interface for viewing all streams
- Display stream statistics and health metrics
- Show consumer group status and lag information
- Visualize offset progression and consumer lag
- Enable basic stream management operations

## Components to Implement

### 1. Backend Endpoints (if needed)
- [ ] **GET /api/v1/streams** - List all streams (with optional tenant/namespace filters)
  - Similar to `ListQueues` endpoint
  - Returns array of stream metadata (tenant, namespace, name, partitions, created_at, etc.)
  
- [ ] **GET /api/v1/streams/{tenant}/{namespace}/{name}/stats** - Get stream statistics
  - Total events/messages
  - Latest offset per partition
  - Total size (optional)
  - Created/updated timestamps

- [ ] **GET /api/v1/streams/{tenant}/{namespace}/{name}/consumer-groups** - List consumer groups
  - All consumer groups for a stream
  - Consumer group state per partition (offset, lag, etc.)

### 2. Frontend Components

#### StreamList Component
- [ ] Display list of all streams in a table/card view
- [ ] Show tenant, namespace, name, partitions
- [ ] Allow filtering by tenant/namespace
- [ ] Click to view stream details
- [ ] Loading and error states

#### StreamDetailsView Component
- [ ] Stream metadata display (tenant, namespace, name, partitions)
- [ ] Stream statistics card
- [ ] Latest offsets per partition
- [ ] Consumer groups list
- [ ] Auto-refresh with polling

#### StreamStatsCard Component
- [ ] Total events count
- [ ] Latest offset per partition
- [ ] Total size (if available)
- [ ] Created/updated timestamps
- [ ] Visual indicators for stream health

#### ConsumerGroupList Component
- [ ] List of consumer groups for the stream
- [ ] Consumer group name
- [ ] Committed offset per partition
- [ ] Consumer lag per partition
- [ ] Latest offset per partition
- [ ] Status indicators (healthy/warning/error based on lag)

#### ConsumerLagChart Component
- [ ] Visual representation of consumer lag
- [ ] Bar chart or line chart showing lag per partition
- [ ] Color coding (green = low lag, yellow = medium, red = high)
- [ ] Tooltips with detailed information

#### PartitionOffsetView Component
- [ ] Display offsets per partition
- [ ] Latest offset
- [ ] Committed offset per consumer group
- [ ] Visual progress indicators

### 3. React Hooks

#### useStreamList Hook
- [ ] Fetch list of streams
- [ ] Support tenant/namespace filtering
- [ ] Handle loading and error states
- [ ] Optional refetch functionality

#### useStreamStats Hook
- [ ] Fetch stream statistics
- [ ] Polling support (configurable interval, default 5s)
- [ ] Handle loading and error states
- [ ] Last checked timestamp

#### useConsumerGroups Hook
- [ ] Fetch consumer groups for a stream
- [ ] Polling support for real-time updates
- [ ] Handle loading and error states

### 4. API Client Functions

#### Stream API Functions (lib/api.ts)
- [ ] `listStreams(tenant?, namespace?)` - List all streams
- [ ] `getStreamStats(tenant, namespace, name)` - Get stream statistics
- [ ] `listConsumerGroups(tenant, namespace, name)` - List consumer groups for a stream
- [ ] `getConsumerGroupState(tenant, namespace, name, consumerGroup, partition?)` - Get consumer group state

### 5. TypeScript Types

#### API Types (types/api.ts)
- [ ] `StreamListItem` - Stream metadata for list view
- [ ] `ListStreamsResponse` - Response from list streams endpoint
- [ ] `StreamStats` - Stream statistics
- [ ] `StreamStatsResponse` - Response from stats endpoint
- [ ] `ConsumerGroupListItem` - Consumer group info
- [ ] `ListConsumerGroupsResponse` - Response from list consumer groups endpoint
- [ ] `ConsumerGroupState` - Already exists, verify completeness

### 6. StreamsPage Update

- [ ] Integrate StreamList and StreamDetailsView
- [ ] Two-column layout (list on left, details on right)
- [ ] Stream selection state management
- [ ] Empty state when no streams exist
- [ ] Error handling and display

## Implementation Order

1. **Backend Endpoints** (if needed)
   - Implement `ListStreams` handler
   - Implement `GetStreamStats` handler
   - Implement `ListConsumerGroups` handler
   - Add routes in router

2. **API Client & Types**
   - Add TypeScript types for new endpoints
   - Implement API client functions
   - Test API calls

3. **React Hooks**
   - Implement `useStreamList`
   - Implement `useStreamStats`
   - Implement `useConsumerGroups`

4. **UI Components**
   - StreamList component
   - StreamStatsCard component
   - ConsumerGroupList component
   - ConsumerLagChart component (optional, can use simple visualization initially)
   - StreamDetailsView component

5. **Page Integration**
   - Update StreamsPage with all components
   - Add proper state management
   - Test user interactions

## Testing Checklist

- [ ] Stream list loads correctly
- [ ] Stream selection works
- [ ] Stream statistics display correctly
- [ ] Consumer groups list correctly
- [ ] Consumer lag is calculated and displayed correctly
- [ ] Polling updates work (auto-refresh)
- [ ] Error states are handled gracefully
- [ ] Empty states display properly
- [ ] Works with multiple streams
- [ ] Works with streams that have no consumer groups

## Success Criteria

- Users can view all streams in the system
- Users can see stream statistics and health metrics
- Users can view consumer group status and lag
- Information updates automatically (polling)
- UI is responsive and user-friendly
- Matches the design pattern of Queue Dashboard

## Future Enhancements (Out of Scope for Phase 21)

- Write events through UI
- Read messages through UI
- Offset management UI (commit offsets)
- Consumer group creation/deletion
- Stream creation/deletion
- Real-time updates via WebSocket (instead of polling)

