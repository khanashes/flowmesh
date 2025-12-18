// Common types

export interface ResourcePath {
  tenant: string;
  namespace: string;
  resource_type: "stream" | "queue" | "kv";
  name: string;
}

export interface Status {
  code: number;
  message?: string;
  details?: string;
}

// Health API

export interface HealthCheckResponse {
  status: Status;
}

export interface ReadinessCheckResponse {
  status: Status;
  ready: boolean;
}

// Stream API

export interface Event {
  payload: string | Uint8Array; // base64 encoded bytes or Uint8Array
  headers?: Record<string, string>;
}

export interface WriteEventsRequest {
  events: Event[];
}

export interface WriteEventsResponse {
  status: string;
  message?: string;
  offsets: number[];
}

export interface Message {
  id: string;
  resource_path: string;
  partition: number;
  offset: number;
  payload: string | Uint8Array; // base64 encoded bytes or Uint8Array
  headers?: Record<string, string>;
  created_at: number; // Unix nanoseconds
  schema_version?: number;
}

export interface ReadStreamResponse {
  status: string;
  message?: string;
  messages: Message[];
}

export interface CommitOffsetResponse {
  status: string;
  message?: string;
}

export interface GetOffsetResponse {
  status: string;
  message?: string;
  offset: number; // -1 if no commits yet
}

export interface GetLatestOffsetResponse {
  status: string;
  message?: string;
  offset: number; // -1 if no messages
}

export interface ConsumerGroupState {
  stream: string;
  group: string;
  partition: number;
  committed_offset: number;
  latest_offset: number;
  lag: number;
}

export interface GetConsumerGroupStateResponse {
  status: string;
  message?: string;
  state: ConsumerGroupState;
}

// Queue API

export interface Job {
  id: string;
  resource_path: string;
  seq: number;
  payload: string | Uint8Array; // base64 encoded bytes or Uint8Array
  headers?: Record<string, string>;
  created_at: number; // Unix nanoseconds
  visible_at: number; // Unix nanoseconds
  reserve_until?: number; // Unix nanoseconds, 0 if not reserved
  attempts: number;
}

export interface EnqueueRequest {
  payload: string | Uint8Array;
  delay_seconds?: number;
  headers?: Record<string, string>;
}

export interface EnqueueResponse {
  status: string;
  message?: string;
  job_id: string;
  seq: number;
}

export interface ReserveRequest {
  visibility_timeout_seconds?: number; // default: 30
  long_poll_timeout_seconds?: number; // default: 0
}

export interface ReserveResponse {
  status: string;
  message?: string;
  job?: Job;
}

export interface ReceiveRequest {
  max_jobs?: number; // 1-100, default: 1
  visibility_timeout_seconds?: number; // default: 30
  long_poll_timeout_seconds?: number; // default: 0
}

export interface ReceiveResponse {
  status: string;
  message?: string;
  jobs: Job[];
}

export interface ACKRequest {
  job_id: string;
}

export interface ACKResponse {
  status: string;
  message?: string;
}

export interface NACKRequest {
  job_id: string;
  delay_seconds?: number; // 0 = use backoff
}

export interface NACKResponse {
  status: string;
  message?: string;
}

export interface QueueStats {
  total_jobs: number;
  pending_jobs: number;
  in_flight_jobs: number;
  completed_jobs: number;
  failed_jobs: number;
  oldest_job_age_seconds: number;
}

export interface QueueStatsResponse {
  status: string;
  message?: string;
  stats: QueueStats;
}

export interface RetryPolicy {
  max_attempts: number; // 0 = unlimited
  initial_backoff_seconds: number;
  max_backoff_seconds: number;
  backoff_multiplier: number;
  backoff_strategy: "fixed" | "linear" | "exponential";
}

export interface SetRetryPolicyRequest {
  policy: RetryPolicy;
}

export interface SetRetryPolicyResponse {
  status: string;
  message?: string;
}

export interface GetRetryPolicyResponse {
  status: string;
  message?: string;
  policy: RetryPolicy;
}

export interface ListDLQJobsResponse {
  status: string;
  message?: string;
  jobs: Job[];
}

export interface ReplayDLQJobResponse {
  status: string;
  message?: string;
  job_id: string;
  seq: number;
}

// Queue List API

export interface QueueListItem {
  tenant: string;
  namespace: string;
  name: string;
  type: string;
  partitions: number;
  created_at: string; // ISO 8601 timestamp
  updated_at: string; // ISO 8601 timestamp
  dlq?: {
    enabled: boolean;
    max_attempts?: number;
    dlq_path?: string;
  };
  retry_policy?: RetryPolicy;
}

export interface ListQueuesResponse {
  status: string;
  message?: string;
  queues: QueueListItem[];
}

// Stream types
export interface StreamListItem {
  tenant: string;
  namespace: string;
  name: string;
  type: string;
  partitions: number;
  created_at: string; // ISO 8601 string
  updated_at: string; // ISO 8601 string
}

export interface ListStreamsResponse {
  status: string;
  message?: string;
  streams: StreamListItem[];
}

export interface StreamStats {
  latest_offset: number;
}

export interface StreamStatsResponse {
  status: string;
  message?: string;
  stats: StreamStats;
}

export interface ConsumerGroupInfo {
  group: string;
  partition: number;
  committed_offset: number;
  latest_offset: number;
  lag: number;
}

export interface ListConsumerGroupsResponse {
  status: string;
  message?: string;
  consumer_groups: ConsumerGroupInfo[];
}

// KV API

export interface SetRequest {
  value: string | Uint8Array; // base64 encoded bytes or Uint8Array
  ttl_seconds?: number; // 0 = no expiration
}

export interface SetResponse {
  status: string;
  message?: string;
}

export interface GetResponse {
  status: string;
  message?: string;
  value: string | Uint8Array; // base64 encoded bytes or Uint8Array
}

export interface DeleteResponse {
  status: string;
  message?: string;
}

export interface ExistsResponse {
  status: string;
  message?: string;
  exists: boolean;
}

export interface ListKeysResponse {
  status: string;
  message?: string;
  keys: string[];
}

// Replay API

export interface ReplaySession {
  session_id: string;
  stream: string;
  partition: number;
  start_offset: number; // -1 if using timestamp
  start_timestamp: number; // Unix nanoseconds, 0 if using offset
  end_offset: number; // -1 if not set or using timestamp
  end_timestamp: number; // Unix nanoseconds, 0 if not set or using offset
  sandbox_consumer_group: string;
  status: "created" | "active" | "paused" | "stopped" | "completed" | "error";
  progress: ReplayProgress;
  created_at: number; // Unix nanoseconds
  updated_at: number; // Unix nanoseconds
}

export interface ReplayProgress {
  current_offset: number;
  messages_replayed: number;
  errors: number;
  started_at: number; // Unix nanoseconds, 0 if not started
  paused_at: number; // Unix nanoseconds, 0 if not paused
  completed_at: number; // Unix nanoseconds, 0 if not completed
}

export interface CreateReplaySessionRequest {
  resource_path: ResourcePath;
  start_offset?: number; // -1 if using timestamp
  start_timestamp?: number; // Unix nanoseconds, 0 if using offset
  end_offset?: number; // -1 if not set or using timestamp
  end_timestamp?: number; // Unix nanoseconds, 0 if not set or using offset
  sandbox_consumer_group: string;
}

export interface CreateReplaySessionResponse {
  status: string;
  message?: string;
  session: ReplaySession;
}

export interface GetReplaySessionResponse {
  status: string;
  message?: string;
  session: ReplaySession;
}

export interface ListReplaySessionsResponse {
  status: string;
  message?: string;
  sessions: ReplaySession[];
}

export interface ReplayControlResponse {
  status: string;
  message?: string;
}

// Error types

export interface APIError {
  status: number;
  statusText: string;
  message: string;
  details?: string;
}
