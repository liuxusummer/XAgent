export interface Message {
  id: string;
  role: 'user' | 'agent' | 'system';
  content: string;
  timestamp: number;
  status?: 'sending' | 'streaming' | 'complete' | 'error' | 'progress';
  toolCalls?: ToolCall[];
  thinking?: string;
  metadata?: {
    exitReason?: string;
    completedAt?: number;
    turn?: number;
  };
}

export interface ToolCall {
  id: string;
  name: string;
  arguments: Record<string, unknown>;
  status: 'pending' | 'running' | 'success' | 'error';
  result?: unknown;
  duration?: number;
}

export interface ChatSession {
  id: string;
  title: string;
  messages: Message[];
  createdAt: number;
  updatedAt: number;
  status: 'idle' | 'running' | 'waiting_for_user' | 'interrupted' | 'error';
  config?: {
    configPath?: string;
    observabilityConfigPath?: string;
    workspaceDir?: string;
    agent?: string;
  };
}

export interface ChatMetadata {
  chat_id: string;
  workspace: string;
  agent: string;
  title: string;
  created_at: number;
  updated_at: number;
  last_message_preview: string;
  message_count: number;
  status: 'idle' | 'running' | 'waiting_for_user' | 'interrupted' | string;
}

export interface ChatState {
  chat_id: string;
  workspace: string;
  agent: string;
  backend_session_id: string;
  checkpoint_id: string;
  resume_available: boolean;
  event_cursor: number;
  runtime_config_key: string;
  messages: Message[];
  llm_history: unknown[];
  waiting_for_user: boolean;
  ask_prompt: string;
  status: 'idle' | 'running' | 'waiting_for_user' | 'interrupted' | string;
  updated_at: number;
}

export interface PersistentChatDetail {
  metadata: ChatMetadata;
  state: ChatState;
}

export interface AgentStatus {
  state: 'idle' | 'thinking' | 'executing' | 'waiting_for_user' | 'interrupted' | 'error';
  currentTurn?: number;
  maxTurns?: number;
  currentTool?: string;
}

export interface SSEEvent {
  id?: number;
  type:
    | 'user_task'
    | 'session_snapshot'
    | 'assistant_delta'
    | 'thinking_delta'
    | 'turn_start'
    | 'tool_call'
    | 'tool_result'
    | 'ask_user'
    | 'user_reply'
    | 'run_done'
    | 'token_usage_delta'
    | 'token_usage_done'
    | 'done'
    | 'log'
    | 'stop'
    | 'error'
    | 'heartbeat';
  data: unknown;
}

export interface TokenUsage {
  input_tokens: number;
  output_tokens: number;
  total_tokens: number;
  cache_creation_input_tokens: number;
  cache_read_input_tokens: number;
  reasoning_tokens: number;
}

export interface LiveTokenUsage {
  session_id: string;
  turn: number;
  usage: TokenUsage;
  totals: TokenUsage;
  updated_at: number;
  running: boolean;
}

export interface UsageSession {
  session_id: string;
  started_at: number;
  ended_at: number;
  duration_ms: number;
  turns: number;
  exit_reason: string;
  event_count: number;
  usage: TokenUsage;
}

export interface UsageSummary {
  configured: boolean;
  log_dir: string;
  message: string;
  totals: TokenUsage;
  sessions: UsageSession[];
  updated_at: number;
}

export type TraceSessionSummary = UsageSession;

export interface TraceEvent {
  session_id: string;
  turn: number;
  kind: string;
  name: string;
  ts: number;
  duration_ms?: number | null;
  data?: Record<string, unknown>;
}

export interface TraceSessionList {
  configured: boolean;
  log_dir: string;
  message: string;
  sessions: TraceSessionSummary[];
  updated_at: number;
}

export interface TraceSessionDetail {
  summary: TraceSessionSummary;
  events: TraceEvent[];
  log_path: string;
}

export interface SubmitTaskRequest {
  task: string;
  session_id?: string;
  chat_id?: string;
  config_path?: string;
  observability_config_path?: string;
  workspace_dir?: string;
  agent?: string;
  team?: string;
  resume?: boolean;
}

export interface ScheduledTask {
  id: string;
  workspace: string;
  name: string;
  prompt: string;
  agent: string;
  repeat: 'none' | 'daily' | 'weekly' | 'custom';
  date: string;
  time: string;
  end_date?: string;
  interval_minutes?: number;
  keep_one_chat: boolean;
  chat_id?: string;
  status: 'running' | 'paused';
  next_run: string | null;
  last_run?: string | null;
  last_session_id?: string;
  last_error?: string;
  last_debug_run?: string | null;
  last_debug_session_id?: string;
  last_debug_error?: string;
  config_path?: string;
  observability_config_path?: string;
  created_at: number;
  updated_at: number;
}

export interface ScheduledTaskWriteRequest {
  ws: string;
  name: string;
  prompt: string;
  agent?: string;
  repeat: 'none' | 'daily' | 'weekly' | 'custom';
  date?: string;
  time?: string;
  end_date?: string;
  interval_minutes?: number;
  keep_one_chat?: boolean;
  status?: 'running' | 'paused';
  config_path?: string;
  observability_config_path?: string;
}

export interface ReplyRequest {
  reply: string;
  session_id?: string;
}

export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: string;
}

export interface WorkspaceAgent {
  name: string;
  description?: string;
  files: string[];
  profile?: AgentProfile;
}

export interface AgentTeamMember {
  agent: string;
  role: string;
  autoDelegate: boolean;
}

export interface AgentTeam {
  name: string;
  description: string;
  leader: string;
  mode: 'manual' | 'leader_delegates' | 'roundtable_review' | string;
  members: AgentTeamMember[];
  created_at?: number;
  updated_at?: number;
}

export interface AgentTeamWorkflowStep {
  id: string;
  agent: string;
  task: string;
  depends_on?: string[];
  context?: string;
  expected_output?: string;
  output?: string;
  on_error?: 'stop' | 'continue' | string;
  max_turns?: number;
}

export interface AgentTeamWorkflow {
  name: string;
  version?: number;
  description?: string;
  steps: AgentTeamWorkflowStep[];
}

export interface WorkspaceSkill {
  name: string;
}

export interface WorkspaceTool {
  name: string;
}

export interface WorkspaceTemplate {
  id: string;
  name: string;
  description: string;
}

export interface WorkspaceCreateRequest {
  name: string;
  template_id: string;
}

export interface WorkspaceCreateResponse {
  name: string;
  template_id: string;
  path: string;
}

export interface WorkspaceFile {
  path: string;
  content: string;
}

export interface WorkspaceFileWriteRequest {
  ws: string;
  path: string;
  content: string;
}

export interface WorkspaceFileWriteResponse {
  path: string;
  content: string;
  bytes: number;
  created: boolean;
}

export interface WorkspaceTreeNode {
  name: string;
  path: string;
  type: 'file' | 'dir';
  children?: WorkspaceTreeNode[];
}

export interface WorkspaceIndexStats {
  status: 'OK';
  exists: boolean;
  workspace: string;
  root: string;
  index_path: string;
  db_size_bytes: number;
  file_count: number;
  bytes: number;
  last_indexed_at: number | null;
}

export interface WorkspaceIndexRefreshResult {
  status: 'OK';
  workspace: string;
  root: string;
  index_path: string;
  scanned: number;
  indexed: number;
  updated: number;
  unchanged: number;
  removed: number;
  skipped: Record<string, number>;
}

export interface WorkspaceIndexMatch {
  path: string;
  score: number;
  match_type: 'path' | 'content';
  line: number | null;
  snippet: string;
  size_bytes: number;
  mtime: number;
}

export interface WorkspaceIndexSearchResult {
  status: 'OK';
  query: string;
  root: string;
  refreshed: boolean;
  index_stats: {
    file_count: number;
    bytes: number;
  };
  refresh_stats?: WorkspaceIndexRefreshResult | null;
  matches: WorkspaceIndexMatch[];
}

export interface WorkspacePreviewFile {
  path: string;
  content: string;
  bytes: number;
  size_bytes: number;
  mtime: number;
  read_only: boolean;
}

export interface EvalDataset {
  id: string;
  name: string;
  format: 'jsonl' | 'json' | 'csv' | string;
  source: Record<string, unknown>;
  created_at: number;
  case_count: number;
  size_bytes: number;
  dataset_path: string;
  imported?: boolean;
  cases?: EvalCase[];
}

export interface EvalCase {
  id: string;
  name: string;
  task: string;
  tags: string[];
  assertions: Record<string, unknown>;
}

export interface EvalRunSummary {
  total: number;
  completed: number;
  passed: number;
  failed: number;
  error: number;
  pass_rate: number;
  failure_rate?: number;
  error_rate?: number;
  avg_duration: number;
  p95_duration?: number;
  avg_turns: number;
  tool_attempts?: number;
  avg_tool_attempts?: number;
  successful_tool_attempts?: number;
  failed_tool_attempts?: number;
  unknown_tool_attempts?: number;
  tool_success_rate?: number;
  recovery_opportunities?: number;
  recovered?: number;
  recovery_rate?: number;
  policy_outcomes?: Record<'allow' | 'deny' | 'require_approval', number>;
  token_usage?: Record<string, number>;
  token_coverage?: number;
  total_token_coverage?: number;
  avg_total_tokens?: number;
  tags?: Record<string, EvalRunSummary>;
}

export interface EvalCaseResult {
  id: string;
  name: string;
  task: string;
  tags: string[];
  status: 'passed' | 'failed' | 'error' | string;
  duration_sec: number;
  turns: number;
  exit_reason: string;
  tool_calls: string[];
  tool_attempts?: number;
  successful_tool_attempts?: number;
  failed_tool_attempts?: number;
  unknown_tool_attempts?: number;
  recovered?: boolean;
  policy_outcomes?: Record<'allow' | 'deny' | 'require_approval', number>;
  token_usage?: Record<string, number>;
  failures: string[];
  response_excerpt: string;
}

export interface EvalRunResult {
  version: number;
  id: string;
  workspace: string;
  dataset_id: string;
  dataset_name: string;
  dataset_digest?: string;
  dataset_source_digest?: string;
  dataset_schema_version?: number;
  dataset_case_count?: number;
  agent: string;
  status: 'pending' | 'running' | 'canceling' | 'canceled' | 'completed' | 'error' | string;
  created_at: number;
  started_at: number | null;
  finished_at: number | null;
  case_limit: number;
  summary: EvalRunSummary;
  cases: EvalCaseResult[];
  error: string;
}

export interface EvalDatasetImportRequest {
  ws: string;
  name?: string;
  format?: string;
  content?: string;
  path?: string;
}

export interface EvalDatasetDownloadRequest {
  ws: string;
  name?: string;
  format?: string;
  url: string;
}

export interface EvalRunCreateRequest {
  ws: string;
  dataset_id: string;
  agent?: string;
  case_limit?: number;
  config_path?: string;
  observability_config_path?: string;
}

export interface AgentProfile {
  name: string;
  description: string;
  tools: string[];
  model: string;
  maxTurns: number;
  memory: string;
  skills: string[];
  project_agents: string[];
}

export interface AgentProfileData {
  agent: string;
  path: string;
  profile: AgentProfile;
  body: string;
  content: string;
  bytes?: number;
}

export interface MemoryEntry {
  id: string;
  content: string;
  category?: string;
  tags?: string[];
  createdAt: string;
  updatedAt?: string;
}
