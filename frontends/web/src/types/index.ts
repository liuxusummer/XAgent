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
  status: 'idle' | 'running' | 'waiting_for_user' | 'error';
  config?: {
    configPath?: string;
    observabilityConfigPath?: string;
    workspaceDir?: string;
    agent?: string;
  };
}

export interface AgentStatus {
  state: 'idle' | 'thinking' | 'executing' | 'waiting_for_user' | 'error';
  currentTurn?: number;
  maxTurns?: number;
  currentTool?: string;
}

export interface SSEEvent {
  type:
    | 'user_task'
    | 'assistant_delta'
    | 'thinking_delta'
    | 'turn_start'
    | 'tool_call'
    | 'tool_result'
    | 'ask_user'
    | 'user_reply'
    | 'run_done'
    | 'done'
    | 'log'
    | 'stop'
    | 'error'
    | 'heartbeat';
  data: unknown;
}

export interface SubmitTaskRequest {
  task: string;
  session_id?: string;
  config_path?: string;
  observability_config_path?: string;
  workspace_dir?: string;
  agent?: string;
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

export interface WorkspaceSkill {
  name: string;
}

export interface WorkspaceTool {
  name: string;
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
  avg_duration: number;
  avg_turns: number;
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
  failures: string[];
  response_excerpt: string;
}

export interface EvalRunResult {
  version: number;
  id: string;
  workspace: string;
  dataset_id: string;
  dataset_name: string;
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
