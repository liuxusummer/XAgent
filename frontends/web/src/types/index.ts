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
