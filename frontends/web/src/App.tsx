import { useCallback, useEffect, useState } from 'react';
import { Sidebar, type PanelTab } from './components/Sidebar';
import { WorkspacePanel } from './components/WorkspacePanel';
import { AgentDetail } from './components/AgentDetail';
import { ChatArea } from './components/ChatArea';
import { MemoryPanel } from './components/MemoryPanel';
import { SystemPanel } from './components/SystemPanel';
import { EvalPanel } from './components/EvalPanel';
import { UsagePanel } from './components/UsagePanel';
import { CronPanel } from './components/CronPanel';
import { SettingsModal, loadConfig, type AgentConfig } from './components/SettingsModal';
import { useChat } from './hooks/useChat';
import { ThemeProvider } from './hooks/useTheme.tsx';
import { api } from './api/client';
import type { ChatMetadata, PersistentChatDetail, ScheduledTask } from './types';

type MainView = 'chat' | 'agents' | 'skills' | 'memory' | 'system' | 'eval' | 'usage' | 'cron' | 'agent-detail';

function App() {
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [agentConfig, setAgentConfig] = useState<AgentConfig>(loadConfig());
  const [currentWorkspace, setCurrentWorkspace] = useState('default.ws');
  const [mainView, setMainView] = useState<MainView>('chat');
  const [selectedAgent, setSelectedAgent] = useState<string | null>(null);
  const [agentChats, setAgentChats] = useState<ChatMetadata[]>([]);
  const [activeChatId, setActiveChatId] = useState('');
  const [chatLoading, setChatLoading] = useState(false);

  const refreshAgentChats = useCallback(() => {
    if (!selectedAgent) {
      setAgentChats([]);
      return;
    }
    setChatLoading(true);
    api.listChats(currentWorkspace, selectedAgent)
      .then((res) => {
        if (res.success && res.data) {
          setAgentChats(res.data);
        }
      })
      .finally(() => setChatLoading(false));
  }, [currentWorkspace, selectedAgent]);

  const {
    session,
    agentStatus,
    isWaitingForUser,
    askPrompt,
    liveTokenUsage,
    submitTask,
    sendReply,
    stopTask,
    clearChat,
    loadPersistentChat,
    attachRunningSession,
  } = useChat({ onPersistentChatUpdated: refreshAgentChats });

  useEffect(() => {
    refreshAgentChats();
  }, [refreshAgentChats]);

  const handleSaveConfig = (config: AgentConfig) => {
    setAgentConfig(config);
  };

  const createAndLoadAgentChat = useCallback(async (agentName: string): Promise<PersistentChatDetail | null> => {
    const res = await api.createChat(currentWorkspace, agentName);
    if (!res.success || !res.data) return null;
    setActiveChatId(res.data.metadata.chat_id);
    loadPersistentChat(res.data);
    refreshAgentChats();
    return res.data;
  }, [currentWorkspace, loadPersistentChat, refreshAgentChats]);

  const handleSubmitTask = async (task: string) => {
    let chatId = activeChatId;
    if (selectedAgent && !chatId) {
      const detail = await createAndLoadAgentChat(selectedAgent);
      chatId = detail?.metadata.chat_id || '';
    }
    submitTask(task, {
      ...agentConfig,
      workspaceDir: agentConfig.workspaceDir || currentWorkspace,
      agent: selectedAgent || undefined,
      chatId: chatId || undefined,
    });
  };

  const handleOpenPanel = (tab: PanelTab) => {
    setMainView(tab);
    setSelectedAgent(null);
    setActiveChatId('');
  };

  const handleNewChat = async () => {
    if (selectedAgent) {
      const detail = await createAndLoadAgentChat(selectedAgent);
      if (detail) {
        setMainView('chat');
      }
      return;
    }
    clearChat();
    setActiveChatId('');
    setMainView('chat');
    setSelectedAgent(null);
  };

  const handleClearChat = () => {
    clearChat();
    setActiveChatId('');
    setSelectedAgent(null);
  };

  const handleSelectAgent = (agentName: string) => {
    setSelectedAgent(agentName);
    setMainView('agent-detail');
  };

  const handleChatWithAgent = (agentName: string) => {
    setSelectedAgent(agentName);
    setActiveChatId('');
    clearChat();
    setMainView('chat');
  };

  const handleBackFromDetail = () => {
    setMainView('agents');
    setSelectedAgent(null);
  };

  const handleWorkspaceChange = (workspace: string) => {
    setCurrentWorkspace(workspace);
    setSelectedAgent(null);
    setActiveChatId('');
    clearChat();
  };

  const handleSelectChat = async (chatId: string) => {
    if (!selectedAgent) return;
    const res = await api.readChat(currentWorkspace, selectedAgent, chatId);
    if (res.success && res.data) {
      setActiveChatId(chatId);
      loadPersistentChat(res.data);
      setMainView('chat');
    }
  };

  const handleDeleteChat = async (chatId: string) => {
    if (!selectedAgent) return;
    const ok = window.confirm('Delete this chat history?');
    if (!ok) return;
    const res = await api.deleteChat(currentWorkspace, selectedAgent, chatId);
    if (res.success) {
      if (activeChatId === chatId) {
        setActiveChatId('');
        clearChat();
      }
      refreshAgentChats();
    }
  };

  const handleDebugRunStarted = (task: ScheduledTask, sessionId: string) => {
    setSelectedAgent(task.agent || null);
    setActiveChatId(task.keep_one_chat && task.chat_id ? task.chat_id : '');
    attachRunningSession(sessionId, {
      title: `Debug: ${task.name}`,
      task: task.prompt,
      config: {
        configPath: task.config_path || agentConfig.configPath,
        observabilityConfigPath: task.observability_config_path || agentConfig.observabilityConfigPath,
        workspaceDir: task.workspace || currentWorkspace,
        agent: task.agent || undefined,
      },
    });
    setMainView('chat');
  };

  const renderMainContent = () => {
    if (mainView === 'chat') {
      return (
        <ChatArea
          messages={session.messages}
          agentStatus={agentStatus}
          isWaitingForUser={isWaitingForUser}
          askPrompt={askPrompt}
          onSubmitTask={handleSubmitTask}
          onSendReply={sendReply}
          onStopTask={stopTask}
          view={mainView}
          activeAgent={selectedAgent}
          chatTitle={selectedAgent ? session.title : undefined}
        />
      );
    }

    if (mainView === 'agent-detail' && selectedAgent) {
      return (
        <AgentDetail
          workspace={currentWorkspace}
          agentName={selectedAgent}
          onBack={handleBackFromDetail}
        />
      );
    }

    if (mainView === 'memory') {
      return (
        <div className="flex-1 flex flex-col min-w-0 bg-bg-primary">
          <MemoryPanel workspace={currentWorkspace} agentName={selectedAgent} />
        </div>
      );
    }

    if (mainView === 'system') {
      return (
        <div className="flex-1 flex flex-col min-w-0 bg-bg-primary">
          <SystemPanel workspace={currentWorkspace} />
        </div>
      );
    }

    if (mainView === 'eval') {
      return (
        <div className="flex-1 flex flex-col min-w-0 bg-bg-primary">
          <EvalPanel
            workspace={currentWorkspace}
            configPath={agentConfig.configPath}
            observabilityConfigPath={agentConfig.observabilityConfigPath}
          />
        </div>
      );
    }

    if (mainView === 'usage') {
      return (
        <div className="flex-1 flex flex-col min-w-0 bg-bg-primary">
          <UsagePanel
            workspace={currentWorkspace}
            observabilityConfigPath={agentConfig.observabilityConfigPath}
            liveUsage={liveTokenUsage}
          />
        </div>
      );
    }

    if (mainView === 'cron') {
      return (
        <div className="flex-1 flex flex-col min-w-0 bg-bg-primary">
          <CronPanel
            workspace={currentWorkspace}
            configPath={agentConfig.configPath}
            observabilityConfigPath={agentConfig.observabilityConfigPath}
            onDebugRunStarted={handleDebugRunStarted}
          />
        </div>
      );
    }

    return (
      <div className="flex-1 flex flex-col min-w-0 bg-bg-primary">
        <WorkspacePanel
          isOpen={true}
          workspace={currentWorkspace}
          defaultTab={mainView === 'skills' ? 'skills' : 'agents'}
          onSelectAgent={handleSelectAgent}
          onChatWithAgent={handleChatWithAgent}
        />
      </div>
    );
  };

  return (
    <ThemeProvider>
      <div className="flex h-screen w-screen bg-bg-primary">
        <Sidebar
          onNewChat={handleNewChat}
          onClearChat={handleClearChat}
          currentSessionId={session.id}
          currentAgent={selectedAgent}
          chats={agentChats}
          activeChatId={activeChatId}
          chatsLoading={chatLoading}
          onSelectChat={handleSelectChat}
          onDeleteChat={handleDeleteChat}
          onOpenSettings={() => setSettingsOpen(true)}
          currentWorkspace={currentWorkspace}
          onWorkspaceChange={handleWorkspaceChange}
          onOpenPanel={handleOpenPanel}
          activeView={mainView === 'agent-detail' ? 'agents' : mainView}
        />
        {renderMainContent()}
        <SettingsModal
          isOpen={settingsOpen}
          onClose={() => setSettingsOpen(false)}
          onSave={handleSaveConfig}
          initialConfig={agentConfig}
        />
      </div>
    </ThemeProvider>
  );
}

export default App;
