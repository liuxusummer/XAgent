import { useState } from 'react';
import { Sidebar, type PanelTab } from './components/Sidebar';
import { WorkspacePanel } from './components/WorkspacePanel';
import { AgentDetail } from './components/AgentDetail';
import { ChatArea } from './components/ChatArea';
import { SettingsModal, loadConfig, type AgentConfig } from './components/SettingsModal';
import { useChat } from './hooks/useChat';
import { ThemeProvider } from './hooks/useTheme.tsx';

type MainView = 'chat' | 'agents' | 'skills' | 'agent-detail';

function App() {
  const {
    session,
    agentStatus,
    isWaitingForUser,
    askPrompt,
    submitTask,
    sendReply,
    stopTask,
    clearChat,
  } = useChat();

  const [settingsOpen, setSettingsOpen] = useState(false);
  const [agentConfig, setAgentConfig] = useState<AgentConfig>(loadConfig());
  const [currentWorkspace, setCurrentWorkspace] = useState('default.ws');
  const [mainView, setMainView] = useState<MainView>('chat');
  const [selectedAgent, setSelectedAgent] = useState<string | null>(null);

  const handleSaveConfig = (config: AgentConfig) => {
    setAgentConfig(config);
  };

  const handleSubmitTask = (task: string) => {
    submitTask(task, {
      ...agentConfig,
      workspaceDir: agentConfig.workspaceDir || currentWorkspace,
      agent: selectedAgent || undefined,
    });
  };

  const handleOpenPanel = (tab: PanelTab) => {
    setMainView(tab);
    setSelectedAgent(null);
  };

  const handleNewChat = () => {
    clearChat();
    setMainView('chat');
    setSelectedAgent(null);
  };

  const handleClearChat = () => {
    clearChat();
    setSelectedAgent(null);
  };

  const handleSelectAgent = (agentName: string) => {
    setSelectedAgent(agentName);
    setMainView('agent-detail');
  };

  const handleChatWithAgent = (agentName: string) => {
    setSelectedAgent(agentName);
    setMainView('chat');
  };

  const handleBackFromDetail = () => {
    setMainView('agents');
    setSelectedAgent(null);
  };

  const handleWorkspaceChange = (workspace: string) => {
    setCurrentWorkspace(workspace);
    setSelectedAgent(null);
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
