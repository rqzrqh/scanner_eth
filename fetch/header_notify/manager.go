package headernotify

import (
	"context"
	"sync"
)

type Manager struct {
	mu sync.Mutex

	notifiers    []*HeaderNotifier
	handleUpdate func(*RemoteChainUpdate)

	updateCh chan *RemoteChainUpdate
	cancel   context.CancelFunc

	notifierWg sync.WaitGroup
	consumerWg sync.WaitGroup
}

func NewManager(notifiers []*HeaderNotifier, handleUpdate func(*RemoteChainUpdate)) *Manager {
	return &Manager{
		notifiers:    notifiers,
		handleUpdate: handleUpdate,
	}
}

func (m *Manager) Start(ctx context.Context) {
	if m == nil {
		return
	}
	m.StartWithChannel(ctx, make(chan *RemoteChainUpdate, 100))
}

func (m *Manager) StartWithChannel(ctx context.Context, ch chan *RemoteChainUpdate) {
	if m == nil || ch == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.stopLocked()

	notifyCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.updateCh = ch

	m.consumerWg.Add(1)
	go m.runConsumer(ch)

	for _, notifier := range m.notifiers {
		if notifier != nil {
			notifier.Run(notifyCtx, ch, &m.notifierWg)
		}
	}
}

func (m *Manager) Stop() {
	if m == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopLocked()
}

func (m *Manager) runConsumer(ch chan *RemoteChainUpdate) {
	defer m.consumerWg.Done()

	for update := range ch {
		if m.handleUpdate != nil {
			m.handleUpdate(update)
		}
	}
}

func (m *Manager) stopLocked() {
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	m.notifierWg.Wait()
	if m.updateCh != nil {
		close(m.updateCh)
		m.updateCh = nil
	}
	m.consumerWg.Wait()
}
