package headernotify

import (
	"context"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/sirupsen/logrus"
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

const consumerWaitTimeout = 30 * time.Second

func NewManager(clients []*ethclient.Client, handleUpdate func(*RemoteChainUpdate)) *Manager {
	notifiers := make([]*HeaderNotifier, len(clients))
	for i, client := range clients {
		notifiers[i] = NewHeaderNotifier(i, client)
	}
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
		notifier.Run(notifyCtx, ch, &m.notifierWg)
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
	done := make(chan struct{})
	go func() {
		m.consumerWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(consumerWaitTimeout):
		logrus.Warnf("header notify consumer stop timeout after %v", consumerWaitTimeout)
	}
}
