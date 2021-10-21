package watcher

import (
	"sync"

	"github.com/fsnotify/fsnotify"
	logging "github.com/ipfs/go-log/v2"
)

var (
	log = logging.Logger("watcher")
)

type Handlers struct {
	onCreate Handler
	onDelete Handler
	onWrite  Handler
}

type Handler func(fileName string) error

type FolderWatcher struct {
	w *fsnotify.Watcher
	Handlers

	stopWatch chan struct{}
	done      chan struct{}

	lock    sync.Mutex
	started bool
	closed  bool
}

// NewHandler specifies a handler in Handlers.
type NewHandler func(*Handlers)

// WithCreateHandler provides control over create handler to use with a watcher.
func WithCreateHandler(ch Handler) NewHandler {
	return func(h *Handlers) {
		h.onCreate = ch
	}
}

// WithDeleteHandler provides control over delete handler to use with a watcher.
func WithDeleteHandler(dh Handler) NewHandler {
	return func(h *Handlers) {
		h.onDelete = dh
	}
}

// WithWriteHandler provides control over write handler to use with a watcher.
func WithWriteHandler(wh Handler) NewHandler {
	return func(h *Handlers) {
		h.onWrite = wh
	}
}

func New(path string, set ...NewHandler) (*FolderWatcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	if err = watcher.Add(path); err != nil {
		return nil, err
	}

	fw := FolderWatcher{
		w:         watcher,
		stopWatch: make(chan struct{}),
		done:      make(chan struct{}),
	}

	for _, set := range set {
		set(&fw.Handlers)
	}

	return &fw, nil
}

func (fw *FolderWatcher) Close() {
	fw.lock.Lock()
	defer fw.lock.Unlock()

	if !fw.started || fw.closed {
		return
	}
	fw.closed = true

	close(fw.stopWatch)
	<-fw.done
	_ = fw.w.Close()
}

func (fw *FolderWatcher) Watch() {
	fw.lock.Lock()
	defer fw.lock.Unlock()
	if fw.started {
		return
	}

	fw.started = true
	go func() {
		for {
			select {
			case <-fw.stopWatch:
				log.Info("grafceful shutdown")
				close(fw.done)
				return
			case event, ok := <-fw.w.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Create == fsnotify.Create {
					log.Debug("created file:", event.Name)

					if err := fw.onCreate(event.Name); err != nil {
						log.Errorf("error when calling onCreate for %s", event.Name)
					}
				}
			case err, ok := <-fw.w.Errors:
				if !ok {
					return
				}
				log.Error(err)
			}
		}
	}()
}
