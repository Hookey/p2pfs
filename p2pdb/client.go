package p2pdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	ipfslite "github.com/hsanjuan/ipfs-lite"
	"github.com/ipfs/go-cid"

	//log "github.com/sirupsen/logrus"

	logging "github.com/ipfs/go-log/v2"

	config "github.com/ipfs/go-ipfs-config"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/phayes/freeport"
	"github.com/textileio/go-threads/common"
	core "github.com/textileio/go-threads/core/db"
	"github.com/textileio/go-threads/core/net"
	"github.com/textileio/go-threads/core/thread"
	"github.com/textileio/go-threads/db"
	kt "github.com/textileio/go-threads/db/keytransform"

	"example.com/p2pfs/fs/watcher"
	"github.com/textileio/go-threads/util"
)

var (
	log = logging.Logger("foldersync")

	collectionName = "sharedFolder"

	cc = db.CollectionConfig{
		Name:   collectionName,
		Schema: util.SchemaFromInstance(inode{}, false),
	}

	errClientAlreadyStarted = errors.New("client already started")
)

//TODO: determine to use object-like or tree-like schema
type inode struct {
	ID     core.InstanceID `json:"_id"`
	Owner  string
	Path   string
	CID    string
	IsDir  bool
	IsRoot bool
	//TODO stat, ACL
}

type Client struct {
	sync.Mutex
	wg         sync.WaitGroup
	started    bool
	closed     bool
	name       string
	rootPath   string
	root       *inode
	store      kt.TxnDatastoreExtended
	db         *db.DB
	collection *db.Collection
	net        net.Net
	peer       *ipfslite.Peer
	closeCh    chan struct{}
}

func newClient(whoami, mount, repo, host, taddr, tid, tkey string) (*Client, error) {
	if tid != "" && tkey != "" && taddr != "" {
		return newJoinerClient(whoami, mount, repo, host, taddr, tid, tkey)
	} else {
		return newRootClient(whoami, mount, repo, host)
	}
}

func newRootClient(name, folderPath, repoPath, host string) (*Client, error) {
	id := thread.NewIDV1(thread.Raw, 32)

	network, err := newNetwork(repoPath, host)
	if err != nil {
		return nil, err
	}

	s, err := util.NewBadgerDatastore(repoPath, "eventstore", false)
	if err != nil {
		return nil, err
	}
	d, err := db.NewDB(context.Background(), s, network, id, db.WithNewCollections(cc))
	if err != nil {
		return nil, err
	}

	return &Client{
		name:       name,
		rootPath:   folderPath,
		db:         d,
		store:      s,
		collection: d.GetCollection(collectionName),
		net:        network,
		peer:       network.GetIpfsLite(),
		closeCh:    make(chan struct{}),
	}, nil
}

func newJoinerClient2(name, folderPath, repoPath, host string, addr ma.Multiaddr, key thread.Key) (*Client, error) {
	network, err := newNetwork(repoPath, host)
	if err != nil {
		return nil, err
	}

	saddr := addr.String()
	for i := 0; i < len(saddr)-len("/thread"); i++ {
		if saddr[i:i+7] == "/thread" {
			saddr = saddr[:i]
			break
		}
	}
	bootstrapPeers, err := config.ParseBootstrapPeers([]string{saddr})
	if err != nil {
		log.Error(err)
		return nil, err
	}

	network.Bootstrap(bootstrapPeers)

	s, err := util.NewBadgerDatastore(repoPath, "eventstore", false)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	d, err := db.NewDBFromAddr(context.Background(), s, network, addr, key, db.WithNewCollections(cc))
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return &Client{
		name:       name,
		rootPath:   folderPath,
		db:         d,
		store:      s,
		collection: d.GetCollection(collectionName),
		net:        network,
		peer:       network.GetIpfsLite(),
		closeCh:    make(chan struct{}),
	}, nil

}

func newJoinerClient(name, folderPath, repoPath, host, taddr, tid, tkey string) (*Client, error) {
	addr, err := ma.NewMultiaddr(taddr + "/thread/" + tid)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	key, err := thread.KeyFromString(tkey)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return newJoinerClient2(name, folderPath, repoPath, host, addr, key)
}

func freeLocalAddr(ip string) ma.Multiaddr {
	hostPort, err := freeport.GetFreePort()
	if err != nil {
		return nil
	}

	addr, _ := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", ip, hostPort))
	return addr
}

func newNetwork(repoPath, host string) (common.NetBoostrapper, error) {
	network, err := common.DefaultNetwork(
		common.WithNetBadgerPersistence(repoPath),
		common.WithNetHostAddr(freeLocalAddr(host)),
		common.WithNetPubSub(true),
		common.WithNetDebug(true),
	)
	if err != nil {
		return nil, err
	}
	return network, nil
}

func (c *Client) getDBInfo() (db.Info, error) {
	info, err := c.db.GetDBInfo()
	if err != nil {
		return db.Info{}, err
	}
	if len(info.Addrs) < 1 {
		return db.Info{}, errors.New("unable to get thread address")
	}
	return info, nil
}

// TODO: when create a new client to a existing thread, should sync db first.
func (c *Client) getOrCreateRoot(path string) (*inode, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err = os.MkdirAll(path, os.ModePerm); err != nil {
			return nil, err
		}
	}

	var myFolder *inode

	res, err := c.collection.Find(db.Where("Owner").Eq(c.name).And("IsRoot").Eq(true))
	if err != nil {
		return nil, err
	}

	if len(res) == 0 {
		ownFolder := inode{ID: core.NewInstanceID(), Owner: c.name, IsDir: true, IsRoot: true}
		jsn := util.JSONFromInstance(ownFolder)
		_, err := c.collection.Create(jsn)
		if err != nil {
			return nil, err
		}
		myFolder = &ownFolder
	} else {
		ownFolder := &inode{}
		if err := json.Unmarshal(res[0], ownFolder); err != nil {
			return nil, err
		}
		myFolder = ownFolder
	}

	return myFolder, nil
}

func (c *Client) startListeningExternalChanges() error {
	l, err := c.db.Listen()
	if err != nil {
		return err
	}
	go func() {
		defer c.wg.Done()
		defer l.Close()
		for {
			select {
			case <-c.closeCh:
				log.Infof("shutting down external changes listener - %v", c.name)
				return
			case a := <-l.Channel():
				instanceBytes, err := c.collection.FindByID(a.ID)
				if err != nil {
					log.Errorf("error when getting changed user folder with ID %v: %v", a.ID, err)
					continue
				}
				n := &inode{}
				util.InstanceFromJSON(instanceBytes, n)

				p := c.fullPath(n)
				if n.IsDir {
					log.Infof("%s: detected new dir %s of user %s", c.name, a.ID, n.Owner)
					if err := os.MkdirAll(p, 0700); err != nil {
						log.Warnf("%s: error ensuring file %s: %v", c.name, p, err)
					}
				} else {
					log.Infof("%s: detected new file %s of user %s", c.name, a.ID, n.Owner)
					if err := c.ensureCID(p, n.CID); err != nil {
						log.Warnf("%s: error ensuring file %s: %v", c.name, p, err)
					}
				}

				if inodes, err := c.getDirectoryTree(); err == nil {
					printTree(inodes)
				}
			}
		}
	}()
	return nil
}

func (c *Client) startFSWatcher() error {
	myFolderPath := path.Join(c.rootPath, c.name)
	myFolder, err := c.getOrCreateRoot(myFolderPath)
	if err != nil {
		return fmt.Errorf("error when getting folder for %v: %v", c.name, err)
	}
	//TODO: c.root may be unnecessary
	c.root = myFolder

	onCreate := func(fullPath string) error {
		if st, err := os.Stat(fullPath); err != nil {
			log.Error(err)
			return err
		} else if st.Mode().IsDir() {
			path := strings.TrimPrefix(fullPath, c.rootPath)
			path = strings.TrimLeft(path, "/")
			d := inode{ID: core.NewInstanceID(), Path: path, IsDir: true}
			log.Debugf("ID: %v", d.ID)
			_, err = c.collection.Create(util.JSONFromInstance(d))
			return err
		} else if st.Mode().IsRegular() {
			fd, err := os.Open(fullPath)
			if err != nil {
				log.Error(err)
				return err
			}

			n, err := c.peer.AddFile(context.Background(), fd, nil)
			if err != nil {
				log.Error(err)
				return err
			}

			path := strings.TrimPrefix(fullPath, c.rootPath)
			path = strings.TrimLeft(path, "/")
			f := inode{ID: core.NewInstanceID(), Path: path, CID: n.Cid().String()}
			log.Debugf("ID: %v", f.ID)
			_, err = c.collection.Create(util.JSONFromInstance(f))
			return err
		} else {
			err := fmt.Errorf("not support filetype %v", st.Mode())
			return err
		}
	}

	w, err := watcher.New(myFolderPath, watcher.WithCreateHandler(onCreate))
	if err != nil {
		return fmt.Errorf("error when creating fs watcher for %v: %v", c.name, err)
	}
	w.Watch()

	go func() {
		<-c.closeCh
		w.Close()
		c.wg.Done()
	}()
	return nil
}

func (c *Client) start() error {
	c.Lock()
	defer c.Unlock()

	if c.started {
		return errClientAlreadyStarted
	}

	c.wg.Add(2)
	if err := c.startListeningExternalChanges(); err != nil {
		return err
	}
	if err := c.startFSWatcher(); err != nil {
		return err
	}
	c.started = true
	return nil
}

func (c *Client) getDirectoryTree() ([]*inode, error) {
	res, err := c.collection.Find(nil)
	if err != nil {
		return nil, err
	}
	inodes := make([]*inode, len(res))
	for i, item := range res {
		inode := &inode{}
		if err := json.Unmarshal(item, inode); err != nil {
			return nil, err
		}
		inodes[i] = inode
	}
	return inodes, nil
}

func (c *Client) fullPath(n *inode) string {
	return filepath.Join(c.rootPath, n.Path)
}

func (c *Client) ensureCID(fullPath, cidStr string) error {
	id, err := cid.Decode(cidStr)
	if err != nil {
		return err
	}
	_, err = os.Stat(fullPath)
	if err == nil {
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}

	log.Infof("fetching file %s", fullPath)
	d1 := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	str, err := c.peer.GetFile(ctx, id)
	if err != nil {
		return err
	}
	defer str.Close()
	if err := os.MkdirAll(filepath.Dir(fullPath), 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0660)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = io.Copy(f, str); err != nil {
		return err
	}
	d2 := time.Now()
	d := d2.Sub(d1)
	log.Infof("done fetching %s in %dms", fullPath, d.Milliseconds())
	return nil
}

func (c *Client) close() error {
	c.Lock()
	defer c.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	close(c.closeCh)

	c.wg.Wait()

	if err := c.db.Close(); err != nil {
		return err
	}
	if err := c.store.Close(); err != nil {
		return err
	}
	if err := c.net.Close(); err != nil {
		return err
	}
	return nil
}

func printTree(inodes []*inode) {
	sort.Slice(inodes, func(i, j int) bool {
		return strings.Compare(inodes[i].Path, inodes[j].Path) < 0
	})

	fmt.Printf("Tree of client \n")
	for _, n := range inodes {
		fmt.Printf("\t%s %s\n", n.Path, n.CID)
	}
	fmt.Println()
}

func Connect(whoami, mount, repo, host, taddr, tid, tkey string) (func(), error) {
	c, err := newClient(whoami, mount, repo, host, taddr, tid, tkey)
	if err != nil {
		return nil, err
	}

	info, _ := c.getDBInfo()
	log.Infof("owner: %s", whoami)
	log.Infof("mnt: %s", mount)
	log.Infof("repo: %s", repo)
	log.Infof("addr: %s", info.Addrs[0])
	log.Infof("key: %s", info.Key.String())
	log.Infof("ID: %s", c.net.Host().ID().String())

	return func() {
		log.Infof("Closing root client %v\n", whoami)
		err := c.close()
		if err != nil {
			log.Error(err)
		}
		os.RemoveAll(repo)
		os.RemoveAll(mount)
		log.Infof("Root client %v closed\n", whoami)
	}, c.start()
}
