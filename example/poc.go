package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
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
	"github.com/textileio/go-threads/integrationtests/foldersync/watcher"
	"github.com/textileio/go-threads/util"
)

var (
	log = logging.Logger("foldersync")

	collectionName = "sharedFolder"

	cc = db.CollectionConfig{
		Name:   collectionName,
		Schema: util.SchemaFromInstance(folder{}, false),
	}

	errClientAlreadyStarted = errors.New("client already started")
)

type folder struct {
	ID    core.InstanceID `json:"_id"`
	Owner string
	Files []file
}

type file struct {
	ID               core.InstanceID `json:"_id"`
	FileRelativePath string
	CID              string

	IsDirectory bool
	Files       []file
}

type client struct {
	sync.Mutex
	wg             sync.WaitGroup
	started        bool
	closed         bool
	name           string
	folderPath     string
	folderInstance *folder
	store          kt.TxnDatastoreExtended
	db             *db.DB
	collection     *db.Collection
	net            net.Net
	peer           *ipfslite.Peer
	closeCh        chan struct{}
}

func newRootClient(name, folderPath, repoPath, host string) (*client, error) {
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

	return &client{
		name:       name,
		folderPath: folderPath,
		db:         d,
		store:      s,
		collection: d.GetCollection(collectionName),
		net:        network,
		peer:       network.GetIpfsLite(),
		closeCh:    make(chan struct{}),
	}, nil
}

func newJoinerClient(name, folderPath, repoPath, host, taddr, tid, tkey string) (*client, error) {
	network, err := newNetwork(repoPath, host)
	if err != nil {
		return nil, err
	}

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

	bootstrapPeers, err := config.ParseBootstrapPeers([]string{taddr})
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

	return &client{
		name:       name,
		folderPath: folderPath,
		db:         d,
		store:      s,
		collection: d.GetCollection(collectionName),
		net:        network,
		peer:       network.GetIpfsLite(),
		closeCh:    make(chan struct{}),
	}, nil
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

func (c *client) getDBInfo() (db.Info, error) {
	info, err := c.db.GetDBInfo()
	if err != nil {
		return db.Info{}, err
	}
	if len(info.Addrs) < 1 {
		return db.Info{}, errors.New("unable to get thread address")
	}
	return info, nil
}

func (c *client) getOrCreateMyFolderInstance(path string) (*folder, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err = os.MkdirAll(path, os.ModePerm); err != nil {
			return nil, err
		}
	}

	var myFolder *folder

	res, err := c.collection.Find(db.Where("Owner").Eq(c.name))
	if err != nil {
		return nil, err
	}

	if len(res) == 0 {
		ownFolder := folder{ID: core.NewInstanceID(), Owner: c.name, Files: []file{}}
		jsn := util.JSONFromInstance(ownFolder)
		_, err := c.collection.Create(jsn)
		if err != nil {
			return nil, err
		}
		myFolder = &ownFolder
	} else {
		ownFolder := &folder{}
		if err := json.Unmarshal(res[0], ownFolder); err != nil {
			return nil, err
		}
		myFolder = ownFolder
	}

	return myFolder, nil
}

func (c *client) startListeningExternalChanges() error {
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
				uf := &folder{}
				util.InstanceFromJSON(instanceBytes, uf)
				log.Infof("%s: detected new file %s of user %s", c.name, a.ID, uf.Owner)
				for _, f := range uf.Files {
					if err := c.ensureCID(c.fullPath(f), f.CID); err != nil {
						log.Warnf("%s: error ensuring file %s: %v", c.name, c.fullPath(f), err)
					}
				}
				if folders, err := c.getDirectoryTree(); err == nil {
					printTree(folders)
				}
			}
		}
	}()
	return nil
}

func (c *client) startFSWatcher() error {
	myFolderPath := path.Join(c.folderPath, c.name)
	myFolder, err := c.getOrCreateMyFolderInstance(myFolderPath)
	if err != nil {
		return fmt.Errorf("error when getting folder for %v: %v", c.name, err)
	}
	c.folderInstance = myFolder

	w, err := watcher.New(myFolderPath, func(fileName string) error {
		f, err := os.Open(fileName)
		if err != nil {
			log.Error(err)
			return err
		}
		n, err := c.peer.AddFile(context.Background(), f, nil)
		if err != nil {
			log.Error(err)
			return err
		}

		fileRelPath := strings.TrimPrefix(fileName, c.folderPath)
		fileRelPath = strings.TrimLeft(fileRelPath, "/")
		newFile := file{ID: core.NewInstanceID(), FileRelativePath: fileRelPath, CID: n.Cid().String(), Files: []file{}}
		c.folderInstance.Files = append(c.folderInstance.Files, newFile)
		return c.collection.Save(util.JSONFromInstance(c.folderInstance))
	})
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

func (c *client) start() error {
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

func (c *client) getDirectoryTree() ([]*folder, error) {
	res, err := c.collection.Find(nil)
	if err != nil {
		return nil, err

	}
	folders := make([]*folder, len(res))
	for i, item := range res {
		folder := &folder{}
		if err := json.Unmarshal(item, folder); err != nil {
			return nil, err
		}
		folders[i] = folder
	}
	return folders, nil
}

func (c *client) fullPath(f file) string {
	return filepath.Join(c.folderPath, f.FileRelativePath)
}

func (c *client) ensureCID(fullPath, cidStr string) error {
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

func (c *client) close() error {
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

func printTree(folders []*folder) {
	sort.Slice(folders, func(i, j int) bool {
		return strings.Compare(folders[i].Owner, folders[j].Owner) < 0
	})

	fmt.Printf("Tree of client \n")
	for _, sf := range folders {
		fmt.Printf("\t%s %s\n", sf.ID, sf.Owner)
		for _, f := range sf.Files {
			fmt.Printf("\t\t %s %s\n", f.FileRelativePath, f.CID)
		}
	}
	fmt.Println()
}

func main() {
	//log.SetLevel(log.DebugLevel)
	//log.SetFormatter(&log.JSONFormatter{})

	repo, _ := ioutil.TempDir("", "")
	notify, _ := ioutil.TempDir("", "")
	repoPath := flag.String("repo", repo, "thread db repo")
	notifyPath := flag.String("mnt", notify, "mountpoint")
	whoami := flag.String("whoami", "p2pdb"+string(rand.Int63()), "whoami")
	host := flag.String("host", "127.0.0.1", "host ip")
	tidStr := flag.String("tid", "", "thread db id")
	tkeyStr := flag.String("tkey", "", "thread db key")
	taddrStr := flag.String("taddr", "", "thread db addr, bootstrap addr")
	debug := flag.Bool("debug", false, "Enables debug logging")
	logFile := flag.String("logFile", "", "File to write logs to")

	flag.Parse()
	flag.PrintDefaults()
	/*if flag.NArg() < 2 {
		fmt.Printf("usage: %s MOUNTPOINT ORIGINAL\n", path.Base(os.Args[0]))
		fmt.Printf("\noptions:\n")
		os.Exit(2)
	}*/

	if err := util.SetupDefaultLoggingConfig(*logFile); err != nil {
		log.Fatal(err)
	}

	if err := util.SetLogLevels(map[string]logging.LogLevel{
		"foldersync": util.LevelFromDebugFlag(*debug),
	}); err != nil {
		log.Fatal(err)
	}

	var c *client
	if *tidStr != "" && *tkeyStr != "" && *taddrStr != "" {
		var err error
		c, err = newJoinerClient(*whoami, *notifyPath, *repoPath, *host, *taddrStr, *tidStr, *tkeyStr)
		if err != nil {
			log.Error(err)
			return
		}
	} else {
		var err error
		c, err = newRootClient(*whoami, *notifyPath, *repoPath, *host)
		if err != nil {
			log.Error(err)
			return
		}
	}
	info, _ := c.getDBInfo()
	log.Infof("owner: %s", c.name)
	log.Infof("mnt: %s", *notifyPath)
	log.Infof("repo: %s", *repoPath)
	log.Infof("addr: %s", info.Addrs[0])
	log.Infof("key: %s", info.Key.String())
	log.Infof("ID: %s", c.net.Host().ID().String())

	defer func() {
		log.Infof("Closing root client %v\n", c.name)
		err := c.close()
		if err != nil {
			log.Error(err)
		}
		os.RemoveAll(*repoPath)
		os.RemoveAll(*notifyPath)
		log.Infof("Root client %v closed\n", c.name)
	}()

	if err := c.start(); err != nil {
		log.Error(err)
		return
	}

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig)
		done <- true
	}()

	fmt.Println("awaiting signal")
	<-done
	fmt.Println("exiting")

	/*id := thread.NewIDV1(thread.Raw, 32)

	network, err := newNetwork(repoPath)

	store, err := util.NewBadgerDatastore(repoPath, "eventstore", false)

	if addrStr != "" && keyStr != "" {
		Addr, _ := ma.NewMultiaddr(addrStr)
		Key, _ := thread.KeyFromString(keyStr)

		p2pdb, err := db.NewDBFromAddr(
			context.Background(),
			store,
			network,
			addr,
			key,
			db.WithNewCollections(cc),
		)
		log.Infof("Connect to existing db - addr:%v, key:%v", string(Addr.bytes), Key.String())
	} else {
		p2pdb, err := db.NewDB(context.Background(), store, network, id, db.WithNewCollections(cc))
		info, err := p2pdb.GetDBInfo()
		log.Infof("Create new db - addr:%v, key:%v", string(Addr.bytes), Key.String())
	}*/

}
