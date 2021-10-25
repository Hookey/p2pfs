package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/signal"
	"syscall"

	//log "github.com/sirupsen/logrus"

	logging "github.com/ipfs/go-log/v2"

	"example.com/p2pfs/p2pdb"
	"github.com/textileio/go-threads/util"
)

var (
	log = logging.Logger("main")
)

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
		"main":       util.LevelFromDebugFlag(*debug),
		"foldersync": util.LevelFromDebugFlag(*debug),
		"watcher":    util.LevelFromDebugFlag(*debug),
	}); err != nil {
		log.Fatal(err)
	}

	if disconnect, err := p2pdb.Connect(*whoami, *notifyPath, *repoPath, *host, *taddrStr, *tidStr, *tkeyStr); err != nil {
		log.Error(err)
		return
	} else {
		defer disconnect()
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
}
