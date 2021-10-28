package fs

import (
	"context"

	logging "github.com/ipfs/go-log/v2"
	ma "github.com/multiformats/go-multiaddr"
	c "github.com/textileio/go-threads/api/client"
	"github.com/textileio/go-threads/core/thread"
	"google.golang.org/grpc"
)

var log = logging.Logger("fs")

// create threads client
func NewClient(ApiAddr, DBAddr, DBKey string) (*c.Client, error) {
	client, err := c.NewClient(ApiAddr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	var id thread.ID
	if DBAddr == "" || DBKey == "" {
		id = thread.NewIDV1(thread.Raw, 32)
		if err := client.NewDB(context.Background(), id); err != nil {
			return nil, err
		}
	} else {
		dbAddr, err := ma.NewMultiaddr(DBAddr)
		if err != nil {
			log.Error(err)
			return nil, err
		}

		dbKey, err := thread.KeyFromString(DBKey)
		if err != nil {
			log.Error(err)
			return nil, err
		}

		if err = client.NewDBFromAddr(context.Background(), dbAddr, dbKey); err != nil {
			return nil, err
		}

		id, _ = thread.FromAddr(dbAddr)
	}

	if info, err := client.GetDBInfo(context.Background(), id); err == nil {
		log.Infof("name: %s", info.Name)
		log.Infof("addr: %s", info.Addrs[0])
		log.Infof("key: %s", info.Key.String())
		/*fmt.Printf("name: %s", info.Name)
		fmt.Printf("addr: %s", info.Addrs[0])
		fmt.Printf("key: %s", info.Key.String())*/

	}

	return client, nil
}
