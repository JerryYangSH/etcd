package main
import (
	"context"
	"flag"
	"log"
	"os"
	"os/exec"
	"io/ioutil"
	"time"
	"strconv"
	"strings"

	"github.com/coreos/etcd/clientv3"

	"google.golang.org/grpc/grpclog"
)


const MAX_RETRY = 5
var gCli *clientv3.Client
var lastCheckTime time.Time = time.Now()
var (
	syncInterval   = 1 * time.Second
	dialTimeout    = 1 * time.Second
	requestTimeout = 1 * time.Second
	endpoints      = []string{"10.247.97.241:2379", "10.247.97.242:2379", "10.247.97.247:2379"}

	// for test key
	testKeyPrefix="/testdir/key"
	valueLen = 512
	// len of magicStr = 128 
	magicStr="0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$"
	
	successCount = 0
	errorCount = 0
	STOP = false
)

func onExit(startTime time.Time) {
	traceDone(startTime, successCount, errorCount)
}

func traceDone(startTime time.Time, successCount int, errorCount int) {
	elapsed := time.Now().Sub(startTime)
	elapsedInMs := int(elapsed / time.Millisecond)
	log.Printf("PERF : It took %s, Success Count %d, Error Count %d\n", elapsed, successCount, errorCount)
	if elapsedInMs == 0 {
		log.Fatal("Invalid paramenter! You need to put more keys.")
	}
	log.Printf("PERF : IOPS %6.2f\n", float32((successCount * 1000.0) / elapsedInMs))
}


func main() {
	totalKeyCountPtr := flag.Int("keycount", 100, "total key count")
	deletePtr := flag.Bool("delete", true, "delete these keys when test completes")
	iopsLimit := flag.Int("limit", 0, "iops limit. Default 0 indicates no limit.")
	grpcDebug := flag.Int("grpcdebug", 0, "enable gRPC verbose tracing for debug")
	flag.Parse()
	*iopsLimit = 250 // HACK
	*deletePtr = false

	log.Printf("PERF : Total key count %d, deleteEnabled %t, iopsLimit %d, grpcDebug %d\n", *totalKeyCountPtr, *deletePtr, *iopsLimit, *grpcDebug)
	
	if *grpcDebug != 0 {
		clientv3.SetLogger(grpclog.NewLoggerV2(os.Stderr, os.Stderr, os.Stderr))
	} else {
		clientv3.SetLogger(grpclog.NewLoggerV2(ioutil.Discard, ioutil.Discard, os.Stderr))
	}
	
	err := error(nil)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("PERF : Total ETCD endpoints %v\n", endpoints)
	defer cli.Close() // make sure to close the client
	gCli = cli

	startTime := time.Now()
	defer onExit(startTime)
	perfMixed(&cli, *totalKeyCountPtr, *deletePtr, *iopsLimit, startTime)

}

func perfMixed(cli **clientv3.Client, keyCount int, delete bool, iopsLimit int, startTime time.Time) {
	valueStr := magicStr 
	for len(valueStr) < valueLen {
		valueStr += magicStr
	}
	valueStr += time.Now().String()
///testdir/key4720210
	for i := 8580000; i < keyCount; i++ {
		keyName := testKeyPrefix + strconv.Itoa(i)

		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(requestTimeout))
		doOp(cli, ctx, "create", keyName, valueStr)
		cancel()
/*
		ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(requestTimeout))
		doOp(cli, ctx, "get", keyName, valueStr)
		cancel()
		
		ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(requestTimeout))
		doOp(cli, ctx, "update", keyName, valueStr)
		cancel()
		
		ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(requestTimeout))
		doOp(cli, ctx, "list", keyName, valueStr)
		cancel()

		if delete {
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(requestTimeout))
			doOp(cli, ctx, "delete", keyName, valueStr)
			cancel()
		}
*/
		if STOP {
			break
		}

		limit, backoff := needThreshold(iopsLimit, startTime, time.Second)
		if limit {
			if false {
				// verbose
				log.Printf("PERF : Hit limit(%d). Backing off %s\n", iopsLimit, backoff)
			}
			time.Sleep(backoff)
		}	
	}

}

func needThreshold(iopsLimit int, start time.Time, interval time.Duration) (bool,time.Duration){
	if iopsLimit == 0 {
		// no limit at all
		return false, 0
	}
	duration := time.Now().Sub(lastCheckTime)
	if duration < interval {
		return false, 0
	}

	lastCheckTime = time.Now()
	elapsed := time.Now().Sub(start)
	elapsedInMs := int(elapsed / time.Millisecond)
	totalLimit := iopsLimit * elapsedInMs / 1000
	if successCount <= totalLimit {
		return false, 0
	}
	return true, time.Duration((successCount - totalLimit) * 1000 / iopsLimit) * time.Millisecond
	

}

// return success or not
func doOp(cli **clientv3.Client, ctx context.Context, opType string, keyName string, valueStr string) bool {
	retry := 0
	for true {
		success := false
		eRet := error(nil)
		switch opType {
			case "create" :
				_, err := (*cli).Put(ctx, keyName, valueStr)
				eRet = err
				if err != nil {
					break
				}
				success = true
			case "update" :
				_, err := (*cli).Put(ctx, keyName, valueStr)
				eRet = err
				if err != nil {
					break
				}
				success = true
			case "get" :
        			resp, err := (*cli).Get(ctx, keyName)
				eRet = err
				if err != nil {
					break
				}
				if resp == nil {
					log.Fatal("Empty response on getting %s", keyName)
					break
				}
        			for _, ev := range resp.Kvs {
					if strings.Compare(string(ev.Key), keyName) != 0 {
						log.Fatal("Wrong result "+ string(ev.Key) + ", while querying key " + keyName)
					}
				}
				success = true
                	
			case "list" :
        			resp, err := (*cli).Get(ctx, keyName, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
				eRet = err
				if err != nil {
					break
				}
				if resp == nil {
					log.Fatal("Empty response on listing %s", keyName)
					break
				}
        			for _, ev := range resp.Kvs {
					if !strings.HasPrefix(string(ev.Key), keyName) {
						log.Fatal("Wrong result, while listing with prefix " + keyName)
					}
        			}
				success = true
			case "delete" :
				_, err := (*cli).Delete(ctx, keyName)
				eRet = err
				if err != nil {
					break
				}
				success = true
			default:
				log.Fatal("Unsupported op ", opType)
		}
	
		if success {
			successCount++
			break
		}	
		/*STOP = clientv3.isRepeatableStopError(eRet)
		if STOP {
			log.Print("PERF : Stopping on hitting error ", eRet)
			break
		}*/

		if eRet == context.DeadlineExceeded {
			// connection dead, re-establish connection?
			log.Printf("PERF : Lose connection on %s %s, %s success=%t\n", opType, keyName, eRet, success)
			//restoreConn()
			/*
			(*cli).Close()
			log.Printf("PERF : Re-establish connection after sleeping 10 s...\n")
			time.Sleep(10 * time.Second)
			
			*cli, eRet = clientv3.New(
				clientv3.Config{
                			Endpoints:   endpoints,
                			DialTimeout: dialTimeout,
        			})
			if eRet != nil {
				log.Printf("PERF : Failed to re-establish connection, give up\n")
				log.Fatal(eRet)
			} else {
				log.Printf("PERF : connection re-established successfully\n")
				log.Printf("PERF : active connection is %d\n", (*cli).ActiveConnection())
			}*/
		} else if eRet == context.Canceled {
			// TODO: 
			log.Println(eRet)
			STOP = true	
		} else if eRet != nil { 
			// TODO : Unknown error, stop and check
			log.Println(eRet)
			STOP = true 
		}

		// fail
		retry++
		if retry > MAX_RETRY {
			errorCount++
			log.Printf("PERF : Retry %s %s exhausted!\n", opType, keyName)
			if eRet != nil {
				log.Println("PERF : ", eRet)
			}
			return false
		}
		retryInterval := time.Duration(1 << uint32(retry)) * time.Second 
		log.Printf("PERF : Retrying %d %s %s after sleeping %s...\n", retry, opType, keyName, retryInterval)
		time.Sleep(retryInterval)
	}

	return true
}

// refer to : https://gist.github.com/yudai/a5e1269ef3d484217b10415af6dd08c4
func restoreConn() {
	cmd := exec.Command("bash", "-c", "netstat -nap | grep ':2379'")
	out, err := cmd.Output()
	if err != nil {
		log.Panicf("netstat failed", err)
	}
	log.Printf("netstat: %s\n", out)

	log.Println("Start dropping packets in 5 seconds")
	time.Sleep(time.Second * 5)

	log.Println("Dropping packets")
	cmd = exec.Command("bash", "-c", "iptables -A INPUT -j DROP -p tcp --destination-port 2379; iptables -A INPUT -j DROP -p tcp --source-port 2379")
	err = cmd.Run()
	if err != nil {
		log.Panicf("iptables failed", err)
	}
	cmd = exec.Command("bash", "-c", "netstat -nap | grep ':2379'")
	out, err = cmd.Output()
	if err != nil {
		log.Panicf("netstat failed", err)
	}
	log.Printf("netstat: %s\n", out)

	log.Println("Waiting for 10 seconds")
	time.Sleep(time.Second * 10)

	log.Println("Restoring connectivity")
	cmd = exec.Command("bash", "-c", "iptables -D INPUT -j DROP -p tcp --destination-port 2379; iptables -D INPUT -j DROP -p tcp --source-port 2379")
	err = cmd.Run()
	if err != nil {
		log.Panicf("iptables failed", err)
	}
	cmd = exec.Command("bash", "-c", "netstat -nap | grep ':2379'")
	out, err = cmd.Output()
	if err != nil {
		log.Panicf("netstat failed", err)
	}
	log.Printf("netstat: %s\n", out)

	log.Println("Waiting for 10 seconds")
	time.Sleep(time.Second * 10)

	cmd = exec.Command("bash", "-c", "netstat -nap | grep ':2379'")
	out, err = cmd.Output()
	if err != nil {
		log.Panicf("netstat failed", err)
	}
	log.Printf("netstat: %s\n", out)
}
