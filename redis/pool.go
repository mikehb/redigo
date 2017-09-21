// Copyright 2012 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package redis

import (
	"bytes"
	"container/list"
	"crypto/sha1"
	"errors"
	"io"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/mikehb/redigo/internal"
)

var nowFunc = time.Now // for testing

// ErrPoolExhausted is returned from a pool connection method (Do, Send,
// Receive, Flush, Err) when the maximum number of database connections in the
// pool has been reached.
var ErrPoolExhausted = errors.New("redigo: connection pool exhausted")

var (
	errPoolClosed = errors.New("redigo: connection pool closed")
	errConnClosed = errors.New("redigo: connection closed")
)

// Redis path selector types
const (
	RPSFixed = iota
	RPSLeastOutstanding = iota
)

type redisPath struct {
	addr string					// address of this redis server
	outstanding uint32
}

// Pool maintains a pool of connections. The application calls the Get method
// to get a connection from the pool and the connection's Close method to
// return the connection's resources to the pool.
//
// The following example shows how to use a pool in a web application. The
// application creates a pool at application startup and makes it available to
// request handlers using a global variable.
//
//  func newPool(server, password string) *redis.Pool {
//      return &redis.Pool{
//          MaxIdle: 3,
//          IdleTimeout: 240 * time.Second,
//          Dial: func () (redis.Conn, error) {
//              c, err := redis.Dial("tcp", server)
//              if err != nil {
//                  return nil, err
//              }
//              if _, err := c.Do("AUTH", password); err != nil {
//                  c.Close()
//                  return nil, err
//              }
//              return c, err
//          },
//          TestOnBorrow: func(c redis.Conn, t time.Time) error {
//              _, err := c.Do("PING")
//              return err
//          },
//      }
//  }
//
//  var (
//      pool *redis.Pool
//      redisServer = flag.String("redisServer", ":6379", "")
//      redisPassword = flag.String("redisPassword", "", "")
//  )
//
//  func main() {
//      flag.Parse()
//      pool = newPool(*redisServer, *redisPassword)
//      ...
//  }
//
// A request handler gets a connection from the pool and closes the connection
// when the handler is done:
//
//  func serveHome(w http.ResponseWriter, r *http.Request) {
//      conn := pool.Get()
//      defer conn.Close()
//      ....
//  }
//
type Pool struct {

	// Dial is an application supplied function for creating and configuring a
	// connection.
	//
	// The connection returned from Dial must not be in a special state
	// (subscribed to pubsub channel, transaction started, ...).
	Dial func() (Conn, error)

	DialDirect func(redisAddr string) (Conn, error)
	
	// TestOnBorrow is an optional application supplied function for checking
	// the health of an idle connection before the connection is used again by
	// the application. Argument t is the time that the connection was returned
	// to the pool. If the function returns an error, then the connection is
	// closed.
	TestOnBorrow func(c Conn, t time.Time) error

	// Maximum number of idle connections in the pool.
	MaxIdle int

	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	MaxActive int

	// Close connections after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout time.Duration

	// If Wait is true and the pool is at the MaxActive limit, then Get() waits
	// for a connection to be returned to the pool before returning.
	Wait bool

	// mu protects fields defined below.
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
	active int

	// Stack of idleConn with most recently used at the front.
	idle list.List
}

type idleConn struct {
	c Conn
	t time.Time
}

// NewPool creates a new pool.
//
// Deprecated: Initialize the Pool directory as shown in the example.
func NewPool(newFn func() (Conn, error), maxIdle int) *Pool {
	return &Pool{Dial: newFn, MaxIdle: maxIdle}
}

// Get gets a connection. The application must close the returned connection.
// This method always returns a valid connection so that applications can defer
// error handling to the first use of the connection. If there is an error
// getting an underlying connection, then the connection Err, Do, Send, Flush
// and Receive methods return that error.
func (p *Pool) Get() Conn {
	redisAddr := ""
	c, err := p.get(redisAddr)
	if err != nil {
		return errorConnection{err}
	}
	return &pooledConnection{p: p, c: c}
}

func (p *Pool) GetByPrefer(redisAddr string) Conn {
	c, err := p.get(redisAddr)
	if err != nil {
		return errorConnection{err}
	}
	return &pooledConnection{p: p, c: c}
}

// ActiveCount returns the number of active connections in the pool.
func (p *Pool) ActiveCount() int {
	p.mu.Lock()
	active := p.active
	p.mu.Unlock()
	return active
}

// Close releases the resources used by the pool.
func (p *Pool) Close() error {
	p.closed = true
	p.closeAll()
	return nil
}

// Close all connections.
func (p *Pool) closeAll() {
	p.mu.Lock()
	idle := p.idle
	p.idle.Init()
	p.active -= idle.Len()
	if p.cond != nil {
		p.cond.Broadcast()
	}
	p.mu.Unlock()
	for e := idle.Front(); e != nil; e = e.Next() {
		e.Value.(idleConn).c.Close()
	}
}

// release decrements the active count and signals waiters. The caller must
// hold p.mu during the call.
func (p *Pool) release() {
	p.active -= 1
	if p.cond != nil {
		p.cond.Signal()
	}
}

// get prunes stale connections and returns a connection from the idle list or
// creates a new connection.
func (p *Pool) get(redisAddr string) (Conn, error) {
	p.mu.Lock()

	// Prune stale connections.
	if timeout := p.IdleTimeout; timeout > 0 {
		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Back()
			if e == nil {
				break
			}
			ic := e.Value.(idleConn)
			if ic.t.Add(timeout).After(nowFunc()) {
				break
			}
			p.idle.Remove(e)
			p.release()
			p.mu.Unlock()
			ic.c.Close()
			p.mu.Lock()
		}
	}

	for {
		// Get idle connection.
		var next *list.Element
		for e := p.idle.Front(); e != nil; e = next {
			next = e.Next()
			ic := e.Value.(idleConn)
			if redisAddr != "" && ic.c.RemoteAddr().String() != redisAddr {
				continue
			}
			p.idle.Remove(e)
			test := p.TestOnBorrow
			p.mu.Unlock()
			if test == nil || test(ic.c, ic.t) == nil {
				return ic.c, nil
			}
			ic.c.Close()
			p.mu.Lock()
			p.release()
		}

		// Check for pool closed before dialing a new connection.
		if p.closed {
			p.mu.Unlock()
			return nil, errors.New("redigo: get on closed pool")
		}

		// Dial new connection if under limit.
		if p.MaxActive == 0 || p.active < p.MaxActive {
			dial := p.Dial
			dialDirect := p.DialDirect
			p.active += 1
			p.mu.Unlock()
			var c Conn
			var err error
			if redisAddr != "" {
				c, err = dialDirect(redisAddr)
			} else {
				c, err = dial()
			}
			if err != nil {
				p.mu.Lock()
				p.release()
				p.mu.Unlock()
				// Try other redis connection if dialing the prefered redis failed
				if redisAddr != "" {
					redisAddr = ""
					p.mu.Lock()
					continue
				}
				c = nil
			}

			return c, err
		}

		if !p.Wait {
			p.mu.Unlock()
			return nil, ErrPoolExhausted
		}

		if p.cond == nil {
			p.cond = sync.NewCond(&p.mu)
		}
		p.cond.Wait()
	}
}

func (p *Pool) put(c Conn, forceClose bool) error {
	err := c.Err()
	return p.putCommon(err, c, forceClose)
}

// putCommon contains operations in common with the Pool and the
// SentinelAwarePool.
func (p *Pool) putCommon(err error, c Conn, forceClose bool) error {
	p.mu.Lock()
	if !p.closed && err == nil && !forceClose {
		p.idle.PushFront(idleConn{t: nowFunc(), c: c})
		if p.idle.Len() > p.MaxIdle {
			c = p.idle.Remove(p.idle.Back()).(idleConn).c
		} else {
			c = nil
		}
	}

	if c == nil {
		if p.cond != nil {
			p.cond.Signal()
		}
		p.mu.Unlock()
		return nil
	}

	p.release()
	p.mu.Unlock()
	return c.Close()
}

// The SentinelAwarePool is identical to the normal Pool implementation except
// the configuration of the monitored master is retained. Change in this
// configuration causes a purge of all stored idle connections. The typical
// use of the UpdateMaster method will be in the Dial() method, see the
// example below.
//
// A TestOnReturn function entry point is also supplied. Users are encouraged
// to put a role test in TestOnReturn to prevent connections that persist
// through unexpected role changes without fatal errors from making it back
// into the pool.
//
// Active connections will not be impacted by the purge. The user is expected
// to be aware of this and be able to handle interruptions to the redis
// connection in the event of a failover, which is _not_ seamless.
//
// Example of usage to contact master:
//
//  func newSentinelPool() *redis.SentinelAwarePool {
//	  sentConn, err := redis.NewSentinelClient("tcp",
//      []string{"127.0.0.1:26379", "127.0.0.2:26379", "127.0.0.2.26379"},
//      nil, 100*time.Millisecond, 100*time.Millisecond, 100*time.Millisecond)
//
//	  if err != nil {
//	    log.Println("Failed to connect to redis sentinel:", err)
//	    return nil
//	  }
//
//	  sap := &redis.SentinelAwarePool{
//	    Pool : redis.Pool{
//	      MaxIdle : 10,
//	      IdleTimeout : 240 * time.Second,
//	      TestOnBorrow: func(c redis.Conn, t time.Time) error {
//	        if !redis.TestRole(c, "master") {
//	          return errors.New("Failed role check")
//	        } else {
//	          return nil
//	        }
//	      },
//	    },
//	    TestOnReturn : func(c redis.Conn) error {
//	      if !redis.TestRole(c, "master") {
//	        return errors.New("Failed role check")
//	      } else {
//	        return nil
//	      }
//	    },
//	  }
//
//	  sap.Pool.Dial = func() (redis.Conn, error) {
//	    masterAddr, err := sentConn.QueryConfForMaster("kvmaster")
//	    if err != nil {
//	      return nil, err
//	    }
//	    sap.UpdateMaster(masterAddr)
//
//	    c, err := redis.Dial("tcp", masterAddr)
//	    if err != nil {
//	      return nil, err
//	    }
//	    if !redis.TestRole(c, "master") {
//	      return nil, errors.New("Failed role check")
//	    }
//	    return c, err
//	  }
//
//	  return sap
//	}
//
// The SentinelClient used above will try all connected sentinels when
// querying for the master address. However, if no sentinels can be
// contacted, it may be impossible to dial for some time. The
// SentinelClient should automatically recover once a sentinel returns.
type SentinelAwarePool struct {
	RedisSet string				// the name of redis set
	SentClient *SentinelClient
	SentBClient *SentinelClient
	Pool						// redis pool for master redis
	ReadPool Pool				// redis pool for any redis instance
	// TestOnReturn is a user-supplied function to test a connection before
	// returning it to the pool. Like TestOnBorrow, TestOnReturn will close the
	// connection if an error is observed. It is strongly suggested to test the
	// role of a connection on return, especially if the sentinels in use are
	// older than 2.8.12.
	TestOnReturn func(c Conn) error

	masterAddr string

	// The redis path selector
	RedisPathSelector int
	nextRedisPath int
	redisPaths []redisPath
	mu sync.Mutex
}

// UpdateMaster updates the internal accounting of the SentinelAwarePool,
// which may trigger a flush of all idle connections if the master
// changes.
func (sap *SentinelAwarePool) UpdateMaster(addr string) {
	if addr != sap.masterAddr {
		sap.masterAddr = addr
		sap.closeAll()
		sap.ReadPool.closeAll()
	}
 }

func (sap *SentinelAwarePool) _probeRedisPaths() {
	var err error
	var redisPaths []redisPath
	var redisAddrs []string
	var rp redisPath
	
	redisAddrs, err = sap.SentClient.Probe(sap.RedisSet)
	if err == nil {
		sap.mu.Lock()
		for i, _ := range redisAddrs {
			rp.addr = redisAddrs[i]
			rp.outstanding = 0
			for j, _ := range sap.redisPaths {
				if sap.redisPaths[j].addr == redisAddrs[i] {
					// Save the previous outstanding
					rp.outstanding = sap.redisPaths[j].outstanding
					break
				}
			}
			redisPaths = append(redisPaths, rp)
		}
		sap.redisPaths = redisPaths
		if len(redisPaths) > 0 {
			sap.nextRedisPath = int(rand.Int31n(int32(len(redisPaths))))
		} else {
			sap.nextRedisPath = -1
		}
		sap.mu.Unlock()
	}
}

func (sap *SentinelAwarePool) ProbeRedisPaths() {
	// Add a timer to probe the redis paths, in case that it may miss the published message
	var t *time.Timer
	go func() {
		for {
			t = time.NewTimer(30 * time.Second)
			select {
				case <- t.C:
				sap._probeRedisPaths()
			}
		}
	} ()

	psc := PubSubConn{Conn: Conn(sap.SentBClient)}
	psc.PSubscribe("*sdown")
	psc.PSubscribe("*odown")
	psc.PSubscribe("*role-change")
	psc.PSubscribe("*failover*")
	psc.PSubscribe("*slave**")
	for {
		switch psc.Receive().(type) {
		case PMessage:
			sap._probeRedisPaths()
			
		case  error:
			// Wait till a good sentinel is conntected.
			for {
				if err := sap.SentBClient.Dial(); err != nil {
					// if no good one, sleep a moment for avoiding making system busy
					time.Sleep(100 * time.Millisecond)
				} else {
					break
				}
			}
		}
	}
}

func (sap *SentinelAwarePool) InitRedisPathSelector() {
	if sap.RedisPathSelector != RPSLeastOutstanding {
		return
	}

	sap.redisPaths = make([]redisPath, 0)
	masterAddr, err := sap.SentClient.QueryConfForMaster(sap.RedisSet)
	if err == nil {
		sap.redisPaths = append(sap.redisPaths, redisPath{addr: masterAddr, outstanding: 0})
	}
	slaves, err := sap.SentClient.QueryConfForSlaves(sap.RedisSet)
	if err == nil {
		for i, _ := range slaves {
			flags := SlaveReadFlags(slaves[i])
			if slaves[i]["master-link-status"] == "ok" && !(flags["disconnected"] || flags["sdown"]) {
				sap.redisPaths = append(sap.redisPaths, redisPath{addr: SlaveAddr(slaves[i]), outstanding: 0})
			}
		}
		if len(sap.redisPaths) > 0 {
			sap.nextRedisPath = int(rand.Int31n(int32(len(sap.redisPaths))))
		} else {
			sap.nextRedisPath = -1
		}
	} else {
		sap.nextRedisPath = -1
	}

	// Detect redis path
	go sap.ProbeRedisPaths()
}

// Get returns a redis connection to either master redis or any redis instance with load balance under consideration
func (sap *SentinelAwarePool) Get(isWrite bool) (c Conn) {
	// Pick the path with least outstanding requests
	redisAddr := ""
	if sap.RedisPathSelector == RPSLeastOutstanding && sap.nextRedisPath >= 0 {
		minOutstanding := uint32(0xFFFFFFFF)
		minIndex := -1
		sap.mu.Lock()
		i := sap.nextRedisPath
		for {
			if sap.redisPaths[i].outstanding < minOutstanding {
				redisAddr = sap.redisPaths[i].addr
				minOutstanding = sap.redisPaths[i].outstanding
				minIndex = i
			}
			i = (i + 1) % len(sap.redisPaths)
			if i == sap.nextRedisPath {
				break
			}
		}
		if minIndex >= 0 {
			sap.redisPaths[minIndex].outstanding++
		}
		// go to the next path for avoiding always picking the same path
		sap.nextRedisPath = (sap.nextRedisPath + 1) % len(sap.redisPaths)
		sap.mu.Unlock()
	}

	if isWrite {
		c = sap.Pool.Get()
	} else {
		// Get the prefer redis connection from the pool
		c = sap.ReadPool.GetByPrefer(redisAddr)
	}

	if sap.RedisPathSelector == RPSLeastOutstanding {
		pc, ok := c.(*pooledConnection)
		if ok {
			pc.postClose = func() {
				sap.mu.Lock()
				for i, _ := range sap.redisPaths {
					if sap.redisPaths[i].addr == redisAddr {
						// reduce the outstanding accessing
						// outstanding may be zero if _probeRedisPath was executed multiple times.
						// First outstanding is increased 1, later this path was removed and added by _probeRedisPath.
						if sap.redisPaths[i].outstanding > 0 {
							sap.redisPaths[i].outstanding--
						}
						break
					}
				}
				sap.mu.Unlock()
			}
		}
	}
	
	return
}

// Entrypoint for TestOnReturn, any error here causes the connection to be
// closed instead of being returned to the pool.
func (sap *SentinelAwarePool) testConn(c Conn) error {
	err := c.Err()
	if err != nil {
		return err
	}
	return sap.TestOnReturn(c)
}

func (sap *SentinelAwarePool) put(c Conn, forceClose bool) error {
	err := sap.testConn(c)
	return sap.putCommon(err, c, forceClose)
}

// New interface to allow the pooledConnection to interact with both
// the Pool and the SentinelAwarePool.
type connToPool interface {
	put(Conn, bool) error
}

type pooledConnection struct {
	p     connToPool
	c     Conn
	state int
	postClose func()
}

var (
	sentinel     []byte
	sentinelOnce sync.Once
)

func initSentinel() {
	p := make([]byte, 64)
	if _, err := rand.Read(p); err == nil {
		sentinel = p
	} else {
		h := sha1.New()
		io.WriteString(h, "Oops, rand failed. Use time instead.")
		io.WriteString(h, strconv.FormatInt(time.Now().UnixNano(), 10))
		sentinel = h.Sum(nil)
	}
}

func (pc *pooledConnection) Close() error {
	c := pc.c
	if _, ok := c.(errorConnection); ok {
		return nil
	}
	pc.c = errorConnection{errConnClosed}

	if pc.state&internal.MultiState != 0 {
		c.Send("DISCARD")
		pc.state &^= (internal.MultiState | internal.WatchState)
	} else if pc.state&internal.WatchState != 0 {
		c.Send("UNWATCH")
		pc.state &^= internal.WatchState
	}
	if pc.state&internal.SubscribeState != 0 {
		c.Send("UNSUBSCRIBE")
		c.Send("PUNSUBSCRIBE")
		// To detect the end of the message stream, ask the server to echo
		// a sentinel value and read until we see that value.
		sentinelOnce.Do(initSentinel)
		c.Send("ECHO", sentinel)
		c.Flush()
		for {
			p, err := c.Receive()
			if err != nil {
				break
			}
			if p, ok := p.([]byte); ok && bytes.Equal(p, sentinel) {
				pc.state &^= internal.SubscribeState
				break
			}
		}
	}
	c.Do("")
	pc.p.put(c, pc.state != 0)
	if pc.postClose != nil {
		pc.postClose()
	}
	return nil
}

func (pc *pooledConnection) Err() error {
	return pc.c.Err()
}

func (pc *pooledConnection) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	ci := internal.LookupCommandInfo(commandName)
	pc.state = (pc.state | ci.Set) &^ ci.Clear
	return pc.c.Do(commandName, args...)
}

func (pc *pooledConnection) Send(commandName string, args ...interface{}) error {
	ci := internal.LookupCommandInfo(commandName)
	pc.state = (pc.state | ci.Set) &^ ci.Clear
	return pc.c.Send(commandName, args...)
}

func (pc *pooledConnection) Flush() error {
	return pc.c.Flush()
}

func (pc *pooledConnection) Receive() (reply interface{}, err error) {
	return pc.c.Receive()
}

func (pc *pooledConnection) LocalAddr() net.Addr {
	return pc.c.LocalAddr()
}

func (pc *pooledConnection) RemoteAddr() net.Addr {
	return pc.c.RemoteAddr()
}

type errorConnection struct{ err error }

func (ec errorConnection) Do(string, ...interface{}) (interface{}, error) { return nil, ec.err }
func (ec errorConnection) Send(string, ...interface{}) error              { return ec.err }
func (ec errorConnection) Err() error                                     { return ec.err }
func (ec errorConnection) Close() error                                   { return ec.err }
func (ec errorConnection) Flush() error                                   { return ec.err }
func (ec errorConnection) Receive() (interface{}, error)                  { return nil, ec.err }
func (ec errorConnection) LocalAddr() net.Addr                            { return nil }
func (ec errorConnection) RemoteAddr() net.Addr                           { return nil }
