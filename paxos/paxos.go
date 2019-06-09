package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	//"golang.org/x/sync/syncmap"
)

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	minDone  map[int]int
	done     int
	instance map[int]inst
}

type PrepareArgs struct {
	N    int // seq number
	Done int
	Me   int
	Seq  int
}

// PrepareReply defines the reply for the Prepare RPC
// Note that all field names must start with a capital letter for an RPC reply struct
type PrepareOkReply struct {
	N       int         // seq number
	NA      int         // highest accepted proposal
	VA      interface{} // highest accepted value
	Done    int
	Me      int
	Success bool
}

type AcceptArgs struct {
	N   int // seq number
	Seq int
	V   interface{}
	Me  int
}

// PrepareReply defines the reply for the Prepare RPC
// Note that all field names must start with a capital letter for an RPC reply struct
type AcceptOkReply struct {
	N       int // seq number
	V       interface{}
	Success bool
}

type DecideArgs struct {
	Seq int
	V   interface{}
}
type DecideReply struct {
	Success bool
}

type inst struct {
	pNum       int
	state      Fate
	n_a        int
	v_a        interface{}
	me         int
	allDecided bool
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	//fmt.Printf("Calling server %s \n", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.

	//	if ok {
	//		instance.pNum++
	//	}
	//px.mu.Lock()
	//defer px.mu.Unlock()
	// _, ok := px.instance[seq]
	// if !ok {
	// 	px.instance[seq] = inst{pNum: 0,
	// 		state: Pending,
	// 		n_a:   0,
	// 		v_a:   nil,
	// 		me:    px.me}
	// }
	// fmt.Printf("min %d\n", px.Min())
	//if seq < px.Min() {
	//	fmt.Printf("min %d\n", px.Min())
	//	return
	//}
	// proposer function

	go func() {

		fmt.Printf("Proposing seq %d for srv %d \n", seq, px.me)
		px.mu.Lock()
		_, ok := px.instance[seq]
		if !ok {
			px.instance[seq] = inst{pNum: 0,
				state: Pending,
				n_a:   0,
				v_a:   nil,
				me:    px.me}
		}

		px.mu.Unlock()
		if seq < px.Min() {
			fmt.Printf("min %d\n", px.Min())
			return
		}

		//fmt.Printf("min %d\n", px.Min())
		// generate proposal ID for that instance on the server
		//proposalID := 1
		// check if this instance exists
		//px.mu.Lock()

		//px.mu.Unlock()
		var end bool
		finish := make(chan bool)
		//for iters := 0; iters < 1000; iters++ {
		for {
			fmt.Printf("Seq %d and \n", seq)
			instance, ok := px.instance[seq]
			if instance.state == Decided {
				return
			}
			// increase proposal id
			if ok {
				instance.pNum++
			}

			// send prepare
			PrepareReplyChan := make(chan *PrepareOkReply, len(px.peers))
			for i := range px.peers {
				go func(srv int) {
					var ok bool
					args := &PrepareArgs{N: instance.pNum, Me: px.me, Done: px.done, Seq: seq}
					reply := &PrepareOkReply{}
					if px.me == srv {
						//local prepare
						err := px.Prepare(args, reply)
						if err == nil {
							ok = true
						} else {
							ok = false
						}
					} else {
						// rpc prepare
						ok = call(px.peers[srv], "Paxos.Prepare", args, reply)
					}
					if ok {
						PrepareReplyChan <- reply
					} else {
						PrepareReplyChan <- nil
					}

				}(i)
			}
			// check promise from the acceptors
			go func() {
				var successReplies []*PrepareOkReply
				var nReplies int
				majority := (len(px.peers) / 2) + 1
				for r := range PrepareReplyChan {
					nReplies++

					if r != nil && r.Success {
						//fmt.Printf("Success promise \n")
						px.minDone[r.Me] = r.Done
						successReplies = append(successReplies, r)
					}
					if len(successReplies) == majority {
						// send accept if success replies meets the majority

						maxN := -1
						var maxV interface{}
						maxV = nil
						for _, replies := range successReplies {
							if replies.NA >= maxN {
								maxN = replies.NA
								maxV = replies.VA

							}
						}

						if maxV == nil {
							maxV = v
						}
						// send accept to all the servers
						// create a go func
						AcceptReplyChan := make(chan *AcceptOkReply, len(px.peers))

						for i := 0; i < len(px.peers); i++ {
							// send accept to all the peers
							go func(server int) {
								args := &AcceptArgs{N: instance.pNum, Seq: seq, V: maxV, Me: px.me}
								reply := &AcceptOkReply{}
								var ok bool
								if server == px.me {
									err := px.Accept(args, reply)
									if err == nil {
										ok = true
									} else {
										ok = false
									}
								} else {
									ok = call(px.peers[server], "Paxos.Accept", args, reply)
								}
								if ok {
									AcceptReplyChan <- reply
								} else {
									AcceptReplyChan <- nil
								}

							}(i)

						}

						// check success from acceptor for accept reply
						go func() {
							var successReplies []*AcceptOkReply
							var nReplies int
							majority := (len(px.peers) / 2) + 1
							var decidedValue interface{}
							for r := range AcceptReplyChan {
								nReplies++

								//	fmt.Printf("accept replies %d \n", nReplies)
								if r != nil && r.Success {
									successReplies = append(successReplies, r)
									decidedValue = r.V
								}
								if len(successReplies) == majority {
									// send decide RPC call
									fmt.Printf("Sending Decide RPC for seq %d from srv %d \n", seq, px.me)
									DecideReplyChan := make(chan *DecideReply, len(px.peers))
									for i := 0; i < len(px.peers); i++ {
										go func(server int) {

											args := &DecideArgs{Seq: seq, V: decidedValue}
											reply := &DecideReply{}
											if server == px.me {
												err := px.Decide(args, reply)
												if err == nil {
													ok = true
												} else {
													ok = false
												}
											} else {
												ok = call(px.peers[server], "Paxos.Decide", args, reply)
											}
											if ok {
												DecideReplyChan <- reply
											} else {
												DecideReplyChan <- nil
											}

										}(i)
									}
									go func() {
										var successReplies []*DecideReply
										var nReplies int
										majority := (len(px.peers) / 2) + 1

										for r := range DecideReplyChan {
											nReplies++
											if r != nil && r.Success {
												successReplies = append(successReplies, r)

											}

											if len(successReplies) == majority {
												fmt.Printf(" Instance done \n")
												end = true
											}
										}
									}()

								}
								if nReplies == len(px.peers) {
									if len(successReplies) == len(px.peers) {

										end = true
										successReplies = nil
									}

								}

							}
						}()

					}
					if nReplies == len(px.peers) {
						var check bool
						//	if len(successReplies) < majority {
						//		finish <- false
						//} else {
						d := time.Duration(rand.Intn(20) + 10)

						for start := time.Now(); time.Since(start) < time.Second/d; {
							//px.mu.Lock()
							if end {
								fmt.Printf("Instance %d finished/decided \n", seq)
								check = true
								break
							}
							//px.mu.Unlock()
							//fmt.Printf("waiting for seq %d \n", seq)
						}
						//	}
						if check {
							finish <- true
						} else {
							fmt.Printf("possible next run for seq %d \n", seq)
							finish <- false
						}
					}

				}

			}()
			iterate := <-finish
			if iterate {
				fmt.Printf("No more iteration as Instance finished/decided \n")

				break
			}
		} // end iters
	}()

}

// Prepare is RPC handler for the Prepare RPC
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareOkReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	// check if this instance exists on the server
	instance, ok := px.instance[args.Seq]
	if !ok {
		instance = inst{
			state: Pending,
			n_a:   0,
			v_a:   nil,
			me:    px.me}
		px.instance[args.Seq] = instance
	}

	if args.N > instance.pNum {
		fmt.Printf("Inside Prepare with seq %d on srv %d with p_num %d from %d \n", args.Seq, px.me, args.N, args.Me)

		instance.pNum = args.N
		reply.N = args.N
		reply.NA = instance.n_a
		reply.VA = instance.v_a
		reply.Me = px.me
		reply.Done = px.done
		px.minDone[args.Me] = args.Done
		px.instance[args.Seq] = instance
		reply.Success = true
	} else {
		fmt.Printf("Inside Else Prepare with seq %d on srv %d with p_num %d from %d \n", args.Seq, px.me, args.N, args.Me)

		//px.log[args.N] = px.vA
		reply.NA = instance.n_a
		reply.VA = instance.v_a
		reply.Me = px.me
		reply.Done = px.done
		px.minDone[args.Me] = args.Done
		reply.Success = false
	}
	//fmt.Printf("Prepare done \n")
	return nil
}

// Accept is the RPC handler for the accept RPC
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptOkReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	instance, ok := px.instance[args.Seq]
	if ok {
		if args.N >= instance.pNum {
			fmt.Printf("Inside accept with seq %d on srv %d with p_num %d from %d with value \n", args.Seq, px.me, args.N, args.Me)
			instance.pNum = args.N
			instance.n_a = args.N
			if args.V == nil {
				fmt.Printf("nil passed in accept \n")
			}
			instance.v_a = args.V
			//instance.state = Decided
			px.instance[args.Seq] = instance
			reply.N = args.N
			reply.V = args.V
			reply.Success = true
		} else {
			//time.Sleep(1 * time.Second)
			fmt.Printf("Else Inside accept with seq %d on srv %d with maxvalue \n", args.N, px.me)
			//px.log[args.N] = px.vA
			reply.N = args.N
			reply.V = instance.v_a
			reply.Success = false
		}
	}
	return nil
}

// RPC function to send decided values
func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	fmt.Printf("Inside decide with seq %d on srv %d  \n", args.Seq, px.me)
	instance, ok := px.instance[args.Seq]
	if ok {

		instance.v_a = args.V
		instance.state = Decided
		px.instance[args.Seq] = instance
		reply.Success = true

	} else {
		reply.Success = false
	}
	return nil
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	//	px.mu.Lock()
	//	defer px.mu.Unlock()
	px.done = seq

}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	//	px.mu.Lock()
	//	defer px.mu.Unlock()
	max := -1
	for k := range px.instance {
		if k > max {
			max = k
		}
	}
	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	done := 100000000
	//fmt.Printf("Done is %d \n", done)
	var tmp int
	for i := 0; i < len(px.peers); i++ {
		tmp = px.minDone[i]
		if tmp <= done {
			done = tmp
		}
	}
	if done <= -1 {
		done = -1
	}
	done = done + 1
	//fmt.Printf("Done is %d \n", done)
	for k := range px.instance {
		if k < done {
			//	fmt.Printf("Done is %d \n", k)
			//delete(px.instance, k)
			//instance := px.instance[k]
			//instance.v_a = nil
			//instance.state = Forgotten
			//px.instance[k] = instance
			if px.instance[k].v_a == nil {
				fmt.Printf("value at k is nil")
			}
			delete(px.instance, k)

		}
		//	fmt.Printf("zize of srv %d is %v\n", px.me, len(px.instance))
	}
	//fmt.Printf("zize of srv %d is %v\n", px.me, len(px.instance))
	return done
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.

	if seq < px.Min() {

		return Forgotten, nil
	}
	px.mu.Lock()
	val, ok := px.instance[seq]
	px.mu.Unlock()

	if !ok {

		return Pending, nil

	}
	//fmt.Printf("status for srv %d ", val.state)
	return val.state, val.v_a
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	//px.nP = -1
	//px.nA = -1
	//px.vA = nil
	px.done = -1
	//var v interface{}
	//px.log = make(map[int]interface{})
	px.instance = make(map[int]inst)
	px.minDone = make(map[int]int)
	for i := 0; i < len(px.peers); i++ {
		px.minDone[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		fmt.Printf("Listening on %s \n", peers[me])
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {

				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						//	fmt.Printf("inside gooooo discard request\n")
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						//	fmt.Printf("inside gooooo discard reply\n")
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						//	fmt.Printf("inside gooooo \n")
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
