package simplepb

//
// This is a outline of primary-backup replication based on a simplifed version of Viewstamp replication.
//
//
//

import (
	"fmt"
	"sync"
	"time"

	"labrpc"
)

// the 3 possible server status
const (
	NORMAL = iota
	VIEWCHANGE
	RECOVERING
)

// PBServer defines the state of a replica server (either primary or backup)
type PBServer struct {
	mu             sync.Mutex          // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // RPC end points of all peers
	me             int                 // this peer's index into peers[]
	currentView    int                 // what this peer believes to be the current active view
	status         int                 // the server's current status (NORMAL, VIEWCHANGE or RECOVERING)
	lastNormalView int                 // the latest view which had a NORMAL status

	log         []interface{} // the log of "commands"
	commitIndex int           // all log entries <= commitIndex are considered to have been committed.

	// ... other state that you might need ...
}

// Prepare defines the arguments for the Prepare RPC
// Note that all field names must start with a capital letter for an RPC args struct
type PrepareArgs struct {
	View          int         // the primary's current view
	PrimaryCommit int         // the primary's commitIndex
	Index         int         // the index position at which the log entry is to be replicated on backups
	Entry         interface{} // the log entry to be replicated
}

// PrepareReply defines the reply for the Prepare RPC
// Note that all field names must start with a capital letter for an RPC reply struct
type PrepareReply struct {
	View    int  // the backup's current view
	Success bool // whether the Prepare request has been accepted or rejected
}

// RecoverArgs defined the arguments for the Recovery RPC
type RecoveryArgs struct {
	View   int // the view that the backup would like to synchronize with
	Server int // the server sending the Recovery RPC (for debugging)
}

type RecoveryReply struct {
	View          int           // the view of the primary
	Entries       []interface{} // the primary's log including entries replicated up to and including the view.
	PrimaryCommit int           // the primary's commitIndex
	Success       bool          // whether the Recovery request has been accepted or rejected
}

type ViewChangeArgs struct {
	View int // the new view to be changed into
}

type ViewChangeReply struct {
	LastNormalView int           // the latest view which had a NORMAL status at the server
	Log            []interface{} // the log at the server
	Success        bool          // whether the ViewChange request has been accepted/rejected
}

type StartViewArgs struct {
	View int           // the new view which has completed view-change
	Log  []interface{} // the log associated with the new new
}

type StartViewReply struct {
}

// GetPrimary is an auxilary function that returns the server index of the
// primary server given the view number (and the total number of replica servers)
func GetPrimary(view int, nservers int) int {
	return view % nservers
}

// IsCommitted is called by tester to check whether an index position
// has been considered committed by this server
func (srv *PBServer) IsCommitted(index int) (committed bool) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.commitIndex >= index {
		return true
	}
	return false
}

// ViewStatus is called by tester to find out the current view of this server
// and whether this view has a status of NORMAL.
func (srv *PBServer) ViewStatus() (currentView int, statusIsNormal bool) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.currentView, srv.status == NORMAL
}

// GetEntryAtIndex is called by tester to return the command replicated at
// a specific log index. If the server's log is shorter than "index", then
// ok = false, otherwise, ok = true
func (srv *PBServer) GetEntryAtIndex(index int) (ok bool, command interface{}) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.log) > index {
		return true, srv.log[index]
	}
	return false, command
}

// Kill is called by tester to clean up (e.g. stop the current server)
// before moving on to the next test
func (srv *PBServer) Kill() {
	// Your code here, if necessary
}

// Make is called by tester to create and initalize a PBServer
// peers is the list of RPC endpoints to every server (including self)
// me is this server's index into peers.
// startingView is the initial view (set to be zero) that all servers start in
func Make(peers []*labrpc.ClientEnd, me int, startingView int) *PBServer {
	srv := &PBServer{
		peers:          peers,
		me:             me,
		currentView:    startingView,
		lastNormalView: startingView,
		status:         NORMAL,
	}
	// all servers' log are initialized with a dummy command at index 0
	var v interface{}
	srv.log = append(srv.log, v)

	// Your other initialization code here, if there's any

	return srv
}

// Start() is invoked by tester on some replica server to replicate a
// command.  Only the primary should process this request by appending
// the command to its log and then return *immediately* (while the log is being replicated to backup servers).
// if this server isn't the primary, returns false.
// Note that since the function returns immediately, there is no guarantee that this command
// will ever be committed upon return, since the primary
// may subsequently fail before replicating the command to all servers
//
// The first return value is the index that the command will appear at
// *if it's eventually committed*. The second return value is the current
// view. The third return value is true if this server believes it is
// the primary.
func (srv *PBServer) Start(command interface{}) (
	index int, view int, ok bool) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	fmt.Printf("Printing the cmd %v on srv %d with view %d \n", command, srv.me, srv.currentView)
	// do not process command if status is not NORMAL
	// and if i am not the primary in the current view
	if srv.status != NORMAL {
		return -1, srv.currentView, false
	} else if GetPrimary(srv.currentView, len(srv.peers)) != srv.me {
		return -1, srv.currentView, false
	}
	index = len(srv.log)

	//index = srv.currentView + 1
	view = srv.currentView
	ok = true
	//fmt.Printf("yo index %d \n", srv.log[1])
	srv.log = append(srv.log, command)
	fmt.Printf("primary srv %d log size %d and log %v @ time %s\n", srv.me, len(srv.log), srv.log, time.Now().String())
	// Your code here
	// iterate through all the backup servers
	ReplyChan := make(chan *PrepareReply, len(srv.peers)-1)
	for i := 0; i < len(srv.peers); i++ {
		if srv.me != i {
			go func(server int) {

				// call the Prepare func
				args := &PrepareArgs{View: srv.currentView,
					PrimaryCommit: srv.commitIndex,
					Index:         index,
					Entry:         command}
				reply := &PrepareReply{}
				//var reply PrepareReply
				//		fmt.Printf("Sending Prepare %v to srv %d \n", args, server)
				ok := srv.sendPrepare(server, args, reply)
				if ok {

					ReplyChan <- reply

				} else {
					ReplyChan <- nil
				}
			}(i)
		}

	}

	go func() {
		var successReplies []*PrepareReply
		var nReplies int
		majority := len(srv.peers) / 2 // you only success from f=1 with quorum of 3
		for r := range ReplyChan {
			nReplies++
			if r != nil && r.Success {
				successReplies = append(successReplies, r)
			}
			if len(successReplies) == majority {
				srv.commitIndex = index

				break
			}
		}

		fmt.Printf("commit index %d for srv %d with view %d\n", srv.commitIndex, srv.me, srv.currentView)
	}()

	//fmt.Printf("yo %d %d \n", index, view)
	return index, view, ok
}

// exmple code to send an AppendEntries RPC to a server.
// server is the index of the target server in srv.peers[].
// expects RPC arguments in args.
// The RPC library fills in *reply with RPC reply, so caller should pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
func (srv *PBServer) sendPrepare(server int, args *PrepareArgs, reply *PrepareReply) bool {
	//	fmt.Printf("inside sendPrepare \n")
	ok := srv.peers[server].Call("PBServer.Prepare", args, reply)

	return ok
}

// Prepare is the RPC handler for the Prepare RPC
func (srv *PBServer) Prepare(args *PrepareArgs, reply *PrepareReply) {
	// Your code here
	//srv.mu.Lock()
	//srv.mu.Lock()
	//defer srv.mu.Unlock()
	// wait to get the right order
	//lag := 1
	//	if len(srv.log) < args.Index {

	//		lag = args.Index - len(srv.log)
	//	}
	//
	//	fmt.Printf("outside for size %d srv: %d args %v   with lag %d \n", len(srv.log), srv.me, args, lag)
	t0 := time.Now()
	time.Sleep(100 * time.Millisecond)
	//c := 0
	for { //|| (time.Since(t0).Seconds() > float64(lag/10))
		//	fmt.Printf("outside for size %d srv: %d args %v   with lag %d \n", len(srv.log), srv.me, args, lag)

		//c = c + 1

		if len(srv.log) == args.Index {
			//	fmt.Printf("in for size %d srv: %d   with view %d and args %v   with lag %d \n",
			//		len(srv.log), srv.me, srv.currentView, args, lag)
			//fmt.Printf("%v \n", args)

			break
		}
		if srv.currentView < args.View {
			break
		}
		// Need better way to order the request
		// Maybe assign a new variables in PBserver to track the index order
		if time.Since(t0).Seconds() > 1 {
			break
		}

	}

	if srv.currentView == args.View && len(srv.log) == args.Index {
		fmt.Printf("inside if %v\n", args)
		time.Sleep(100 * time.Millisecond)
		//srv.mu.Lock()
		srv.log = append(srv.log, args.Entry)
		reply.View = args.View
		reply.Success = true
		srv.commitIndex = args.PrimaryCommit
		//srv.mu.Unlock()
		//fmt.Printf("Log for server %d is %v \n", srv.me, srv.log)
	} else if srv.currentView > args.View {
		//fmt.Printf("No prepapre success %v\n", args)
		reply.Success = false
	} else if srv.currentView >= args.View && len(srv.log) > args.Index {
		//srv.mu.Lock()
		//fmt.Printf("No prepapre success %v\n", args)
		reply.Success = false
		//srv.mu.Unlock()
	} else {
		//fmt.Printf("Failed Replies %v for backup %d log_size %d \n", args, srv.me, len(srv.log))
		//reply.Success = false
		// call recovery
		rargs := &RecoveryArgs{
			View:   args.View,
			Server: srv.me,
		}
		recoveryReply := &RecoveryReply{}
		ok := srv.peers[GetPrimary(args.View, len(srv.peers))].Call("PBServer.Recovery", rargs, recoveryReply)
		if ok {
			fmt.Printf("Recover: %v and current view of srv %d is %d\n", args, srv.me, srv.currentView)
			srv.log = recoveryReply.Entries
			srv.commitIndex = recoveryReply.PrimaryCommit
			reply.View = recoveryReply.View
			reply.Success = true

		}
	}
	//srv.mu.Unlock()
}

// Recovery is the RPC handler for the Recovery RPC
func (srv *PBServer) Recovery(args *RecoveryArgs, reply *RecoveryReply) {
	// Your code here
	//srv.mu.Lock()
	fmt.Printf("inside Recovery with Primary Server size %d \n", len(srv.log))
	reply.Success = true
	reply.View = srv.currentView
	reply.PrimaryCommit = srv.commitIndex
	reply.Entries = srv.log
	//srv.mu.Unlock()

}

// Some external oracle prompts the primary of the newView to
// switch to the newView.
// PromptViewChange just kicks start the view change protocol to move to the newView
// It does not block waiting for the view change process to complete.
func (srv *PBServer) PromptViewChange(newView int) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	newPrimary := GetPrimary(newView, len(srv.peers))
	//	fmt.Printf("View change for %d \n", newView)
	if newPrimary != srv.me { //only primary of newView should do view change
		return
	} else if newView <= srv.currentView {
		return
	}
	vcArgs := &ViewChangeArgs{
		View: newView,
	}
	vcReplyChan := make(chan *ViewChangeReply, len(srv.peers))
	// send ViewChange to all servers including myself
	for i := 0; i < len(srv.peers); i++ {
		go func(server int) {
			var reply ViewChangeReply
			ok := srv.peers[server].Call("PBServer.ViewChange", vcArgs, &reply)
			// fmt.Printf("node-%d (nReplies %d) received reply ok=%v reply=%v\n", srv.me, nReplies, ok, r.reply)
			if ok {
				vcReplyChan <- &reply
			} else {
				vcReplyChan <- nil
			}
		}(i)
	}

	// wait to receive ViewChange replies
	// if view change succeeds, send StartView RPC
	go func() {
		var successReplies []*ViewChangeReply
		var nReplies int
		majority := len(srv.peers)/2 + 1
		for r := range vcReplyChan {
			nReplies++
			if r != nil && r.Success {
				successReplies = append(successReplies, r)
			}
			if nReplies == len(srv.peers) || len(successReplies) == majority {
				//	fmt.Printf("size of successfule replies %d for view %d \n", len(successReplies), newView)
				break
			}
		}
		ok, log := srv.determineNewViewLog(successReplies)
		if !ok {
			fmt.Printf("No view change for %d \n", newView)
			return
		}
		svArgs := &StartViewArgs{
			View: vcArgs.View,
			Log:  log,
		}
		// send StartView to all servers including myself
		for i := 0; i < len(srv.peers); i++ {
			var reply StartViewReply
			go func(server int) {
				// fmt.Printf("node-%d sending StartView v=%d to node-%d\n", srv.me, svArgs.View, server)
				srv.peers[server].Call("PBServer.StartView", svArgs, &reply)
			}(i)
		}
	}()
}

// determineNewViewLog is invoked to determine the log for the newView based on
// the collection of replies for successful ViewChange requests.
// if a quorum of successful replies exist, then ok is set to true.
// otherwise, ok = false.
func (srv *PBServer) determineNewViewLog(successReplies []*ViewChangeReply) (
	ok bool, newViewLog []interface{}) {
	// Your code here

	var nReplies int
	majority := len(srv.peers)/2 + 1
	latestView := 0
	longestLog := 0
	//var Log interface{}
	for _, replies := range successReplies {
		if replies.Success {
			nReplies++
			if replies.LastNormalView > latestView {
				latestView = replies.LastNormalView
				//fmt.Printf("this %d maybe the longest Log is %v & view is %d\n", nReplies, replies, srv.currentView)
				longestLog = len(replies.Log)
				newViewLog = replies.Log

			}
			if replies.LastNormalView == latestView {
				if len(replies.Log) >= longestLog {
					//fmt.Printf("Longest Log is %v \n", replies.Log)
					longestLog = len(replies.Log)
					newViewLog = replies.Log
				}
			}

		}
	}

	if nReplies >= majority {
		ok = true
		//	fmt.Printf("Longest logs determined for pri %d,%d,%v \n", srv.me, srv.currentView, newViewLog)
	} else {
		ok = false
		//newViewLog
	}
	//fmt.Printf("log size %d   for view %d \n", longestLog, latestView)
	return ok, newViewLog

}

// ViewChange is the RPC handler to process ViewChange RPC.
func (srv *PBServer) ViewChange(args *ViewChangeArgs, reply *ViewChangeReply) {
	// Your code here
	//srv.mu.Lock()
	//defer srv.mu.Unlock()
	time.Sleep(80 * time.Millisecond)
	if srv.currentView < args.View {
		srv.currentView = args.View
		srv.status = VIEWCHANGE
		//	fmt.Printf("len is %d and commit index is %d \n", len(srv.log), srv.commitIndex)
		//reply.Log = srv.log[0 : srv.commitIndex+1]
		reply.Log = srv.log
		reply.LastNormalView = srv.lastNormalView
		reply.Success = true
		//	fmt.Printf("View change %d needed for srv %d and log is %v \n", args.View, srv.me, reply.Log)
	} else {
		//	fmt.Printf("Didn;t need the view change for server %d as curr view is %d and args view is %d \n",
		//			srv.me, srv.currentView, args.View)
		reply.Success = false
	}
}

// StartView is the RPC handler to process StartView RPC.
func (srv *PBServer) StartView(args *StartViewArgs, reply *StartViewReply) {
	// Your code here
	//srv.mu.Lock()
	//defer srv.mu.Unlock()
	//fmt.Printf("Sview for %d \n", args.View)
	time.Sleep(80 * time.Millisecond)
	if srv.currentView <= args.View {
		//srv.mu.Lock()
		//fmt.Printf("Started view for %d and log is %v \n", args.View, args.Log)
		srv.log = args.Log
		srv.lastNormalView = srv.currentView
		srv.status = NORMAL
		fmt.Printf("Started view for %d and log is %v and status %d for srv %d with %s \n", args.View, args.Log, srv.status, srv.me, time.Now().String())
		//fmt.Printf("Log during sview is %v\n", args.Log)

		//srv.mu.Unlock()
	} else {
		fmt.Printf("cannot start view as curr view is %d \n", srv.currentView)
	}
}
