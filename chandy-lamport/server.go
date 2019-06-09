package chandy_lamport

import (
	"fmt"
	"log"
)

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	// TODO: ADD MORE FIELDS HERE
	//isSnapServer int

	//markers map[int][]*Link
	markers *SyncMap
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		//make(map[int][]*Link),
		NewSyncMap(),
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	// TODO: IMPLEMENT ME
	switch message := message.(type) {
	case TokenMessage:
		// add the tokens
		server.Tokens += message.numTokens
		fmt.Println("inside token from", src)
		fmt.Println(server.Id)
		// check if the server has received any marker message
		// from the src server
		size := 0
		server.markers.Range(func(k interface{}, v interface{}) bool {
			size++
			return true
		})

		if size > 0 {
			//fmt.Println("inside forloop token")
			//		for id, markerMessages := range server.markers {
			server.markers.Range(func(k interface{}, v interface{}) bool {
				val, ok := server.markers.Load(k)
				markerMessages := val.([]*Link)
				id := k.(int)
				if ok {
					check := true
					for _, m := range markerMessages {
						// if snapshotted then don't record on this channel
						if m.src == src {
							check = false
						}
					}

					if check {
						// save the tokens on this channel for the snapshot

						tokenMessage := SnapshotMessage{src, server.Id, message}
						fmt.Println("tokens,", tokenMessage)
						fmt.Println("id:", id)
						fmt.Println("id:", id)
						server.sim.snapshots[id].messages = append(server.sim.snapshots[id].messages, &tokenMessage)
					}

				}
				//}
				return true
			})
		}

	case MarkerMessage:
		l := Link{src, server.Id, NewQueue()}
		// add marker snapshot ID to the map as the key
		// append the link information to the snapshot ID
		fmt.Println("inside marker from", src)
		fmt.Println(server.Id)

		val, ok := server.markers.Load(message.snapshotId)

		if ok == false {
			//fmt.Println("inside marker if")
			var newVal []*Link
			newVal = append(newVal, &l)
			server.markers.Store(message.snapshotId, newVal)
			server.SendToNeighbors(MarkerMessage{message.snapshotId})
			server.sim.snapshots[message.snapshotId].tokens[server.Id] = server.Tokens
		} else {
			//fmt.Println("inside marker else")
			//var newVal []*Link
			newVal := val.([]*Link)
			newVal = append(newVal, &l)
			server.markers.Store(message.snapshotId, newVal)

		}

		//server.markers[message.snapshotId] = append(server.markers[message.snapshotId], &l)

		// send it to the neighbors

		// check if all the inbound links have received this snapshotID
		vals, ok := server.markers.Load(message.snapshotId)
		fmt.Println("inside marker before notify", vals)
		size := vals.([]*Link)
		fmt.Println("inside marker before notify")
		if len(size) >= len(server.inboundLinks) {
			// notify simulator
			fmt.Println("notify")
			server.sim.NotifySnapshotComplete(server.Id, message.snapshotId)
		}

	default:
		log.Fatal("Unknown event command: ")
	}
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME
	// Record its local state
	//server.isSnapServer = 1
	//SnapshotState{snapshotId, t, nil}
	tokens := make(map[string]int)
	tokens[server.Id] = server.Tokens
	l := Link{server.Id, server.Id, NewQueue()}
	var newVal []*Link
	newVal = append(newVal, &l)
	server.markers.Store(snapshotId, newVal)
	//server.markers[snapshotId] = append(server.markers[snapshotId], &l)
	snap := SnapshotState{snapshotId, tokens, nil}
	server.sim.snapshots[snapshotId] = &snap
	// Send marker message to the neighbors
	server.SendToNeighbors(MarkerMessage{snapshotId})
}
