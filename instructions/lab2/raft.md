### Lab 2 A
Implement leader election and heartbeats (AppendEntries RPCs with no log entries). The goal for Part 2A is for a single leader to be elected, for the leader to remain the leader if there are no failures, and for a new leader to take over if the old leader fails or if packets to/from the old leader are lost. Run `go test -race -run 2A` to test your 2A code. 

### Lab 2B 
Implement the leader and follower code to append new log entries. This will involve implementing Start(), completing the AppendEntries RPC structs, sending them, fleshing out the AppendEntry RPC handler, and advancing the commitIndex at the leader. Your first goal should be to pass the TestBasicAgree2B() test (in test_test.go). Once you have that working, you should get all the 2B tests to pass (go test -run 2B). 
