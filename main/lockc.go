package main

//
// see comments in lockd.go
//

import "github.com/lysu/6824/lockservice"
import "os"
import "fmt"

func usageLockc() {
	fmt.Printf("Usage: lockc -l|-u primaryport backupport lockname\n")
	os.Exit(1)
}

func main() {
	if len(os.Args) == 5 {
		ck := lockservice.MakeClerk(os.Args[2], os.Args[3])
		var ok bool
		if os.Args[1] == "-l" {
			ok = ck.Lock(os.Args[4])
		} else if os.Args[1] == "-u" {
			ok = ck.Unlock(os.Args[4])
		} else {
			usageLockc()
		}
		fmt.Printf("reply: %v\n", ok)
	} else {
		usageLockc()
	}
}
