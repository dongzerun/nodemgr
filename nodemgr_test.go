package nodemgr

import (
	"testing"
	"time"
)

func TestGoMap(t *testing.T) {
	err := Init("../../../conf/nodemgr.json")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50000; i++ {
		go func() {
			for {
				GetNode("dusepool", "")
				Vote("dusepool", "127.0.0.1:9800", 1)
			}
		}()
	}
	time.Sleep(30 * time.Second)
}
