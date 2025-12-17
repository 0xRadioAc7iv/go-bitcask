// Package bitcask provides a client for interacting with a Bitcask key-value store
// over TCP.
//
// Example:
//
//	client, err := bitcask.Connect()
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
//	err = client.Set("foo", "bar")
//	val, err := client.Get("foo")
package bitcask
