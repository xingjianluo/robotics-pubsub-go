package pubsub

import "time"

func timerC() <-chan time.Time { return time.After(2 * time.Second) }
