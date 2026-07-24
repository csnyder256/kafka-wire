package main

import (
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/pkg/kgo"
)

// scramOpt is isolated in its own file so the SASL import does not spread
// through the command files.
func scramOpt(user, pass string) kgo.Opt {
	return kgo.SASL(scram.Auth{User: user, Pass: pass}.AsSha256Mechanism())
}
