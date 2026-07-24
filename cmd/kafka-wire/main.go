// Command kafka-wire is a single-node, Kafka-wire-protocol-compatible message
// broker: one binary, one data directory, no coordination service, and an
// optional cold-storage tier on any object store.
//
// Run it with no arguments and it starts a broker on localhost with sane
// defaults and no configuration file. Everything past that is opt-in.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
)

// version is stamped at build time:
//
//	go build -ldflags "-X main.version=v0.1.0 -X main.commit=$(git rev-parse --short HEAD)"
var (
	version = "dev"
	commit  = "none"
)

const usage = `kafka-wire - a single-node Kafka-compatible broker in one binary

USAGE
  kafka-wire <command> [flags]

COMMANDS
  serve       run the broker (default when no command is given)
  produce     send records to a topic from arguments or stdin
  consume     read records from a topic and print them
  topic       list, create, describe, and delete topics
  config      init, print, and validate configuration
  doctor      check this machine and this configuration for common problems
  version     print version information

Run "kafka-wire <command> --help" for the flags of a single command.

QUICKSTART
  kafka-wire serve &
  kafka-wire topic create demo
  echo hello | kafka-wire produce demo
  kafka-wire consume demo --from-beginning

DOCUMENTATION
  https://github.com/csnyder256/kafka-wire
`

func main() {
	args := os.Args[1:]

	// Bare "kafka-wire" runs the broker. Making the common case need no
	// subcommand is worth the small ambiguity, and an unknown first argument
	// is still reported rather than silently treated as a flag.
	if len(args) == 0 {
		os.Exit(run(context.Background(), "serve", nil))
	}
	if strings.HasPrefix(args[0], "-") {
		switch args[0] {
		case "-h", "--help", "-help":
			fmt.Print(usage)
			return
		case "-v", "--version":
			printVersion()
			return
		}
		os.Exit(run(context.Background(), "serve", args))
	}
	os.Exit(run(context.Background(), args[0], args[1:]))
}

func run(ctx context.Context, cmd string, args []string) int {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var err error
	switch cmd {
	case "serve", "start":
		err = cmdServe(ctx, args)
	case "produce":
		err = cmdProduce(ctx, args)
	case "consume":
		err = cmdConsume(ctx, args)
	case "topic", "topics":
		err = cmdTopic(ctx, args)
	case "config":
		err = cmdConfig(ctx, args)
	case "doctor":
		err = cmdDoctor(ctx, args)
	case "version":
		printVersion()
		return 0
	case "help":
		fmt.Print(usage)
		return 0
	default:
		fmt.Fprintf(os.Stderr, "kafka-wire: unknown command %q\n\n%s", cmd, usage)
		return 2
	}

	if err != nil {
		// Configuration problems are already formatted as multi-line reports
		// with fix instructions, so they print as-is rather than being
		// squeezed behind an "error:" prefix.
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

func printVersion() {
	fmt.Printf("kafka-wire %s (commit %s, %s, %s/%s)\n",
		version, commit, runtime.Version(), runtime.GOOS, runtime.GOARCH)
}
