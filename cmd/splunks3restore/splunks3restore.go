package main

import (
	"github.com/crosseyed/splunks3restore/internal"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	args := os.Args
	runner := internal.GetUsage(args[1:], internal.Version)
	trapC := trapSignals()
	runner.Run(trapC)
}

func trapSignals() <-chan os.Signal {
	sigTrap := make(chan os.Signal)
	signal.Notify(sigTrap, syscall.SIGTERM)
	signal.Notify(sigTrap, syscall.SIGINT)
	signal.Notify(sigTrap, syscall.SIGQUIT)
	return sigTrap
}
