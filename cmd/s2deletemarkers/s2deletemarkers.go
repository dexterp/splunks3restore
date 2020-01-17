package main

import (
	"cd.splunkdev.com/dplameras/s2deletemarkers/internal"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	args := os.Args
	runner := internal.GetUsage(args[1:])
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
