package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type clickhouseConfig struct {
	Servers     []string `json:"servers"`
	DownTimeout int      `json:"down_timeout"`
}

type config struct {
	Listen        string           `json:"listen"`
	Clickhouse    clickhouseConfig `json:"clickhouse"`
	FlushCount    int              `json:"flush_count"`
	FlushInterval int              `json:"flush_interval"`
	DumpDir       string           `json:"dump_dir"`
	Debug         bool             `json:"debug"`
}

func safeQuit(collect *Collector, sender Sender) {
	collect.FlushAll()
	if count := sender.Len(); count > 0 {
		log.Printf("Sending %+v tables\n", count)
	}
	for !sender.Empty() && !collect.Empty() {
		collect.WaitFlush()
	}
	collect.WaitFlush()

	os.Exit(1)
}

func main() {

	log.SetOutput(os.Stdout)

	configFile := flag.String("config", "config.json", "config file (json)")

	flag.Parse()
	cnf := config{}
	err := ReadJSON(*configFile, &cnf)
	if err != nil {
		log.Printf("Config file %+v not found. Use config.sample.json\n", *configFile)
		err := ReadJSON("config.sample.json", &cnf)
		if err != nil {
			log.Fatalf("Read config: %+v\n", err.Error())
		}
	}

	dumper := new(FileDumper)
	dumper.Path = cnf.DumpDir
	sender := NewClickhouse(cnf.Clickhouse.DownTimeout)
	sender.Dumper = dumper
	for _, url := range cnf.Clickhouse.Servers {
		sender.AddServer(url)
	}

	collect := NewCollector(sender, cnf.FlushCount, cnf.FlushInterval)

	// send collected data on SIGTERM and exit
	signals := make(chan os.Signal)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	srv := InitServer(cnf.Listen, collect, cnf.Debug)

	_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		for {
			_ = <-signals
			log.Printf("STOP signal\n")
			safeQuit(collect, sender)
		}
	}()

	err = srv.Start()
	if err != nil {
		log.Printf("ListenAndServe: %+v\n", err)
		safeQuit(collect, sender)
	}
}
