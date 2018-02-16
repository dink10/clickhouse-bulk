package main

import (
	"fmt"
	"github.com/buaazp/fasthttprouter"
	"github.com/valyala/fasthttp"
	"log"
)

// Server - main server object
type Server struct {
	Listen    string
	Collector *Collector
	Debug     bool
	router    *fasthttprouter.Router
}

// NewServer - create server
func NewServer(listen string, collector *Collector, debug bool) *Server {
	return &Server{Listen: listen, Collector: collector, Debug: debug}
}

func (server *Server) writeHandler(c *fasthttp.RequestCtx) {
	q := c.Request.Body()
	s := string(q)

	if server.Debug {
		log.Printf("query %+v %+v\n", c.RequestURI(), s)
	}

	qs := string(c.RequestURI())
	params, content, insert := server.Collector.ParseQuery(qs, s)
	if insert {
		go server.Collector.Push(params, content)
		c.SetStatusCode(fasthttp.StatusOK)
		fmt.Print(c, "")
	} else {
		resp, status := server.Collector.Sender.SendQuery(params, content)
		c.SetStatusCode(status)
		fmt.Print(c, resp)
	}
}

func (server *Server) statusHandler(c *fasthttp.RequestCtx) {
	c.SetStatusCode(fasthttp.StatusOK)
	fmt.Fprintf(c, "%s", "Ok")
}

func (server *Server) Start() error {
	return fasthttp.ListenAndServe(server.Listen, server.router.Handler)
}

// RunServer - run server
func InitServer(listen string, collector *Collector, debug bool) *Server {
	server := NewServer(listen, collector, debug)
	router := fasthttprouter.New()
	router.POST("/", server.writeHandler)
	router.GET("/status", server.statusHandler)
	server.router = router
	return server
}
