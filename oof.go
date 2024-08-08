// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The nntp package implements a client for the news protocol NNTP,
// as defined in RFC 3977.
package nntp

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ...

// A Conn represents a connection to an NNTP server. The connection with
// an NNTP server is stateful; it keeps track of what group you have
// selected, if any, and (if you have a group selected) which article is
// current, next, or previous.
//
// Some methods that return information about a specific message take
// either a message-id, which is global across all NNTP servers, groups,
// and messages, or a message-number, which is an integer number that is
// local to the NNTP session and currently selected group.
//
// For all methods that return an io.Reader (or an *Article, which contains
// an io.Reader), that io.Reader is only valid until the next call to a
// method of Conn.
type Conn struct {
	conn  io.WriteCloser
	r     *bufio.Reader
	br    *bodyReader
	close bool

	// Pipelining support
	pipeline []string
	pipelinedArticles []Article
	pipelineErrors []error
	supportsPipelining bool
}

// ...

// Capabilities returns a list of features this server performs.
// Not all servers support capabilities.
func (c *Conn) Capabilities() ([]string, error) {
	if _, _, err := c.cmd(101, "CAPABILITIES"); err != nil {
		return nil, err
	}
	caps, err := c.readStrings()
	if err != nil {
		return nil, err
	}
	// Check if the server supports pipelining
	for _, cap := range caps {
		if cap == "PIPELINE" {
			c.supportsPipelining = true
			c.pipeline = make([]string, 0, 10)
			c.pipelinedArticles = make([]Article, 0, 10)
			c.pipelineErrors = make([]error, 0, 10)
		}
	}
	return caps, nil
}

// RawPost reads a text-formatted article from r and posts it to the server.
func (c *Conn) RawPost(r io.Reader) error {
	if _, _, err := c.cmd(3, "POST"); err != nil {
		return err
	}
	if err := c.sendLines(r); err != nil {
		return err
	}
	if _, _, err := c.cmd(240, "."); err != nil {
		return err
	}
	return nil
}

// Post posts an article to the server.
func (c *Conn) Post(a *Article) error {
	if c.supportsPipelining && len(c.pipeline) > 0 {
		// Use pipelining if available
		c.pipelinedArticles = append(c.pipelinedArticles, *a)
		c.pipeline = append(c.pipeline, "POST")
		c.pipelineErrors = append(c.pipelineErrors, nil)
		return nil
	} else {
		// Fall back to normal posting
		return c.RawPost(&articleReader{a: a})
	}
}

// flushPipeline flushes the pipeline and returns any errors that occurred
func (c *Conn) flushPipeline() error {
	var err error
	for i, cmd := range c.pipeline {
		switch cmd {
		case "POST":
			_, _, err = c.cmd(240, ".")
			if err != nil {
				// If posting failed, keep the article in the pipeline to retry later
				c.pipelineErrors[i] = err
			} else {
				c.pipelineErrors[i] = nil
			}
		}
	}
	// Retry any failed article posts
	for i, err := range c.pipelineErrors {
		if err != nil {
			if err := c.RawPost(&articleReader{a: &c.pipelinedArticles[i]}); err != nil {
				return err
			}
		}
	}
	c.pipeline = c.pipeline[:0]
	c.pipelinedArticles = c.pipelinedArticles[:0]
	c.pipelineErrors = c.pipelineErrors[:0]
	return nil
}

// Quit sends the QUIT command and closes the connection to the server.
func (c *Conn) Quit() error {
	_, _, err := c.cmd(0, "QUIT")
	if c.supportsPipelining {
		if err := c.flushPipeline(); err != nil {
			return err
		}
	}
	c.conn.Close()
	c.close = true
	return err
}

// ...
