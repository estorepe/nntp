package nntp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Conn represents a connection to an NNTP server.
type Conn struct {
	conn       io.ReadWriteCloser
	r          *bufio.Reader
	w          *bufio.Writer
	pipelining bool
}

// Article represents an NNTP article.
type Article struct {
	Header map[string][]string
	Body   io.Reader
}

// BufferPool manages a pool of byte buffers.
type BufferPool struct {
	pool sync.Pool
}

// Date returns the current time on the server.
func (c *Conn) Date() (time.Time, error) {
	_, line, err := c.cmd(111, "DATE")
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse("20060102150405", line)
}

// Group selects a group and returns its statistics.
func (c *Conn) Group(name string) (number, low, high int, err error) {
	_, line, err := c.cmd(211, "GROUP %s", name)
	if err != nil {
		return 0, 0, 0, err
	}
	_, err = fmt.Sscanf(line, "%d %d %d", &number, &low, &high)
	return
}

// List returns a list of groups.
func (c *Conn) List() ([]Group, error) {
	_, _, err := c.cmd(215, "LIST")
	if err != nil {
		return nil, err
	}
	var groups []Group
	for {
		line, err := c.r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if line == ".\r\n" {
			break
		}
		var g Group
		_, err = fmt.Sscanf(line, "%s %d %d %s", &g.Name, &g.High, &g.Low, &g.Status)
		if err != nil {
			return nil, err
		}
		groups = append(groups, g)
	}
	return groups, nil
}

// Group represents a newsgroup.
type Group struct {
	Name   string
	High   int
	Low    int
	Status string
}

// Article retrieves an article by its number or message ID.
func (c *Conn) Article(id string) (*Article, error) {
	_, _, err := c.cmd(220, "ARTICLE %s", id)
	if err != nil {
		return nil, err
	}
	return c.readArticle()
}

func (c *Conn) readArticle() (*Article, error) {
	article := &Article{Header: make(map[string][]string)}

	// Read headers
	for {
		line, err := c.r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimSpace(line)
		if line == "" {
			break
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		article.Header[key] = append(article.Header[key], value)
	}

	// Read body
	var body bytes.Buffer
	for {
		line, err := c.r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if line == ".\r\n" {
			break
		}
		body.WriteString(line)
	}
	article.Body = &body

	return article, nil
}

// NewGroups returns a list of newsgroups created since the given time.
func (c *Conn) NewGroups(since time.Time) ([]Group, error) {
	_, _, err := c.cmd(231, "NEWGROUPS %s GMT", since.Format("20060102 150405"))
	if err != nil {
		return nil, err
	}
	return c.readGroupList()
}

func (c *Conn) readGroupList() ([]Group, error) {
	var groups []Group
	for {
		line, err := c.r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if line == ".\r\n" {
			break
		}
		var g Group
		_, err = fmt.Sscanf(line, "%s %d %d %s", &g.Name, &g.High, &g.Low, &g.Status)
		if err != nil {
			return nil, err
		}
		groups = append(groups, g)
	}
	return groups, nil
}

// NewBufferPool creates a new BufferPool.
func NewBufferPool() *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
}

// Get retrieves a buffer from the pool.
func (bp *BufferPool) Get() *bytes.Buffer {
	return bp.pool.Get().(*bytes.Buffer)
}

// Put returns a buffer to the pool.
func (bp *BufferPool) Put(buf *bytes.Buffer) {
	buf.Reset()
	bp.pool.Put(buf)
}

var globalBufferPool = NewBufferPool()

// Dial connects to an NNTP server.
func Dial(network, addr string) (*Conn, error) {
	c, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	return newConn(c)
}

// DialTLS connects to an NNTP server using TLS.
func DialTLS(network, addr string, config *tls.Config) (*Conn, error) {
	c, err := tls.Dial(network, addr, config)
	if err != nil {
		return nil, err
	}
	return newConn(c)
}

// DialWithContext connects to an NNTP server with context support.
func DialWithContext(ctx context.Context, network, addr string) (*Conn, error) {
	dialer := net.Dialer{}
	c, err := dialer.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	return newConn(c)
}

// DialTLSWithContext connects to an NNTP server using TLS with context support.
func DialTLSWithContext(ctx context.Context, network, addr string, config *tls.Config) (*Conn, error) {
	dialer := net.Dialer{}
	conn, err := tls.DialWithDialer(&dialer, network, addr, config)
	if err != nil {
		return nil, err
	}
	return newConn(conn)
}

func newConn(c net.Conn) (*Conn, error) {
	conn := &Conn{
		conn: c,
		r:    bufio.NewReaderSize(c, 8192),
		w:    bufio.NewWriterSize(c, 8192),
	}
	_, err := conn.r.ReadString('\n')
	return conn, err
}

// cmd executes an NNTP command and reads the response.
func (c *Conn) cmd(expectCode uint, format string, args ...interface{}) (uint, string, error) {
	fmt.Fprintf(c.w, format+"\r\n", args...)
	c.w.Flush()
	return c.readResponse()
}

func (c *Conn) readResponse() (uint, string, error) {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return 0, "", err
	}
	line = strings.TrimSpace(line)
	if len(line) < 4 || line[3] != ' ' {
		return 0, "", errors.New("short response: " + line)
	}
	code, err := strconv.ParseUint(line[0:3], 10, 0)
	if err != nil {
		return 0, "", errors.New("invalid response code: " + line)
	}
	return uint(code), line[4:], nil
}

// Authenticate logs in to the NNTP server.
// It only sends the password if the server requires one.
func (c *Conn) Authenticate(username, password string) error {
	code, _, err := c.cmd(2, "AUTHINFO USER %s", username)
	if code/100 == 3 {
		_, _, err = c.cmd(2, "AUTHINFO PASS %s", password)
	}
	return err
}

// nextLastStat performs the work for NEXT, LAST, and STAT.
func (c *Conn) nextLastStat(cmd, id string) (string, string, error) {
	_, line, err := c.cmd(223, maybeId(cmd, id))
	if err != nil {
		return "", "", err
	}
	ss := strings.SplitN(line, " ", 3) // optional comment ignored
	if len(ss) < 2 {
		return "", "", ProtocolError("Bad response to " + cmd + ": " + line)
	}
	return ss[0], ss[1], nil
}

// Stat looks up the message with the given id and returns its
// message number in the current group, and vice versa.
// The returned message number can be "0" if the current group
// isn't one of the groups the message was posted to.
func (c *Conn) Stat(id string) (number, msgid string, err error) {
	return c.nextLastStat("STAT", id)
}

// Last selects the previous article, returning its message number and id.
func (c *Conn) Last() (number, msgid string, err error) {
	return c.nextLastStat("LAST", "")
}

// Next selects the next article, returning its message number and id.
func (c *Conn) Next() (number, msgid string, err error) {
	return c.nextLastStat("NEXT", "")
}

// Post posts an article to the server.
func (c *Conn) Post(a *Article) error {
	return c.RawPost(&articleReader{a: a})
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

// RawIHave reads a text-formatted article from r and presents it to the server with the IHAVE command.
func (c *Conn) RawIHave(r io.Reader) error {
	if _, _, err := c.cmd(3, "IHAVE"); err != nil {
		return err
	}
	if err := c.sendLines(r); err != nil {
		return err
	}
	if _, _, err := c.cmd(235, "."); err != nil {
		return err
	}
	return nil
}

func (c *Conn) sendLines(r io.Reader) error {
	buf := globalBufferPool.Get()
	defer globalBufferPool.Put(buf)

	_, err := buf.ReadFrom(r)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(buf)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, ".") {
			c.w.WriteByte('.')
		}
		c.w.WriteString(line)
		c.w.WriteString("\r\n")
	}
	c.w.WriteString(".\r\n")
	return c.w.Flush()
}

// EnablePipelining enables NNTP pipelining if supported by the server.
func (c *Conn) EnablePipelining() error {
	_, _, err := c.cmd(200, "XPIPELINING")
	if err == nil {
		c.pipelining = true
	}
	return err
}

// PipelinedCmd sends multiple commands in a pipeline.
func (c *Conn) PipelinedCmd(cmds ...string) ([]uint, []string, error) {
	if !c.pipelining {
		return nil, nil, errors.New("pipelining not enabled")
	}

	for _, cmd := range cmds {
		fmt.Fprintf(c.w, cmd+"\r\n")
	}
	c.w.Flush()

	codes := make([]uint, len(cmds))
	responses := make([]string, len(cmds))

	for i := range cmds {
		code, response, err := c.readResponse()
		if err != nil {
			return codes, responses, err
		}
		codes[i] = code
		responses[i] = response
	}

	return codes, responses, nil
}

// Quit sends the QUIT command and closes the connection.
func (c *Conn) Quit() error {
	_, _, err := c.cmd(0, "QUIT")
	c.conn.Close()
	return err
}

// ConnPool manages a pool of NNTP connections.
type ConnPool struct {
	network, addr string
	tlsConfig     *tls.Config
	conns         chan *Conn
	maxSize       int
	username      string
	password      string
}

// NewConnPool creates a new connection pool.
func NewConnPool(
	network, addr string,
	maxSize int,
	tlsConfig *tls.Config,
	username, password string,
) *ConnPool {
	return &ConnPool{
		network:   network,
		addr:      addr,
		tlsConfig: tlsConfig,
		conns:     make(chan *Conn, maxSize),
		maxSize:   maxSize,
		username:  username,
		password:  password,
	}
}

// Get retrieves a connection from the pool or creates a new one.
func (p *ConnPool) Get() (*Conn, error) {
	select {
	case conn := <-p.conns:
		return conn, nil
	default:
		var conn *Conn
		var err error
		if p.tlsConfig != nil {
			conn, err = DialTLS(p.network, p.addr, p.tlsConfig)
		} else {
			conn, err = Dial(p.network, p.addr)
		}
		if err != nil {
			return nil, err
		}
		if p.username != "" {
			err = conn.Authenticate(p.username, p.password)
			if err != nil {
				conn.Quit()
				return nil, err
			}
		}
		return conn, nil
	}
}

// GetWithContext retrieves a connection from the pool or creates a new one with context support.
func (p *ConnPool) GetWithContext(ctx context.Context) (*Conn, error) {
	select {
	case conn := <-p.conns:
		return conn, nil
	default:
		var conn *Conn
		var err error
		if p.tlsConfig != nil {
			conn, err = DialTLSWithContext(ctx, p.network, p.addr, p.tlsConfig)
		} else {
			conn, err = DialWithContext(ctx, p.network, p.addr)
		}
		if err != nil {
			return nil, err
		}
		if p.username != "" {
			err = conn.Authenticate(p.username, p.password)
			if err != nil {
				conn.Quit()
				return nil, err
			}
		}
		return conn, nil
	}
}

// Put returns a connection to the pool.
func (p *ConnPool) Put(conn *Conn) {
	select {
	case p.conns <- conn:
	default:
		conn.Quit()
	}
}

// Close closes all connections in the pool.
func (p *ConnPool) Close() {
	close(p.conns)
	for conn := range p.conns {
		conn.Quit()
	}
}

type articleReader struct {
	a *Article
}

func (r *articleReader) Read(p []byte) (n int, err error) {
	buf := globalBufferPool.Get()
	defer globalBufferPool.Put(buf)

	for k, vs := range r.a.Header {
		for _, v := range vs {
			fmt.Fprintf(buf, "%s: %s\r\n", k, v)
		}
	}
	buf.WriteString("\r\n")
	if r.a.Body != nil {
		_, err = io.Copy(buf, r.a.Body)
		if err != nil {
			return 0, err
		}
	}

	return buf.Read(p)
}

// PostWithRetry attempts to post an article with retries.
func (c *Conn) PostWithRetry(a *Article, maxRetries int) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		err = c.Post(a)
		if err == nil {
			return nil
		}
		time.Sleep(time.Second * time.Duration(i+1))
	}
	return err
}
