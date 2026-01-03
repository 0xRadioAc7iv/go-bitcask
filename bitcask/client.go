package bitcask

import (
	"fmt"
	"net"

	"github.com/0xRadioAc7iv/go-bitcask/internal"
	"github.com/0xRadioAc7iv/go-bitcask/internal/protocol"
)

type Client struct {
	conn net.Conn
}

func Connect(opts ...Option) (*Client, error) {
	cfg := internal.DefaultConfig()

	for _, opt := range opts {
		opt(cfg)
	}

	addr := net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", cfg.Port))

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{conn: conn}, nil
}

func (c *Client) GET(key string) (string, error) {
	res, err := c.sendCommand("get", key, "")
	if err != nil {
		fmt.Println(err)
		return "", err
	}

	return res, nil
}

func (c *Client) SET(key, value string) (string, error) {
	res, err := c.sendCommand("set", key, value)
	if err != nil {
		fmt.Println(err)
		return "", err
	}

	return res, nil
}

func (c *Client) DELETE(key string) (string, error) {
	res, err := c.sendCommand("delete", key, "")
	if err != nil {
		fmt.Println(err)
		return "", err
	}

	return res, nil
}

func (c *Client) COUNT() (string, error) {
	res, err := c.sendCommand("count", "", "")
	if err != nil {
		fmt.Println(err)
		return "", err
	}

	return res, nil
}

func (c *Client) EXISTS(key string) (string, error) {
	res, err := c.sendCommand("exists", key, "")
	if err != nil {
		fmt.Println(err)
		return "", err
	}

	return res, nil
}

func (c *Client) LIST() (string, error) {
	res, err := c.sendCommand("list", "", "")
	if err != nil {
		fmt.Println(err)
		return "", err
	}

	return res, nil
}

func (c *Client) Close() {
	err := c.conn.Close()
	if err != nil {
		fmt.Println(err.Error())
	}
}

func (c *Client) Execute(cmd, key, value string) (string, error) {
	return c.sendCommand(cmd, key, value)
}

func (c *Client) sendCommand(cmd, key, value string) (string, error) {
	payload, err := protocol.EncodeCommand(cmd, key, value)
	if err != nil {
		return "", err
	}

	_, err = c.conn.Write(payload)
	if err != nil {
		return "", err
	}

	response, err := protocol.DecodeResponse(c.conn)
	if err != nil {
		return "", err
	}

	return response, nil
}
