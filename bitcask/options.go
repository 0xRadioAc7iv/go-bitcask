package bitcask

import "github.com/0xRadioAc7iv/go-bitcask/internal"

type Option func(*internal.Config)

func WithHost(host string) Option {
	return func(c *internal.Config) {
		c.Host = host
	}
}

func WithPort(port int) Option {
	return func(c *internal.Config) {
		c.Port = port
	}
}
