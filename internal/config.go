package internal

type Config struct {
	Host string
	Port int
}

const DEFAULT_HOST = "127.0.0.1"
const DEFAULT_PORT = 9999

func DefaultConfig() *Config {
	return &Config{
		Host: DEFAULT_HOST,
		Port: DEFAULT_PORT,
	}
}
