package lucky

import (
	"gopkg.in/edn.v1"
	"gopkg.in/go-playground/validator.v8"
	"io/ioutil"
)

var validate = validator.New(&validator.Config{TagName: "validate"})

type Config struct {
	Balancers []*BalancerConfig `validate:"dive,required"`
}

type BalancerConfig struct {
	Name  string        `validate:"required"`
	Front *SocketConfig `edn:"front" validate:"required"`
	Back  *SocketConfig `edn:"back" validate:"required"`
}

type SocketConfig struct {
	Bind []string `edn:"bind" validate:"omitempty,min=1,dive,required"`
}

func ParseConfig(path string) (*Config, error) {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var config Config
	err = edn.Unmarshal(content, &config)
	if err != nil {
		return nil, err
	}
	err = validate.Struct(config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}
