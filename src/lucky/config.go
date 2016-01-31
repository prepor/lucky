package lucky

import (
	"io/ioutil"

	"gopkg.in/edn.v1"
	"gopkg.in/go-playground/validator.v8"
)

var validate = validator.New(&validator.Config{TagName: "validate"})

type Config struct {
	Frontends []*FrontendConfig              `validate:"dive,required"`
	Backends  map[edn.Keyword]*BackendConfig `validate:"dive,required"`
}

type FrontendConfig struct {
	Type     edn.Keyword
	Endpoint string
	Bind     []string `validate:"omitempty,min=1,dive,required"`
	Backend  edn.Keyword
}

type BackendConfig struct {
	Bind []string `validate:"omitempty,min=1,dive,required"`
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
