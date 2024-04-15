// Copyright © 2024 Ken Robertson <ken@invalidlogic.com>

package main

type config struct {
	SkipDirectoryFile string                  `yaml:"skip_directory_file"`
	Cache             *configGroup            `yaml:"cache"`
	Destinations      map[string]*configGroup `yaml:"destinations"`
}

type configGroup struct {
	Concurrency int64    `yaml:"concurrency"`
	Paths       []string `yaml:"paths"`
}
