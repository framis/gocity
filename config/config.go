package config

import (
	"fmt"
	"github.com/spf13/viper"
)

//go generate embed file -var config --source config.yml
var config = ""

func Init() {
	viper.AutomaticEnv()
	viper.SetEnvPrefix("gocity")

	viper.SetConfigName("config")
	viper.AddConfigPath("config")

	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Sprintf("%s: %s", "Unable to read Config File", err))
	}
}