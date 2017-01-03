package config

import (
	"fmt"
	"github.com/spf13/viper"
	"strings"
)

//go generate embed file -var config --source config.yml
var config = ""

func Init() {

	viper.SetConfigName("config")
	viper.AddConfigPath("config")

	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Sprintf("%s: %s", "Unable to read Config File", err))
	}

	viper.AutomaticEnv()
	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)
}