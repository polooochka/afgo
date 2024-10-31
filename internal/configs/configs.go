package configs

import (
	"log"

	"github.com/spf13/viper"
)

type Config struct {
	AppLevel   string `mapstructure:"APPLEVEL"`
	AppPort    string `mapstructure:"APPPORT"`
	ChHost     string `mapstructure:"CHHOST"`
	ChPort     string `mapstructure:"CHPORT"`
	ChDatabase string `mapstructure:"CHDB"`
	ChUser     string `mapstructure:"CHUSER"`
	ChPassword string `mapstructure:"CHPASS"`
}

func MustLoadConfig() *Config {
	var config *Config
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.SetConfigFile(".env")

	viper.AutomaticEnv()

	err := viper.ReadInConfig()
	if err != nil {
		log.Fatal("config didnt load, ", err)
	}
	err = viper.Unmarshal(&config)
	if err != nil {
		log.Fatal("config didnt unmarshal, ", err)
	}
	log.Println(config)
	return config
}
