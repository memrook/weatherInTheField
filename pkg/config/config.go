package config

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)

// Config содержит настройки приложения
type Config struct {
	// Данные для API
	ApiLogin    string
	ApiPassword string
	ApiBaseURL  string

	// Данные для базы данных
	DbServer   string
	DbLogin    string
	DbPassword string
	DbName     string

	// Интервал сбора данных в минутах
	CollectionInterval int
}

// LoadConfig загружает конфигурацию из .env файла и переменных окружения
func LoadConfig() *Config {
	// Попытка загрузить .env файл, если он существует
	_ = godotenv.Load()

	cfg := &Config{
		// API данные
		ApiLogin:    getEnv("API_LOGIN", ""),
		ApiPassword: getEnv("API_PASSWORD", ""),
		ApiBaseURL:  getEnv("API_BASE_URL", "https://api3.ttrackagro.ru"),

		// Данные базы данных
		DbServer:   getEnv("DB_SERVER", "ACLSDWHODS001.acl.agroconcern.ru"),
		DbLogin:    getEnv("DB_LOGIN", ""),
		DbPassword: getEnv("DB_PASSWORD", ""),
		DbName:     getEnv("DB_NAME", "WeatherData"),

		// Интервал сбора данных (по умолчанию 15 минут)
		CollectionInterval: getEnvAsInt("COLLECTION_INTERVAL", 15),
	}

	// Проверка обязательных полей
	if cfg.ApiLogin == "" || cfg.ApiPassword == "" {
		log.Fatal("API_LOGIN и API_PASSWORD должны быть указаны")
	}

	if cfg.DbLogin == "" || cfg.DbPassword == "" {
		log.Fatal("DB_LOGIN и DB_PASSWORD должны быть указаны")
	}

	return cfg
}

// getEnv получает значение из переменной окружения или возвращает значение по умолчанию
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// getEnvAsInt получает значение из переменной окружения как int или возвращает значение по умолчанию
func getEnvAsInt(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	intValue := defaultValue
	_, err := fmt.Sscanf(value, "%d", &intValue)
	if err != nil {
		return defaultValue
	}

	return intValue
}
