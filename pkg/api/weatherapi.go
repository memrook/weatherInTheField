package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"weatherInTheField/pkg/config"
)

// WeatherAPI представляет API клиент для работы с погодавполе.рф
type WeatherAPI struct {
	Config    *config.Config
	Client    *http.Client
	SessionID string
}

// Структуры для запросов и ответов API

// LoginRequest представляет собой запрос на аутентификацию
type LoginRequest struct {
	Login    string `json:"login"`
	Password string `json:"password"`
}

// LoginResponse представляет собой ответ на аутентификацию
type LoginResponse struct {
	Status       string `json:"status"`
	RecordsCount int    `json:"records_count"`
	Data         struct {
		Sid         string      `json:"sid"`
		Refresh     string      `json:"refresh"`
		Account     string      `json:"account"`
		IsAdmin     bool        `json:"is_admin"`
		SpecialType interface{} `json:"special_type"`
	} `json:"data"`
}

// DevicesRequest представляет собой запрос на получение списка устройств
type DevicesRequest struct {
	Sid    string      `json:"sid"`
	Filter interface{} `json:"filter,omitempty"`
}

// Device представляет собой устройство (метеостанцию)
type Device struct {
	ID            string  `json:"id"`
	Name          string  `json:"name"`
	Type          string  `json:"type"`
	Imei          string  `json:"imei"`
	Label         string  `json:"label"`
	SourceType    string  `json:"source_type"`
	LastMsg       int64   `json:"last_msg"`
	Address       string  `json:"address"`
	Latitude      float64 `json:"latitude"`
	Longitude     float64 `json:"longitude"`
	Airtemp       float64 `json:"airtemp"`
	Rainfall      float64 `json:"rainfall"`
	BatteryCharge float64 `json:"battery.charge"`
	Clients       []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"clients"`
	Sensors map[string]struct {
		Active     bool        `json:"active"`
		Name       string      `json:"name"`
		CustomName string      `json:"custom_name"`
		Unit       interface{} `json:"unit"`
		Formula    string      `json:"formula"`
		LastValue  interface{} `json:"last_value"`
		Ts         int64       `json:"ts"`
	} `json:"sensors"`
}

// DevicesResponse представляет собой ответ на получение списка устройств
type DevicesResponse struct {
	Status       string   `json:"status"`
	RecordsCount int      `json:"records_count"`
	PageNum      int      `json:"page_num"`
	PageSize     int      `json:"page_size"`
	Data         []Device `json:"data"`
}

// TelemetryRequest представляет собой запрос на получение телеметрии
type TelemetryRequest struct {
	Sid     string   `json:"sid"`
	Devices []string `json:"devices"`
	Keys    []string `json:"keys,omitempty"`
	TsFrom  int64    `json:"ts_from"`
	TsTo    int64    `json:"ts_to"`
}

// TelemetryData представляет собой точку данных телеметрии в новом формате
type TelemetryData struct {
	EntityID string      `json:"entity_id"`
	Key      string      `json:"key"`
	Ts       int64       `json:"ts"`
	DblV     float64     `json:"dbl_v"`
	StrV     interface{} `json:"str_v"`
}

// TelemetryPoint представляет собой точку данных телеметрии
type TelemetryPoint struct {
	Ts    int64       `json:"ts"`
	Value interface{} `json:"value"`
}

// TelemetryResponse представляет собой ответ на получение телеметрии
type TelemetryResponse struct {
	Status       string          `json:"status"`
	RecordsCount int             `json:"records_count"`
	Data         []TelemetryData `json:"data"`
}

// ErrorResponse представляет собой ответ с ошибкой
type ErrorResponse struct {
	Status         string `json:"status"`
	Error          string `json:"error"`
	AdditionalCode string `json:"additional_code,omitempty"`
}

// NewWeatherAPI создает новый экземпляр API клиента
func NewWeatherAPI(cfg *config.Config) *WeatherAPI {
	return &WeatherAPI{
		Config: cfg,
		Client: &http.Client{
			Timeout: 120 * time.Second,
		},
	}
}

// Login выполняет аутентификацию и получает токен сессии
func (w *WeatherAPI) Login() error {
	loginReq := LoginRequest{
		Login:    w.Config.ApiLogin,
		Password: w.Config.ApiPassword,
	}

	jsonData, err := json.Marshal(loginReq)
	if err != nil {
		return fmt.Errorf("ошибка при сериализации запроса: %w", err)
	}

	resp, err := w.Client.Post(
		w.Config.ApiBaseURL+"/login",
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return fmt.Errorf("ошибка при выполнении запроса: %w", err)
	}
	defer resp.Body.Close()

	var loginResp LoginResponse
	if err := json.NewDecoder(resp.Body).Decode(&loginResp); err != nil {
		return fmt.Errorf("ошибка при десериализации ответа: %w", err)
	}

	if loginResp.Status == "error" {
		return fmt.Errorf("ошибка аутентификации")
	}

	if loginResp.Data.Sid == "" {
		return fmt.Errorf("отсутствует токен сессии в ответе")
	}

	w.SessionID = loginResp.Data.Sid
	return nil
}

// GetDevices получает список всех устройств (метеостанций)
func (w *WeatherAPI) GetDevices() ([]Device, error) {
	if w.SessionID == "" {
		if err := w.Login(); err != nil {
			return nil, err
		}
	}

	devicesReq := DevicesRequest{
		Sid: w.SessionID,
	}

	jsonData, err := json.Marshal(devicesReq)
	if err != nil {
		return nil, fmt.Errorf("ошибка при сериализации запроса: %w", err)
	}

	resp, err := w.Client.Post(
		w.Config.ApiBaseURL+"/devices",
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return nil, fmt.Errorf("ошибка при выполнении запроса: %w", err)
	}
	defer resp.Body.Close()

	var devicesResp DevicesResponse
	if err := json.NewDecoder(resp.Body).Decode(&devicesResp); err != nil {
		return nil, fmt.Errorf("ошибка при десериализации ответа: %w", err)
	}

	if devicesResp.Status != "OK" {
		// Предполагаем, что если статус не OK, то сессия может быть недействительной
		// Пробуем войти снова и повторить запрос
		if err := w.Login(); err != nil {
			return nil, err
		}
		return w.GetDevices()
	}

	return devicesResp.Data, nil
}

// GetTelemetry получает телеметрию для устройства за указанный период
func (w *WeatherAPI) GetTelemetry(deviceID string, keys []string, tsFrom int64, tsTo int64) (map[string][]TelemetryPoint, error) {
	if w.SessionID == "" {
		if err := w.Login(); err != nil {
			return nil, err
		}
	}

	telemetryReq := TelemetryRequest{
		Sid:     w.SessionID,
		Devices: []string{deviceID},
		Keys:    keys,
		TsFrom:  tsFrom,
		TsTo:    tsTo,
	}

	jsonData, err := json.Marshal(telemetryReq)
	if err != nil {
		return nil, fmt.Errorf("ошибка при сериализации запроса: %w", err)
	}

	resp, err := w.Client.Post(
		w.Config.ApiBaseURL+"/telemetry",
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return nil, fmt.Errorf("ошибка при выполнении запроса: %w", err)
	}
	defer resp.Body.Close()

	var telemetryResp TelemetryResponse
	if err := json.NewDecoder(resp.Body).Decode(&telemetryResp); err != nil {
		return nil, fmt.Errorf("ошибка при десериализации ответа: %w", err)
	}

	if telemetryResp.Status != "OK" {
		// Предполагаем, что если статус не OK, то сессия может быть недействительной
		// Пробуем войти снова и повторить запрос
		if err := w.Login(); err != nil {
			return nil, err
		}
		return w.GetTelemetry(deviceID, keys, tsFrom, tsTo)
	}

	// Преобразуем данные из нового формата в карту для совместимости
	result := make(map[string][]TelemetryPoint)
	for _, data := range telemetryResp.Data {
		point := TelemetryPoint{
			Ts: data.Ts,
		}

		// Используем числовое значение, если оно есть
		if data.DblV != 0 {
			point.Value = data.DblV
		} else {
			point.Value = data.StrV
		}

		// Добавляем точку в соответствующий массив по ключу
		result[data.Key] = append(result[data.Key], point)
	}

	return result, nil
}

// GetLatestTelemetry получает последние данные телеметрии для устройств
func (w *WeatherAPI) GetLatestTelemetry(deviceIDs []string, keys []string) (map[string][]TelemetryPoint, error) {
	if w.SessionID == "" {
		if err := w.Login(); err != nil {
			return nil, err
		}
	}

	// Для получения последней телеметрии используем текущее время и время 24 часа назад
	now := time.Now().UnixNano() / int64(time.Millisecond)
	dayAgo := now - 24*60*60*1000 // 24 часа в миллисекундах

	telemetryReq := TelemetryRequest{
		Sid:     w.SessionID,
		Devices: deviceIDs,
		Keys:    keys,
		TsFrom:  dayAgo,
		TsTo:    now,
	}

	jsonData, err := json.Marshal(telemetryReq)
	if err != nil {
		return nil, fmt.Errorf("ошибка при сериализации запроса: %w", err)
	}

	resp, err := w.Client.Post(
		w.Config.ApiBaseURL+"/last_telemetry",
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return nil, fmt.Errorf("ошибка при выполнении запроса: %w", err)
	}
	defer resp.Body.Close()

	var telemetryResp TelemetryResponse
	if err := json.NewDecoder(resp.Body).Decode(&telemetryResp); err != nil {
		return nil, fmt.Errorf("ошибка при десериализации ответа: %w", err)
	}

	if telemetryResp.Status != "OK" {
		// Предполагаем, что если статус не OK, то сессия может быть недействительной
		// Пробуем войти снова и повторить запрос
		if err := w.Login(); err != nil {
			return nil, err
		}
		return w.GetLatestTelemetry(deviceIDs, keys)
	}

	// Преобразуем данные из нового формата в карту для совместимости
	result := make(map[string][]TelemetryPoint)
	for _, data := range telemetryResp.Data {
		point := TelemetryPoint{
			Ts: data.Ts,
		}

		// Используем числовое значение, если оно есть
		if data.DblV != 0 {
			point.Value = data.DblV
		} else {
			point.Value = data.StrV
		}

		// Добавляем точку в соответствующий массив по ключу
		result[data.Key] = append(result[data.Key], point)
	}

	return result, nil
}
