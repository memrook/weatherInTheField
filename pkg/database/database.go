package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"weatherInTheField/pkg/api"
	"weatherInTheField/pkg/config"

	_ "github.com/denisenkom/go-mssqldb"
)

// DBManager представляет собой менеджер для работы с базой данных
type DBManager struct {
	Config *config.Config
	DB     *sql.DB
}

// NewDBManager создает новый экземпляр менеджера БД
func NewDBManager(cfg *config.Config) (*DBManager, error) {
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;database=%s",
		cfg.DbServer, cfg.DbLogin, cfg.DbPassword, cfg.DbName)

	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		return nil, fmt.Errorf("ошибка подключения к базе данных: %w", err)
	}

	// Проверка соединения
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ошибка при проверке соединения с базой данных: %w", err)
	}

	// Установка параметров пула соединений
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Minute * 5)

	return &DBManager{
		Config: cfg,
		DB:     db,
	}, nil
}

// Close закрывает соединение с базой данных
func (d *DBManager) Close() error {
	return d.DB.Close()
}

// CreateTablesIfNotExists создает необходимые таблицы, если они не существуют
func (d *DBManager) CreateTablesIfNotExists() error {
	// Создаем таблицу для метеостанций
	_, err := d.DB.Exec(`
	IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Stations' AND xtype='U')
	CREATE TABLE Stations (
		ID NVARCHAR(100) PRIMARY KEY,
		Name NVARCHAR(100) NOT NULL,
		Label NVARCHAR(255),
		Latitude FLOAT,
		Longitude FLOAT,
		LastUpdate DATETIME
	)
	`)
	if err != nil {
		return fmt.Errorf("ошибка при создании таблицы Stations: %w", err)
	}

	// Создаем таблицу для телеметрии
	_, err = d.DB.Exec(`
	IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Telemetry' AND xtype='U')
	CREATE TABLE Telemetry (
		ID INT IDENTITY(1,1) PRIMARY KEY,
		StationID NVARCHAR(100) NOT NULL,
		SensorKey NVARCHAR(100) NOT NULL,
		Timestamp BIGINT NOT NULL,
		DateValue DATETIME NOT NULL,
		Value FLOAT,
		CONSTRAINT FK_Telemetry_Stations FOREIGN KEY (StationID) REFERENCES Stations(ID),
		CONSTRAINT UQ_Telemetry_Station_Sensor_Date UNIQUE (StationID, SensorKey, Timestamp)
	)
	`)
	if err != nil {
		return fmt.Errorf("ошибка при создании таблицы Telemetry: %w", err)
	}

	// Создаем индексы для быстрого поиска
	_, err = d.DB.Exec(`
	IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Telemetry_StationID_SensorKey_Timestamp' AND object_id = OBJECT_ID('Telemetry'))
	CREATE INDEX IX_Telemetry_StationID_SensorKey_Timestamp ON Telemetry (StationID, SensorKey, Timestamp)
	`)
	if err != nil {
		return fmt.Errorf("ошибка при создании индекса: %w", err)
	}

	_, err = d.DB.Exec(`
	IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Telemetry_DateValue' AND object_id = OBJECT_ID('Telemetry'))
	CREATE INDEX IX_Telemetry_DateValue ON Telemetry (DateValue)
	`)
	if err != nil {
		return fmt.Errorf("ошибка при создании индекса: %w", err)
	}

	return nil
}

// StoreStations сохраняет информацию о метеостанциях в базу данных
func (d *DBManager) StoreStations(devices []api.Device) error {
	// Начинаем транзакцию
	tx, err := d.DB.Begin()
	if err != nil {
		return fmt.Errorf("ошибка при начале транзакции: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // паника после отката
		}
	}()

	// Подготавливаем запрос на вставку
	stmt, err := tx.Prepare(`
	MERGE INTO Stations AS target
	USING (VALUES (@ID, @Name, @Label, @Latitude, @Longitude)) AS source (ID, Name, Label, Latitude, Longitude)
	ON target.ID = source.ID
	WHEN MATCHED THEN
		UPDATE SET 
			Name = source.Name,
			Label = source.Label,
			Latitude = source.Latitude,
			Longitude = source.Longitude,
			LastUpdate = GETDATE()
	WHEN NOT MATCHED THEN
		INSERT (ID, Name, Label, Latitude, Longitude, LastUpdate)
		VALUES (source.ID, source.Name, source.Label, source.Latitude, source.Longitude, GETDATE());
	`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("ошибка при подготовке запроса: %w", err)
	}
	defer stmt.Close()

	// Вставляем каждую метеостанцию
	for _, device := range devices {
		_, err := stmt.Exec(
			sql.Named("ID", device.ID),
			sql.Named("Name", device.Name),
			sql.Named("Label", device.Label),
			sql.Named("Latitude", device.Latitude),
			sql.Named("Longitude", device.Longitude),
		)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("ошибка при вставке метеостанции: %w", err)
		}
	}

	// Коммитим транзакцию
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("ошибка при коммите транзакции: %w", err)
	}

	return nil
}

// StoreTelemetry сохраняет телеметрию в базу данных
func (d *DBManager) StoreTelemetry(deviceID string, data map[string][]api.TelemetryPoint) error {
	// Начинаем транзакцию
	tx, err := d.DB.Begin()
	if err != nil {
		return fmt.Errorf("ошибка при начале транзакции: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // паника после отката
		}
	}()

	// Подготавливаем запрос на вставку
	stmt, err := tx.Prepare(`
	IF NOT EXISTS (SELECT 1 FROM Telemetry WHERE StationID = @StationID AND SensorKey = @SensorKey AND Timestamp = @Timestamp)
	BEGIN
		INSERT INTO Telemetry (StationID, SensorKey, Timestamp, DateValue, Value)
		VALUES (@StationID, @SensorKey, @Timestamp, @DateValue, @Value)
	END
	ELSE
	BEGIN
		UPDATE Telemetry 
		SET Value = @Value
		WHERE StationID = @StationID AND SensorKey = @SensorKey AND Timestamp = @Timestamp
	END
	`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("ошибка при подготовке запроса: %w", err)
	}
	defer stmt.Close()

	// Вставляем каждую точку данных
	for sensorKey, points := range data {
		for _, point := range points {
			// Конвертируем timestamp в DateTime
			dateValue := time.Unix(point.Ts/1000, 0)

			// Преобразуем значение в float64
			var floatValue float64
			switch v := point.Value.(type) {
			case float64:
				floatValue = v
			case float32:
				floatValue = float64(v)
			case int:
				floatValue = float64(v)
			case int64:
				floatValue = float64(v)
			default:
				// Пропускаем значения, которые не могут быть преобразованы в float64
				continue
			}

			// Выполняем запрос с именованными параметрами
			_, err := stmt.Exec(
				sql.Named("StationID", deviceID),
				sql.Named("SensorKey", sensorKey),
				sql.Named("Timestamp", point.Ts),
				sql.Named("DateValue", dateValue),
				sql.Named("Value", floatValue),
			)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("ошибка при вставке телеметрии: %w", err)
			}
		}
	}

	// Коммитим транзакцию
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("ошибка при коммите транзакции: %w", err)
	}

	return nil
}

// GetLatestTelemetryTimestamp получает последний timestamp для указанной станции и датчика
func (d *DBManager) GetLatestTelemetryTimestamp(stationID, sensorKey string) (int64, error) {
	var ts int64
	err := d.DB.QueryRow(`
	SELECT TOP 1 Timestamp 
	FROM Telemetry 
	WHERE StationID = @StationID AND SensorKey = @SensorKey 
	ORDER BY Timestamp DESC
	`, sql.Named("StationID", stationID), sql.Named("SensorKey", sensorKey)).Scan(&ts)

	if err == sql.ErrNoRows {
		// Если записей нет, вернем 0
		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("ошибка при получении последнего timestamp: %w", err)
	}

	return ts, nil
}

// GetStations получает список всех станций из базы данных
func (d *DBManager) GetStations() ([]string, error) {
	rows, err := d.DB.Query("SELECT ID FROM Stations")
	if err != nil {
		return nil, fmt.Errorf("ошибка при запросе станций: %w", err)
	}
	defer rows.Close()

	var stations []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("ошибка при сканировании ID станции: %w", err)
		}
		stations = append(stations, id)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ошибка при итерации результатов: %w", err)
	}

	return stations, nil
}
