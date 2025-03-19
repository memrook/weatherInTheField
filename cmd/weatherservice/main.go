package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"weatherInTheField/pkg/api"
	"weatherInTheField/pkg/config"
	"weatherInTheField/pkg/database"
)

// Определяем ключи датчиков, которые нам нужны
var sensorKeys = []string{
	"airtemp",        // Температура воздуха
	"soiltemp",       // Температура почвы
	"airmoist",       // Влажность воздуха
	"rainfall",       // Количество осадков
	"rainfall_daily", // Количество осадков за предыдущие сутки
	"windspeed",      // Скорость ветра
	"windspeedmax",   // Порывы ветра
	"winddir",        // Направление ветра
	"winddirang",     // Направление ветра в градусах
}

func main() {
	// Загружаем конфигурацию
	cfg := config.LoadConfig()

	// Инициализируем API клиент
	weatherAPI := api.NewWeatherAPI(cfg)

	// Логин в API
	if err := weatherAPI.Login(); err != nil {
		log.Fatalf("Ошибка при авторизации: %v", err)
	}

	// Инициализируем менеджер БД
	dbManager, err := database.NewDBManager(cfg)
	if err != nil {
		log.Fatalf("Ошибка при подключении к БД: %v", err)
	}
	defer dbManager.Close()

	// Создаем таблицы, если они не существуют
	if err := dbManager.CreateTablesIfNotExists(); err != nil {
		log.Fatalf("Ошибка при создании таблиц: %v", err)
	}

	// Канал для остановки сервиса
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)

	// Запускаем регулярный сбор данных в отдельной горутине
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Запускаем первый сбор данных немедленно
		collectData(weatherAPI, dbManager)

		// Настраиваем периодический запуск
		ticker := time.NewTicker(time.Duration(cfg.CollectionInterval) * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				collectData(weatherAPI, dbManager)
			case <-stopChan:
				log.Println("Получен сигнал остановки. Завершаем работу...")
				return
			}
		}
	}()

	// Ожидаем сигнал остановки
	<-stopChan
	log.Println("Ожидаем завершения всех задач...")
	wg.Wait()
	log.Println("Сервис остановлен")
}

// collectData выполняет сбор данных со всех метеостанций и их сохранение в БД
func collectData(weatherAPI *api.WeatherAPI, dbManager *database.DBManager) {
	log.Println("Начинаем сбор данных...")

	// Получаем список всех устройств
	devices, err := weatherAPI.GetDevices()
	if err != nil {
		log.Printf("Ошибка при получении списка устройств: %v", err)
		return
	}

	log.Printf("Найдено устройств: %d", len(devices))

	// Сохраняем информацию о станциях в базу данных
	if err := dbManager.StoreStations(devices); err != nil {
		log.Printf("Ошибка при сохранении информации о станциях: %v", err)
	}

	// Обрабатываем каждое устройство
	for _, device := range devices {
		processDevice(weatherAPI, dbManager, device)
	}

	log.Println("Сбор данных завершен")
}

// processDevice обрабатывает отдельное устройство (метеостанцию)
func processDevice(weatherAPI *api.WeatherAPI, dbManager *database.DBManager, device api.Device) {
	log.Printf("Обрабатываем устройство: %s (%s)", device.Label, device.ID)

	// Текущее время в миллисекундах
	now := time.Now().UnixNano() / int64(time.Millisecond)

	// Стандартный интервал для получения данных (если нет данных в БД)
	intervalMs := int64(15 * 60 * 1000) // 15 минут в миллисекундах

	// Создаем карту для хранения данных о последнем timestamp для каждого датчика
	sensorLastTs := make(map[string]int64)

	// Создаем два списка датчиков - новые (без данных) и существующие
	var newSensors []string
	var existingSensors []string

	// Определяем минимальный tsFrom для существующих датчиков
	minTsFrom := now

	// Пытаемся получить время последних данных для каждого ключа датчика
	for _, sensorKey := range sensorKeys {
		// Получаем время последней записи из базы данных
		lastTs, err := dbManager.GetLatestTelemetryTimestamp(device.ID, sensorKey)
		if err != nil {
			log.Printf("Ошибка при получении последнего timestamp для %s-%s: %v", device.ID, sensorKey, err)
			// Если ошибка, считаем что данных нет
			newSensors = append(newSensors, sensorKey)
			continue
		}

		sensorLastTs[sensorKey] = lastTs

		// Проверяем, есть ли для этого датчика данные в базе
		if lastTs > 0 {
			existingSensors = append(existingSensors, sensorKey)

			// Определяем минимальный timestamp для всех существующих датчиков
			if lastTs < minTsFrom {
				minTsFrom = lastTs
			}
		} else {
			// Датчик есть в списке, но данных по нему нет
			newSensors = append(newSensors, sensorKey)
		}
	}

	// Рассчитываем tsFrom для существующих датчиков
	tsFrom := now - intervalMs
	if minTsFrom < now && minTsFrom > 0 {
		// Добавляем 1 миллисекунду, чтобы не получать повторно ту же запись
		tsFrom = minTsFrom + 1
	}

	// Общее количество полученных записей
	totalRecordsCount := 0

	// Обрабатываем новые датчики, если они есть
	if len(newSensors) > 0 {
		log.Printf("Для устройства %s запрашиваем годовые данные для %d новых датчиков: %v",
			device.ID, len(newSensors), newSensors)

		// Определяем время начала годового периода
		oneYearAgo := now - 365*24*60*60*1000 // 365 дней в миллисекундах

		// Разбиваем год на месячные интервалы
		periods := splitTimePeriodByMonth(oneYearAgo, now)

		// Обрабатываем каждый временной период
		for _, period := range periods {
			// Получаем телеметрию за текущий период только для новых датчиков
			telemetry, err := weatherAPI.GetTelemetry(device.ID, newSensors, period.from, period.to)
			if err != nil {
				log.Printf("Ошибка при получении телеметрии для новых датчиков устройства %s за период %s - %s: %v",
					device.ID,
					time.Unix(period.from/1000, 0).Format("2006-01-02 15:04:05"),
					time.Unix(period.to/1000, 0).Format("2006-01-02 15:04:05"),
					err)
				continue
			}

			recordsCount := processAndSaveTelemetry(device.ID, telemetry, dbManager)
			totalRecordsCount += recordsCount
		}
	}

	// Обрабатываем существующие датчики, если они есть
	if len(existingSensors) > 0 {
		log.Printf("Для устройства %s запрашиваем обновленные данные для %d существующих датчиков с %s",
			device.ID,
			len(existingSensors),
			time.Unix(tsFrom/1000, 0).Format("2006-01-02 15:04:05"))

		// Определяем период запроса данных для существующих датчиков
		var periods []timePeriod

		// Если последняя запись старше месяца, разбиваем запросы на промежутки
		oneMonthAgo := now - 30*24*60*60*1000 // 30 дней в миллисекундах
		if tsFrom < oneMonthAgo {
			log.Printf("Для устройства %s данные старше месяца. Разбиваем запрос на меньшие интервалы.", device.ID)
			// Разбиваем период по 30 дней
			periods = splitTimePeriodByDays(tsFrom, now, 30)
		} else {
			// Если период небольшой, делаем один запрос
			minutesAgo := (now - tsFrom) / 1000 / 60
			log.Printf("Для устройства %s запрашиваем данные за последние %d минут", device.ID, minutesAgo)
			periods = []timePeriod{{tsFrom, now}}
		}

		// Обрабатываем каждый временной период
		for _, period := range periods {
			// Получаем телеметрию за текущий период только для существующих датчиков
			telemetry, err := weatherAPI.GetTelemetry(device.ID, existingSensors, period.from, period.to)
			if err != nil {
				log.Printf("Ошибка при получении телеметрии для существующих датчиков устройства %s за период %s - %s: %v",
					device.ID,
					time.Unix(period.from/1000, 0).Format("2006-01-02 15:04:05"),
					time.Unix(period.to/1000, 0).Format("2006-01-02 15:04:05"),
					err)
				continue
			}

			recordsCount := processAndSaveTelemetry(device.ID, telemetry, dbManager)
			totalRecordsCount += recordsCount
		}
	}

	if totalRecordsCount > 0 {
		log.Printf("Данные для устройства %s успешно обработаны. Всего получено %d записей.", device.ID, totalRecordsCount)
	} else {
		log.Printf("Для устройства %s не получено никаких новых данных.", device.ID)
	}
}

// processAndSaveTelemetry обрабатывает и сохраняет полученную телеметрию
func processAndSaveTelemetry(deviceID string, telemetry map[string][]api.TelemetryPoint, dbManager *database.DBManager) int {
	// Считаем количество полученных записей
	recordsCount := 0
	for _, points := range telemetry {
		recordsCount += len(points)
	}

	if recordsCount == 0 {
		log.Printf("Для устройства %s новых данных не получено", deviceID)
		return 0
	}

	log.Printf("Для устройства %s получено %d новых записей. Сохраняем в базу данных...", deviceID, recordsCount)

	// Сохраняем телеметрию в базу данных
	startTime := time.Now()
	if err := dbManager.StoreTelemetry(deviceID, telemetry); err != nil {
		log.Printf("Ошибка при сохранении телеметрии для устройства %s: %v", deviceID, err)
		return 0
	}

	// Вычисляем, сколько времени заняло сохранение данных
	elapsed := time.Since(startTime)
	log.Printf("Данные для устройства %s успешно сохранены в базу (время: %.2f сек., скорость: %.1f записей/сек.)",
		deviceID,
		elapsed.Seconds(),
		float64(recordsCount)/elapsed.Seconds())

	return recordsCount
}

// timePeriod представляет временной период с началом и концом
type timePeriod struct {
	from int64 // начало периода в миллисекундах
	to   int64 // конец периода в миллисекундах
}

// splitTimePeriodByMonth разбивает большой временной период на месячные интервалы
func splitTimePeriodByMonth(tsFrom, tsTo int64) []timePeriod {
	var periods []timePeriod

	// Преобразуем timestamp в time.Time для удобства работы с месяцами
	fromTime := time.Unix(tsFrom/1000, 0)
	toTime := time.Unix(tsTo/1000, 0)

	// Устанавливаем начало на первый день текущего месяца
	currentMonth := time.Date(fromTime.Year(), fromTime.Month(), 1, 0, 0, 0, 0, fromTime.Location())

	// Добавляем первый период от начальной даты до конца месяца
	if fromTime.Day() > 1 {
		// Начинаем с текущей даты до конца месяца
		nextMonth := currentMonth.AddDate(0, 1, 0)
		periodEnd := min(nextMonth.UnixNano()/int64(time.Millisecond), tsTo)
		periods = append(periods, timePeriod{tsFrom, periodEnd})
		currentMonth = nextMonth
	}

	// Добавляем полные месячные периоды
	for currentMonth.Before(toTime) {
		nextMonth := currentMonth.AddDate(0, 1, 0)
		periodStart := currentMonth.UnixNano() / int64(time.Millisecond)
		periodEnd := nextMonth.UnixNano() / int64(time.Millisecond)

		// Если конец периода выходит за пределы запрашиваемого диапазона, ограничиваем его
		if periodEnd > tsTo {
			periodEnd = tsTo
		}

		periods = append(periods, timePeriod{periodStart, periodEnd})
		currentMonth = nextMonth

		// Если достигли конечной даты, выходим из цикла
		if currentMonth.UnixNano()/int64(time.Millisecond) >= tsTo {
			break
		}
	}

	return periods
}

// splitTimePeriodByDays разбивает большой временной период на интервалы по указанному количеству дней
func splitTimePeriodByDays(tsFrom, tsTo int64, days int) []timePeriod {
	var periods []timePeriod

	// Вычисляем длину одного интервала в миллисекундах
	intervalMs := int64(days * 24 * 60 * 60 * 1000)

	// Начальная точка
	current := tsFrom

	// Разбиваем период на интервалы указанной длины
	for current < tsTo {
		periodEnd := current + intervalMs
		if periodEnd > tsTo {
			periodEnd = tsTo
		}

		periods = append(periods, timePeriod{current, periodEnd})
		current = periodEnd
	}

	return periods
}
