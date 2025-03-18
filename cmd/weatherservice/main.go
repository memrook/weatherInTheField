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
	"soilmoist",      // Влажность почвы
	"rainfall_daily", // Осадки за день
	"rainfall",       // Текущие осадки
	"windspeed",      // Скорость ветра
	"winddir",        // Направление ветра
	"airhum",         // Влажность воздуха
	"pressure",       // Атмосферное давление
	"battery",        // Заряд батареи
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
	tsFrom := now - intervalMs

	// Флаг, указывающий что по станции нет данных вообще
	noDataForStation := true

	// Пытаемся получить время последних данных для каждого ключа датчика
	for _, sensorKey := range sensorKeys {
		// Получаем время последней записи из базы данных
		lastTs, err := dbManager.GetLatestTelemetryTimestamp(device.ID, sensorKey)
		if err != nil {
			log.Printf("Ошибка при получении последнего timestamp для %s-%s: %v", device.ID, sensorKey, err)
			continue
		}

		// Если для этого датчика есть хоть какие-то данные,
		// то метеостанция уже добавлена в базу с некоторыми данными
		if lastTs > 0 {
			noDataForStation = false

			// Если данные старше текущего начального времени запроса,
			// обновляем начальное время для обеспечения непрерывности данных
			if lastTs < tsFrom {
				// Добавляем 1 миллисекунду, чтобы не получать повторно ту же запись
				tsFrom = lastTs + 1
				log.Printf("Для устройства %s и датчика %s используем время последней записи: %d",
					device.ID, sensorKey, lastTs)
				break // Достаточно найти хотя бы один датчик с данными
			}
		}
	}

	// Определяем период запроса данных
	var periods []timePeriod

	// Если по метеостанции нет никаких данных, запрашиваем данные за последний год по месяцам
	if noDataForStation {
		// Разбиваем год на месячные интервалы
		oneYearAgo := now - 365*24*60*60*1000 // 365 дней в миллисекундах
		log.Printf("Для устройства %s нет данных в базе. Разбиваем запрос данных за последний год на месячные интервалы.", device.ID)
		periods = splitTimePeriodByMonth(oneYearAgo, now)
	} else {
		// Если данные есть, но они слишком старые (больше месяца),
		// разбиваем запросы на промежутки по 30 дней
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
	}

	// Общее количество полученных записей
	totalRecordsCount := 0

	// Обрабатываем каждый временной период
	for _, period := range periods {
		// Получаем телеметрию за текущий период
		telemetry, err := weatherAPI.GetTelemetry(device.ID, sensorKeys, period.from, period.to)
		if err != nil {
			log.Printf("Ошибка при получении телеметрии для устройства %s за период %s - %s: %v",
				device.ID,
				time.Unix(period.from/1000, 0).Format("2006-01-02 15:04:05"),
				time.Unix(period.to/1000, 0).Format("2006-01-02 15:04:05"),
				err)
			continue // Продолжаем с следующим периодом
		}

		// Считаем количество полученных записей
		recordsCount := 0
		for _, points := range telemetry {
			recordsCount += len(points)
		}

		if recordsCount == 0 {
			log.Printf("Для устройства %s за период %s - %s новых данных не получено",
				device.ID,
				time.Unix(period.from/1000, 0).Format("2006-01-02 15:04:05"),
				time.Unix(period.to/1000, 0).Format("2006-01-02 15:04:05"))
			continue
		}

		log.Printf("Для устройства %s за период %s - %s получено %d новых записей. Сохраняем в базу данных...",
			device.ID,
			time.Unix(period.from/1000, 0).Format("2006-01-02 15:04:05"),
			time.Unix(period.to/1000, 0).Format("2006-01-02 15:04:05"),
			recordsCount)

		// Сохраняем телеметрию в базу данных
		startTime := time.Now()
		if err := dbManager.StoreTelemetry(device.ID, telemetry); err != nil {
			log.Printf("Ошибка при сохранении телеметрии для устройства %s: %v", device.ID, err)
			continue
		}

		// Вычисляем, сколько времени заняло сохранение данных
		elapsed := time.Since(startTime)
		log.Printf("Данные для устройства %s успешно сохранены в базу (время: %.2f сек., скорость: %.1f записей/сек.)",
			device.ID,
			elapsed.Seconds(),
			float64(recordsCount)/elapsed.Seconds())

		totalRecordsCount += recordsCount
	}

	if totalRecordsCount > 0 {
		log.Printf("Данные для устройства %s успешно обработаны. Всего получено %d записей.", device.ID, totalRecordsCount)
	} else {
		log.Printf("Для устройства %s не получено никаких новых данных.", device.ID)
	}
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
