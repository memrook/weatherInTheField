# Этап сборки
FROM golang:1.22-alpine AS builder

# Установка необходимых утилит
RUN apk add --no-cache git

# Установка рабочей директории
WORKDIR /app

# Копирование и загрузка зависимостей
COPY go.mod go.sum ./
RUN go mod download

# Копирование исходного кода
COPY . .

# Сборка приложения
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o weatherservice ./cmd/weatherservice

# Этап запуска
FROM alpine:3.18

RUN apk add --no-cache ca-certificates tzdata

# Установка временной зоны
ENV TZ=Europe/Moscow

# Создание непривилегированного пользователя
RUN adduser -D -h /app appuser
USER appuser

WORKDIR /app

# Копирование исполняемого файла из этапа сборки
COPY --from=builder /app/weatherservice .

# Запуск приложения
CMD ["./weatherservice"] 