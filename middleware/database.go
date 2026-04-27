package middleware

import (
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	gormv2logrus "github.com/thomas-tacquet/gormv2-logrus"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// Database holds MySQL connection settings.
type Database struct {
	Host      string
	Port      int
	User      string
	Password  string
	DBName    string
	Charset   string
	ParseTime bool
	Loc       string
	// SlowQueryThresholdMs is passed to GORM as SlowThreshold. When 0, defaults to 300.
	// Block storage issues large multi-row INSERTs (tx, event_log, balances, …) with many
	// secondary indexes; sub-100ms is uncommon on real MySQL, so 100ms spams WARN "SLOW SQL".
	SlowQueryThresholdMs int `mapstructure:"slow_query_threshold_ms"`
}

// InitDB opens a GORM MySQL connection.
func InitDB(config Database) (*gorm.DB, error) {

	logrusLogger := logrus.New()

	slowMs := config.SlowQueryThresholdMs
	if slowMs <= 0 {
		slowMs = 300
	}

	opts := gormv2logrus.GormOptions{
		SlowThreshold: time.Duration(slowMs) * time.Millisecond,
		LogLevel:      gormlogger.Info,
		TruncateLen:   1000,
		LogLatency:    true,
	}

	gormLogger := gormv2logrus.NewGormlog(gormv2logrus.WithGormOptions(opts), gormv2logrus.WithLogrus(logrusLogger))
	gormLogger.LogMode(gormlogger.Warn)
	gormLogger.SkipErrRecordNotFound = true

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=%t&loc=%s&timeout=10s",
		config.User,
		config.Password,
		config.Host,
		config.Port,
		config.DBName,
		config.Charset,
		config.ParseTime,
		config.Loc,
	)

	var err error
	DB, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: gormLogger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect database: %w", err)
	}

	logrus.Infof("Database connected successfully")

	return DB, nil
}
