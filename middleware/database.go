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

// Config 数据库配置
type Database struct {
	Host      string
	Port      int
	User      string
	Password  string
	DBName    string
	Charset   string
	ParseTime bool
	Loc       string
}

// InitDB 初始化数据库连接
func InitDB(config Database) (*gorm.DB, error) {

	logrusLogger := logrus.New()

	opts := gormv2logrus.GormOptions{
		SlowThreshold: 100 * time.Millisecond,
		LogLevel:      gormlogger.Info,
		TruncateLen:   1000,
		LogLatency:    true,
	}

	gormLogger := gormv2logrus.NewGormlog(gormv2logrus.WithGormOptions(opts), gormv2logrus.WithLogrus(logrusLogger))
	gormLogger.LogMode(gormlogger.Warn)
	gormLogger.SkipErrRecordNotFound = true
	//gormLogger.SlowThreshold = 200 * time.Millisecond

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
