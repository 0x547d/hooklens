package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	addr := flag.String("addr", ":8787", "监听地址")
	ttl := flag.Duration("ttl", DefaultTTL, "接收箱的存活时间")
	allowPrivate := flag.Bool("allow-private", false, "允许回放到回环/内网地址（仅本地开发）")
	storage := flag.String("storage", "redis", "存储后端：redis 或 memory")
	redisAddr := flag.String("redis", "", "Redis 地址或 redis:// URL（默认读 REDIS_URL / REDIS_ADDR，最后回退 127.0.0.1:6379）")
	redisPassword := flag.String("redis-password", "", "Redis 密码（默认读 REDIS_PASSWORD 环境变量）")
	redisDB := flag.Int("redis-db", 0, "Redis 数据库编号")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	store, err := openStore(*storage, *ttl, *redisAddr, *redisPassword, *redisDB, log)
	if err != nil {
		log.Error("存储初始化失败", "storage", *storage, "err", err)
		os.Exit(1)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Warn("关闭存储时出错", "err", err)
		}
	}()

	h := NewServer(store, log, *allowPrivate)

	// Cancelling the base context during shutdown releases every in-flight
	// request context, which is what lets long-lived SSE streams exit cleanly.
	baseCtx, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()

	srv := &http.Server{
		Addr:    *addr,
		Handler: h.Handler(),
		BaseContext: func(net.Listener) context.Context {
			return baseCtx
		},
		// WriteTimeout is intentionally unset: SSE connections are long-lived.
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if n := store.GC(); n > 0 {
					log.Info("清理过期接收箱", "count", n)
				}
			}
		}
	}()

	go func() {
		log.Info("hooklens 已启动",
			"addr", *addr, "storage", store.Name(), "ttl", ttl.String(), "allowPrivate", *allowPrivate)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("服务启动失败", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("正在关闭服务")
	cancelBase()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

func openStore(kind string, ttl time.Duration, addr, password string, db int, log *slog.Logger) (Store, error) {
	switch kind {
	case "redis":
		opts, err := redisOptions(addr, password, db)
		if err != nil {
			return nil, err
		}
		return NewRedisStore(opts, ttl, log)
	case "memory":
		log.Warn("使用内存存储：重启后数据丢失，且 SSE 仅在本进程内生效")
		return NewMemStore(ttl), nil
	default:
		return nil, fmt.Errorf("未知的 -storage=%q，可选 redis 或 memory", kind)
	}
}

func redisOptions(addr, password string, db int) (*redis.Options, error) {
	// 环境变量优先于默认值，方便 Fly.io / Render / Compose 直接注入。
	if addr == "" {
		switch {
		case os.Getenv("REDIS_URL") != "":
			addr = os.Getenv("REDIS_URL")
		case os.Getenv("REDIS_ADDR") != "":
			addr = os.Getenv("REDIS_ADDR")
		default:
			addr = "127.0.0.1:6379"
		}
	}

	var opts *redis.Options
	if strings.HasPrefix(addr, "redis://") || strings.HasPrefix(addr, "rediss://") {
		parsed, err := redis.ParseURL(addr)
		if err != nil {
			return nil, fmt.Errorf("Redis URL 解析失败: %w", err)
		}
		opts = parsed
	} else {
		opts = &redis.Options{Addr: addr, DB: db}
	}
	if password != "" {
		opts.Password = password
	} else if env := os.Getenv("REDIS_PASSWORD"); env != "" {
		opts.Password = env
	}
	if db != 0 {
		opts.DB = db
	}
	opts.DialTimeout = 5 * time.Second
	opts.ReadTimeout = 3 * time.Second
	opts.WriteTimeout = 3 * time.Second
	opts.MaxRetries = 2
	return opts, nil
}
