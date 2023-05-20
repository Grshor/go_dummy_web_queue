package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// просто канал нам не подойдет (это было бы намного проще),
// потому-что нам нужно хранилище неограниченной длинны (
// нам неизвестно, какое поведение будет у клиентов этого сервера
// )
//
// выбор стоит между container/list и slice и кольцевым буфером, я выберу slice
// для простоты реализации
type queue struct {
	// mu    sync.RWMutex
	write chan<- string // PUT пишут сюда
	read  <-chan string // GET читают отсюда
	store []string      // чтобы не блокировать PUT-ы (тут много пространства для оптимизации)
}

func newQueue() *queue {
	// в in приходят значения для записи/передачи из PUT
	// в out уходят значения для передачи на GET
	out, in := make(chan string, 2), make(chan string, 2)

	q := &queue{
		// mu:    sync.RWMutex{},
		read:  out,
		write: in,
		store: make([]string, 0, 16),
	}

	// эта обрабатывающая горутина только одна на очередь,
	// поэтому гарантии синхронизации, данные нам каналами, соблюдаются и нами
	// не учитываем случаи закрытия каналов намеренно, цель у этой очереди другая
	go func() {
		for {
			val := <-in
			select {
			case out <- val: // try write
				continue
			default:
			}

			q.store = append(q.store, val)
			// переходим в режим с нашим дополнительным бафером (store) пока он не освободится (значит)
		bufL:
			for {
				select {
				case val := <-in: // try read
					q.store = append(q.store, val)

				case out <- q.store[0]: // try write
					q.store = q.store[1:]  // pop
					if len(q.store) == 0 { // дополнительный бафер очищен, можно писать в out напрямую
						break bufL
					}
				}
			}
		}
	}()

	return q
}

type queueStore struct {
	Data sync.Map
}

func (q *queueStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// определяем очередь
	// (r.URL.Path=/queue следовательно убираем первый символ)
	queue := r.URL.Path[1:]

	switch r.Method {
	case "PUT":
		if param := r.FormValue("v"); param == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write(nil)
			return
		} else {
			q.put(queue, param)
		}

		w.WriteHeader(http.StatusOK)
		w.Write(nil)

	case "GET":
		var ctx context.Context

		// пытаемся прочесть параметр timeout (если есть)
		if timeoutStr := r.FormValue("timeout"); timeoutStr != "" {
			timeoutSeconds, err := strconv.Atoi(timeoutStr)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write(nil)
				return
			}
			ctx, _ = context.WithTimeout(r.Context(), time.Duration(timeoutSeconds)*time.Second)
		} else {
			ctx = r.Context()
		}

		val := q.subcribe(ctx, queue)
		// если время вышло
		if val == "" {
			w.WriteHeader(http.StatusNotFound)
			w.Write(nil)
			return
		}

		w.Write([]byte(val))
	}
}

func main() {
	http.Handle("/", new(queueStore))
	flagPort := flag.Int("p", 80, "port (default 80) app listens to")
	flag.Parse()

	if err := http.ListenAndServe(fmt.Sprint(":", *flagPort), nil); err != nil {
		log.Fatal(err)
	}

	// слушаем сигналы от ОС на отключение
	// заодно блокируем	несанкционированное завершение программы
	quit := make(chan os.Signal, 1)
	signal.Notify(quit,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)
	<-quit
}

func (q *queueStore) getOrMakeQueue(queueKey string) (queueP *queue) {
	_queueP, exists := q.Data.Load(queueKey)
	if !exists {
		queueP = newQueue()

		q.Data.Store(queueKey, queueP)
	} else {
		queueP = _queueP.(*queue)
	}

	return queueP
}

func (q *queueStore) subcribe(ctx context.Context, queueKey string) string {
	queueP := q.getOrMakeQueue(queueKey)
	if queueP == nil {
		return ""
	}

	select {
	case <-ctx.Done():
		return ""
	case value := <-queueP.read:
		return value
	}
}

func (q *queueStore) put(queueKey, value string) {
	queue := q.getOrMakeQueue(queueKey)
	queue.write <- value
}
