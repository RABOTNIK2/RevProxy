package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
	"sync"
)

const cookineName = "BALANCER_ID"

type Backend struct{
	URL *url.URL
	Proxy *httputil.ReverseProxy
	Alive atomic.Bool
	ActiveConnections int64
}

type ReverseProxy struct{
	backends []*Backend
	byHost map[string]*Backend
	mu sync.RWMutex
}

func NewReverseProxy(backends []string) *ReverseProxy{
	backs := make([]*Backend, len(backends))
	byHost := make(map[string]*Backend)

	for i, v := range backends{
		parseUrl, err := url.Parse(v)
		if err != nil {
			log.Fatalf("Invalid url: %v", err)
		}
		backs[i] = &Backend{URL: parseUrl, Proxy: httputil.NewSingleHostReverseProxy(parseUrl)}
		backs[i].Alive.Store(true)
		backs[i].Proxy.Transport = &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: 5*time.Second,
				KeepAlive: 30*time.Second,
			}).DialContext,
			MaxIdleConns: 100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout: 90 * time.Second,
		    ResponseHeaderTimeout: 30 * time.Second,
		    TLSHandshakeTimeout: 10 * time.Second,
		}
		backs[i].Proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			log.Printf("backend %s failed: %v", parseUrl, err)
    		http.Error(w, "bad gateway", http.StatusBadGateway)
		}
		byHost[parseUrl.Host] = backs[i]
	}

	return &ReverseProxy{backends: backs, byHost: byHost}
}

func (p *ReverseProxy) leastConnected() *Backend{

	var bestConn *Backend
	var minConns int64 = -1

	for _, b := range p.backends{
		if !b.Alive.Load(){continue}
		conns := atomic.LoadInt64(&b.ActiveConnections)
		if minConns == -1 || conns < minConns{
			minConns = conns
			bestConn = b
		}
	}

	return bestConn
}

func (p *ReverseProxy) healthCheck(ctx context.Context){
	client := &http.Client{Timeout: 5*time.Second}
	ticker := time.NewTicker(10*time.Second)
	defer ticker.Stop()

	for {
		select{
		case <- ctx.Done():
			return
		case <- ticker.C:
			for _, back := range p.backends{
				resp, err := client.Get(back.URL.String()+"/health/")
				if err != nil{
					log.Printf("[err] %v, at %s", err, back.URL.String())
					back.Alive.Store(false)
					continue
				}

				ok := resp.StatusCode>=200 && resp.StatusCode<300
				back.Alive.Store(ok)
				resp.Body.Close()
			}
		}
	}
}

func (p *ReverseProxy) ServeHTTP(w http.ResponseWriter, r *http.Request){
	var back *Backend

	if cookie, err := r.Cookie(cookineName); err == nil{
		p.mu.Lock()
		b, exists := p.byHost[cookie.Value]
		p.mu.Unlock()
		
		if exists && b.Alive.Load(){
			back = b
		}
	}

	if back == nil{
		back = p.leastConnected()
		if back == nil{
			http.Error(w, "no healthy backs", http.StatusServiceUnavailable)
			return
		}

		http.SetCookie(w, &http.Cookie{
			Name: cookineName,
			Value: back.URL.Host,
			Path: "/",
			HttpOnly: true,
		})
	}

	atomic.AddInt64(&back.ActiveConnections, 1)
	defer atomic.AddInt64(&back.ActiveConnections, -1)

	back.Proxy.ServeHTTP(w, r)
}

func main(){
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	backends := []string{"http://localhost:8001", "http://localhost:8002", "http://localhost:8003"}
    proxy := NewReverseProxy(backends)

    go proxy.healthCheck(ctx)

    serv := &http.Server{
		Addr: ":8080",
		Handler: proxy,
		IdleTimeout: 60*time.Second,
		ReadHeaderTimeout: 5*time.Second,
	}

	go func(){
		if err := serv.ListenAndServe(); err != nil && err != http.ErrServerClosed{
			log.Fatal(err)
		}
	}()

	<- ctx.Done()
	log.Print("closing")
    
    shutdownctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    if err := serv.Shutdown(shutdownctx); err != nil{
    	log.Printf("[err] shutdown err: %v",err)
    }
}