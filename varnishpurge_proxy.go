package main

import (
	"fmt"
	"log"
	"net/http"
	"runtime"
	"sync"
	"time"

	"encoding/json"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/net"

	"os"
	"os/signal"
	"strings"
	"syscall"
)

const config_file_path = "/etc/varnishpurge_proxy.conf"

var (
	queueMutex             sync.Mutex
	enqueued               = make(map[string]map[string]bool)
	retryQueue             = make(chan *RetryJob, 1000)
	backendServers         []string
	portToListen           int
	firstTimeout           time.Duration
	retryTimeout           time.Duration
	maxRetries             int
	initialDelay           time.Duration
	maxDelayBetweenRetries time.Duration
	updateStats            time.Duration
	retryQueueSize         int
)

var httpFirstClient = &http.Client{
	Timeout: firstTimeout,
}

var httpRetryClient = &http.Client{
	Timeout: retryTimeout,
}

type Configuration struct {
	BackendServers         []string `json:"backend_servers"`
	PortToListen           int      `json:"port_to_listen"`
	FirstTimeout           int      `json:"first_timeout"`
	RetryTimeout           int      `json:"retry_timeout"`
	MaxRetries             int      `json:"max_retries"`
	InitialDelay           int      `json:"initial_delay"`
	MaxDelayBetweenRetries int      `json:"max_delay_between_retries"`
	UpdateStats            int      `json:"update_stats"`
	RetryQueueSize         int      `json:"retry_queue_size"`
}

// Backend represents a web server backend
type Backend struct {
	URL string
}

type RetryJob struct {
	Request      *http.Request
	requestID    string
	RetryCount   int
	MaxRetries   int
	MaxDelay     time.Duration
	RetryBackoff time.Duration
}

func main() {

	if err := validateConfigFile(config_file_path); err != nil {
		log.Fatalf("Configuration validation failed: %v", err)
	}
	if !readConfig(config_file_path) {
		fmt.Println("Error reading configuration")
		return
	}

	// backendServers = config.BackendServers
	// portToListen = config.PortToListen
	// firstTimeout = time.Duration(config.FirstTimeout) * time.Millisecond
	// retryTimeout = time.Duration(config.RetryTimeout) * time.Millisecond
	// maxRetries = config.MaxRetries
	// initialDelay = time.Duration(config.InitialDelay) * time.Millisecond
	// maxDelayBetweenRetries = time.Duration(config.MaxDelayBetweenRetries) * time.Millisecond
	// updateStats = time.Duration(config.UpdateStats) * time.Millisecond
	// retryQueueSize = config.RetryQueueSize

	// if retryQueueSize < 100 {
	// 	retryQueueSize = 100
	// }
	// if maxRetries < 1 {
	// 	maxRetries = 1
	// }

	// if firstTimeout < 5*time.Second || firstTimeout == 0 {
	// 	firstTimeout = 5 * time.Second
	// }
	// if retryTimeout < 5*time.Second || retryTimeout == 0 {
	// 	retryTimeout = 5 * time.Second
	// }
	// if initialDelay < 10*time.Second || initialDelay == 0 {
	// 	initialDelay = 10 * time.Second
	// }
	// if maxDelayBetweenRetries < 10*time.Second || maxDelayBetweenRetries == 0 {
	// 	maxDelayBetweenRetries = 10 * time.Second
	// }
	// if updateStats < 10*time.Second || updateStats == 0 {
	// 	updateStats = 10 * time.Second
	// }

	go processRetryQueue(retryQueue, 10)
	go printSystemStats()
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGHUP)

	// Start a goroutine to handle reload signals
	go reloadConfigOnSignal(signalCh)

	// Initialize HTTP server
	http.HandleFunc("/", requestHandler)

	// Start HTTP server

	// log.Fatal(http.ListenAndServe(":8080", nil))
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", portToListen), nil))
}

func requestHandler(w http.ResponseWriter, r *http.Request) {
	// Return 200 OK immediately
	if checkIncomingRequest(w, r) {
		w.WriteHeader(http.StatusOK)
		go func() {
			requestID := generateID()
			done := make(chan struct{})
			// Forward request to backend servers
			for _, backendURL := range backendServers {
				go func(backendURL string) {
					forwardRequest(backendURL, r, requestID)
					done <- struct{}{} // Signal that the goroutine is done
				}(backendURL)
			}
			for range backendServers {
				<-done
			}
		}()
	}
}

// func requestHandler2(w http.ResponseWriter, r *http.Request) {
// 	// Return 200 OK immediately
// 	w.WriteHeader(http.StatusOK)
// 	requestID := generateID()

// 	go func() {
// 		done := make(chan struct{})

// 		// Forward request to backend servers
// 		for _, backend := range backendServers {
// 			go func(backendURL string) {
// 				forwardRequest(backendURL, r, requestID)
// 				done <- struct{}{} // Signal that the goroutine is done
// 			}(backend.URL)
// 		}
// 		for range backendServers {
// 			<-done
// 		}
// 	}()
// }

func checkIncomingRequest(w http.ResponseWriter, r *http.Request) bool {
	// Check request method
	method := strings.ToUpper(r.Method)
	if method != "PURGE" && method != "HPURGE" && method != "SPURGE" {
		http.Error(w, "Invalid request method", http.StatusBadRequest)
		return false
	}
	log.Print(r.Header)
	// Check for x-purge-token header
	if _, ok := r.Header["X-Purge-Token"]; !ok {
		http.Error(w, "Missing x-purge-token header", http.StatusBadRequest)
		return false
	}
	purgeToken := r.Header.Get("X-Purge-Token")

	// Check for x-xkey-purge header
	if _, ok := r.Header["X-Xkey-Purge"]; !ok {
		http.Error(w, "Missing x-xkey-purge header", http.StatusBadRequest)
		return false
	}
	xKeyPurge := r.Header.Get("X-Xkey-Purge")

	// Check if headers have non-empty values
	if purgeToken == "" || xKeyPurge == "" {
		http.Error(w, "Empty header value", http.StatusBadRequest)
		return false
	}

	// All checks passed, return true indicating the request is valid
	return true
}

func forwardRequest(backendURL string, r *http.Request, requestID string) {
	retry_request := true
	// Forward request to backend server
	req, err := http.NewRequest(r.Method, backendURL+r.URL.Path, r.Body)
	if err != nil {
		log.Printf("Error creating request to %s: %v", backendURL, err)
		return
	}

	// Copy headers
	req.Header = r.Header
	ip := r.RemoteAddr
	xKeyPurgeHeaderValue := r.Header.Get("X-XKey-Purge")
	// Send request to backend server

	resp, err := httpFirstClient.Do(req)

	if err != nil {

		log.Printf("%s, Request %s -->  %s Failed ,key %s, Error:%s", string(ip), requestID, backendURL, xKeyPurgeHeaderValue, err)
		retry_request = true

	} else {

		log.Printf("%s, Request %s -->  %s,(%d),key %s, msg: %s", string(ip), requestID, backendURL, resp.StatusCode, xKeyPurgeHeaderValue, string(resp.Status))
		switch resp.StatusCode {

		case 200, 404, 403:
			retry_request = false

		case 500, 501, 502, 503, 504:
			retry_request = true

		default:
			log.Printf("Request %s  NOT accepted (%d). Not Retrying. Err:  %s", requestID, resp.StatusCode, (resp.Status))
			retry_request = false

		}
		defer resp.Body.Close()

	}
	if retry_request {

		queueMutex.Lock()
		if headers, ok := enqueued[backendURL]; ok {
			if headers[xKeyPurgeHeaderValue] {
				// Existe la url y la key en la cola
				log.Printf("Request %s, %s already in retry queue. Not adding new one. Queue Size: %d\n", backendURL, xKeyPurgeHeaderValue, len(retryQueue))
				queueMutex.Unlock()
				return
			}

		} else {
			// No existe la url, por tqanto tampoco la key, creamos la entrada de la URL
			enqueued[backendURL] = make(map[string]bool)
		}
		//Añadimos la key de Purge.

		retryJob := &RetryJob{
			Request:      req,
			RetryCount:   0,
			MaxRetries:   maxRetries,
			requestID:    requestID,
			MaxDelay:     maxDelayBetweenRetries, // Maximum delay between retries
			RetryBackoff: initialDelay,           // Initial retry delay
		}
		retryQueue <- retryJob

		enqueued[backendURL][xKeyPurgeHeaderValue] = true
		queueMutex.Unlock()

		log.Printf("%s, Request %s -->  %s, Not in Queue, added. Key %s, Queue Size: %d", string(ip), requestID, backendURL, xKeyPurgeHeaderValue, len(retryQueue))

	}

}

func processRetryQueue(queue chan *RetryJob, maxConcurrency int) {
	// Create a buffered channel to limit concurrency
	semaphore := make(chan struct{}, maxConcurrency)

	for {
		select {
		case job := <-queue:
			// Acquire semaphore
			semaphore <- struct{}{}

			go func(job *RetryJob) {
				defer func() {
					// Release semaphore after job completes
					<-semaphore
				}()

				if job.RetryCount < job.MaxRetries {
					log.Printf("Retrying Request %s, %s, %s (%d/%d), Queue Size:%d \n", job.requestID, job.Request.Header.Get("X-XKey-Purge"), job.Request.URL, job.RetryCount+1, job.MaxRetries, len(retryQueue))

					if err := retryRequest(job); err != nil {
						log.Printf("Retrying Request ERROR %s, KEY:%s, URL:%s (%d/%d),Error: %s\n", job.requestID, job.Request.Header.Get("X-XKey-Purge"), job.Request.URL, job.RetryCount+1, job.MaxRetries, err)
						job.RetryCount++
						// Increase the retry delay exponentially up to MaxDelay
						delay := job.RetryBackoff * time.Duration(1<<uint(job.RetryCount))
						if delay > job.MaxDelay {
							delay = job.MaxDelay
						}
						log.Printf("Delaying Request  %s, KEY:%s, URL:%s (%d/%d), %d seconds\n, Queue Size: %d", job.requestID, job.Request.Header.Get("X-XKey-Purge"), job.Request.URL, job.RetryCount+1, job.MaxRetries, int(delay.Seconds()), len(retryQueue))
						time.AfterFunc(delay, func() {
							queue <- job // Retry the job after the calculated delay
						})
					}
				} else {
					log.Printf("Retrying Request %s, KEY:%s, URL:%s MAX error Reached QueueSize: %d", job.requestID, job.Request.Header.Get("X-XKey-Purge"), job.Request.URL, len(retryQueue))
				}
			}(job)
		}
	}
}

// func processRetryQueue(queue chan *RetryJob) {
// 	for {
// 		select {
// 		case job := <-queue:
// 			if job.RetryCount < job.MaxRetries {

// 				log.Printf("Retrying Request %s, %s, %s (%d/%d), Queue Size:%d \n", job.requestID, job.Request.Header.Get("X-XKey-Purge"), job.Request.URL, job.RetryCount+1, job.MaxRetries, len(retryQueue))
// 				go func(job *RetryJob) {
// 					if err := retryRequest(job); err != nil {
// 						log.Printf("Retrying Request ERROR %s, KEY:%s, URL:%s (%d/%d),Error: %s\n", job.requestID, job.Request.Header.Get("X-XKey-Purge"), job.Request.URL, job.RetryCount+1, job.MaxRetries, err)
// 						job.RetryCount++
// 						// Increase the retry delay exponentially up to MaxDelay
// 						delay := job.RetryBackoff * time.Duration(1<<uint(job.RetryCount))
// 						if delay > job.MaxDelay {
// 							delay = job.MaxDelay
// 						}
// 						log.Printf("Delaying Request  %s, KEY:%s, URL:%s (%d/%d), %d seconds\n, Queue Size: %d", job.requestID, job.Request.Header.Get("X-XKey-Purge"), job.Request.URL, job.RetryCount+1, job.MaxRetries, int(delay.Seconds()), len(retryQueue))
// 						time.AfterFunc(delay, func() {
// 							queue <- job // Retry the job after the calculated delay
// 						})
// 					}
// 				}(job)
// 			} else {
// 				log.Printf("Retrying Request %s, KEY:%s, URL:%s MAX error Reached QueueSize: %d", job.requestID, job.Request.Header.Get("X-XKey-Purge"), job.Request.URL, len(retryQueue))
// 			}
// 		}
// 	}
// }

func retryRequest(job *RetryJob) error {

	resp, err := httpRetryClient.Do(job.Request)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// Check response status code and handle as needed
	if resp.StatusCode != http.StatusOK {
		log.Printf("Reintento ok pero error de Status request %s", job.requestID)
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)

	}
	return nil
}

func generateID() string {
	// Generate a unique ID using current timestamp
	currentTime := time.Now()
	return currentTime.Format("150405.999999999")
}

func printSystemStats() {
	for {
		// Memory Usage
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		memUsageKB := mem.Alloc / 1024 // Convert bytes to kilobytes
		memUsage := fmt.Sprintf("Memory Usage: %v KB", memUsageKB)

		// CPU Usage
		cpuPercent, err := cpu.Percent(time.Second, false)
		var cpuUsage string
		if err == nil && len(cpuPercent) > 0 {
			cpuUsage = fmt.Sprintf("CPU Usage: %.2f%%", cpuPercent[0])
		}

		// Sockets
		netStats, err := net.Connections("all")
		var numSockets string
		if err == nil {
			numSockets = fmt.Sprintf("Number of Sockets: %d", len(netStats))
		}

		log.Printf("%s, %s, %s\n", memUsage, cpuUsage, numSockets)

		time.Sleep(updateStats) // Adjust the interval as needed
	}
}

func readConfig(filename string) bool {
	var config Configuration

	// Read the file content
	file, err := os.ReadFile(filename)
	if err != nil {
		return false
	}

	// Unmarshal JSON content into configuration struct
	err = json.Unmarshal(file, &config)
	if err != nil {
		return false
	}

	backendServers = config.BackendServers
	portToListen = config.PortToListen
	firstTimeout = time.Duration(config.FirstTimeout) * time.Millisecond
	retryTimeout = time.Duration(config.RetryTimeout) * time.Millisecond
	maxRetries = config.MaxRetries
	initialDelay = time.Duration(config.InitialDelay) * time.Millisecond
	maxDelayBetweenRetries = time.Duration(config.MaxDelayBetweenRetries) * time.Millisecond
	updateStats = time.Duration(config.UpdateStats) * time.Millisecond
	retryQueueSize = config.RetryQueueSize

	if retryQueueSize < 100 {
		retryQueueSize = 100
	}
	if maxRetries < 1 {
		maxRetries = 1
	}

	if firstTimeout < 5*time.Second || firstTimeout == 0 {
		firstTimeout = 5 * time.Second
	}
	if retryTimeout < 5*time.Second || retryTimeout == 0 {
		retryTimeout = 5 * time.Second
	}
	if initialDelay < 10*time.Second || initialDelay == 0 {
		initialDelay = 10 * time.Second
	}
	if maxDelayBetweenRetries < 10*time.Second || maxDelayBetweenRetries == 0 {
		maxDelayBetweenRetries = 10 * time.Second
	}
	if updateStats < 10*time.Second || updateStats == 0 {
		updateStats = 10 * time.Second
	}

	log.Println("---------Configuration Settings---------")
	log.Println("Backend Servers:", backendServers)
	log.Println("Port to Listen:", portToListen)
	log.Println("First Timeout:", firstTimeout)
	log.Println("Retry Timeout:", retryTimeout)
	log.Println("Max Retries:", maxRetries)
	log.Println("Initial Delay:", initialDelay)
	log.Println("Max Delay:", maxDelayBetweenRetries)
	log.Println("Update Stats Every:", updateStats)
	log.Println("Retry Queue Size:", retryQueueSize)
	log.Println("---------End of  Settings---------")

	return true
}

func validateConfigFile(configFile string) error {
	// Read the config file
	configData, err := os.ReadFile(configFile)
	if err != nil {
		return fmt.Errorf("error reading configuration file: %v", err)
	}

	// Unmarshal the config data into Configuration struct
	var config Configuration
	if err := json.Unmarshal(configData, &config); err != nil {
		return fmt.Errorf("error unmarshaling configuration data: %v", err)
	}

	// Perform validation checks here
	// For example, check if required fields are present, if values are within valid ranges, etc.
	// You can also add custom validation logic based on your requirements

	// Example validation check: Ensure portToListen is within a valid range
	if config.PortToListen < 1024 || config.PortToListen > 65535 {
		return fmt.Errorf("port to listen is not within valid range (1024-65535)")
	}
	return nil
}

func reloadConfigOnSignal(signalCh <-chan os.Signal) {
	for {
		select {
		case <-signalCh:
			fmt.Println("Received reload signal. Reloading configuration...")
			// Validate the configuration file after receiving the reload signal
			if err := validateConfigFile(config_file_path); err != nil {
				log.Printf("Reloaded configuration validation failed: %v", err)
			} else {
				fmt.Println("Reloaded configuration is valid")
				if !readConfig(config_file_path) {
					fmt.Println("Error reading reloaded configuration, not modifying values")
					return
				}

			}
		}
	}
}
