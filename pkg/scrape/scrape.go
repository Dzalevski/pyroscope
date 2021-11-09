package scrape

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/common/config"
	//	"github.com/prometheus/prometheus/config"
	//	"github.com/prometheus/prometheus/pkg/labels"
	//	"github.com/prometheus/prometheus/pkg/pool"
	"github.com/sirupsen/logrus"

	"github.com/pyroscope-io/pyroscope/pkg/agent/upstream"
	"github.com/pyroscope-io/pyroscope/pkg/build"
)

// scrapePool manages scrapes for sets of targets.
type scrapePool struct {
	upstream upstream.Upstream
	logger   *logrus.Logger

	ctx    context.Context
	cancel context.CancelFunc

	// mtx must not be taken after targetMtx.
	mtx    sync.Mutex
	config *ScrapeConfig
	client *http.Client
	loops  map[uint64]*scrapeLoop

	targetMtx sync.Mutex
	// activeTargets and loops must always be synchronized to have the same
	// set of hashes.
	activeTargets  map[uint64]*Target
	droppedTargets []*Target
}

func newScrapePool(cfg *ScrapeConfig, u upstream.Upstream, logger *logrus.Logger) (*scrapePool, error) {
	client, err := config.NewClientFromConfig(cfg.HTTPClientConfig, cfg.JobName)
	if err != nil {
		return nil, fmt.Errorf("creating HTTP client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	sp := scrapePool{
		ctx:           ctx,
		cancel:        cancel,
		upstream:      u,
		config:        cfg,
		client:        client,
		activeTargets: make(map[uint64]*Target),
		loops:         make(map[uint64]*scrapeLoop),
		logger:        logger,
	}

	return &sp, nil
}

func (sp *scrapePool) newScrapeLoop(s *scraper, i, t time.Duration) *scrapeLoop {
	x := scrapeLoop{
		scraper:  s,
		logger:   sp.logger,
		upstream: sp.upstream,
		stopped:  make(chan struct{}),
		interval: i,
		timeout:  t,
	}
	x.ctx, x.cancel = context.WithCancel(sp.ctx)
	return &x
}

func (sp *scrapePool) ActiveTargets() []*Target {
	sp.targetMtx.Lock()
	defer sp.targetMtx.Unlock()
	var tActive []*Target
	for _, t := range sp.activeTargets {
		tActive = append(tActive, t)
	}
	return tActive
}

func (sp *scrapePool) DroppedTargets() []*Target {
	sp.targetMtx.Lock()
	defer sp.targetMtx.Unlock()
	return sp.droppedTargets
}

// stop terminates all scrapers and returns after they all terminated.
func (sp *scrapePool) stop() {
	sp.mtx.Lock()
	defer sp.mtx.Unlock()
	sp.cancel()
	sp.targetMtx.Lock()
	var wg sync.WaitGroup
	wg.Add(len(sp.loops))
	for fp, l := range sp.loops {
		go func(l *scrapeLoop) {
			l.stop()
			wg.Done()
		}(l)
		delete(sp.loops, fp)
		delete(sp.activeTargets, fp)
	}
	sp.targetMtx.Unlock()
	wg.Wait()
	sp.client.CloseIdleConnections()
}

// reload the scrape pool with the given scrape configuration. The target state is preserved
// but all scrapers are restarted with the new scrape configuration.
func (sp *scrapePool) reload(cfg *ScrapeConfig) error {
	sp.mtx.Lock()
	defer sp.mtx.Unlock()

	client, err := config.NewClientFromConfig(cfg.HTTPClientConfig, cfg.JobName)
	if err != nil {
		return fmt.Errorf("creating HTTP client: %w", err)
	}

	sp.config = cfg
	oldClient := sp.client
	sp.client = client

	var (
		wg            sync.WaitGroup
		interval      = time.Duration(sp.config.ScrapeInterval)
		timeout       = time.Duration(sp.config.ScrapeTimeout)
		bodySizeLimit = int64(sp.config.BodySizeLimit)
	)

	sp.targetMtx.Lock()
	for fp, oldLoop := range sp.loops {
		wg.Add(1)
		s := &scraper{
			Target:        sp.activeTargets[fp],
			client:        sp.client,
			timeout:       timeout,
			bodySizeLimit: bodySizeLimit,
		}
		n := sp.newScrapeLoop(s, interval, timeout)
		go func(oldLoop, newLoop *scrapeLoop) {
			oldLoop.stop()
			wg.Done()
			newLoop.run()
		}(oldLoop, n)
		sp.loops[fp] = n
	}

	sp.targetMtx.Unlock()
	wg.Wait()
	oldClient.CloseIdleConnections()
	return nil
}

// Sync converts target groups into actual scrape targets and synchronizes
// the currently running scraper with the resulting set and returns all scraped and dropped targets.
func (sp *scrapePool) Sync(tgs []Group) {
	sp.mtx.Lock()
	defer sp.mtx.Unlock()
	sp.targetMtx.Lock()
	var all []*Target
	sp.droppedTargets = []*Target{}
	for _, tg := range tgs {
		targets, failures := TargetsFromGroup(tg, sp.config)
		for _, err := range failures {
			sp.logger.WithError(err).Errorf("creating target")
		}
		for _, t := range targets {
			if t.Labels().Len() > 0 {
				all = append(all, t)
			} else if t.DiscoveredLabels().Len() > 0 {
				sp.droppedTargets = append(sp.droppedTargets, t)
			}
		}
	}
	sp.targetMtx.Unlock()
	sp.sync(all)
}

func (sp *scrapePool) sync(targets []*Target) {
	var (
		uniqueLoops   = make(map[uint64]*scrapeLoop)
		interval      = time.Duration(sp.config.ScrapeInterval)
		timeout       = time.Duration(sp.config.ScrapeTimeout)
		bodySizeLimit = int64(sp.config.BodySizeLimit)
	)

	sp.targetMtx.Lock()
	for _, t := range targets {
		hash := t.hash()
		_, ok := sp.activeTargets[hash]
		if !ok {
			// TODO: handle error
			interval, timeout, _ = t.intervalAndTimeout(interval, timeout)
			s := &scraper{
				Target:        t,
				client:        sp.client,
				timeout:       timeout,
				bodySizeLimit: bodySizeLimit,
			}

			l := sp.newScrapeLoop(s, interval, timeout)
			sp.activeTargets[hash] = t
			sp.loops[hash] = l
			uniqueLoops[hash] = l
		} else {
			if _, ok := uniqueLoops[hash]; !ok {
				uniqueLoops[hash] = nil
			}
		}
	}

	var wg sync.WaitGroup
	for hash := range sp.activeTargets {
		if _, ok := uniqueLoops[hash]; !ok {
			wg.Add(1)
			go func(l *scrapeLoop) {
				l.stop()
				wg.Done()
			}(sp.loops[hash])
			delete(sp.loops, hash)
			delete(sp.activeTargets, hash)
		}
	}

	sp.targetMtx.Unlock()
	for _, l := range uniqueLoops {
		if l != nil {
			go l.run()
		}
	}

	wg.Wait()
}

type scrapeLoop struct {
	scraper  *scraper
	logger   logrus.FieldLogger
	upstream upstream.Upstream

	parentCtx context.Context
	ctx       context.Context
	cancel    func()
	stopped   chan struct{}

	forcedErr    error
	forcedErrMtx sync.Mutex
	interval     time.Duration
	timeout      time.Duration
}

func (sl *scrapeLoop) run() {
	defer close(sl.stopped)
	select {
	case <-time.After(sl.scraper.offset(sl.interval)):
	case <-sl.ctx.Done():
		return
	}
	ticker := time.NewTicker(sl.interval)
	defer ticker.Stop()
	for {
		select {
		case <-sl.ctx.Done():
			return
		case <-ticker.C:
			// TODO: reuse buffer
			buf := bytes.NewBuffer(nil)
			ctx, cancel := context.WithTimeout(sl.ctx, sl.timeout)
			// TODO: handle error
			_, _ = sl.scraper.scrape(ctx, buf)
			cancel()
			// TODO: write to upstream
		}
	}
}

func (sl *scrapeLoop) stop() {
	sl.cancel()
	<-sl.stopped
}

var errBodySizeLimit = errors.New("body size limit exceeded")

const acceptHeader = `application/openmetrics-text; version=0.0.1,text/plain;version=0.0.4;q=0.5,*/*;q=0.1`

var UserAgent = fmt.Sprintf("Pyroscope/%s", build.Version)

// targetScraper implements the scraper interface for a target.
type scraper struct {
	*Target

	client  *http.Client
	req     *http.Request
	timeout time.Duration

	gzipr *gzip.Reader
	buf   *bufio.Reader

	bodySizeLimit int64
}

func (s *scraper) scrape(ctx context.Context, w io.Writer) (string, error) {
	if s.req == nil {
		req, err := http.NewRequest("GET", s.URL().String(), nil)
		if err != nil {
			return "", err
		}
		req.Header.Add("Accept", acceptHeader)
		req.Header.Add("Accept-Encoding", "gzip")
		req.Header.Set("User-Agent", UserAgent)
		req.Header.Set("X-Pyroscope-Scrape-Timeout-Seconds", strconv.FormatFloat(s.timeout.Seconds(), 'f', -1, 64))

		s.req = req
	}

	resp, err := s.client.Do(s.req.WithContext(ctx))
	if err != nil {
		return "", err
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("server returned HTTP status %s", resp.Status)
	}

	if s.bodySizeLimit <= 0 {
		s.bodySizeLimit = math.MaxInt64
	}
	if resp.Header.Get("Content-Encoding") != "gzip" {
		n, err := io.Copy(w, io.LimitReader(resp.Body, s.bodySizeLimit))
		if err != nil {
			return "", err
		}
		if n >= s.bodySizeLimit {
			return "", errBodySizeLimit
		}
		return resp.Header.Get("Content-Type"), nil
	}

	if s.gzipr == nil {
		s.buf = bufio.NewReader(resp.Body)
		s.gzipr, err = gzip.NewReader(s.buf)
		if err != nil {
			return "", err
		}
	} else {
		s.buf.Reset(resp.Body)
		if err = s.gzipr.Reset(s.buf); err != nil {
			return "", err
		}
	}

	n, err := io.Copy(w, io.LimitReader(s.gzipr, s.bodySizeLimit))
	s.gzipr.Close()
	if err != nil {
		return "", err
	}
	if n >= s.bodySizeLimit {
		return "", errBodySizeLimit
	}
	return resp.Header.Get("Content-Type"), nil
}
