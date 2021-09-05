// Copyright 2018 The conprof Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"bytes"
	"context"
	"fmt"
	"github.com/conprof/conprof/codec"
	"github.com/dgraph-io/badger/v3"
	"math"
	"net"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/conprof/db/storage"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/google/pprof/profile"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"

	"github.com/conprof/conprof/config"
	"github.com/conprof/conprof/internal/pprof/measurement"
	"github.com/conprof/conprof/scrape"
)

var (
	defaultMetadataTimeRange = 24 * time.Hour
	LocalhostRepresentations = []string{"127.0.0.1", "localhost"}
)

type TargetRetriever interface {
	TargetsActive() map[string][]*scrape.Target
	TargetsDropped() map[string][]*scrape.Target
}

// NoTargets is passed to the API when only the API is served and no scraping is happening.
var NoTargets = func(_ context.Context) TargetRetriever { return NoTargetRetriever{} }

// NoTargetRetriever is passed to the API when only the API is served and no scraping is happening.
type NoTargetRetriever struct{}

func (t NoTargetRetriever) TargetsActive() map[string][]*scrape.Target {
	return map[string][]*scrape.Target{}
}

func (t NoTargetRetriever) TargetsDropped() map[string][]*scrape.Target {
	return map[string][]*scrape.Target{}
}

type Symbolizer interface {
	Symbolize(context.Context, *profile.Profile) error
}

type API struct {
	logger            log.Logger
	registry          *prometheus.Registry
	db                *badger.DB
	reloadCh          chan struct{}
	maxMergeBatchSize int64
	targets           func(context.Context) TargetRetriever
	globalURLOptions  GlobalURLOptions
	prefix            string
	queryRangeHist    prometheus.Histogram
	mergeSizeHist     prometheus.Histogram
	queryTimeout      time.Duration

	symbolizer Symbolizer

	mu     sync.RWMutex
	config *config.Config
}

type Option func(*API)

func New(
	logger log.Logger,
	registry *prometheus.Registry,

	opts ...Option,
) *API {

	a := &API{
		logger:   logger,
		registry: registry,
		prefix:   "/api/v1/",
		reloadCh: make(chan struct{}),
		globalURLOptions: GlobalURLOptions{ // TODO pass into from flags
			ListenAddress: "0.0.0.0:10902",
			Host:          "0.0.0.0:10902",
			Scheme:        "http",
		},
		queryRangeHist: promauto.With(registry).NewHistogram(prometheus.HistogramOpts{
			Name:    "query_range_duration_seconds",
			Help:    "A histogram of the duration of the query range",
			Buckets: prometheus.ExponentialBuckets(15*60, 2, 10), // smallest bucket 15m
		}),
		mergeSizeHist: promauto.With(registry).NewHistogram(prometheus.HistogramOpts{
			Name:    "merge_size_num_profiles",
			Help:    "A histogram of number of profiles merged",
			Buckets: prometheus.LinearBuckets(10, 10, 10),
		}),
	}

	for _, opt := range opts {
		opt(a)
	}

	return a
}

func WithDB(db *badger.DB) Option {
	return func(a *API) {
		a.db = db
	}
}

func WithSymbolizer(s Symbolizer) Option {
	return func(a *API) {
		a.symbolizer = s
	}
}

func WithMaxMergeBatchSize(max int64) Option {
	return func(a *API) {
		a.maxMergeBatchSize = max
	}
}

func WithTargets(targets func(ctx context.Context) TargetRetriever) Option {
	return func(a *API) {
		a.targets = targets
	}
}

func WithPrefix(prefix string) Option {
	return func(a *API) {
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
		a.prefix = prefix
	}
}

func WithReloadChannel(reloadCh chan struct{}) Option {
	return func(a *API) {
		a.reloadCh = reloadCh
	}
}

func WithQueryTimeout(t time.Duration) Option {
	return func(a *API) {
		a.queryTimeout = t
	}
}

// Routes returns a http.Handler containing all routes of the API so that it can be mounted into a mux.
func (a *API) Routes() http.Handler {
	r := httprouter.New()
	r.RedirectTrailingSlash = false
	ins := extpromhttp.NewInstrumentationMiddleware(a.registry, nil)
	instr := Instr(a.logger, ins)

	if a.db != nil {
		r.GET(path.Join(a.prefix, "/query_range"), instr("query_range", a.QueryRange))
		r.GET(path.Join(a.prefix, "/query"), instr("query", a.Query))
		r.GET(path.Join(a.prefix, "/series"), instr("series", a.Series))
		r.GET(path.Join(a.prefix, "/labels"), instr("label_names", a.LabelNames))
		r.GET(path.Join(a.prefix, "/label/:name/values"), instr("label_values", a.LabelValues))
	}
	if a.config != nil {
		r.GET(path.Join(a.prefix, "/status/config"), instr("config", a.Config))
	}

	r.GET(path.Join(a.prefix, "/targets"), instr("targets", a.Targets))

	return r
}

func (a *API) ApplyConfig(c *config.Config) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.config = c
	return nil
}

type Series struct {
	Labels     map[string]string `json:"labels"`
	Timestamps []int64           `json:"timestamps"`
}

type QueryCondition struct {
	field string
	value string
}

func (c *QueryCondition) Match(key *codec.ProfileKey) bool {
	if key == nil {
		return false
	}
	switch c.field {
	case "job":
		return key.Job == c.value
	case "tp":
		return key.Tp == c.value
	case "instance":
		return key.Instance == c.value
	}
	return false
}

func parseQueryCondition(query string) ([]QueryCondition, error) {
	result := make([]QueryCondition, 0, 2)
	conds := strings.Split(query, ",")
	for _, cond := range conds {
		exprs := strings.Split(cond, "=")
		if len(exprs) != 2 {
			return nil, fmt.Errorf("invalid query %v, condition %v", query, cond)
		}
		result = append(result, QueryCondition{
			field: strings.TrimSpace(exprs[0]),
			value: strings.TrimSpace(exprs[1]),
		})
	}
	return result, nil
}

func buildQueryRange(from, to int64, conds []QueryCondition) (start, end *codec.ProfileKey) {
	buildKey := func(k *codec.ProfileKey) {
		for _, c := range conds {
			switch c.field {
			case "job":
				k.Job = c.value
			case "tp":
				k.Tp = c.value
			case "instance":
				k.Instance = c.value
			}
		}
	}

	startKey := &codec.ProfileKey{Ts: from}
	endKey := &codec.ProfileKey{Ts: to}
	buildKey(startKey)
	buildKey(endKey)
	return startKey, endKey
}

func buildLabelsFromProfileKey(key *codec.ProfileKey) map[string]string {
	labels := make(map[string]string, 4)
	labels["job"] = key.Job
	labels["__name__"] = key.Tp
	labels["instance"] = key.Instance
	return labels
}

func (a *API) QueryRange(r *http.Request) (interface{}, []error, *ApiError) {
	from, err := parseTime(r.URL.Query().Get("from"))
	if err != nil {
		return nil, nil, &ApiError{Typ: ErrorBadData, Err: fmt.Errorf("failed to parse \"from\" time: %w", err)}
	}

	to, err := parseTime(r.URL.Query().Get("to"))
	if err != nil {
		return nil, nil, &ApiError{Typ: ErrorBadData, Err: fmt.Errorf("failed to parse \"to\" time: %w", err)}
	}

	limitString := r.URL.Query().Get("limit")
	applyLimit := limitString != ""
	limit := 0
	if applyLimit {
		var err error
		limit, err = strconv.Atoi(r.URL.Query().Get("limit"))
		if err != nil {
			return nil, nil, &ApiError{Typ: ErrorBadData, Err: fmt.Errorf("failed to parse \"limit\": %w", err)}
		}
	}

	if to.Before(from) {
		err := errors.New("to timestamp must not be before from time")
		return nil, nil, &ApiError{Typ: ErrorBadData, Err: err}
	}

	queryString := r.URL.Query().Get("query")
	if queryString == "" {
		return nil, nil, &ApiError{Typ: ErrorBadData, Err: errors.New("query cannot be empty")}
	}

	conds, err := parseQueryCondition(queryString)
	if err != nil {
		return nil, nil, &ApiError{Typ: ErrorBadData, Err: fmt.Errorf("failed to parse \"query\": %w", err)}
	}

	startKey, endKey := buildQueryRange(timestamp.FromTime(from), timestamp.FromTime(to), conds)

	res := []Series{}
	a.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := startKey.EncodeForRangeQuery()
		end := endKey.EncodeForRangeQuery()
		resSeries := Series{}
		var lastKey *codec.ProfileKey
		cnt := 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			if bytes.Compare(k, end) >= 0 {
				break
			}

			key, err := codec.DecodeProfileKey(k)
			if err != nil {
				continue
			}

			match := true
			for _, cond := range conds {
				match = match && cond.Match(key)
			}
			if !match {
				continue
			}

			cnt++
			if cnt > limit {
				break
			}

			if len(resSeries.Timestamps) == 0 {
				resSeries.Labels = buildLabelsFromProfileKey(key)
				resSeries.Timestamps = append(resSeries.Timestamps, key.Ts)
				lastKey = key
				continue
			}
			if lastKey != nil && lastKey.Job == key.Job &&
				lastKey.Tp == key.Tp &&
				lastKey.Instance == key.Instance {
				resSeries.Timestamps = append(resSeries.Timestamps, key.Ts)
			} else {
				if len(resSeries.Timestamps) > 0 {
					res = append(res, resSeries)
				}
				resSeries = Series{}
			}
			lastKey = key
		}
		return nil
	})
	//_ = limit
	//_ = ctx
	//return nil, nil, &ApiError{Typ: ErrorBadData, Err: fmt.Errorf("no implements")}
	//q, err := a.db.Querier(ctx, timestamp.FromTime(from), timestamp.FromTime(to))
	//if err != nil {
	//	return nil, nil, &ApiError{Typ: ErrorExec, Err: err}
	//}
	//
	//level.Debug(a.logger).Log("query", queryString, "from", from, "to", to)
	//sel, err := parser.ParseMetricSelector(queryString)
	//if err != nil {
	//	return nil, nil, &ApiError{Typ: ErrorBadData, Err: err}
	//}
	//
	//// Record query window
	//a.queryRangeHist.Observe(to.Sub(from).Seconds())
	//
	//set := q.Select(true, &storage.SelectHints{
	//	Start: timestamp.FromTime(from),
	//	End:   timestamp.FromTime(to),
	//	Func:  "timestamps",
	//}, sel...)
	//j := 0
	//limitReached := false
	//for set.Next() {
	//	series := set.At()
	//	ls := series.Labels()
	//
	//	resSeries := Series{Labels: ls.Map()}
	//	i := series.Iterator()
	//	for i.Next() {
	//		t, _ := i.At()
	//		resSeries.Timestamps = append(resSeries.Timestamps, t)
	//	}
	//
	//	if err := i.Err(); err != nil {
	//		level.Error(a.logger).Log("err", err, "series", ls.String())
	//	}
	//
	//	res = append(res, resSeries)
	//	j++
	//	if applyLimit && j == limit {
	//		limitReached = true
	//		break
	//	}
	//}
	//if err := set.Err(); err != nil {
	//	return nil, nil, &ApiError{Typ: ErrorInternal, Err: set.Err()}
	//}
	//
	//warn := set.Warnings()
	//if limitReached {
	//	warn = append(warn, fmt.Errorf("retrieved %d series, more available", j))
	//}
	//
	return res, nil, nil
}

func (a *API) findProfile(ctx context.Context, t time.Time, sel []*labels.Matcher) (*profile.Profile, error) {
	// Timestamps don't have to match exactly and staleness kicks in within 5
	// minutes of no samples, so we need to search the range of -5min to +5min
	// for possible samples.
	return nil, fmt.Errorf("no implement")
	/*
		q, err := a.db.Querier(ctx, timestamp.FromTime(t.Add(-time.Minute*5)), timestamp.FromTime(t.Add(time.Minute*5)))
		if err != nil {
			return nil, err
		}

		requestedTime := timestamp.FromTime(t)

		set := q.Select(false, nil, sel...)
		for set.Next() {
			series := set.At()
			i := series.Iterator()
			for i.Next() {
				ts, b := i.At()
				if ts >= requestedTime {
					// First profile whose timestamp is larger than or equal to the timestamp being searched for.
					return profile.ParseData(b)
				}
			}
			err = i.Err()
			if err != nil {
				return nil, err
			}
		}

		return nil, set.Err()
	*/
}

func (a *API) SingleProfileQuery(r *http.Request) (*profile.Profile, storage.Warnings, *ApiError) {
	ctx := r.Context()

	return a.profileByParameters(
		ctx,
		"single",
		r.URL.Query().Get("time"),
		r.URL.Query().Get("query"),
		"",
		"",
	)
}

func (a *API) profileByParameters(ctx context.Context, mode, time, query, from, to string) (*profile.Profile, storage.Warnings, *ApiError) {
	switch mode {
	case "merge":
		f, err := parseTime(from)
		if err != nil {
			return nil, nil, &ApiError{Typ: ErrorBadData, Err: err}
		}

		t, err := parseTime(to)
		if err != nil {
			return nil, nil, &ApiError{Typ: ErrorBadData, Err: err}
		}

		if t.Before(f) {
			err := errors.New("to timestamp must not be before from time")
			return nil, nil, &ApiError{Typ: ErrorBadData, Err: err}
		}

		sel, err := parser.ParseMetricSelector(query)
		if err != nil {
			return nil, nil, &ApiError{Typ: ErrorBadData, Err: err}
		}

		return a.mergeProfiles(ctx, f, t, sel)
	case "single":
		t, err := parseTime(time)
		if err != nil {
			err = fmt.Errorf("unable to parse time: %w", err)
			return nil, nil, &ApiError{Typ: ErrorBadData, Err: err}
		}

		sel, err := parser.ParseMetricSelector(query)
		if err != nil {
			err = fmt.Errorf("unable to parse query: %w", err)
			return nil, nil, &ApiError{Typ: ErrorBadData, Err: err}
		}

		profile, err := a.findProfile(ctx, t, sel)
		// TODO(bwplotka): Handle warnings.
		if err != nil {
			err = fmt.Errorf("unable to find profile: %w", err)
			return nil, nil, &ApiError{Typ: ErrorInternal, Err: err}
		}
		if profile == nil {
			return nil, nil, &ApiError{Typ: ErrorNotFound, Err: errors.New("profile not found")}
		}

		return profile, nil, nil
	default:
		return nil, nil, &ApiError{Typ: ErrorBadData, Err: errors.New("no mode specified")}
	}
}

func (a *API) DiffProfiles(r *http.Request) (*profile.Profile, storage.Warnings, *ApiError) {
	ctx := r.Context()

	profileA, warningsA, apiErr := a.profileByParameters(ctx,
		r.URL.Query().Get("mode_a"),
		r.URL.Query().Get("time_a"),
		r.URL.Query().Get("query_a"),
		r.URL.Query().Get("from_a"),
		r.URL.Query().Get("to_a"),
	)
	if apiErr != nil {
		return nil, nil, apiErr
	}

	profileB, warningsB, apiErr := a.profileByParameters(ctx,
		r.URL.Query().Get("mode_b"),
		r.URL.Query().Get("time_b"),
		r.URL.Query().Get("query_b"),
		r.URL.Query().Get("from_b"),
		r.URL.Query().Get("to_b"),
	)
	if apiErr != nil {
		return nil, nil, apiErr
	}

	warnings := append(warningsA, warningsB...)

	// compare totals of profiles, skip this to subtract profiles from each other
	profileA.SetLabel("pprof::base", []string{"true"})

	profileA.Scale(-1)

	profiles := []*profile.Profile{profileA, profileB}

	// Merge profiles.
	if err := measurement.ScaleProfiles(profiles); err != nil {
		return nil, nil, &ApiError{Typ: ErrorInternal, Err: err}
	}

	p, err := profile.Merge(profiles)
	if err != nil {
		return nil, nil, &ApiError{Typ: ErrorInternal, Err: err}
	}

	return p, warnings, nil
}

func (a *API) Query(r *http.Request) (interface{}, []error, *ApiError) {
	var (
		profile  *profile.Profile
		warnings storage.Warnings
		apiErr   *ApiError
	)

	ctx, cancel := context.WithTimeout(r.Context(), a.queryTimeout)
	defer cancel()

	r = r.WithContext(ctx)

	switch r.URL.Query().Get("mode") {
	case "diff":
		profile, warnings, apiErr = a.DiffProfiles(r)
		if apiErr != nil {
			return nil, nil, apiErr
		}
	case "merge":
		profile, warnings, apiErr = a.MergeProfiles(r)
		if apiErr != nil {
			return nil, nil, apiErr
		}
	case "single":
		profile, warnings, apiErr = a.SingleProfileQuery(r)
		if apiErr != nil {
			return nil, nil, apiErr
		}
	default:
		profile, warnings, apiErr = a.SingleProfileQuery(r)
		if apiErr != nil {
			return nil, nil, apiErr
		}
	}

	// Attempt to symbolize all unsymbolized data.
	err := a.symbolizeProfile(r.Context(), profile)
	if err != nil {
		return nil, nil, &ApiError{Typ: ErrorInternal, Err: err}
	}

	return &ProfileResponseRenderer{
		logger:   a.logger,
		profile:  profile,
		warnings: warnings,
		req:      r,
	}, warnings, nil
}

func (a *API) symbolizeProfile(ctx context.Context, p *profile.Profile) error {
	level.Debug(a.logger).Log("msg", "remote symbolizing decision", "decision", a.symbolizer != nil)
	if a.symbolizer != nil {
		level.Debug(a.logger).Log("msg", "attempt remote symbolizing profile")
		return a.symbolizer.Symbolize(ctx, p)
	}

	return nil
}

func parseMetadataTimeRange(r *http.Request, defaultMetadataTimeRange time.Duration) (time.Time, time.Time, error) {
	// If start and end time not specified as query parameter, we get the range from the beginning of time by default.
	var defaultStartTime, defaultEndTime time.Time
	if defaultMetadataTimeRange == 0 {
		defaultStartTime = timestamp.Time(math.MinInt64)
		defaultEndTime = timestamp.Time(math.MaxInt64)
	} else {
		now := time.Now()
		defaultStartTime = now.Add(-defaultMetadataTimeRange)
		defaultEndTime = now
	}

	start, err := parseTimeParam(r, "start", defaultStartTime)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	end, err := parseTimeParam(r, "end", defaultEndTime)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	if end.Before(start) {
		return time.Time{}, time.Time{}, errors.New("end timestamp must not be before start time")
	}
	return start, end, nil
}

func parseTimeParam(r *http.Request, paramName string, defaultValue time.Time) (time.Time, error) {
	val := r.FormValue(paramName)
	if val == "" {
		return defaultValue, nil
	}
	result, err := parseTime(val)
	if err != nil {
		return time.Time{}, errors.Wrapf(err, "Invalid time value for '%s'", paramName)
	}
	return result, nil
}

const millisInSecond = 1000
const nsInSecond = 1000000

// Converts Unix Epoch from milliseconds to time.Time
func fromUnixMilli(ms int64) time.Time {
	return time.Unix(ms/int64(millisInSecond), (ms%int64(millisInSecond))*int64(nsInSecond))
}

func parseTime(s string) (time.Time, error) {
	t, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("cannot parse %q to an int: %w", s, err)
	}

	return fromUnixMilli(t), nil
}

func (a *API) Series(r *http.Request) (interface{}, []error, *ApiError) {
	return nil, nil, &ApiError{Typ: ErrorBadData, Err: errors.New("no implemented")}
	/*
		ctx := r.Context()

		if err := r.ParseForm(); err != nil {
			return nil, nil, &ApiError{Typ: ErrorInternal, Err: errors.Wrap(err, "parse form")}
		}

		if len(r.Form["match[]"]) == 0 {
			return nil, nil, &ApiError{Typ: ErrorBadData, Err: errors.New("no match[] parameter provided")}
		}

		start, end, err := parseMetadataTimeRange(r, defaultMetadataTimeRange)
		if err != nil {
			return nil, nil, &ApiError{Typ: ErrorBadData, Err: err}
		}

		var matcherSets [][]*labels.Matcher
		for _, s := range r.Form["match[]"] {
			matchers, err := parser.ParseMetricSelector(s)
			if err != nil {
				return nil, nil, &ApiError{Typ: ErrorBadData, Err: err}
			}
			matcherSets = append(matcherSets, matchers)
		}

		q, err := a.db.Querier(ctx, timestamp.FromTime(start), timestamp.FromTime(end))
		if err != nil {
			return nil, nil, &ApiError{Typ: ErrorExec, Err: err}
		}

		var (
			metrics = []labels.Labels{}
			sets    []storage.SeriesSet
		)
		for _, mset := range matcherSets {
			sets = append(sets, q.Select(false, &storage.SelectHints{
				Start: timestamp.FromTime(start),
				End:   timestamp.FromTime(end),
				Func:  "series",
			}, mset...))
		}

		set := storage.NewMergeSeriesSet(sets, storage.ChainedSeriesMerge)
		for set.Next() {
			metrics = append(metrics, set.At().Labels())
		}
		if set.Err() != nil {
			return nil, nil, &ApiError{Typ: ErrorInternal, Err: err}
		}

		return metrics, nil, nil
	*/
}

func (a *API) LabelNames(r *http.Request) (interface{}, []error, *ApiError) {
	return nil, nil, &ApiError{Typ: ErrorBadData, Err: errors.New("no implemented")}
	/*
		ctx := r.Context()

		start, end, err := parseMetadataTimeRange(r, defaultMetadataTimeRange)
		if err != nil {
			return nil, nil, &ApiError{Typ: ErrorBadData, Err: err}
		}

		matcherSets := [][]*labels.Matcher{}
		for _, s := range r.Form["match[]"] {
			matchers, err := parser.ParseMetricSelector(s)
			if err != nil {
				return nil, nil, &ApiError{
					Typ: ErrorBadData,
					Err: err,
				}
			}
			matcherSets = append(matcherSets, matchers)
		}

		q, err := a.db.Querier(ctx, timestamp.FromTime(start), timestamp.FromTime(end))
		if err != nil {
			return nil, nil, &ApiError{Typ: ErrorExec, Err: err}
		}

		hints := &storage.SelectHints{
			Start: timestamp.FromTime(start),
			End:   timestamp.FromTime(end),
			Func:  "series", // There is no series function, this token is used for lookups that don't need samples.
		}

		var names []string
		var warnings storage.Warnings
		if len(r.Form["match[]"]) > 0 {
			// Get all series which match matchers.
			var sets []storage.SeriesSet
			for _, mset := range matcherSets {
				s := q.Select(false, hints, mset...)
				sets = append(sets, s)
			}
			names, warnings, err = labelNamesByMatchers(sets)
			if err != nil {
				return nil, nil, &ApiError{Typ: ErrorExec, Err: err}
			}
		} else {
			names, warnings, err = q.LabelNames()
			if err != nil {
				return nil, nil, &ApiError{Typ: ErrorExec, Err: err}
			}
		}

		return names, warnings, nil
	*/
}

func (a *API) LabelValues(r *http.Request) (interface{}, []error, *ApiError) {
	return nil, nil, &ApiError{Typ: ErrorBadData, Err: errors.New("no implemented")}

	/*
		ctx := r.Context()
		name := route.Param(ctx, "name")

		if !model.LabelNameRE.MatchString(name) {
			return nil, nil, &ApiError{Typ: ErrorBadData, Err: errors.Errorf("invalid label name: %q", name)}
		}

		start, end, err := parseMetadataTimeRange(r, defaultMetadataTimeRange)
		if err != nil {
			return nil, nil, &ApiError{Typ: ErrorBadData, Err: err}
		}

		var matcherSets [][]*labels.Matcher
		for _, s := range r.Form["match[]"] {
			matchers, err := parser.ParseMetricSelector(s)
			if err != nil {
				return nil, nil, &ApiError{Typ: ErrorBadData, Err: err}
			}
			matcherSets = append(matcherSets, matchers)
		}

		q, err := a.db.Querier(ctx, timestamp.FromTime(start), timestamp.FromTime(end))
		if err != nil {
			return nil, nil, &ApiError{Typ: ErrorExec, Err: err}
		}

		hints := &storage.SelectHints{
			Start: timestamp.FromTime(start),
			End:   timestamp.FromTime(end),
			Func:  "series", // There is no series function, this token is used for lookups that don't need samples.
		}

		var vals []string
		var warnings storage.Warnings
		if len(r.Form["match[]"]) > 0 {
			// Get all series which match matchers.
			var sets []storage.SeriesSet
			for _, mset := range matcherSets {
				s := q.Select(false, hints, mset...)
				sets = append(sets, s)
			}
			vals, warnings, err = labelValuesByMatchers(sets, name)
			if err != nil {
				return nil, nil, &ApiError{Typ: ErrorExec, Err: err}
			}
		} else {
			vals, warnings, err = q.LabelValues(name)
			if err != nil {
				return nil, nil, &ApiError{Typ: ErrorExec, Err: err}
			}
		}

		return vals, warnings, nil
	*/
}

// LabelValuesByMatchers uses matchers to filter out matching series, then label values are extracted.
func labelValuesByMatchers(sets []storage.SeriesSet, name string) ([]string, storage.Warnings, error) {
	set := storage.NewMergeSeriesSet(sets, storage.ChainedSeriesMerge)
	labelValuesSet := make(map[string]struct{})
	for set.Next() {
		series := set.At()
		labelValue := series.Labels().Get(name)
		labelValuesSet[labelValue] = struct{}{}
	}

	warnings := set.Warnings()
	if set.Err() != nil {
		return nil, warnings, set.Err()
	}
	// Convert the map to an array.
	labelValues := make([]string, 0, len(labelValuesSet))
	for key := range labelValuesSet {
		labelValues = append(labelValues, key)
	}
	sort.Strings(labelValues)
	return labelValues, warnings, nil
}

// LabelNamesByMatchers uses matchers to filter out matching series, then label names are extracted.
func labelNamesByMatchers(sets []storage.SeriesSet) ([]string, storage.Warnings, error) {
	set := storage.NewMergeSeriesSet(sets, storage.ChainedSeriesMerge)
	labelNamesSet := make(map[string]struct{})
	for set.Next() {
		series := set.At()
		labelNames := series.Labels()
		for _, labelName := range labelNames {
			labelNamesSet[labelName.Name] = struct{}{}
		}
	}

	warnings := set.Warnings()
	if set.Err() != nil {
		return nil, warnings, set.Err()
	}
	// Convert the map to an array.
	labelNames := make([]string, 0, len(labelNamesSet))
	for key := range labelNamesSet {
		labelNames = append(labelNames, key)
	}
	sort.Strings(labelNames)
	return labelNames, warnings, nil
}

func (a *API) Reload(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	a.reloadCh <- struct{}{}
}

type conprofConfig struct {
	YAML string `json:"yaml"`
}

func (a *API) Config(_ *http.Request) (interface{}, []error, *ApiError) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return conprofConfig{
		YAML: a.config.String(),
	}, nil, nil
}

// TargetDiscovery has all the active targets.
type TargetDiscovery struct {
	ActiveTargets  []*Target        `json:"activeTargets"`
	DroppedTargets []*DroppedTarget `json:"droppedTargets"`
}

// Target has the information for one target.
type Target struct {
	// Labels before any processing.
	DiscoveredLabels map[string]string `json:"discoveredLabels"`
	// Any labels that are added to this target and its metrics.
	Labels map[string]string `json:"labels"`

	ScrapePool string `json:"scrapePool"`
	ScrapeURL  string `json:"scrapeUrl"`
	GlobalURL  string `json:"globalUrl"`

	LastError          string              `json:"lastError"`
	LastScrape         time.Time           `json:"lastScrape"`
	LastScrapeDuration float64             `json:"lastScrapeDuration"`
	Health             scrape.TargetHealth `json:"health"`
}

// DroppedTarget has the information for one target that was dropped during relabelling.
type DroppedTarget struct {
	// Labels before any processing.
	DiscoveredLabels map[string]string `json:"discoveredLabels"`
}

func (a *API) Targets(r *http.Request) (interface{}, []error, *ApiError) {
	sortKeys := func(targets map[string][]*scrape.Target) ([]string, int) {
		var n int
		keys := make([]string, 0, len(targets))
		for k := range targets {
			keys = append(keys, k)
			n += len(targets[k])
		}
		sort.Strings(keys)
		return keys, n
	}
	flatten := func(targets map[string][]*scrape.Target) []*scrape.Target {
		keys, n := sortKeys(targets)
		res := make([]*scrape.Target, 0, n)
		for _, k := range keys {
			res = append(res, targets[k]...)
		}
		return res
	}

	state := strings.ToLower(r.URL.Query().Get("state"))
	showActive := state == "" || state == "any" || state == "active"
	showDropped := state == "" || state == "any" || state == "dropped"

	res := &TargetDiscovery{
		ActiveTargets:  []*Target{},
		DroppedTargets: []*DroppedTarget{},
	}

	if showActive {
		targets := a.targets(r.Context()).TargetsActive()
		activeKeys, numTargets := sortKeys(targets)
		res.ActiveTargets = make([]*Target, 0, numTargets)

		for _, key := range activeKeys {
			for _, target := range targets[key] {
				lastErrStr := ""
				lastErr := target.LastError()
				if lastErr != nil {
					lastErrStr = lastErr.Error()
				}

				globalURL, err := getGlobalURL(target.URL(), a.globalURLOptions)

				res.ActiveTargets = append(res.ActiveTargets, &Target{
					DiscoveredLabels: target.DiscoveredLabels().Map(),
					Labels:           target.Labels().Map(),
					ScrapePool:       key,
					ScrapeURL:        target.URL().String(),
					GlobalURL:        globalURL.String(),
					LastError: func() string {
						if err == nil && lastErrStr == "" {
							return ""
						} else if err != nil {
							return errors.Wrapf(err, lastErrStr).Error()
						}
						return lastErrStr
					}(),
					LastScrape:         target.LastScrape(),
					LastScrapeDuration: target.LastScrapeDuration().Seconds(),
					Health:             target.Health(),
				})
			}
		}
	}

	if showDropped {
		dropped := flatten(a.targets(r.Context()).TargetsDropped())
		res.DroppedTargets = make([]*DroppedTarget, 0, len(dropped))
		for _, t := range dropped {
			res.DroppedTargets = append(res.DroppedTargets, &DroppedTarget{
				DiscoveredLabels: t.DiscoveredLabels().Map(),
			})
		}
	}

	return res, nil, nil
}

// GlobalURLOptions contains fields used for deriving the global URL for local targets.
type GlobalURLOptions struct {
	ListenAddress string
	Host          string
	Scheme        string
}

func getGlobalURL(u *url.URL, opts GlobalURLOptions) (*url.URL, error) {
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		return u, err
	}

	for _, lhr := range LocalhostRepresentations {
		if host == lhr {
			_, ownPort, err := net.SplitHostPort(opts.ListenAddress)
			if err != nil {
				return u, err
			}

			if port == ownPort {
				// Only in the case where the target is on localhost and its port is
				// the same as the one we're listening on, we know for sure that
				// we're monitoring our own process and that we need to change the
				// scheme, hostname, and port to the externally reachable ones as
				// well. We shouldn't need to touch the path at all, since if a
				// path prefix is defined, the path under which we scrape ourselves
				// should already contain the prefix.
				u.Scheme = opts.Scheme
				u.Host = opts.Host
			} else {
				// Otherwise, we only know that localhost is not reachable
				// externally, so we replace only the hostname by the one in the
				// external URL. It could be the wrong hostname for the service on
				// this port, but it's still the best possible guess.
				host, _, err := net.SplitHostPort(opts.Host)
				if err != nil {
					return u, err
				}
				u.Host = host + ":" + port
			}
			break
		}
	}

	return u, nil
}
